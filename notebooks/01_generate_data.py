# Databricks notebook source
# MAGIC %md
# MAGIC # Atlas RADIAL: Synthetic Data Generation Pipeline
# MAGIC
# MAGIC Generates scaled synthetic trading data from the 15 RADIAL sample files.
# MAGIC
# MAGIC **Data Model Entities:**
# MAGIC - **Dimensions:** Book Universe, Instrument, FX Rates (deterministic GBP conversion)
# MAGIC - **Facts:** Position
# MAGIC - **Risk / Sensitivity:** Risk Greeks (2 levels), Unit Sensitivities (2 tables)
# MAGIC
# MAGIC **FK Integrity:**
# MAGIC Core tables (Book, Instrument, Position, Risk Greeks) share consistent ID ranges
# MAGIC so that `Position.Folder_Id` overlaps with `Book_Universe.Folder_Id`, etc.

# COMMAND ----------

%pip install dbldatagen "numpy<2" --quiet
dbutils.library.restartPython()

# COMMAND ----------

import math
import dbldatagen as dg
from pyspark.sql.types import *
from pyspark.sql import functions as F
from datetime import datetime

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration
# MAGIC
# MAGIC Parameters can be passed via notebook widgets (DABs job) or set manually.

# COMMAND ----------

dbutils.widgets.text("catalog", "", "Unity Catalog")
dbutils.widgets.text("schema", "", "Schema")
dbutils.widgets.text("scale", "1.0", "Scale multiplier (0.01=quick test, 1.0=demo, 10.0=production)")
dbutils.widgets.text("partitions", "8", "Spark partitions (match to cluster cores)")
dbutils.widgets.text("sample_dir_override", "", "Optional: override sample .dat files path")
dbutils.widgets.text("sample_schema", "", "Schema where the sample Volume lives (defaults to output schema)")
dbutils.widgets.text("years", "10", "Years of history to generate (dev=1, prod=10)")

OUTPUT_CATALOG = dbutils.widgets.get("catalog")
OUTPUT_SCHEMA  = dbutils.widgets.get("schema")
SCALE          = float(dbutils.widgets.get("scale"))
PARTITIONS     = int(dbutils.widgets.get("partitions"))
YEARS          = int(dbutils.widgets.get("years"))
_sample_dir_override = dbutils.widgets.get("sample_dir_override")
_sample_schema = dbutils.widgets.get("sample_schema") or OUTPUT_SCHEMA

# Sample .dat files live in a UC Volume. The dev schema is a separate output target
# but the Volume (seed data) always lives in the prod schema — use sample_schema to
# decouple the sample path from the output schema.
SAMPLE_DIR = _sample_dir_override if _sample_dir_override else \
    f"/Volumes/{OUTPUT_CATALOG}/{_sample_schema}/radial_sample/RADIALSAMPLE"

DATE_END   = "2024-12-31"
DATE_BEGIN = f"{2024 - YEARS + 1}-01-01"

print(f"Output:     {OUTPUT_CATALOG}.{OUTPUT_SCHEMA}")
print(f"Scale:      {SCALE}x")
print(f"Years:      {YEARS} ({DATE_BEGIN} → {DATE_END})")
print(f"Partitions: {PARTITIONS}")
print(f"Sample dir: {SAMPLE_DIR}")

# COMMAND ----------

ROW_TARGETS = {
    "book_universe":       int(2_000      * SCALE),  # 2K books
    "instrument":          int(400_000    * SCALE),  # 300-500K, midpoint
    "position":            int(750_000    * SCALE),  # 500K-1M, midpoint
}
ROWS_RISK_DEFAULT = int(20_000_000 * SCALE)          # 20M per sensitivity table

# ID ranges — sized to match row counts so FK joins produce meaningful overlap.
# Range width ≈ row count means each ID appears ~1 time on average, giving
# ~1 match per join row and the right cardinality for derived tables.
INSTRUMENT_ID_MIN = 60_000_000
INSTRUMENT_ID_MAX = INSTRUMENT_ID_MIN + ROW_TARGETS["instrument"]
FOLDER_ID_MIN     = 5_000
FOLDER_ID_MAX     = FOLDER_ID_MIN  + ROW_TARGETS["book_universe"]
POSITION_ID_MIN   = 10_000_000
POSITION_ID_MAX   = POSITION_ID_MIN + ROW_TARGETS["position"]
BOOK_ID_MIN       = FOLDER_ID_MIN
BOOK_ID_MAX       = FOLDER_ID_MAX

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Load All Sample Files

# COMMAND ----------

samples = {}

for entry in dbutils.fs.ls(SAMPLE_DIR):
    if not entry.name.endswith(".dat"):
        continue

    table_name = entry.name.rsplit("_LON_", 1)[0].lower()

    df = (spark.read
          .option("header", "true")
          .option("inferSchema", "true")
          .option("delimiter", "|")
          .csv(entry.path))

    samples[table_name] = df
    print(f"  {table_name:<60} {len(df.columns):>3} cols   {df.count():>4} rows")

print(f"\nLoaded {len(samples)} sample files")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Helper Functions

# COMMAND ----------

def build_spec_from_sample(spark, table_name, sample_df, target_rows, partitions,
                           column_overrides=None):
    """Build a DataGenerator spec from a sample DataFrame.

    Each column gets a generation rule inferred from the sample:
    - String columns rotate through observed non-null values
    - Numeric columns use the observed min/max range
    - Date/Timestamp columns span 2015-2024 for 10-year coverage
    - FK columns can be overridden via column_overrides to use shared ID ranges
    """
    spec = (dg.DataGenerator(spark, name=f"atlas_{table_name}",
                             rows=target_rows, partitions=partitions,
                             seedColumnName="_seed")
            .withSchema(sample_df.schema))

    total = sample_df.count()

    for field in sample_df.schema.fields:
        col = field.name
        dtype = field.dataType

        if column_overrides and col in column_overrides:
            try:
                spec = spec.withColumnSpec(col, **column_overrides[col])
            except Exception as e:
                print(f"    [override] {col}: {e}")
            continue

        try:
            if isinstance(dtype, StringType):
                vals = [r[0] for r in
                        sample_df.select(col).distinct().collect()
                        if r[0] is not None]
                null_count = sample_df.filter(F.col(col).isNull() | (F.col(col) == "")).count()

                if vals:
                    kwargs = {"values": vals, "random": True}
                    if null_count > 0 and total > 0:
                        kwargs["percentNulls"] = round(null_count / total, 2)
                    spec = spec.withColumnSpec(col, **kwargs)
                else:
                    spec = spec.withColumnSpec(col, percentNulls=1.0, values=[""])

            elif isinstance(dtype, (IntegerType, LongType)):
                stats = sample_df.agg(F.min(col), F.max(col)).collect()[0]
                if stats[0] is not None:
                    lo, hi = int(stats[0]), int(stats[1])
                    if lo == hi:
                        hi = lo + 1000
                    spec = spec.withColumnSpec(col, minValue=lo, maxValue=hi, random=True)

            elif isinstance(dtype, (DoubleType, FloatType)):
                # Filter NaN before computing min/max — float NaN passes `is not None`
                # but causes NRange to blow up inside dbldatagen
                stats = sample_df.agg(
                    F.min(F.when(~F.isnan(F.col(col)), F.col(col))),
                    F.max(F.when(~F.isnan(F.col(col)), F.col(col))),
                ).collect()[0]
                if stats[0] is not None:
                    lo, hi = float(stats[0]), float(stats[1])
                    if lo == hi or math.isnan(hi) or math.isnan(lo):
                        lo, hi = 0.0, 1.0
                    spec = spec.withColumnSpec(col, minValue=lo, maxValue=hi, random=True)

            elif isinstance(dtype, DateType):
                spec = spec.withColumnSpec(col, begin=DATE_BEGIN,
                                           end=DATE_END, random=True)

            elif isinstance(dtype, TimestampType):
                spec = spec.withColumnSpec(col,
                                           begin=f"{DATE_BEGIN} 00:00:00",
                                           end=f"{DATE_END} 23:59:59",
                                           interval="1 second", random=True)

            elif isinstance(dtype, BooleanType):
                spec = spec.withColumnSpec(col, values=[True, False], random=True)

        except Exception as e:
            print(f"    [auto] {col}: {e}")

    return spec

# COMMAND ----------

CORE_TABLES = {
    "book_universe", "instrument", "position",
}

# Sensitivity tables whose Parent_Instrument_Id / Position_Id FKs must be pinned
# to the same ID ranges as their parent tables.
SENSITIVITY_TABLES = {
    "radial_unit_sensitivities_asset_greeks",
    "radial_unit_sensitivities_asset_greeks_fully_decomposed",
}

FK_COLUMN_RANGES = {
    "Instrument_Id":        (INSTRUMENT_ID_MIN, INSTRUMENT_ID_MAX),
    "Parent_Instrument_Id": (INSTRUMENT_ID_MIN, INSTRUMENT_ID_MAX),
    "Folder_Id":            (FOLDER_ID_MIN, FOLDER_ID_MAX),
    "Book_Id":              (BOOK_ID_MIN, BOOK_ID_MAX),
    "Position_Id":          (POSITION_ID_MIN, POSITION_ID_MAX),
}


# Realistic global investment bank locations, desks, and trading areas.
# The sample .dat only has LON — these overrides create a multi-location
# book hierarchy suitable for global risk reporting demos.
GLOBAL_LOCATIONS = ["LON", "NYC", "TKY", "HKG", "SGP", "FRA", "SYD"]
TRADING_DESKS = [
    "EQ-FLOW", "EQ-EXOTICS", "FI-RATES", "FI-CREDIT", "FX-SPOT",
    "FX-OPTIONS", "COMMODITIES", "STRUCTURED", "EM-TRADING", "MACRO",
]
TRADING_AREAS = [
    "Equities", "Fixed Income", "FX & Rates", "Commodities",
    "Structured Products", "Emerging Markets",
]

# 11-level book path hierarchy for SDS_Book_Path generation.
# Each list must have ≥3 values so every level has sufficient enumeration for
# drill-down analysis. Level 11 (leaf) is derived from Folder_Id for uniqueness.
BOOK_PATH_LEVELS = [
    # L1  — Group entity
    ["Barclays Group", "BCSL Holdings", "Barclays International"],
    # L2  — Division
    ["Markets", "Banking", "Corporate"],
    # L3  — Asset Class
    ["Equities", "Fixed Income", "FX & Rates", "Commodities", "Multi-Asset"],
    # L4  — Business Area
    ["Flow Trading", "Prime Services", "Derivatives", "Origination", "Macro Rates"],
    # L5  — Sub-Business
    ["Delta One", "Cash Products", "Options & Exotics", "Credit Trading", "Rates Swaps"],
    # L6  — Region
    ["EMEA", "Americas", "Asia Pacific"],
    # L7  — Sub-Region
    ["UK", "Continental Europe", "North America", "Japan", "SE Asia"],
    # L8  — Legal Entity
    ["BBPLC", "BSAG", "BNAC", "BBIL"],
    # L9  — Book Group
    ["Active Books", "Legacy Portfolio", "Run-Off"],
    # L10 — Book Sub-Group
    ["In-Scope", "Out-of-Scope", "Regulatory Capital"],
    # L11 leaf: f"BK-{Folder_Id}" — unique per book (synthesised in _add_book_path)
]

# ~95 realistic stock tickers spanning major sectors and geographies.
# Assigned to position_risk_greeks_currency via hash of Instrument_Id so every
# currency-decomposed risk row carries a primary asset reference for drill-down.
PRIMARY_ASSET_REFS = [
    # US Tech
    "AAPL.O", "MSFT.O", "GOOGL.O", "AMZN.O", "META.O", "NVDA.O", "TSLA.O",
    "CRM.O", "ADBE.O", "ORCL.N", "INTC.O", "CSCO.O", "IBM.N", "PYPL.O",
    "NFLX.O", "UBER.N", "SNOW.N", "PLTR.N",
    # US Semiconductors
    "AMD.O", "AVGO.O", "QCOM.O", "TXN.O", "MU.O", "LRCX.O", "KLAC.O",
    "MRVL.O", "NVMI.O", "AMAT.O",
    # US Banks & Financials
    "JPM.N", "BAC.N", "GS.N", "MS.N", "C.N", "WFC.N", "BLK.N", "SCHW.N",
    "AXP.N", "V.N", "MA.N",
    # US Healthcare & Pharma
    "JNJ.N", "UNH.N", "PFE.N", "ABBV.N", "LLY.N", "MRK.N", "TMO.N",
    "AMGN.O", "GILD.O", "ISRG.O",
    # US Energy
    "XOM.N", "CVX.N", "COP.N", "SLB.N", "EOG.N", "OXY.N",
    # US Consumer & Industrial
    "WMT.N", "PG.N", "KO.N", "PEP.O", "MCD.N", "NKE.N", "DIS.N",
    "HD.N", "CAT.N", "BA.N", "UPS.N", "HON.O",
    # UK
    "BARC.L", "HSBA.L", "SHEL.L", "BP.L", "AZN.L", "GSK.L", "ULVR.L",
    "RIO.L", "LLOY.L", "VOD.L",
    # EU
    "SAP.DE", "SIE.DE", "ALV.DE", "BAS.DE", "ASML.AS", "INGA.AS",
    "SAN.MC", "BNP.PA", "TTE.PA", "OR.PA",
    # Asia
    "7203.T", "9984.T", "6758.T", "005930.KS", "000660.KS",
    "0700.HK", "9988.HK", "3690.HK",
    # Emerging Markets
    "VALE3.SA", "ITUB4.SA", "RELIANCE.NS", "TCS.NS", "INFY.NS",
]


def _add_primary_asset_ref(df):
    """Add Primary_Asset_Ref column using a hash of Instrument_Id.

    Same deterministic-hash pattern as _add_book_path: each instrument maps to
    one of ~95 realistic stock tickers so analysts can aggregate currency-level
    risk exposure by primary asset reference.
    """
    n = len(PRIMARY_ASSET_REFS)
    arr = F.array([F.lit(v) for v in PRIMARY_ASSET_REFS])
    idx = (
        F.abs(F.hash(F.concat(F.col("Instrument_Id").cast("string"), F.lit("|par"))))
        % n
    ).cast(IntegerType())
    return df.withColumn("Primary_Asset_Ref", arr.getItem(idx))


def get_fk_overrides(table_name, schema):
    if table_name not in CORE_TABLES and table_name not in SENSITIVITY_TABLES:
        return {}

    overrides = {}
    fields = {f.name: f.dataType for f in schema.fields}

    for col_name, (lo, hi) in FK_COLUMN_RANGES.items():
        if col_name in fields and isinstance(fields[col_name], (IntegerType, LongType)):
            overrides[col_name] = {"minValue": lo, "maxValue": hi, "random": True}

    if table_name == "position" and "Position_Id" in fields:
        if isinstance(fields["Position_Id"], (IntegerType, LongType)):
            overrides["Position_Id"] = {
                "minValue": POSITION_ID_MIN,
                "maxValue": POSITION_ID_MAX,
                "random": True,
            }

    # Override book hierarchy columns with realistic global locations
    if table_name == "book_universe":
        if "Book_Location_Code" in fields:
            overrides["Book_Location_Code"] = {"values": GLOBAL_LOCATIONS, "random": True}
        if "Desk_Code" in fields:
            overrides["Desk_Code"] = {"values": TRADING_DESKS, "random": True}
        if "Trading_Area_Name" in fields:
            overrides["Trading_Area_Name"] = {"values": TRADING_AREAS, "random": True}

    # Override position Location_Code to match book locations
    if table_name == "position" and "Location_Code" in fields:
        overrides["Location_Code"] = {"values": GLOBAL_LOCATIONS, "random": True}

    # Override Currency_Id in sensitivity tables to ensure all 4 trading
    # currencies appear, matching position.Currency_Code for FX conversion.
    if table_name in SENSITIVITY_TABLES and "Currency_Id" in fields:
        if isinstance(fields["Currency_Id"], StringType):
            overrides["Currency_Id"] = {"values": ["CHF", "EUR", "GBP", "USD"], "random": True}

    return overrides


def _add_book_path(df):
    """Replace SDS_Book_Path with a structured 11-level colon-separated hierarchy.

    Levels 1-10 are drawn from BOOK_PATH_LEVELS using a per-level hash of Folder_Id
    so that:
    - Assignment is deterministic and reproducible across runs
    - Values distribute evenly, ensuring each level has ≥3 distinct enumerations
    Level 11 is a unique leaf derived from Folder_Id (e.g. 'BK-550734').
    """
    result = df
    path_parts = []

    for i, values in enumerate(BOOK_PATH_LEVELS):
        _col = f"_bp_{i}"
        n    = len(values)
        arr  = F.array([F.lit(v) for v in values])
        # Hash of (string(Folder_Id), "|", level_index) → stable, even distribution
        idx  = (
            F.abs(F.hash(F.concat(F.col("Folder_Id").cast("string"), F.lit(f"|{i}"))))
            % n
        ).cast(IntegerType())
        result = result.withColumn(_col, arr.getItem(idx))
        path_parts.append(F.col(_col))

    # Level 11: unique leaf from Folder_Id
    path_parts.append(F.concat(F.lit("BK-"), F.col("Folder_Id").cast("string")))

    result = result.withColumn("SDS_Book_Path", F.concat_ws(":", *path_parts))
    for i in range(len(BOOK_PATH_LEVELS)):
        result = result.drop(f"_bp_{i}")

    return result

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Generate All Tables

# COMMAND ----------

# position_risk_greeks_* are excluded — derived via join in Step 3b
# fx_rates is excluded — generated deterministically in Step 3a
GENERATION_ORDER = [
    "book_universe", "instrument",
    "position",
    # Unit sensitivities — must be generated before position risk greeks
    "radial_unit_sensitivities_asset_greeks",
    "radial_unit_sensitivities_asset_greeks_fully_decomposed",
]

RISK_GREEKS_TABLES = {
    "position_risk_greeks_assetlevel",
    "position_risk_greeks_currency",
}

ordered_names = [n for n in GENERATION_ORDER if n in samples]
extras = [n for n in samples if n not in ordered_names and n not in RISK_GREEKS_TABLES]
ordered_names.extend(extras)

results = {}

for table_name in ordered_names:
    sample_df = samples[table_name]
    target_rows = ROW_TARGETS.get(table_name, ROWS_RISK_DEFAULT)

    # Some sensitivity sample files store FK columns as STRING — cast to LongType
    # so that dbldatagen can apply numeric range overrides for FK consistency.
    if table_name in SENSITIVITY_TABLES:
        for _fk_col in ("Parent_Instrument_Id", "Position_Id"):
            if _fk_col in sample_df.columns and isinstance(
                sample_df.schema[_fk_col].dataType, StringType
            ):
                sample_df = sample_df.withColumn(_fk_col, F.col(_fk_col).cast(LongType()))
                print(f"  Pre-cast {_fk_col} STRING→LongType in {table_name}")

    overrides = get_fk_overrides(table_name, sample_df.schema)
    override_info = f"  FK overrides: {list(overrides.keys())}" if overrides else ""

    print(f"\n{'='*70}")
    print(f"  {table_name}")
    print(f"  Sample: {sample_df.count()} rows  →  Target: {target_rows:,}")
    if override_info:
        print(override_info)

    start = datetime.now()
    spec = build_spec_from_sample(spark, table_name, sample_df,
                                  target_rows, PARTITIONS, overrides)
    gen_df = spec.build()

    # Post-process: replace SDS_Book_Path with a structured 11-level hierarchy so
    # the silver flat table has meaningful drill-down granularity at every level.
    if table_name == "book_universe":
        gen_df = _add_book_path(gen_df)

    # Expand position to monthly snapshots by cross-joining with a date spine.
    # Each position appears in every month with a varying Position_Qty derived
    # from hash(Position_Id, month), so book-level risk exposure changes over time
    # when joined with unit sensitivities.
    if table_name == "position":
        month_spine = spark.sql(f"""
            SELECT explode(sequence(
                DATE_TRUNC('MONTH', DATE '{DATE_BEGIN}'),
                DATE_TRUNC('MONTH', DATE '{DATE_END}'),
                INTERVAL 1 MONTH
            )) AS Business_Date
        """)
        n_months = month_spine.count()
        base_rows = gen_df.count()
        gen_df = gen_df.crossJoin(month_spine)

        # Vary Position_Qty per month: multiply by a hash-derived factor in [0.2, 1.8]
        # so each position's size drifts over time, driving genuine variation in
        # book-level risk exposure through the sensitivity join.
        gen_df = gen_df.withColumn(
            "Position_Qty",
            F.col("Position_Qty") * (
                0.2 + 1.6 * (
                    F.abs(F.hash(F.concat(
                        F.col("Position_Id").cast("string"),
                        F.lit("_"),
                        F.col("Business_Date").cast("string")
                    ))) % 1000
                ) / 1000.0
            )
        )

        print(f"  Monthly expansion: {base_rows:,} positions × {n_months} months "
              f"= {base_rows * n_months:,} rows (Position_Qty varies per month)")

    elapsed = (datetime.now() - start).total_seconds()

    results[table_name] = {
        "df": gen_df,
        "rows": gen_df.count(),
        "cols": len(gen_df.columns),
        "elapsed": elapsed,
        "sample_rows": sample_df.count(),
    }
    print(f"  Generated {results[table_name]['rows']:,} rows in {elapsed:.1f}s")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3a: Generate Deterministic FX Rates for GBP Conversion
# MAGIC
# MAGIC Monthly FX rates for the 4 trading currencies (CHF, EUR, GBP, USD) with
# MAGIC `Reporting_Currency = 'GBP'`. Rates drift ±5% around realistic base values so
# MAGIC downstream silver tables can LEFT JOIN to convert `Sensitivity_Value` to GBP.
# MAGIC
# MAGIC FK relationship: `Risk_Greeks.Currency_Id` → `FX_Rates.Base_Currency`

# COMMAND ----------

FX_BASE_RATES = [("CHF", 0.87), ("EUR", 0.86), ("USD", 0.79), ("GBP", 1.00)]

_month_spine_fx = spark.sql(f"""
    SELECT explode(sequence(
        DATE_TRUNC('MONTH', DATE '{DATE_BEGIN}'),
        DATE_TRUNC('MONTH', DATE '{DATE_END}'),
        INTERVAL 1 MONTH
    )) AS Business_Date
""")

_fx_base = spark.createDataFrame(FX_BASE_RATES, ["Base_Currency", "_base_rate"])
_fx_df = _fx_base.crossJoin(_month_spine_fx)

# Hash-based ±5% monthly drift for non-GBP; GBP/GBP is always 1.0
_fx_df = (
    _fx_df
    .withColumn("Reporting_Currency", F.lit("GBP"))
    .withColumn(
        "Fx_Rate",
        F.when(F.col("Base_Currency") == "GBP", 1.0)
         .otherwise(
            F.col("_base_rate") * (
                0.95 + 0.10 * (
                    F.abs(F.hash(F.concat(
                        F.col("Base_Currency"),
                        F.lit("_"),
                        F.col("Business_Date").cast("string"),
                    ))) % 1000
                ) / 1000.0
            )
        )
    )
    .drop("_base_rate")
)

# Pad remaining sample columns with nulls to match the RADIAL schema
if "fx_rates" in samples:
    for field in samples["fx_rates"].schema.fields:
        if field.name not in _fx_df.columns:
            _fx_df = _fx_df.withColumn(field.name, F.lit(None).cast(field.dataType))

_fx_count = _fx_df.count()
results["fx_rates"] = {
    "df": _fx_df,
    "rows": _fx_count,
    "cols": len(_fx_df.columns),
    "elapsed": 0,
    "sample_rows": samples["fx_rates"].count() if "fx_rates" in samples else 0,
}
print(f"  fx_rates: {_fx_count} deterministic monthly GBP rates "
      f"({len(FX_BASE_RATES)} currencies × {_month_spine_fx.count()} months)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3b: Derive Position Risk Greeks via Join
# MAGIC
# MAGIC Position risk greeks are not generated independently — they are the join product of
# MAGIC the position table and the corresponding unit sensitivity tables. Each position gets
# MAGIC one risk row per sensitivity type, matching real RADIAL risk table cardinality.

# COMMAND ----------

RISK_GREEKS_JOIN_MAP = {
    "position_risk_greeks_assetlevel":     "radial_unit_sensitivities_asset_greeks",
    "position_risk_greeks_currency":        "radial_unit_sensitivities_asset_greeks_fully_decomposed",
}


def build_risk_greeks_via_join(spark, risk_table, position_df, unit_sens_df,
                                sample_df, target_rows, date_begin, years):
    """Derive a position_risk_greeks table by equi-joining positions with unit_sensitivities
    on position.Instrument_Id = unit_sens.Parent_Instrument_Id (and optionally
    Instrument_Id_Type = Parent_Instrument_Id_Type when both columns exist).

    Sensitivity_Value flows from unit_sensitivities, preserving the real RADIAL FK
    relationship. unit_sensitivities is deduplicated to one row per
    (instrument, sensitivity_type) before the join to prevent fan-out.
    """
    required = ("Parent_Instrument_Id", "Sensitivity_Type", "Sensitivity_Value")
    missing = [c for c in required if c not in unit_sens_df.columns]
    if missing:
        print(f"  Warning: {risk_table} — unit_sens missing {missing}, skipping")
        return None

    total_days = years * 365

    # Include type column in the join key when both tables have the matching columns.
    use_type_join = (
        "Parent_Instrument_Id_Type" in unit_sens_df.columns
        and "Instrument_Id_Type" in position_df.columns
    )

    # Deduplicate unit_sens: one row per (instrument [, type], sensitivity_type)
    # This gives exactly one Sensitivity_Value per instrument+type combination and
    # avoids a fan-out explosion on the join.
    dedup_keys = ["Parent_Instrument_Id", "Sensitivity_Type"]
    unit_cols  = ["Parent_Instrument_Id", "Sensitivity_Type", "Sensitivity_Value"]
    if use_type_join:
        dedup_keys = ["Parent_Instrument_Id", "Parent_Instrument_Id_Type", "Sensitivity_Type"]
        unit_cols  = ["Parent_Instrument_Id", "Parent_Instrument_Id_Type",
                      "Sensitivity_Type", "Sensitivity_Value"]
    # Carry Currency_Id through from unit_sens (needed for currency-decomposed tables)
    if "Currency_Id" in unit_sens_df.columns:
        dedup_keys = dedup_keys + ["Currency_Id"]
        unit_cols  = unit_cols + ["Currency_Id"]

    unit_sens_slim = unit_sens_df.dropDuplicates(dedup_keys).select(unit_cols)

    # Build explicit join condition so both sides' columns are retained.
    # position has no column named Parent_Instrument_Id, so there is no ambiguity.
    if use_type_join:
        join_cond = (
            (position_df["Instrument_Id"] == unit_sens_slim["Parent_Instrument_Id"]) &
            (position_df["Instrument_Id_Type"] == unit_sens_slim["Parent_Instrument_Id_Type"])
        )
    else:
        join_cond = position_df["Instrument_Id"] == unit_sens_slim["Parent_Instrument_Id"]

    joined = position_df.join(unit_sens_slim, on=join_cond, how="inner")

    # Vary Sensitivity_Value per (instrument, month) to reflect changing market
    # conditions, then scale by Position_Qty (which also varies per month).
    # Result: position-level risk = unit_sensitivity × market_factor × position_size
    if "Business_Date" in joined.columns:
        joined = joined.withColumn(
            "Sensitivity_Value",
            F.col("Sensitivity_Value") * (
                0.5 + 1.0 * (
                    F.abs(F.hash(F.concat(
                        F.col("Instrument_Id").cast("string"),
                        F.lit("_sv_"),
                        F.col("Business_Date").cast("string")
                    ))) % 1000
                ) / 1000.0
            )
        )
    if "Position_Qty" in joined.columns:
        joined = joined.withColumn(
            "Sensitivity_Value",
            F.col("Sensitivity_Value") * F.col("Position_Qty")
        )

    # Drop the FK columns from unit_sens side (equal to position's Instrument_Id/Type)
    # then re-expose them under the Parent_* names expected by the target schema.
    joined = joined.drop("Parent_Instrument_Id")
    joined = joined.withColumn("Parent_Instrument_Id", F.col("Instrument_Id"))
    if use_type_join:
        joined = joined.drop("Parent_Instrument_Id_Type")
        joined = joined.withColumn("Parent_Instrument_Id_Type", F.col("Instrument_Id_Type"))

    # Map to target schema: select matching columns, fill any missing with null.
    # Business_Date is inherited from position (monthly spine) — not in the RADIAL
    # sample schema but preserved here so gold views can group by month correctly.
    target_fields    = sample_df.schema.fields
    target_col_names = [f.name for f in target_fields]
    if "Business_Date" in joined.columns and "Business_Date" not in target_col_names:
        target_col_names = target_col_names + ["Business_Date"]

    for field in target_fields:
        if field.name not in joined.columns:
            joined = joined.withColumn(field.name, F.lit(None).cast(field.dataType))

    # Add Primary_Asset_Ref to the currency-decomposed table so analysts can
    # aggregate risk exposure by stock ticker across books and time.
    if risk_table == "position_risk_greeks_currency":
        joined = _add_primary_asset_ref(joined)
        if "Primary_Asset_Ref" not in target_col_names:
            target_col_names = target_col_names + ["Primary_Asset_Ref"]

    result = joined.select(target_col_names)

    actual = result.count()
    print(f"  {risk_table}: equi-join → {actual:,} rows (target {target_rows:,})")
    return result


for risk_table, unit_sens_table in RISK_GREEKS_JOIN_MAP.items():
    if risk_table not in samples:
        print(f"  Skipping {risk_table} — no sample file found")
        continue
    if unit_sens_table not in results:
        print(f"  Skipping {risk_table} — {unit_sens_table} was not generated")
        continue

    start = datetime.now()
    df = build_risk_greeks_via_join(
        spark, risk_table,
        position_df=results["position"]["df"],
        unit_sens_df=results[unit_sens_table]["df"],
        sample_df=samples[risk_table],
        target_rows=ROWS_RISK_DEFAULT,
        date_begin=DATE_BEGIN,
        years=YEARS,
    )
    if df is None:
        continue

    elapsed = (datetime.now() - start).total_seconds()
    results[risk_table] = {
        "df": df,
        "rows": df.count(),
        "cols": len(df.columns),
        "elapsed": elapsed,
        "sample_rows": samples[risk_table].count(),
    }
    print(f"  Generated {results[risk_table]['rows']:,} rows in {elapsed:.1f}s")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Write Delta Tables

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {OUTPUT_CATALOG}.{OUTPUT_SCHEMA}")

for table_name, info in results.items():
    full_table = f"{OUTPUT_CATALOG}.{OUTPUT_SCHEMA}.{table_name}"
    print(f"  Writing {full_table} ({info['rows']:,} rows) ...", end="")
    start = datetime.now()
    info["df"].write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(full_table)
    print(f" {(datetime.now()-start).total_seconds():.1f}s")

print(f"\n{len(results)} tables written to {OUTPUT_CATALOG}.{OUTPUT_SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

total_rows = sum(r["rows"] for r in results.values())
print(f"Scale: {SCALE}x  |  Output: {OUTPUT_CATALOG}.{OUTPUT_SCHEMA}  |  Tables: {len(results)}  |  Total rows: {total_rows:,}")
