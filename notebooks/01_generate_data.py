# Databricks notebook source
# MAGIC %md
# MAGIC # Atlas RADIAL: Synthetic Data Generation Pipeline
# MAGIC
# MAGIC Generates scaled synthetic trading data from the 15 RADIAL sample files.
# MAGIC
# MAGIC **Data Model Entities:**
# MAGIC - **Dimensions:** Book Universe, Instrument, RADIAL Instrument, FX Rates
# MAGIC - **Facts:** Position, Package Composition
# MAGIC - **Risk / Sensitivity:** Risk Greeks (3 levels), Arb Rules, Monikers, PnL Sensitivity, Unit Sensitivities (3 tables)
# MAGIC
# MAGIC **FK Integrity:**
# MAGIC Core tables (Book, Instrument, Position, Risk Greeks) share consistent ID ranges
# MAGIC so that `Position.Folder_Id` overlaps with `Book_Universe.Folder_Id`, etc.

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
    "radial_instrument":   int(400_000    * SCALE),  # matched to instrument
    "fx_rates":            int(50_000     * SCALE),
    "position":            int(750_000    * SCALE),  # 500K-1M, midpoint
    "package_composition": int(400_000    * SCALE),  # 300-500K, midpoint
    "radial_arbrules":     int(5_000      * SCALE),  # 5K
    "radial_monikers":     int(1_000_000  * SCALE),  # 1M
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
    "book_universe", "instrument", "position", "package_composition",
}

# Sensitivity tables whose Parent_Instrument_Id / Position_Id FKs must be pinned
# to the same ID ranges as their parent tables.
SENSITIVITY_TABLES = {
    "radial_unit_sensitivities_asset_greeks",
    "radial_unit_sensitivities_asset_greeks_fully_decomposed",
    "radial_unit_sensitivities_instruments_greeks",
    "radial_pnl_sensitivity_commodities",
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
GENERATION_ORDER = [
    "book_universe", "instrument", "radial_instrument", "fx_rates",
    "position", "package_composition",
    "radial_arbrules", "radial_monikers",
    "radial_pnl_sensitivity_commodities",
    # Unit sensitivities — must be generated before position risk greeks
    "radial_unit_sensitivities_asset_greeks",
    "radial_unit_sensitivities_asset_greeks_fully_decomposed",
    "radial_unit_sensitivities_instruments_greeks",
]

RISK_GREEKS_TABLES = {
    "position_risk_greeks_assetlevel",
    "position_risk_greeks_currency",
    "position_risk_greeks_instrumentlevel",
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

    # Expand position to a realistic monthly time-series with position turnover.
    # Each position is assigned a start month and duration (1-24 months) derived
    # from its Position_Id hash, so the portfolio composition changes each month.
    # Positions that share an instrument carry that instrument's sensitivity through
    # to risk_greeks, giving genuine time-variation in book-level risk exposure.
    if table_name == "position":
        month_spine = spark.sql(f"""
            SELECT
                explode(sequence(
                    DATE_TRUNC('MONTH', DATE '{DATE_BEGIN}'),
                    DATE_TRUNC('MONTH', DATE '{DATE_END}'),
                    INTERVAL 1 MONTH
                )) AS Business_Date
        """)
        n_months = month_spine.count()

        # Index the spine so we can filter by position lifecycle
        month_spine_idx = month_spine.withColumn(
            "month_idx",
            F.months_between(F.col("Business_Date"), F.lit(DATE_BEGIN)).cast(IntegerType())
        )

        # Assign each position a deterministic start month and duration
        base_rows = gen_df.count()
        gen_df = gen_df \
            .withColumn(
                "start_month_idx",
                (F.abs(F.hash(F.col("Position_Id").cast("string"))) % n_months)
                .cast(IntegerType())
            ) \
            .withColumn(
                "duration_months",
                # Range 1-24 months: most positions short-lived, some long-running
                ((F.abs(F.hash(F.concat(
                    F.col("Position_Id").cast("string"), F.lit("_dur")
                ))) % 24) + 1).cast(IntegerType())
            )

        # Explode each position into its active months without a full cross-join
        gen_df = gen_df \
            .withColumn(
                "active_month_indices",
                F.expr(
                    f"sequence(start_month_idx, "
                    f"least(start_month_idx + duration_months - 1, {n_months - 1}))"
                )
            ) \
            .select(
                *[c for c in gen_df.columns
                  if c not in ("start_month_idx", "duration_months")],
                F.explode("active_month_indices").alias("month_idx")
            ) \
            .join(month_spine_idx, "month_idx") \
            .drop("month_idx")

        print(f"  Position turnover: {base_rows:,} base positions × avg ~12 months "
              f"= {gen_df.count():,} rows across {n_months} monthly snapshots")

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
# MAGIC ## Step 3b: Derive Position Risk Greeks via Join
# MAGIC
# MAGIC Position risk greeks are not generated independently — they are the join product of
# MAGIC the position table and the corresponding unit sensitivity tables. Each position gets
# MAGIC one risk row per sensitivity type, matching real RADIAL risk table cardinality.

# COMMAND ----------

RISK_GREEKS_JOIN_MAP = {
    "position_risk_greeks_assetlevel":     "radial_unit_sensitivities_asset_greeks",
    "position_risk_greeks_currency":        "radial_unit_sensitivities_asset_greeks_fully_decomposed",
    "position_risk_greeks_instrumentlevel": "radial_unit_sensitivities_instruments_greeks",
}


def build_risk_greeks_via_join(spark, risk_table, position_df, unit_sens_df,
                                sample_df, target_rows, date_begin, years,
                                sensitivity_multiplier_df=None):
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

    # Drop the FK columns from unit_sens side (equal to position's Instrument_Id/Type)
    # then re-expose them under the Parent_* names expected by the target schema.
    joined = joined.drop("Parent_Instrument_Id")
    joined = joined.withColumn("Parent_Instrument_Id", F.col("Instrument_Id"))
    if use_type_join:
        joined = joined.drop("Parent_Instrument_Id_Type")
        joined = joined.withColumn("Parent_Instrument_Id_Type", F.col("Instrument_Id_Type"))

    # Apply time-varying sensitivity multiplier so risk exposure evolves realistically
    # over time. Each instrument is bucketed and assigned a random-walk multiplier
    # per month derived from a pre-computed lookup table.
    if sensitivity_multiplier_df is not None:
        n_buckets = sensitivity_multiplier_df.select("instrument_bucket").distinct().count()
        joined = joined \
            .withColumn(
                "instrument_bucket",
                (F.abs(F.hash(F.col("Instrument_Id").cast("string"))) % n_buckets)
                .cast(IntegerType())
            ) \
            .withColumn(
                "month_idx",
                F.months_between(F.col("Business_Date"), F.lit(date_begin)).cast(IntegerType())
            )
        joined = joined.join(sensitivity_multiplier_df, on=["instrument_bucket", "month_idx"], how="left")
        joined = joined \
            .withColumn(
                "Sensitivity_Value",
                F.col("Sensitivity_Value") * F.coalesce(F.col("sensitivity_multiplier"), F.lit(1.0))
            ) \
            .drop("instrument_bucket", "month_idx", "sensitivity_multiplier")

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

    result = joined.select(target_col_names)

    actual = result.count()
    print(f"  {risk_table}: equi-join → {actual:,} rows (target {target_rows:,})")
    return result


# Build a sensitivity multiplier lookup table: (instrument_bucket, month_idx) → multiplier.
# Each bucket follows an independent random walk so book-level risk exposure shows
# genuine trends and vol over the 10-year history rather than a flat line.
_n_months  = YEARS * 12
_n_buckets = 200
import numpy as _np
_np.random.seed(42)
_monthly_returns   = _np.random.normal(0, 0.06, (_n_buckets, _n_months))
_cum_multipliers   = _np.cumprod(1 + _monthly_returns, axis=1)
sensitivity_multiplier_df = spark.createDataFrame(
    [
        (int(b), int(m), float(_cum_multipliers[b, m]))
        for b in range(_n_buckets)
        for m in range(_n_months)
    ],
    schema="instrument_bucket INT, month_idx INT, sensitivity_multiplier DOUBLE"
).cache()

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
        sensitivity_multiplier_df=sensitivity_multiplier_df,
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
