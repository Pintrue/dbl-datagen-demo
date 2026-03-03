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

# Shared ID ranges — used across tables for FK consistency
INSTRUMENT_ID_MIN, INSTRUMENT_ID_MAX = 60_000_000, 310_000_000
FOLDER_ID_MIN,     FOLDER_ID_MAX     = 5_000, 3_500_000
POSITION_ID_MIN,   POSITION_ID_MAX   = 10_000_000, 40_000_000
BOOK_ID_MIN,       BOOK_ID_MAX       = 5_000, 3_500_000

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

FK_COLUMN_RANGES = {
    "Instrument_Id":        (INSTRUMENT_ID_MIN, INSTRUMENT_ID_MAX),
    "Parent_Instrument_Id": (INSTRUMENT_ID_MIN, INSTRUMENT_ID_MAX),
    "Folder_Id":            (FOLDER_ID_MIN, FOLDER_ID_MAX),
    "Book_Id":              (BOOK_ID_MIN, BOOK_ID_MAX),
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


def get_fk_overrides(table_name, schema):
    if table_name not in CORE_TABLES:
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
                                sample_df, target_rows, date_begin, years):
    """Derive a position_risk_greeks table by cross-joining positions with the
    distinct sensitivity types from the corresponding unit_sensitivities table.
    Each position gets one row per sensitivity type, so row count ≈
    position_count × sensitivity_type_count. Positions are sampled to hit target_rows.
    """
    if "Sensitivity_Type" not in unit_sens_df.columns:
        print(f"  Warning: {risk_table} — unit sensitivities table has no Sensitivity_Type column, skipping")
        return None

    sens_types_df = unit_sens_df.select("Sensitivity_Type").distinct()
    n_sens_types  = sens_types_df.count()

    n_positions  = position_df.count()
    pos_target   = max(1, target_rows // max(1, n_sens_types))
    pos_fraction = min(1.0, pos_target / n_positions)

    print(f"  {risk_table}: {n_positions:,} positions × {n_sens_types} sensitivity types "
          f"→ target {target_rows:,} rows (sampling {pos_fraction:.1%} of positions)")

    base = (position_df.sample(fraction=pos_fraction, seed=42)
                       .crossJoin(sens_types_df))

    # Synthetic Sensitivity_Value — symmetric range reflecting real long/short exposures
    base = base.withColumn(
        "Sensitivity_Value",
        (F.rand(seed=42) * 2 - 1) * F.lit(10_000_000.0)
    )

    # Business_Date — daily risk snapshot
    total_days = years * 365
    base = base.withColumn(
        "Business_Date",
        F.date_add(F.lit(date_begin), (F.rand(seed=42) * total_days).cast("int"))
    )

    # Select columns matching the target schema; fill any missing with nulls
    target_fields = sample_df.schema.fields
    result = base.select([c for c in [f.name for f in target_fields] if c in base.columns])
    for field in target_fields:
        if field.name not in result.columns:
            result = result.withColumn(field.name, F.lit(None).cast(field.dataType))

    return result.select([f.name for f in target_fields])


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
