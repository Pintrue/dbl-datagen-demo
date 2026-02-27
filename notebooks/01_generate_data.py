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
    "book_universe":       int(20_000   * SCALE),   # production: ~20K books
    "instrument":          int(10_000   * SCALE),
    "radial_instrument":   int(10_000   * SCALE),
    "fx_rates":            int(50_000   * SCALE),
    "position":            int(500_000  * SCALE),   # production: ~500K positions
    "package_composition": int(100_000  * SCALE),
}
ROWS_RISK_DEFAULT = int(20_000_000 * SCALE)         # production: ~20M risk rows per table

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
    "position_risk_greeks_assetlevel",
    "position_risk_greeks_currency",
    "position_risk_greeks_instrumentlevel",
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

    # Override Sensitivity_Value with a symmetric range for risk tables.
    # Real trading books have a mix of long (positive) and short (negative)
    # exposures — the sample data was skewed entirely negative.
    RISK_TABLES = {
        "position_risk_greeks_assetlevel",
        "position_risk_greeks_currency",
        "position_risk_greeks_instrumentlevel",
    }
    if table_name in RISK_TABLES and "Sensitivity_Value" in fields:
        overrides["Sensitivity_Value"] = {
            "minValue": -10_000_000.0, "maxValue": 10_000_000.0, "random": True,
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

GENERATION_ORDER = [
    "book_universe", "instrument", "radial_instrument", "fx_rates",
    "position", "package_composition",
    "position_risk_greeks_assetlevel", "position_risk_greeks_currency",
    "position_risk_greeks_instrumentlevel",
    "radial_arbrules", "radial_monikers",
    "radial_pnl_sensitivity_commodities",
    "radial_unit_sensitivities_asset_greeks",
    "radial_unit_sensitivities_asset_greeks_fully_decomposed",
    "radial_unit_sensitivities_instruments_greeks",
]

ordered_names = [n for n in GENERATION_ORDER if n in samples]
extras = [n for n in samples if n not in ordered_names]
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

    # Add Business_Date to risk tables — simulates daily risk calculation snapshots.
    # In a real RADIAL system, each row is one day's risk computation for a position.
    RISK_TABLES_WITH_DATE = {
        "position_risk_greeks_assetlevel",
        "position_risk_greeks_currency",
        "position_risk_greeks_instrumentlevel",
    }
    if table_name in RISK_TABLES_WITH_DATE:
        total_days = YEARS * 365
        gen_df = gen_df.withColumn(
            "Business_Date",
            F.date_add(F.lit(DATE_BEGIN), (F.rand(seed=42) * total_days).cast("int"))
        )

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
