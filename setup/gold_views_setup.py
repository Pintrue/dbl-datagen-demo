# Databricks notebook source
# MAGIC %md
# MAGIC # Atlas RADIAL: Gold Layer Views
# MAGIC
# MAGIC Creates Unity Catalog Gold views over the raw generated tables.
# MAGIC These views are what Genie and the AI/BI Dashboard query directly.
# MAGIC
# MAGIC **Prerequisite:** Run `01_generate_data` first (or data must already exist).

# COMMAND ----------

dbutils.widgets.text("catalog", "", "Unity Catalog")
dbutils.widgets.text("schema", "", "Schema")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA  = dbutils.widgets.get("schema")

print(f"Creating Gold views in: {CATALOG}.{SCHEMA}")

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold View 1: Position Summary by Currency

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {CATALOG}.{SCHEMA}.gold_position_by_currency AS
SELECT
    Currency_Code,
    COUNT(*)                         AS position_count,
    ROUND(SUM(Position_Qty), 2)      AS total_quantity,
    COUNT(DISTINCT Folder_Id)        AS unique_books,
    COUNT(DISTINCT Instrument_Id)    AS unique_instruments
FROM {CATALOG}.{SCHEMA}.position
WHERE Business_Date = (SELECT MAX(Business_Date) FROM {CATALOG}.{SCHEMA}.position)
GROUP BY Currency_Code
ORDER BY position_count DESC
""")
print("Created: gold_position_by_currency")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold View 2: Book Hierarchy Summary

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {CATALOG}.{SCHEMA}.gold_book_hierarchy AS
SELECT
    Book_Location_Code,
    COUNT(*)                              AS book_count,
    COUNT(DISTINCT Desk_Code)             AS desk_count,
    COUNT(DISTINCT Trading_Area_Name)     AS trading_area_count
FROM {CATALOG}.{SCHEMA}.book_universe
GROUP BY Book_Location_Code
ORDER BY book_count DESC
""")
print("Created: gold_book_hierarchy")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold View 3: Instrument Breakdown

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {CATALOG}.{SCHEMA}.gold_instrument_breakdown AS
SELECT
    Type                  AS instrument_type,
    Class_Code,
    COUNT(*)              AS instrument_count,
    COUNT(DISTINCT Currency) AS currency_count
FROM {CATALOG}.{SCHEMA}.instrument
GROUP BY Type, Class_Code
ORDER BY instrument_count DESC
""")
print("Created: gold_instrument_breakdown")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold View 4: FX Rates Summary

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {CATALOG}.{SCHEMA}.gold_fx_rates_summary AS
SELECT
    Base_Currency,
    Reporting_Currency,
    ROUND(AVG(Fx_Rate), 8)   AS avg_rate,
    ROUND(MIN(Fx_Rate), 8)   AS min_rate,
    ROUND(MAX(Fx_Rate), 8)   AS max_rate,
    COUNT(*)                 AS rate_observations
FROM {CATALOG}.{SCHEMA}.fx_rates
GROUP BY Base_Currency, Reporting_Currency
ORDER BY rate_observations DESC
""")
print("Created: gold_fx_rates_summary")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold View 5: Risk Greeks — Sensitivity Exposure

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {CATALOG}.{SCHEMA}.gold_risk_greeks_exposure AS
SELECT
    Sensitivity_Type,
    COUNT(*)                              AS record_count,
    ROUND(AVG(Sensitivity_Value), 4)      AS avg_sensitivity,
    ROUND(SUM(Sensitivity_Value), 2)      AS total_exposure,
    ROUND(MIN(Sensitivity_Value), 4)      AS min_value,
    ROUND(MAX(Sensitivity_Value), 4)      AS max_value
FROM {CATALOG}.{SCHEMA}.position_risk_greeks_assetlevel
GROUP BY Sensitivity_Type
ORDER BY ABS(total_exposure) DESC
""")
print("Created: gold_risk_greeks_exposure")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold View 6: Currency-Level Risk Exposure

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {CATALOG}.{SCHEMA}.gold_currency_risk_exposure AS
SELECT
    Currency_Id,
    Sensitivity_Type,
    COUNT(*)                              AS record_count,
    ROUND(SUM(Sensitivity_Value), 2)      AS total_exposure
FROM {CATALOG}.{SCHEMA}.position_risk_greeks_currency
GROUP BY Currency_Id, Sensitivity_Type
ORDER BY ABS(total_exposure) DESC
""")
print("Created: gold_currency_risk_exposure")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold View 7: Position × Book Join (Multi-table)

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {CATALOG}.{SCHEMA}.gold_position_book_summary AS
SELECT
    p.Currency_Code,
    b.Book_Location_Code,
    b.Desk_Code,
    COUNT(*)                  AS position_count,
    ROUND(SUM(p.Position_Qty), 2) AS total_quantity
FROM {CATALOG}.{SCHEMA}.position p
JOIN {CATALOG}.{SCHEMA}.book_universe b ON p.Folder_Id = b.Folder_Id
WHERE p.Business_Date = (SELECT MAX(Business_Date) FROM {CATALOG}.{SCHEMA}.position)
GROUP BY p.Currency_Code, b.Book_Location_Code, b.Desk_Code
ORDER BY position_count DESC
""")
print("Created: gold_position_book_summary")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold View 8: Position × Instrument Join

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {CATALOG}.{SCHEMA}.gold_position_instrument_summary AS
SELECT
    i.Type       AS instrument_type,
    i.Class_Code,
    i.Currency   AS instrument_currency,
    COUNT(*)                      AS position_count,
    ROUND(SUM(p.Position_Qty), 2) AS total_quantity
FROM {CATALOG}.{SCHEMA}.position p
JOIN {CATALOG}.{SCHEMA}.instrument i ON p.Instrument_Id = i.Instrument_Id
WHERE p.Business_Date = (SELECT MAX(Business_Date) FROM {CATALOG}.{SCHEMA}.position)
GROUP BY i.Type, i.Class_Code, i.Currency
ORDER BY position_count DESC
""")
print("Created: gold_position_instrument_summary")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold View 9: Book Risk Exposure (3-way join)

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {CATALOG}.{SCHEMA}.gold_book_risk_exposure AS
SELECT
    b.Book_Location_Code,
    b.Desk_Code,
    r.Sensitivity_Type,
    COUNT(*)                              AS risk_records,
    ROUND(SUM(r.Sensitivity_Value), 2)    AS total_sensitivity,
    ROUND(AVG(r.Sensitivity_Value), 4)    AS avg_sensitivity
FROM {CATALOG}.{SCHEMA}.position_risk_greeks_assetlevel r
JOIN {CATALOG}.{SCHEMA}.book_universe b ON r.Folder_Id = b.Folder_Id
GROUP BY b.Book_Location_Code, b.Desk_Code, r.Sensitivity_Type
ORDER BY ABS(total_sensitivity) DESC
""")
print("Created: gold_book_risk_exposure")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold View 10: Package Composition Summary

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {CATALOG}.{SCHEMA}.gold_package_composition AS
SELECT
    COUNT(DISTINCT Parent_Instrument_Id)  AS unique_packages,
    COUNT(DISTINCT Instrument_Id)         AS unique_components,
    COUNT(*)                              AS total_links,
    ROUND(AVG(Weight), 4)                 AS avg_weight,
    ROUND(AVG(Nominal), 6)                AS avg_nominal
FROM {CATALOG}.{SCHEMA}.package_composition
""")
print("Created: gold_package_composition")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold View 11: Risk Exposure Monthly (Time Series)
# MAGIC
# MAGIC Aggregates risk Greeks by month for historical trend analysis and backtesting.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {CATALOG}.{SCHEMA}.gold_risk_exposure_monthly AS
SELECT
    DATE_TRUNC('MONTH', Business_Date)    AS Business_Month,
    Sensitivity_Type,
    COUNT(*)                              AS record_count,
    ROUND(SUM(Sensitivity_Value), 2)      AS total_exposure,
    ROUND(AVG(Sensitivity_Value), 4)      AS avg_sensitivity
FROM {CATALOG}.{SCHEMA}.position_risk_greeks_assetlevel
GROUP BY DATE_TRUNC('MONTH', Business_Date), Sensitivity_Type
ORDER BY Business_Month, Sensitivity_Type
""")
print("Created: gold_risk_exposure_monthly")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold View 12: FX Rates Time Series
# MAGIC
# MAGIC Monthly FX rate trends by currency pair for historical analysis.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {CATALOG}.{SCHEMA}.gold_fx_rates_timeseries AS
SELECT
    DATE_TRUNC('MONTH', Business_Date)    AS Business_Month,
    Base_Currency,
    Reporting_Currency,
    CONCAT(Base_Currency, '/', Reporting_Currency) AS Currency_Pair,
    ROUND(AVG(Fx_Rate), 6)               AS avg_rate,
    ROUND(MIN(Fx_Rate), 6)               AS min_rate,
    ROUND(MAX(Fx_Rate), 6)               AS max_rate,
    COUNT(*)                              AS observations
FROM {CATALOG}.{SCHEMA}.fx_rates
GROUP BY DATE_TRUNC('MONTH', Business_Date), Base_Currency, Reporting_Currency
ORDER BY Business_Month
""")
print("Created: gold_fx_rates_timeseries")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Table: Flattened Book Hierarchy
# MAGIC
# MAGIC Splits `SDS_Book_Path` into 11 named level columns so analysts can group,
# MAGIC filter and aggregate positions at any node of the book family tree without
# MAGIC needing to write string-splitting logic in every query.

# COMMAND ----------

NUM_BOOK_LEVELS = 11
_level_cols = ",\n    ".join(
    f"SPLIT(SDS_Book_Path, ':')[{i}] AS book_level_{i + 1}"
    for i in range(NUM_BOOK_LEVELS)
)
spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.silver_book_hierarchy_flat AS
SELECT
    Folder_Id,
    Book_Id,
    Book_Location_Code,
    Desk_Code,
    Trading_Area_Name,
    SDS_Book_Path,
    {_level_cols}
FROM {CATALOG}.{SCHEMA}.book_universe
""")
print("Created: silver_book_hierarchy_flat")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Table: Risk Exposure by Book Level (Materialized)
# MAGIC
# MAGIC Pre-aggregates risk sensitivity by all 11 book levels + month + sensitivity type.
# MAGIC This is the source of truth for all book-level risk time-series queries.

# COMMAND ----------

_group_cols = ", ".join(f"b.book_level_{i}" for i in range(1, NUM_BOOK_LEVELS + 1))
spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.silver_risk_book_level_monthly AS
SELECT
    DATE_TRUNC('MONTH', r.Business_Date) AS Business_Month,
    {_group_cols},
    r.Sensitivity_Type,
    ROUND(SUM(r.Sensitivity_Value), 2) AS total_exposure,
    COUNT(*)                           AS record_count
FROM {CATALOG}.{SCHEMA}.position_risk_greeks_assetlevel r
JOIN {CATALOG}.{SCHEMA}.silver_book_hierarchy_flat b ON r.Folder_Id = b.Folder_Id
GROUP BY DATE_TRUNC('MONTH', r.Business_Date), {_group_cols}, r.Sensitivity_Type
""")
print("Created: silver_risk_book_level_monthly")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold View: Risk Exposure by Book Level (Long Format)
# MAGIC
# MAGIC Stacks all 11 levels into `level_label` / `level_value` columns so the
# MAGIC dashboard can filter to any level dynamically without changing the query.

# COMMAND ----------

_union_parts = []
for i in range(NUM_BOOK_LEVELS):
    lvl = i + 1
    _union_parts.append(
        f"SELECT Business_Month, Sensitivity_Type, "
        f"{lvl} AS level_num, 'Level {lvl}' AS level_label, "
        f"book_level_{lvl} AS level_value, "
        f"SUM(total_exposure) AS total_exposure "
        f"FROM {CATALOG}.{SCHEMA}.silver_risk_book_level_monthly "
        f"GROUP BY Business_Month, Sensitivity_Type, book_level_{lvl}"
    )
spark.sql(
    f"CREATE OR REPLACE VIEW {CATALOG}.{SCHEMA}.gold_risk_book_level_long AS\n"
    + "\nUNION ALL\n".join(_union_parts)
)
print("Created: gold_risk_book_level_long")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold View 13: Position Summary by Book Level

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {CATALOG}.{SCHEMA}.gold_position_by_book_level AS
SELECT
    b.book_level_1,
    b.book_level_2,
    b.book_level_3,
    b.book_level_4,
    COUNT(*)                          AS position_count,
    ROUND(SUM(p.Position_Qty), 2)     AS total_quantity
FROM {CATALOG}.{SCHEMA}.position p
JOIN {CATALOG}.{SCHEMA}.silver_book_hierarchy_flat b ON p.Folder_Id = b.Folder_Id
WHERE p.Business_Date = (SELECT MAX(Business_Date) FROM {CATALOG}.{SCHEMA}.position)
GROUP BY b.book_level_1, b.book_level_2, b.book_level_3, b.book_level_4
ORDER BY position_count DESC
""")
print("Created: gold_position_by_book_level")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold View 14: Monthly Position Exposure by Book Level (Time Series)

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {CATALOG}.{SCHEMA}.gold_position_book_level_monthly AS
SELECT
    DATE_TRUNC('MONTH', r.Business_Date)   AS Business_Month,
    b.book_level_1,
    b.book_level_2,
    b.book_level_3,
    r.Sensitivity_Type,
    COUNT(*)                               AS record_count,
    ROUND(SUM(r.Sensitivity_Value), 2)     AS total_exposure
FROM {CATALOG}.{SCHEMA}.position_risk_greeks_assetlevel r
JOIN {CATALOG}.{SCHEMA}.silver_book_hierarchy_flat b
    ON r.Folder_Id = b.Folder_Id
GROUP BY
    DATE_TRUNC('MONTH', r.Business_Date),
    b.book_level_1, b.book_level_2, b.book_level_3, r.Sensitivity_Type
ORDER BY Business_Month
""")
print("Created: gold_position_book_level_monthly")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

silver_tables = [
    "silver_book_hierarchy_flat",
    "silver_risk_book_level_monthly",
]
gold_views = [
    "gold_position_by_currency",
    "gold_book_hierarchy",
    "gold_instrument_breakdown",
    "gold_fx_rates_summary",
    "gold_risk_greeks_exposure",
    "gold_currency_risk_exposure",
    "gold_position_book_summary",
    "gold_position_instrument_summary",
    "gold_book_risk_exposure",
    "gold_package_composition",
    "gold_risk_exposure_monthly",
    "gold_fx_rates_timeseries",
    "gold_position_by_book_level",
    "gold_position_book_level_monthly",
    "gold_risk_book_level_long",
]

print(f"\n{'='*60}")
print(f"Silver tables created in {CATALOG}.{SCHEMA}:")
for t in silver_tables:
    print(f"  - {t}")
print(f"\nGold views created in {CATALOG}.{SCHEMA}:")
for v in gold_views:
    print(f"  - {v}")
print(f"\nTotal: {len(silver_tables)} silver tables + {len(gold_views)} gold views")
print(f"{'='*60}")
