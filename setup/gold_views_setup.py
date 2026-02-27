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
# MAGIC ## Summary

# COMMAND ----------

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
]

print(f"\n{'='*60}")
print(f"Gold views created in {CATALOG}.{SCHEMA}:")
for v in gold_views:
    print(f"  - {v}")
print(f"\nTotal: {len(gold_views)} views ready for Genie and AI/BI Dashboard")
print(f"{'='*60}")
