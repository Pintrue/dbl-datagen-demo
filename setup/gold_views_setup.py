# Databricks notebook source
# MAGIC %md
# MAGIC # Atlas RADIAL: Gold Layer Views
# MAGIC
# MAGIC Creates Unity Catalog Gold views over the raw generated tables.
# MAGIC These views are what Genie and the AI/BI Dashboard query directly.
# MAGIC
# MAGIC **Prerequisite:** Run `01_generate_data` first (or data must already exist).
# MAGIC
# MAGIC **FX Conversion:** Silver tables LEFT JOIN `fx_rates` (on `Currency_Id = Base_Currency`)
# MAGIC to convert `Sensitivity_Value` to GBP. `COALESCE(avg_rate, 1.0)` handles GBP rows.

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
# MAGIC Sensitivity values are converted to GBP via LEFT JOIN on `Currency_Id → Base_Currency`.
# MAGIC This is the source of truth for all book-level risk time-series queries.

# COMMAND ----------

_group_cols = ", ".join(f"b.book_level_{i}" for i in range(1, NUM_BOOK_LEVELS + 1))
spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.silver_risk_book_level_monthly AS
WITH fx_monthly AS (
    SELECT
        Base_Currency,
        DATE_TRUNC('MONTH', Business_Date) AS Business_Month,
        AVG(Fx_Rate) AS avg_rate
    FROM {CATALOG}.{SCHEMA}.fx_rates
    WHERE Reporting_Currency = 'GBP'
    GROUP BY Base_Currency, DATE_TRUNC('MONTH', Business_Date)
)
SELECT
    DATE_TRUNC('MONTH', r.Business_Date) AS Business_Month,
    {_group_cols},
    r.Sensitivity_Type,
    ROUND(SUM(r.Sensitivity_Value * COALESCE(fx.avg_rate, 1.0)), 2) AS total_exposure,
    COUNT(*)                           AS record_count
FROM {CATALOG}.{SCHEMA}.position_risk_greeks_assetlevel r
JOIN {CATALOG}.{SCHEMA}.silver_book_hierarchy_flat b ON r.Folder_Id = b.Folder_Id
LEFT JOIN fx_monthly fx
    ON r.Currency_Id = fx.Base_Currency
    AND DATE_TRUNC('MONTH', r.Business_Date) = fx.Business_Month
GROUP BY DATE_TRUNC('MONTH', r.Business_Date), {_group_cols}, r.Sensitivity_Type
""")
print("Created: silver_risk_book_level_monthly")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Table: Risk Exposure by Primary Asset x Book (Materialized)
# MAGIC
# MAGIC Joins currency-level risk with the flattened book hierarchy so analysts can
# MAGIC drill into a specific ticker's exposure within a book or division. Includes
# MAGIC Book_Id for individual-book drill-down alongside book_level_1-4 for hierarchy.
# MAGIC Sensitivity values are converted to GBP via LEFT JOIN on `Currency_Id → Base_Currency`.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.silver_risk_asset_book_monthly AS
WITH fx_monthly AS (
    SELECT
        Base_Currency,
        DATE_TRUNC('MONTH', Business_Date) AS Business_Month,
        AVG(Fx_Rate) AS avg_rate
    FROM {CATALOG}.{SCHEMA}.fx_rates
    WHERE Reporting_Currency = 'GBP'
    GROUP BY Base_Currency, DATE_TRUNC('MONTH', Business_Date)
)
SELECT
    DATE_TRUNC('MONTH', r.Business_Date) AS Business_Month,
    r.Primary_Asset_Ref,
    r.Book_Id,
    b.book_level_1,
    b.book_level_2,
    b.book_level_3,
    b.book_level_4,
    r.Sensitivity_Type,
    ROUND(SUM(r.Sensitivity_Value * COALESCE(fx.avg_rate, 1.0)), 2) AS total_exposure,
    COUNT(*)                           AS record_count
FROM {CATALOG}.{SCHEMA}.position_risk_greeks_currency r
JOIN {CATALOG}.{SCHEMA}.silver_book_hierarchy_flat b ON r.Folder_Id = b.Folder_Id
LEFT JOIN fx_monthly fx
    ON r.Currency_Id = fx.Base_Currency
    AND DATE_TRUNC('MONTH', r.Business_Date) = fx.Business_Month
WHERE r.Primary_Asset_Ref IS NOT NULL
GROUP BY DATE_TRUNC('MONTH', r.Business_Date), r.Primary_Asset_Ref, r.Book_Id,
         b.book_level_1, b.book_level_2, b.book_level_3, b.book_level_4,
         r.Sensitivity_Type
""")
print("Created: silver_risk_asset_book_monthly")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold View: Position Summary by Book Level

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
# MAGIC ## Gold View: Risk Exposure by Primary Asset x Book (Monthly Time Series)
# MAGIC
# MAGIC Enables: "NVDA.O exposure in Equities division over time" (filter book_level_3)
# MAGIC and "NVDA.O exposure in Book 5976 specifically" (filter Book_Id).

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {CATALOG}.{SCHEMA}.gold_risk_asset_book_monthly AS
SELECT
    Business_Month,
    Primary_Asset_Ref,
    Book_Id,
    book_level_1,
    book_level_2,
    book_level_3,
    book_level_4,
    Sensitivity_Type,
    total_exposure,
    record_count
FROM {CATALOG}.{SCHEMA}.silver_risk_asset_book_monthly
ORDER BY Business_Month, Primary_Asset_Ref, book_level_3
""")
print("Created: gold_risk_asset_book_monthly")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

silver_tables = [
    "silver_book_hierarchy_flat",
    "silver_risk_asset_book_monthly",
    "silver_risk_book_level_monthly",
]
gold_views = [
    "gold_position_by_book_level",
    "gold_risk_book_level_long",
    "gold_risk_asset_book_monthly",
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
