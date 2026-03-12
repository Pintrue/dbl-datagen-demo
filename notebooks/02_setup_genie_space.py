# Databricks notebook source
# MAGIC %md
# MAGIC # Atlas RADIAL: Genie Space Setup
# MAGIC
# MAGIC Creates a Genie space programmatically via the Databricks Python SDK
# MAGIC (`WorkspaceClient`) and saves the space ID for downstream use.
# MAGIC
# MAGIC **Programmatic coverage:**
# MAGIC - Genie space creation: Python SDK (`w.genie.create_space`) ✅
# MAGIC - Dataset registration (6 tables/views): Python SDK (`w.genie.update_space` with `serialized_space`) ✅
# MAGIC - Space ID persisted to UC metadata table ✅
# MAGIC - Connecting Genie to AI/BI Dashboard: **manual step in UI** ⚠️
# MAGIC   (dashboard widget → Genie integration has no public API as of Feb 2026)
# MAGIC
# MAGIC **Prerequisite:** Run `gold_views_setup` first.

# COMMAND ----------

dbutils.widgets.text("catalog",      "",                                "Unity Catalog")
dbutils.widgets.text("schema",       "",                                "Schema")
dbutils.widgets.text("space_title",  "Atlas RADIAL Risk Intelligence",  "Genie Space Title")
dbutils.widgets.text("warehouse_id", "",                                "SQL Warehouse ID")

CATALOG      = dbutils.widgets.get("catalog")
SCHEMA       = dbutils.widgets.get("schema")
SPACE_TITLE  = dbutils.widgets.get("space_title")
WAREHOUSE_ID = dbutils.widgets.get("warehouse_id")

# COMMAND ----------

%pip install "databricks-sdk>=0.40.0" --quiet
dbutils.library.restartPython()

# COMMAND ----------

import json
from databricks.sdk import WorkspaceClient

# Explicitly pass host + token so the SDK uses the cluster's credentials.
# WorkspaceClient() without arguments looks for a ~/.databrickscfg file which
# does not exist on serverless compute, causing silent authentication failure.
WORKSPACE_HOST = spark.conf.get("spark.databricks.workspaceUrl")
TOKEN = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()

w = WorkspaceClient(host=f"https://{WORKSPACE_HOST}", token=TOKEN)

# Note: on Azure AAD workspaces, getContext().apiToken() returns a short-lived
# AAD execution-context token. This token is scoped to the job run and may not
# have access to newer Databricks APIs such as Genie. If create_space fails below
# you will see the error printed; the job will still succeed (graceful fallback).
# To use programmatic creation, store a PAT in Databricks Secrets and pass it here.

print(f"Workspace: {WORKSPACE_HOST}")
print(f"SDK host:  {w.config.host}")
print(f"Target:    {CATALOG}.{SCHEMA}")
print(f"Warehouse: {WAREHOUSE_ID}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Create Genie Space

# COMMAND ----------

import hashlib

# Concise text instructions — kept short to avoid Genie "long instruction" warning.
# SQL query patterns are in EXAMPLE_QUESTION_SQLS below (more effective for Genie).
GENIE_TEXT_INSTRUCTIONS = [
    (
        "You are a financial risk analyst assistant for an investment bank's trading desk. "
        "All exposure values (total_exposure) are in GBP, converted via FX rates. "
        "Data spans January 2015 to December 2024 (120 monthly snapshots). "
        "When users ask about 'risk' without specifying a type, default to Sensitivity_Type = 'Delta'. "
        "When users ask about 'all risk' or 'total exposure', aggregate across all Sensitivity_Type values. "
        "For 'latest' or 'current' queries, use: Business_Month = (SELECT MAX(Business_Month) FROM <table>). "
        "Business_Month is a timestamp truncated to the first of each month (e.g. 2024-01-01)."
    ),
    (
        "Sensitivity_Type values (case-sensitive): Delta, DeltaCash, Gamma, GammaCash, SpotLevel, SpotLevelCash, Vega, VegaCash. "
        "Delta = sensitivity to underlying price change. Gamma = sensitivity to delta change (convexity). Vega = sensitivity to volatility. "
        "Cash variants (DeltaCash, GammaCash, VegaCash) are cash-equivalent of the Greek in GBP."
    ),
    (
        "Book hierarchy has 11 levels. "
        "Level 1 (Group Entity): Barclays Group, BCSL Holdings, Barclays International. "
        "Level 2 (Division): Banking, Corporate, Markets. "
        "Level 3 (Asset Class): Commodities, Equities, FX & Rates, Fixed Income, Multi-Asset. "
        "Level 4 (Business Area): Derivatives, Flow Trading, Macro Rates, Origination, Prime Services. "
        "Level 5 (Sub-Business): Cash Products, Credit Trading, Delta One, Options & Exotics, Rates Swaps. "
        "Level 6 (Region): Americas, Asia Pacific, EMEA. "
        "Level 7 (Country): Continental Europe, Japan, North America, SE Asia, UK. "
        "Level 8 (Legal Entity): BBIL, BBPLC, BNAC, BSAG. "
        "Level 9 (Portfolio Status): Active Books, Legacy Portfolio, Run-Off. "
        "Level 10 (Regulatory Scope): In-Scope, Out-of-Scope, Regulatory Capital. "
        "Level 11: Individual Book (BK-{Folder_Id})."
    ),
    (
        "Primary_Asset_Ref is a stock ticker with exchange suffixes: .O=NASDAQ, .N=NYSE, .L=LSE, .DE=XETRA, .T=Tokyo, .HK=HKEX. "
        "Map company names to tickers: Apple=AAPL.O, Microsoft=MSFT.O, Google=GOOGL.O, Amazon=AMZN.O, NVIDIA=NVDA.O, Tesla=TSLA.O, "
        "Barclays=BARC.L, HSBC=HSBA.L, Shell=SHEL.L, BP=BP.L, AstraZeneca=AZN.L, Vodafone=VOD.L, "
        "JPMorgan=JPM.N, Goldman Sachs=GS.N, SAP=SAP.DE, ASML=ASML.AS, Samsung=005930.KS, Tencent=0700.HK."
    ),
    (
        "gold_risk_book_level_long has columns: Business_Month, Sensitivity_Type, level_num (1-11), level_label, level_value, total_exposure. "
        "Filter by level_num to choose aggregation level. For example level_num=3 gives asset class breakdown."
    ),
]

# Example SQL pairs — Genie learns these patterns and generalises to similar questions.
# Each entry needs: id (32-hex), question (array), sql (array).
EXAMPLE_QUESTION_SQLS = [
    ("What is our overall delta exposure right now?",
     "SELECT SUM(total_exposure) AS total_delta_exposure FROM {s}.silver_risk_book_level_monthly WHERE Sensitivity_Type = 'Delta' AND Business_Month = (SELECT MAX(Business_Month) FROM {s}.silver_risk_book_level_monthly)"),
    ("Break down our risk by asset class",
     "SELECT level_value AS asset_class, SUM(total_exposure) AS total_exposure FROM {s}.gold_risk_book_level_long WHERE level_num = 3 AND Sensitivity_Type = 'Delta' AND Business_Month = (SELECT MAX(Business_Month) FROM {s}.gold_risk_book_level_long) GROUP BY level_value ORDER BY ABS(total_exposure) DESC"),
    ("Which stocks are we most exposed to?",
     "SELECT Primary_Asset_Ref, SUM(total_exposure) AS total_exposure FROM {s}.gold_risk_asset_book_monthly WHERE Sensitivity_Type = 'Delta' AND Business_Month = (SELECT MAX(Business_Month) FROM {s}.gold_risk_asset_book_monthly) GROUP BY Primary_Asset_Ref ORDER BY ABS(total_exposure) DESC LIMIT 20"),
    ("How does risk compare across EMEA, Americas, and Asia Pacific?",
     "SELECT level_value AS region, Business_Month, SUM(total_exposure) AS total_exposure FROM {s}.gold_risk_book_level_long WHERE level_num = 6 AND Sensitivity_Type = 'Delta' GROUP BY level_value, Business_Month ORDER BY Business_Month"),
    ("How has our risk changed year on year?",
     "SELECT YEAR(Business_Month) AS year, SUM(total_exposure) AS total_exposure FROM {s}.silver_risk_book_level_monthly WHERE Sensitivity_Type = 'Delta' GROUP BY YEAR(Business_Month) ORDER BY year"),
]

# COMMAND ----------

SPACE_ID = None

# Step 1: check persisted metadata table for an existing space ID
try:
    meta = spark.table(f"{CATALOG}.{SCHEMA}._genie_space_metadata").collect()
    if meta:
        SPACE_ID = meta[0]["space_id"]
        print(f"Found existing space ID in metadata: {SPACE_ID}")
except Exception:
    pass  # table doesn't exist yet on first run

# Step 2: if metadata had an ID, verify it still exists in the workspace
if SPACE_ID:
    try:
        w.genie.get_space(space_id=SPACE_ID)
        print(f"Verified space still exists: {SPACE_TITLE} (ID: {SPACE_ID})")
    except Exception:
        print(f"Saved space ID {SPACE_ID} no longer exists — will create a new one.")
        SPACE_ID = None

# Step 3: if still no ID, search by title before creating
if not SPACE_ID:
    try:
        for s in (w.genie.list_spaces().spaces or []):
            if s.title == SPACE_TITLE:
                SPACE_ID = s.space_id
                print(f"Found existing space by title: {SPACE_TITLE} (ID: {SPACE_ID})")
                break
    except Exception as e:
        print(f"list_spaces failed: {e}")

# Step 4: create only if no existing space found
if not SPACE_ID:
    try:
        space = w.genie.create_space(
            warehouse_id=WAREHOUSE_ID,
            serialized_space=json.dumps({"version": 1}),
            title=SPACE_TITLE,
            description="Atlas RADIAL Risk Intelligence",
        )
        SPACE_ID = space.space_id
        print(f"Genie space created: {SPACE_TITLE} (ID: {SPACE_ID})")
    except Exception as e:
        print(f"create_space failed: {type(e).__name__}: {e}")

if not SPACE_ID:
    print(f"Could not create or find space '{SPACE_TITLE}'. Job will continue.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Register Gold Views as Datasets

# COMMAND ----------

GOLD_TABLES = [
    "gold_position_by_book_level",
    "gold_risk_asset_book_monthly",
    "gold_risk_book_level_long",
    "silver_book_hierarchy_flat",
    "silver_risk_asset_book_monthly",
    "silver_risk_book_level_monthly",
]

if SPACE_ID:
    try:
        # Tables must be sorted by identifier — API requirement
        tables_payload = sorted(
            [{"identifier": f"{CATALOG}.{SCHEMA}.{t}"} for t in GOLD_TABLES],
            key=lambda x: x["identifier"],
        )
        fq = f"{CATALOG}.{SCHEMA}"
        example_sqls = sorted([
            {
                "id": hashlib.md5(q.encode()).hexdigest(),
                "question": [q],
                "sql": [sql.format(s=fq)],
            }
            for q, sql in EXAMPLE_QUESTION_SQLS
        ], key=lambda x: x["id"])
        w.genie.update_space(
            space_id=SPACE_ID,
            serialized_space=json.dumps({
                "version": 2,
                "data_sources": {"tables": tables_payload},
                "instructions": {
                    "text_instructions": [
                        {"content": GENIE_TEXT_INSTRUCTIONS}
                    ],
                    "example_question_sqls": example_sqls,
                },
            }),
        )
        print(f"Registered {len(tables_payload)} tables + {len(GENIE_TEXT_INSTRUCTIONS)} instructions + {len(example_sqls)} example SQLs:")
        for entry in tables_payload:
            print(f"  {entry['identifier']}")
    except Exception as e:
        print(f"update_space (datasets) failed: {e}")
else:
    print("Skipping dataset registration — no space ID.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Save Space ID & Manual Steps

# COMMAND ----------

# Persist space_id to UC so downstream jobs / README can reference it
if SPACE_ID:
    spark.createDataFrame(
        [(SPACE_ID, SPACE_TITLE, f"{CATALOG}.{SCHEMA}")],
        ["space_id", "space_title", "catalog_schema"]
    ).write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}._genie_space_metadata")
    print(f"Space ID saved to {CATALOG}.{SCHEMA}._genie_space_metadata")

print(f"\n{'='*60}")
print(f"Genie Space Setup Complete")
print(f"{'='*60}")
print(f"Space ID:    {SPACE_ID}")
print(f"Space Title: {SPACE_TITLE}")

if SPACE_ID:
    print(f"""
> ONE MANUAL STEP REQUIRED <

Embed Genie in the AI/BI Dashboard:
  - Open the dashboard in Databricks UI
  - Add a Genie widget → Select: '{SPACE_TITLE}'
  - Space URL: https://{WORKSPACE_HOST}/genie/spaces/{SPACE_ID}

The Genie ↔ Dashboard widget connection has no public API (Feb 2026).
""")
else:
    print("""
Space creation failed. Create manually:
  1. Go to Genie in the left nav
  2. Create new space titled: """ + SPACE_TITLE + """
  3. Add Gold views as tables (see list above)
""")

print(f"{'='*60}")
