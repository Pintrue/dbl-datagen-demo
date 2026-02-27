# Databricks notebook source
# MAGIC %md
# MAGIC # Atlas RADIAL: Genie Space Setup
# MAGIC
# MAGIC Creates a Genie space programmatically via the Databricks Python SDK
# MAGIC (`WorkspaceClient`) and saves the space ID for downstream use.
# MAGIC
# MAGIC **Programmatic coverage:**
# MAGIC - Genie space creation: Python SDK (`w.genie.create_space`) ✅
# MAGIC - Dataset registration (10 Gold views): Python SDK (`w.genie.update_space` with `serialized_space`) ✅
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

GENIE_INSTRUCTIONS = """
You are a financial risk analyst assistant for an investment bank's trading desk.
The data covers synthetic trading positions, risk sensitivities (Greeks), instruments, and FX rates
based on the RADIAL risk management system schema.

## Data Model
- **position**: Individual trading positions with currency, quantity, book and instrument references
- **book_universe**: Trading book hierarchy — location, desk, trading area
- **instrument**: Financial instruments with type, class, and currency
- **fx_rates**: FX rate time series across currency pairs
- **position_risk_greeks_assetlevel**: Asset-level risk sensitivities (delta, gamma, vega, etc.)
- **position_risk_greeks_currency**: Currency-level aggregated risk exposure
- **package_composition**: Structured product decompositions

## Gold Views (use these for analytics)

### Current Snapshot Views
- **gold_position_by_currency**: Position counts and quantities aggregated by currency
- **gold_book_hierarchy**: Book structure by location, desk and trading area
- **gold_instrument_breakdown**: Instrument types and classes
- **gold_fx_rates_summary**: FX rate ranges across currency pairs
- **gold_risk_greeks_exposure**: Risk sensitivity exposure by type (delta, gamma, vega, rho, theta)
- **gold_currency_risk_exposure**: Currency-level sensitivity totals
- **gold_position_book_summary**: Positions joined to book hierarchy
- **gold_position_instrument_summary**: Positions joined to instrument details
- **gold_book_risk_exposure**: Book-level aggregated risk (3-way join)
- **gold_package_composition**: Structured product composition summary

### Historical / Time-Series Views
- **gold_risk_exposure_monthly**: Monthly risk exposure by sensitivity type — use for trends, backtesting, period comparisons. Columns: Business_Month, Sensitivity_Type, record_count, total_exposure, avg_sensitivity
- **gold_fx_rates_timeseries**: Monthly FX rates by currency pair — use for FX trend analysis. Columns: Business_Month, Base_Currency, Reporting_Currency, Currency_Pair, avg_rate, min_rate, max_rate, observations

## Common Questions

### Current State
- "What is our total delta exposure?" → query gold_risk_greeks_exposure where Sensitivity_Type = 'DELTA'
- "Which books have the most positions?" → query gold_position_book_summary
- "Show FX exposure by currency pair" → query gold_currency_risk_exposure or gold_fx_rates_summary
- "Break down instruments by type" → query gold_instrument_breakdown
- "What currencies do we trade?" → query gold_position_by_currency

### Historical / Backtesting
- "How did our delta exposure change over the last year?" → query gold_risk_exposure_monthly WHERE Sensitivity_Type = 'DELTA' AND Business_Month >= '2024-01-01'
- "Compare vega exposure between 2022 and 2024" → query gold_risk_exposure_monthly WHERE Sensitivity_Type = 'VEGA' and compare years
- "Show me the GBP/USD FX rate trend" → query gold_fx_rates_timeseries WHERE Currency_Pair = 'GBP/USD'
- "What was our total risk exposure in Q3 2023?" → query gold_risk_exposure_monthly WHERE Business_Month BETWEEN '2023-07-01' AND '2023-09-01'
- "Which quarter had the highest gamma exposure?" → query gold_risk_exposure_monthly WHERE Sensitivity_Type = 'GAMMA', group by quarter

## Terminology
- Greeks: delta, gamma, vega, rho, theta — risk sensitivity measures
- DV01: Dollar value of a basis point (interest rate sensitivity)
- Book/Folder: Trading book organisational unit
- Position_Qty: Number of contracts or units held
- Business_Date/Business_Month: The date of the risk calculation snapshot
- Backtesting: Comparing historical risk predictions against actual outcomes
""".strip()

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
    "gold_book_hierarchy", "gold_book_risk_exposure", "gold_currency_risk_exposure",
    "gold_fx_rates_summary", "gold_fx_rates_timeseries",
    "gold_instrument_breakdown", "gold_package_composition",
    "gold_position_book_summary", "gold_position_by_currency",
    "gold_position_instrument_summary", "gold_risk_exposure_monthly",
    "gold_risk_greeks_exposure",
]

if SPACE_ID:
    try:
        # Tables must be sorted by identifier — API requirement
        tables_payload = sorted(
            [{"identifier": f"{CATALOG}.{SCHEMA}.{t}"} for t in GOLD_TABLES],
            key=lambda x: x["identifier"],
        )
        w.genie.update_space(
            space_id=SPACE_ID,
            serialized_space=json.dumps({
                "version": 2,
                "data_sources": {"tables": tables_payload},
            }),
        )
        print(f"Registered {len(tables_payload)} Gold views as Genie datasets:")
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
