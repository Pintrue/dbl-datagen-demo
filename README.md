# Atlas RADIAL — Risk Intelligence Demo

**Industry:** Investment Banking / Trading Risk
**Platform:** Databricks (Unity Catalog, AI/BI Dashboards, Genie)

---

## Overview

An AI-powered financial risk intelligence platform built on Databricks, demonstrating how a trading risk analyst can explore 10 years of synthetic RADIAL trading data through interactive dashboards and natural language queries.

Risk managers and traders use the **AI/BI Dashboard** to monitor position concentrations across the book hierarchy and track exposure to individual stocks over time — drilling from group entity down to individual book level, or filtering by region, asset class, and sensitivity type.

When they need answers the dashboard doesn't directly show — "How does our NVIDIA exposure in Equities compare to last year?" or "Which legal entity carries the most volatility risk?" — they switch to **Genie**, which translates natural language into SQL against the same gold views, returning results in seconds. Together, the dashboard and Genie give risk teams a self-service analytics layer over their RADIAL data without requiring SQL expertise or ad-hoc report requests.

### What's in the data

8 Delta tables generated from the Atlas RADIAL risk system schema:
- **750K+ positions** across CHF, EUR, GBP and USD, expanded to monthly snapshots over 10 years
- **Risk Greeks** (delta, gamma, vega, spot level) at asset-level and currency-level, derived via position × unit sensitivity joins
- **Book hierarchy**: 7 global locations (LON, NYC, TKY, HKG, SGP, FRA, SYD) × 10 desks × 6 trading areas, with an 11-level colon-delimited book path
- **~95 primary asset references** (stock tickers) across US Tech, Semiconductors, Banks, Healthcare, Energy, UK, EU, Asia and Emerging Markets
- **Deterministic FX rates** (CHF/EUR/USD/GBP → GBP) with monthly drift for currency conversion
- **FK-consistent IDs** — all joins work across synthetic tables

### Data scale (at `scale=1.0`, `years=10`)

| Table | Size | Rows |
|-------|------|------|
| `position_risk_greeks_assetlevel` | 13.6 GB | 2,143,646,040 |
| `position_risk_greeks_currency` | 6.5 GB | 1,026,720,360 |
| `silver_risk_asset_book_monthly` | 1.9 GB | 169,536,480 |
| `position` | 669 MB | 90,000,000 |
| `radial_unit_sensitivities_asset_greeks` | 278 MB | 20,000,000 |
| `radial_unit_sensitivities_asset_greeks_fully_decomposed` | 227 MB | 20,000,000 |
| `silver_risk_book_level_monthly` | 16 MB | 1,211,520 |
| `instrument` | 12 MB | 400,000 |
| `book_universe` / `silver_book_hierarchy_flat` | 0.1 MB | 2,000 |
| `fx_rates` | <0.1 MB | 480 |
| **Total** | **23.1 GB** | **3.47 billion** |

### FX conversion

All risk exposure values are converted to GBP at the silver layer. The two materialized silver tables LEFT JOIN with `fx_rates` on `Currency_Id = Base_Currency` and multiply `Sensitivity_Value` by `COALESCE(avg_rate, 1.0)`. When customers plug in real FX rates, the conversion works automatically.

---

## Architecture

```
                   databricks bundle deploy
                   (single command deployment)
                          |
              +-----------+-----------+
              |                       |
      +-------v-------+    +---------v-----------+
      |  Job: Data    |    | Dashboard Resource  |
      |  Generation   |    | atlas_radial_risk.  |
      |               |    | lvdash.json         |
      |  Task 1:      |    +---------------------+
      |  data gen     |              |
      |  Task 2:      |              v
      |  gold views   |    +---------------------+
      |  Task 3:      |    |  AI/BI Dashboard    |
      |  Genie API    |    |  2 pages, 12 charts |
      +---------------+    +----------+----------+
                                      |
                             Manual UI step
                                      |
                           +----------v----------+
                           |  Genie Space        |
                           |  (created via API)  |
                           |  6 UC tables/views  |
                           +---------------------+
```

---

## Quick Start

### Prerequisites
- Databricks workspace with Unity Catalog enabled
- SQL Warehouse for dashboard queries
- Access to upload the RADIAL `.dat` sample files to a UC Volume
- Databricks CLI installed: `pip install databricks-cli`

Throughout all commands below, replace these placeholders with your actual values before running:

| Placeholder | Description | Example |
|---|---|---|
| `YOUR_WORKSPACE` | Databricks workspace URL | `https://myworkspace.cloud.databricks.com` |
| `YOUR_CATALOG` | Unity Catalog catalog name | `main` |
| `YOUR_SCHEMA` | Schema for generated tables | `atlas_radial_demo` |
| `YOUR_SAMPLE_SCHEMA` | Schema where the sample Volume lives (can be the same) | `atlas_radial_demo` |
| `YOUR_WAREHOUSE_ID` | SQL Warehouse ID (found in Compute > SQL Warehouses > Connection details) | `abc123def456` |

### 1. Upload sample data files (one-time)

Upload the `.dat` files to a UC Volume. By default the pipeline expects them at `/Volumes/YOUR_CATALOG/YOUR_SAMPLE_SCHEMA/radial_sample/RADIALSAMPLE/`:

```bash
databricks fs cp -r ./sample_data/ \
  dbfs:/Volumes/YOUR_CATALOG/YOUR_SAMPLE_SCHEMA/radial_sample/RADIALSAMPLE/
```

If your files are at a different path, pass it as a variable in the deploy command instead:

```bash
--var="volume_path=/Volumes/YOUR_CATALOG/YOUR_SCHEMA/your/custom/path"
```

### 2. Update the dashboard queries (one-time)

The dashboard JSON (`resources/atlas_radial_risk.lvdash.json`) embeds SQL queries that reference your catalog and schema directly. Open the file and replace all occurrences of `<YOUR_CATALOG>` and `<YOUR_SCHEMA>` with your actual values before deploying.

### 3. Deploy and run (dev — quick test)

```bash
# Deploy lightweight dev dataset (scale=0.1, years=1)
databricks bundle deploy \
  --var="host=YOUR_WORKSPACE" \
  --var="catalog=YOUR_CATALOG" \
  --var="schema=YOUR_SCHEMA" \
  --var="sample_schema=YOUR_SAMPLE_SCHEMA"

# Run the pipeline: data generation → gold views → Genie space
databricks bundle run atlas_radial_data_generation
```

### 4. Deploy full solution (prod — full scale + dashboard)

```bash
# Deploy full dataset + dashboard
databricks bundle deploy --target prod \
  --var="host=YOUR_WORKSPACE" \
  --var="catalog=YOUR_CATALOG" \
  --var="schema=YOUR_SCHEMA" \
  --var="sample_schema=YOUR_SAMPLE_SCHEMA" \
  --var="warehouse_id=YOUR_WAREHOUSE_ID"

# Run the pipeline
databricks bundle run atlas_radial_data_generation --target prod
```

### 5. Connect Genie to Dashboard (manual step)

1. Open the deployed dashboard in Databricks UI
2. Add a Genie widget and select "Atlas RADIAL Risk Intelligence"
3. Position it on the dashboard

---

## Project Structure

```
atlas_radial_risk_intelligence/
├── README.md
├── databricks.yml                     # DABs: job + dashboard resource
├── notebooks/
│   ├── 01_generate_data.py            # dbldatagen pipeline + deterministic FX rates
│   └── 02_setup_genie_space.py        # Genie space + instructions + example SQLs
├── resources/
│   └── atlas_radial_risk.lvdash.json  # Lakeview dashboard (2 pages)
└── setup/
    └── gold_views_setup.py            # 3 silver tables + 3 gold views (FX conversion)
```

---

## Tables & Views

### Generated Tables (8)

| Table | Description |
|-------|-------------|
| `book_universe` | Trading book hierarchy — location, desk, trading area, 11-level book path |
| `instrument` | Financial instruments (used for FK range generation) |
| `position` | Trading positions expanded to monthly snapshots with varying Position_Qty |
| `radial_unit_sensitivities_asset_greeks` | Unit-level asset sensitivities |
| `radial_unit_sensitivities_asset_greeks_fully_decomposed` | Fully decomposed asset sensitivities with Currency_Id |
| `position_risk_greeks_assetlevel` | Asset-level risk (derived via position × unit sensitivity join) |
| `position_risk_greeks_currency` | Currency-level risk with Primary_Asset_Ref (derived via join) |
| `fx_rates` | Deterministic monthly FX rates (CHF/EUR/USD/GBP → GBP) |

### Silver Tables (3, materialized)

| Table | Description |
|-------|-------------|
| `silver_book_hierarchy_flat` | Flattened book hierarchy with book_level_1 through book_level_11 |
| `silver_risk_book_level_monthly` | Risk by all 11 book levels × month × sensitivity type (GBP-converted via FX join) |
| `silver_risk_asset_book_monthly` | Risk by ticker × Book_Id × book levels 1-4 × month (GBP-converted via FX join) |

### Gold Views (3)

| View | Description |
|------|-------------|
| `gold_position_by_book_level` | Latest-month position count and quantity by book levels 1-4 |
| `gold_risk_book_level_long` | Monthly risk at any book hierarchy level (long format, filter by level_num 1-11) |
| `gold_risk_asset_book_monthly` | Monthly exposure by ticker × Book_Id × book hierarchy levels 1-4 |

---

## Dashboard Pages

1. **Position by Book Hierarchy** — Position count by book level, risk exposure by division/asset class/region over time, book-level drill-down
2. **Risk by Primary Asset** — Top tickers by exposure, ticker-level time series, book-level breakdown per ticker

---

## Genie Space

The Genie space is configured with:
- **5 text instructions** — context, sensitivity types, book hierarchy values, ticker mappings, query patterns
- **5 example SQL pairs** — Genie learns these patterns and generalises to similar questions

Sample questions:
- "What is our overall delta exposure right now?"
- "Break down our risk by asset class"
- "Which stocks are we most exposed to?"
- "How does risk compare across EMEA, Americas, and Asia Pacific?"
- "How has our risk changed year on year?"

**Note:** The "Common questions" shown in the Genie space settings UI must be set manually — this is the only configuration not deployable via the repo.

---

## Technologies

- **dbldatagen** — Synthetic data generation at scale
- **Databricks Asset Bundles (DABs)** — Programmatic deployment
- **AI/BI Dashboards (Lakeview)** — Interactive analytics
- **Genie** — Natural language querying over trading data
- **Unity Catalog** — Data governance and Gold views
- **Databricks Python SDK** — Programmatic Genie space management

---

## Customisation

### Scaling the data
Pass the `scale` variable in the deploy command:
- `0.01` — Quick test (~200K risk rows)
- `0.1` — Dev (~2M risk rows)
- `1.0` — Demo (~20M risk rows, default for prod)
- `10.0` — Production (~200M risk rows)

```bash
databricks bundle deploy --target prod \
  --var="host=YOUR_WORKSPACE" \
  --var="catalog=YOUR_CATALOG" \
  --var="schema=YOUR_SCHEMA" \
  --var="sample_schema=YOUR_SAMPLE_SCHEMA" \
  --var="warehouse_id=YOUR_WAREHOUSE_ID" \
  --var="scale=0.01"
```

### Years of history
Pass the `years` variable (default: `10` for prod, `1` for dev):
```bash
--var="years=5"
```

### Adding locations, desks, or trading areas
Edit the override lists in `notebooks/01_generate_data.py`:
- `GLOBAL_LOCATIONS` — Trading locations (default: LON, NYC, TKY, HKG, SGP, FRA, SYD)
- `TRADING_DESKS` — Desk codes (default: 10 desks)
- `TRADING_AREAS` — Trading area names (default: 6 areas)
