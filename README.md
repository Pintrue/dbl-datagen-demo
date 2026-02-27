# Atlas RADIAL — Risk Intelligence Demo

**Industry:** Investment Banking / Trading Risk
**Platform:** Databricks (Unity Catalog, AI/BI Dashboards, Genie)

---

## Overview

An AI-powered financial risk intelligence platform built on Databricks, demonstrating how a trading risk analyst can explore 10 years of synthetic RADIAL trading data through interactive dashboards and natural language queries.

### What's in the data

15 Delta tables generated with `dbldatagen` from the Atlas RADIAL risk system schema:
- **500K+ positions** across multiple currencies, books and instruments
- **1.5M+ risk Greek records** (delta, gamma, vega, rho, theta) with daily Business_Date
- **Book hierarchy**: 7 global locations (LON, NYC, TKY, HKG, SGP, FRA, SYD) x 10 desks x 6 trading areas
- **10K instruments** with type and class breakdown
- **50K FX rate observations** across currency pairs
- **FK-consistent IDs** — all joins work across synthetic tables

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
      |  Genie API    |    |  5 pages, 15+ charts|
      +---------------+    +----------+----------+
                                      |
                             Manual UI step
                                      |
                           +----------v----------+
                           |  Genie Space        |
                           |  (created via API)  |
                           |  12 Gold UC views   |
                           +---------------------+
```

---

## Quick Start

### Prerequisites
- Databricks workspace with Unity Catalog enabled
- SQL Warehouse for dashboard queries
- Access to upload the 15 RADIAL `.dat` sample files to a UC Volume
- Databricks CLI installed: `pip install databricks-cli`

Throughout all commands below, replace these placeholders with your actual values before running:

| Placeholder | Description | Example |
|---|---|---|
| `YOUR_CATALOG` | Unity Catalog catalog name | `main` |
| `YOUR_SCHEMA` | Schema for generated tables | `atlas_radial_demo` |
| `YOUR_WAREHOUSE_ID` | SQL Warehouse ID (found in Compute > SQL Warehouses > Connection details) | `abc123def456` |

### 1. Upload sample data files (one-time)

```bash
databricks fs cp -r ./sample_data/ \
  dbfs:/Volumes/YOUR_CATALOG/YOUR_SCHEMA/radial_sample/RADIALSAMPLE/
```

### 2. Update the dashboard queries (one-time)

The dashboard JSON (`resources/atlas_radial_risk.lvdash.json`) embeds SQL queries that reference your catalog and schema directly. Open the file and replace all occurrences of `<YOUR_CATALOG>` and `<YOUR_SCHEMA>` with your actual values before deploying.

### 3. Deploy and run (dev — quick test)

```bash
# Deploy lightweight dev dataset (scale=0.1, ~2M risk rows)
databricks bundle deploy \
  -var="catalog=YOUR_CATALOG" \
  -var="schema=YOUR_SCHEMA"

# Run the pipeline: data generation → gold views → Genie space
databricks bundle run atlas_radial_data_generation
```

### 4. Deploy full solution (prod — full scale + dashboard)

```bash
# Deploy full dataset + dashboard
databricks bundle deploy --target prod \
  -var="catalog=YOUR_CATALOG" \
  -var="schema=YOUR_SCHEMA" \
  -var="warehouse_id=YOUR_WAREHOUSE_ID"

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
│   ├── 01_generate_data.py            # dbldatagen pipeline (parameterised)
│   └── 02_setup_genie_space.py        # Genie space + table registration via SDK
├── resources/
│   └── atlas_radial_risk.lvdash.json  # Lakeview dashboard definition
└── setup/
    └── gold_views_setup.py            # UC Gold views (12 views for Genie + Dashboard)
```

---

## Gold Views

12 Unity Catalog views power both the AI/BI Dashboard and Genie:

### Current Snapshot Views
| View | Description |
|------|-------------|
| `gold_position_by_currency` | Position counts and quantities by currency |
| `gold_book_hierarchy` | Book structure by location, desk and trading area |
| `gold_instrument_breakdown` | Instrument types and classes |
| `gold_fx_rates_summary` | FX rate ranges across currency pairs |
| `gold_risk_greeks_exposure` | Risk sensitivity exposure by type (delta, gamma, vega, etc.) |
| `gold_currency_risk_exposure` | Currency-level sensitivity totals |
| `gold_position_book_summary` | Positions joined to book hierarchy |
| `gold_position_instrument_summary` | Positions joined to instrument details |
| `gold_book_risk_exposure` | Book-level aggregated risk (3-way join) |
| `gold_package_composition` | Structured product composition summary |

### Historical / Time-Series Views
| View | Description |
|------|-------------|
| `gold_risk_exposure_monthly` | Monthly risk exposure by sensitivity type — for trends and backtesting |
| `gold_fx_rates_timeseries` | Monthly FX rates by currency pair — for FX trend analysis |

---

## Dashboard Pages

1. **Position Overview** — Top currencies by quantity, book count by location, total positions
2. **Instruments & FX** — Instrument breakdown, FX rate summary table, instrument count
3. **Risk Greeks Exposure** — Total exposure by sensitivity type, currency-level delta, total exposure KPI
4. **Book Risk** — Location-level risk table, delta exposure by location, sensitivity type filter
5. **Historical Trends** — Monthly risk exposure trends, FX rate time series, date range and Greek filters

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
  -var="catalog=YOUR_CATALOG" \
  -var="schema=YOUR_SCHEMA" \
  -var="warehouse_id=YOUR_WAREHOUSE_ID" \
  -var="scale=0.01"
```

### Years of history
Pass the `years` variable (default: `10` for prod, `1` for dev):
```bash
databricks bundle deploy --target prod \
  -var="catalog=YOUR_CATALOG" \
  -var="schema=YOUR_SCHEMA" \
  -var="warehouse_id=YOUR_WAREHOUSE_ID" \
  -var="years=5"
```

### Adding locations, desks, or trading areas
Edit the override lists in `notebooks/01_generate_data.py`:
- `GLOBAL_LOCATIONS` — Trading locations (default: LON, NYC, TKY, HKG, SGP, FRA, SYD)
- `TRADING_DESKS` — Desk codes (default: 10 desks)
- `TRADING_AREAS` — Trading area names (default: 6 areas)
