# E-Commerce & Supply Chain Analytics Lakehouse

An end-to-end analytics project on a **synthetic** e-commerce dataset: data generation, a Bronze/Silver/Gold lakehouse on Databricks, Spark SQL analysis, a delay-prediction model, and a Streamlit dashboard.

**Stack:** Databricks (Delta Lake, Unity Catalog), PySpark, Spark SQL, Spark MLlib, Python (Pandas, NumPy), Streamlit, Plotly
**Live demo:** [add link]

## About the data

All data in this project is **synthetic**. I generated it with `src/generator.py` (NumPy/Pandas) so I could practice working at scale: 1,000,000 orders, 1,449,520 order items, 10 distribution centers, 30 months (2022-01 to 2024-06).

Because I wrote the generator, the findings below reflect the rules and assumptions I built into it, not real customer or carrier behavior. Dollar figures are illustrative. This project demonstrates method, not business discovery.

## What I built

| Layer | What it does |
|---|---|
| **Bronze** | Ingests four chunked CSV volumes into a Unity Catalog schema; runs a six-dimension data audit (including foreign-key orphan checks) |
| **Silver** | Removes 10,000 duplicate order records, corrects 2,564 categorical typos, applies four delivery-case rules, upserts with Delta `MERGE INTO`, partitions by month |
| **Gold** | Business marts: hub bottlenecks, category margin and COGS, top 3 SKUs per category, 30-month growth |
| **App** | Streamlit dashboard with four tabs |

## Results on the synthetic dataset

| Metric | Result | Note |
|---|---|---|
| Net revenue | $585.66M | Simulated, 30 months |
| Fulfilled orders | 919,804 | 1,000,000 orders minus 80,196 cancellations |
| Delivery delay rate | 37.44% | Cancelled orders excluded from the denominator |
| Commercial OTIF rate | 55.28% | On-time and not returned |
| Delay model accuracy | 81.07% | vs. 62.56% majority-class baseline (+18.5 percentage points) |
| Delay precision / recall | 79.15% / 66.99% | Class 1 (delayed) on 184,017 unseen test orders |

## Modeling decisions

**1. Cancelled-order denominator.** Setting `is_delayed = 0` on cancelled orders counts them as on-time and understates the delay rate by about 8%. I set the delay flags to `NULL` for cancelled orders so they drop out of the denominator.

**2. Carrier SLA vs. commercial OTIF.** A customer returning an item because of size should not count against the carrier. I split `is_on_time_delivery` (carrier transit compliance) from `is_otif` (on time and kept by the customer).

**3. Delay attribution.** One combined "delay" bucket hides where failures happen. I split duration into a warehouse packing SLA (2 days) and a carrier transit SLA.

**4. Point-in-time shipping address.** Customers move. I stored `shipping_city` and `shipping_state` on the order fact so past deliveries are not linked to a customer's current address.

## SQL analysis

Ten queries in `notebooks/03_enterprise_sql_queries.sql`:

| # | Scenario | Technique | Result on this dataset |
|---|---|---|---|
| 1 | Monthly OTIF and SLA compliance | Denominator-protected filter | 37.44% carrier delay rate vs. 55.28% commercial OTIF |
| 2 | Warehouse vs. carrier bottlenecks | Multi-condition aggregation | Carrier delays are over 78% of failures at coastal hubs |
| 3 | Category margin | `GROUP BY`, `HAVING` | Electronics is highest in GMV ($380.5M); Beauty has the highest margin (46.9%) |
| 4 | Top 3 SKUs per category | `DENSE_RANK() OVER (PARTITION BY ...)` | 18 SKUs identified |
| 5 | Month-over-month growth | `LAG() OVER (ORDER BY month)` | Apparent February dip disappears on a per-day basis |
| 6 | Running totals and milestones | `SUM() OVER (ROWS UNBOUNDED PRECEDING)` | Year-to-date resets verified; $100M, $250M, $500M crossings found |
| 7 | Repeat-customer cohorts | Multi-stage CTEs | 37,154 repeat buyers (74.3%) account for 81.6% of revenue |
| 8 | Join fan-out prevention | Pre-aggregation CTE | Naive join inflated freight fees by 45% ($6.3M vs. $4.35M) |
| 9 | Pareto 80/20 SKU profit | Cumulative profit % | 150 SKUs (15%) produce 50% of profit |
| 10 | Shipping lanes and SLA | Origin-to-destination cross-tab | 80.8% of $14.99 cross-country express orders missed SLA |

## Delay prediction model

`notebooks/04_predictive_delay_modeling.py`: a Spark MLlib Random Forest (50 trees, max depth 8) trained on 735,787 orders and tested on 184,017.

- **Leakage control:** only features known at checkout are used. Ship date, delivery date, dispatch and transit days, and order status are excluded.
- **Top features:** `promised_days` (33.59%), `shipping_fee` (22.80%), `warehouse_id` (17.58%), `shipping_state` (16.06%), `is_interstate_lane` (7.20%).
- **Limitation:** delay in this dataset is produced by rules in my generator, so the model has learned those rules. It would need real data to say anything about real delivery risk.

## Dashboard

`src/dashboard.py` (Streamlit + Plotly), four tabs: fulfillment bottlenecks, merchandising economics, financial trajectory and retention, and a checkout delay-risk simulator.

```
pip install -r requirements.txt
streamlit run src/dashboard.py
```

## Illustrative recommendations

`docs/strategic_decision_brief.md` turns the findings into four example recommendations (central-hub inventory placement, express-order dispatch times, warehouse slotting by SKU tier, and priority handling for high-value customers). The brief models a **$14.2M** annual EBITDA effect under the assumptions listed in that file. Because the data is synthetic, treat this as a worked example of scenario modeling, not a forecast.

## Repository structure

```
├── data/
│   ├── bronze/                     # Chunked raw CSVs
│   └── gold/                       # Curated marts (CSV/JSON)
├── docs/
│   ├── audit_findings/             # Six-dimension audit report
│   └── strategic_decision_brief.md
├── notebooks/
│   ├── 00_data_generator.ipynb
│   ├── 01_bronze_ingestion_and_audit.py
│   ├── 02_silver_cleansing_and_conformation.py
│   ├── 03_enterprise_sql_queries.sql
│   └── 04_predictive_delay_modeling.py
├── src/
│   ├── dashboard.py
│   ├── generator.py
│   └── model_inference.py
├── PROJECT_BLUEPRINT.md
├── PROJECT_LOG.md
├── SCHEMA_DOCUMENTATION.md
└── requirements.txt
```
