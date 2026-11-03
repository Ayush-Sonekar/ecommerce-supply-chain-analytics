# 📦 Enterprise E-Commerce & Supply Chain Logistics Lakehouse
## Project Blueprint: Big Data Analyst Portfolio Edition

---

## 1. Executive Summary & Vision
A production-grade Big Data analytics and applied machine learning platform built on **1,000,000+ e-commerce orders** across 10 fulfillment hubs.

Specifically engineered to prove the **core competencies evaluated in Big Data Analyst / Analytics Engineer technical interviews** (Barclays, Cognizant, Capgemini, Amazon, Walmart):
1. **Big Data Wrangling (PySpark on Databricks):** Ingesting, auditing, and cleaning multi-million row datasets on a cloud lakehouse (Medallion architecture: Bronze $\rightarrow$ Silver $\rightarrow$ Gold).
2. **Advanced SQL Mastery (Spark SQL):** CTEs, Window Functions (`DENSE_RANK()`, `LAG()`, running aggregates), Pareto 80/20 analysis, cohort retention, and **Join Fan-Out-safe** metric aggregation.
3. **Supply Chain Operations & Unit Economics:** Diagnosing fulfillment bottlenecks using **OTIF (On-Time In-Full)**, separating Warehouse Dispatch SLAs from Carrier Transit SLAs, and calculating Net Contribution Margins after return and delay losses.
4. **Applied Predictive Analytics (Spark ML / XGBoost):** Training a zero-leakage binary classifier to identify the top 3 drivers of shipping failure (Feature Importance).
5. **Executive Decision Storytelling (Streamlit BI):** Interactive executive dashboard translating technical query outputs into actionable commercial recommendations.

---

## 2. Top 1% Enterprise Architecture Highlights

* **1. Snapshot Shipping Address Grain:** Decouples point-in-time delivery address (`shipping_city`, `shipping_state`) on the `orders` fact table from the slowly changing `customers` master dimension, preserving historical transit distance integrity.
* **2. Idempotent Incremental Upserts (`MERGE INTO`):** Demonstrating in PySpark how late-arriving order status updates (`Pending` $\rightarrow$ `Shipped` $\rightarrow$ `Completed`) are upserted into Delta tables without duplicating rows.
* **3. Fan-Out-Free Star Schema:** Order Header (`orders`) + Order Line Items (`order_items`) ensures order-level shipping fees are never multiplied across multi-item purchases.
* **4. Root-Cause Bottleneck Decomposition:** Splitting fulfillment into **Warehouse Packing SLA (2 days)** vs. **Carrier Transit SLA** to isolate operational hub congestion from transit distance factors.
* **5. Commercial Unit Economics:** Evaluating SKU profitability after factoring in freight revenue, item discounts, return rates, and delay concessions.

---

## 3. Phase-by-Phase Implementation Roadmap

### Phase 1: High-Volume Data Generation (`src/generator.py` / `notebooks/00_data_generator.ipynb`)
* Vectorized NumPy/Pandas generator creating realistic supply chain dynamics:
  * Customer-to-warehouse geographic distances calculate realistic transit durations (Haversine formula).
  * Heavy items and peak Q4 holiday months drive natural delay probabilities.
  * Fulfillment decomposes into **Warehouse Packing/Dispatch SLA** vs **Carrier Transit SLA**.
  * Injected data quality quirks: customer segment typos (`Consumr`, `Corp`), missing delivery dates on cancellations, and 1% duplicate keys for deduplication tests.
  * Outputs: Dimension CSVs (`customers`, `products`, `warehouses`) + Fact CSVs chunked into 4 files each (`orders_part_0..3.csv`, `order_items_part_0..3.csv`).

### Phase 2: Databricks Cloud Lakehouse (Medallion Architecture)
* **Bronze Layer (`notebooks/01_bronze_ingestion_and_audit.py`):**
  * Ingest raw chunked CSVs from Databricks Volumes (`ecommerce_raw`) with metadata columns (`_ingested_at`, `_source_file`).
  * Run programmatic data audit: null rate scan across all columns, boundary checks (`unit_price > 0`), row count reconciliation.
* **Silver Layer (`notebooks/02_silver_cleansing_and_conformation.py`):**
  * Deduplication via PySpark window functions (`ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY order_date)`).
  * Categorical standardization (`Consumr` $\rightarrow$ `Consumer`, `Corp` $\rightarrow$ `Corporate`).
  * SLA metric derivation: `dispatch_days`, `transit_days`, `is_dispatch_delayed`, `is_transit_delayed`, `is_delayed`, and `is_otif`.
  * Demonstrating idempotent `MERGE INTO` (upsert) logic for incremental status updates.
* **Gold Layer (`data/gold/` Business Marts & Streamlit BI App):**
  * Aggregated dimensional business marts: `gold_warehouse_bottlenecks`, `gold_shipping_lanes`, `gold_category_margin`, `gold_kpis_cube`.
  * Lakehouse performance tuning & multi-dimensional executive serving.

### Phase 3: The 10 Enterprise SQL Business Queries (`notebooks/03_enterprise_sql_queries.sql`)
Active learning hands-on queries solved directly by the user:
1. **OTIF & Delivery SLA Compliance:** Monthly On-Time In-Full fulfillment rates and SLA breach trends.
2. **Warehouse vs Carrier Bottleneck Analysis:** Decomposing fulfillment delays into warehouse packing backlog vs highway transit delays.
3. **Category Revenue Contribution & Filtering:** Net margin analysis with `GROUP BY` and `HAVING`.
4. **Top 3 Revenue SKUs per Category:** `DENSE_RANK() OVER (PARTITION BY category ORDER BY revenue DESC)`.
5. **Month-over-Month Growth with Churn Filtering:** `LAG() OVER (ORDER BY order_month)` window function.
6. **Cumulative Running Revenue over Time:** Multi-partition `SUM() OVER ()`.
7. **High-Value Customer Segmentation & Repeat Order Rates:** Multi-stage CTEs.
8. **Join Fan-Out Prevention:** Safely aggregating line-item SKU revenue with order-level freight fees.
9. **Pareto 80/20 SKU Profitability:** Cumulative contribution of top catalog items.
10. **Geographical Distance vs Delay Correlation:** Delivery performance grouped by distance zones.

### Phase 4: Applied Predictive Analytics (`notebooks/04_predictive_delay_modeling.py`)
* Pre-dispatch binary classifier predicting `is_delayed` (0 or 1) at order placement.
* **Zero Data Leakage:** Exclude post-fulfillment features (`ship_date`, `delivery_date`, `actual_delivery_days`, `dispatch_days`). Rely only on pre-dispatch signals (`distance_km`, `order_weight_kg`, `order_month`, `warehouse_id`, `shipping_fee`, `promised_days`).
* Train using Spark MLlib (`GBTClassifier` / `RandomForestClassifier`) to prevent driver OOM on Databricks Community Edition.
* Evaluate with Confusion Matrix, Precision, Recall, and ROC-AUC ($> 0.75$).
* Extract Feature Importance to determine top logistical failure drivers.

### Phase 5: Executive BI Dashboard & Strategic Decision Brief
* Multi-tab Streamlit dashboard reading Gold marts.
* **Executive Insights Brief:** Translating technical findings into strategic recommendations (e.g., inventory reallocation, carrier contract renegotiation).
