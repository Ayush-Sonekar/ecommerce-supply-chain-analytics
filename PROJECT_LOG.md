# 📦 Project Work Log & Status: E-Commerce & Supply Chain Logistics Lakehouse

## 📌 Project Overview
* **Domain:** Enterprise E-Commerce & Supply Chain Logistics Lakehouse
* **Target Role:** **Big Data Analyst / Analytics Engineer**
* **Target Scale:** 1,000,000 Orders across 10 Fulfillment Hubs
* **Target Environment:** Pure Databricks Cloud (Databricks Community Edition / Cloud Workspace)
* **Status:** 🟡 **Phase 3 In Progress (Enterprise SQL Scenarios & Business KPIs)**

---

## 🧭 Phase-by-Phase Progress Tracker

| Phase | Milestone | Scope / Deliverables | Status |
| :--- | :--- | :--- | :---: |
| **Phase 1** | **High-Volume Data Generation** | Distance/weight-aware generator producing 1M orders + 1.46M line items chunked across 4 CSVs | 🟢 **Completed** |
| **Phase 2** | **Databricks Medallion Lakehouse** | Bronze ingestion, 6D audit suite, Silver deduplication, typo cleaning, Delivery Cases, and Delta MERGE | 🟢 **Completed** |
| **Phase 3** | **10 Enterprise SQL Scenarios** | Active learning hands-on queries (Window functions, CTEs, OTIF, Fan-Out prevention) | 🟢 **Completed** |
| **Phase 4** | **Applied Predictive Analytics** | Pre-dispatch feature engineering, binary classifier (Spark ML / XGBoost), ROC-AUC, feature importance | 🟢 **Completed** |
| **Phase 5** | **Executive BI & Decision Brief** | Streamlit executive dashboard, executive business recommendations brief, GitHub repository | 🟢 **Completed** |

---

## 📅 Chronological Session Logs

### Session 1: Project Alignment & Real-World Gap Analysis
* Comprehensive review of initial blueprints, rules, and schema against industry benchmarks (Amazon, Walmart, Barclays).
* Evaluated project fit for Big Data Analyst technical interviews.
* Identified 4 key gaps in naive implementations: single-table fact fan-out, missing Gold layer, missing OTIF supply chain metrics, and synthetic randomness in delay labels.

### Session 2: Architectural Alignment via `/grill-me` Mode
* Systematically resolved all implementation dependencies one-by-one.
* Approved specifications: Dual-fact Star Schema (`orders` + `order_items`), Pure Databricks Cloud execution, 1M dataset in 4 chunks, 5-notebook modular pipeline, and Active Learning coaching workflow.

### Session 3: Top 1% Enterprise Upgrades (Big Data Analyst Edition)
* Refined focus to Big Data Analyst / Analytics Engineer track.
* Added Snapshot Shipping Address Grain, Idempotent Incremental Upserts (`MERGE INTO`), and Unit Economics to blueprint and schemas.

### Session 4: Phase 1 Execution & Notebook 1 Bronze Data Audit (Completed)
* **Phase 1 Complete:** Successfully generated 1,010,000 orders and 1,464,019 line items across 4 chunked CSV files into `data/bronze/`.
* **Bronze Ingestion Complete:** Uploaded to Databricks Volume and ingested into Bronze Delta tables (`bronze_orders`, `bronze_order_items`, `bronze_customers`, `bronze_products`, `bronze_warehouses`) with Unity Catalog metadata (`_ingested_at`, `_source_file`).
* **Automated Data Audit Suite Executed:**
  * **Null Profile:** Confirmed 0.00% nulls across dimensions and line items; isolated 8.02% nulls in orders strictly to cancelled orders.
  * **Duplicate Scan:** Quantified exactly 10,000 duplicate orders and 14,499 duplicate line items.
  * **Referential Integrity:** Verified 0 orphan foreign keys across all 4 Star Schema relationships using `broadcast(left_anti)` joins.
  * **Categorical Anomaly Discovery:** Profiled and identified 2,564 segment typos (`Consumr`: 1,524; `Corp`: 1,040).
* **Documentation Landed:** Published comprehensive formal audit report at [`docs/audit_findings/01_bronze_data_audit_report.md`](file:///c:/Users/thega/.gemini/antigravity/scratch/ecommerce-supply-chain-analytics/docs/audit_findings/01_bronze_data_audit_report.md).

### Session 5: Comprehensive 6-Dimensional Profiling & The 4 Delivery Cases
* **Complete Multi-Dimensional Profiling Executed:**
  * **Module A (Orders):** 8.02% nulls isolated strictly to cancellations; 10,000 exact batch replay duplicate keys; 0 chronological paradoxes; 911/911 continuous calendar days; verified additivity & duration quantiles.
  * **Module B (Line Items):** 14,499 duplicates mirroring parent replay; 566 cart split collisions; discovered 4,679 rows with 1-cent Banker's Rounding vs Round-Half-Up friction.
  * **Module C (Dimensions):** Standardized 2,564 segment typos; validated 1,000 catalog SKUs (~48% margin) and 10 regional hubs (4.9M capacity).
  * **Module D (Relational Integrity):** 0 orphan foreign keys; discovered 65,969 guest checkout orders placed prior to customer account registration.
* **Architectural Stress-Testing (The 4 Delivery Cases):**
  1. *Cancelled Order Denominator Trap:* Set `is_delayed = NULL` for cancellations to prevent diluting corporate delay rate by 8%.
  2. *Returned Order SLA Decoupling:* Separated Carrier Logistics SLA (`is_on_time_delivery`) from Commercial Customer SLA (`is_otif`) so carriers aren't penalized for merchandise returns.
  3. *Root-Cause Delay Attribution:* Decomposed delays into Warehouse SLA vs Carrier Transit SLA.
  4. *Snapshot Delivery Grain:* Preserved immutable point-in-time `shipping_city/state` to prevent moving customer address corruption.

### Session 6: Notebook 2 Silver Conformation Complete
* **Conformed Master Dimensions Landed:**
  * `silver_customers`: 50,000 rows. Re-routed 2,564 typos (`Consumr`, `Corp`) into canonical tiers (`Consumer`: 34,100; `Corporate`: 10,988; `Small Business`: 4,912).
  * `silver_products`: 1,000 SKUs materialized with pre-calculated `gross_margin_pct`.
  * `silver_warehouses`: 10 regional distribution hubs persisted with lineage tracking.
* **Conformed Fact Header Landed (`silver_orders`):**
  * Eliminated 10,000 duplicate clones via window deduplication (`ROW_NUMBER()`) $\rightarrow$ exactly **1,000,000 unique orders**.
  * Enforced the **4 Delivery Cases**:
    * 80,196 cancelled orders have `is_delayed = NULL`, `is_on_time_delivery = NULL`, `is_transit_delayed = NULL`, `is_dispatch_delayed = NULL` (100% denominator protection).
    * Decoupled Carrier Delivery SLA (`is_on_time_delivery`) from Commercial Retained SLA (`is_otif`).
    * Decomposed delays into: `None (On-Time)` (575,421), `Carrier Delay` (239,174), `Compound` (64,481), `Warehouse Delay` (40,728).
    * Identified 65,339 guest checkouts and floored tenure at 0.
    * Partitioned by `order_year_month` across 30 monthly partitions.
* **Conformed Fact Line Items Landed (`silver_order_items`):**
  * Eliminated 14,499 duplicates $\rightarrow$ exactly **1,449,520 unique line items**.
  * Preserved raw invoice `line_total` while materializing `gross_revenue` and `discount_amount`.
* **Delta Lake MERGE INTO Demonstrated:**
  * Successfully demonstrated schema-safe idempotent incremental upsert via `DeltaTable.merge()`, updating 3 returns in-place and inserting 2 new orders to reach 1,000,002 rows.

---

## 🎙️ Interview Defense & Talking Points Guide

### 1. Data Modeling: Header vs. Line Items (Join Fan-Out Defense)
* **Interview Question:** *"What was the grain of your fact tables, and how did you prevent metric distortion?"*
* **Defense Talking Point:**
  > *"We separated transactions into an Order Header table (order-level freight fees, delivery milestones) and an Order Line Items table (SKU-level prices, quantities, discounts). If shipping fees were stored at the line-item level, summing freight fees on an order with 3 items would triple reported shipping revenue. By separating grains, we eliminated SQL Join Fan-Out and preserved granular SKU margins."*

### 2. Supply Chain Operations: The 4 Delivery Cases & Delay Attribution
* **Interview Question:** *"How did you handle cancelled orders, returns, and delivery delay metrics in your Lakehouse?"*
* **Defense Talking Point:**
  > *"We stress-tested four distinct delivery scenarios. First, on cancelled orders, we set `is_delayed = NULL` because setting it to 0 would falsely treat cancellations as on-time deliveries, diluting the corporate delay rate by 8%. Second, for returned orders, we decoupled Carrier Delivery SLA (`is_on_time_delivery`) from Commercial Retained SLA (`is_otif`) so shipping carriers wouldn't be penalized for customer size preference returns. Third, we split delivery duration into Warehouse Dispatch SLA vs. Carrier Transit SLA to isolate internal fulfillment backlog from external freight bottlenecks. Finally, we preserved snapshot shipping addresses on the order fact to ensure transit physics remained accurate even if customers later moved."*

### 3. Exploratory Data Auditing: Proving Integrity Without Guessing
* **Interview Question:** *"How did you validate data quality before moving raw data to your curated Lakehouse layer?"*
* **Defense Talking Point:**
  > *"Rather than guessing queries or running destructive blind filters, we built an automated Multi-Table Data Audit Suite across all 5 tables in our Bronze Lakehouse. Using dynamic null profiling, row-to-distinct key reconciliation, and broadcast anti-joins, we confirmed 100% referential integrity, verified that null milestones were strictly confined to cancelled orders, and isolated 10,000 duplicate transactions, 4,679 Banker's rounding instances, and 2,564 categorical typos to drive our Silver transformation contract."*

---

### Session 7: Enterprise SQL Scenarios 1–10 Execution & Verification (Complete)
* **All 10 Enterprise SQL Queries Executed & Mathematically Verified on Databricks:**
  1. *Scenario 1 (OTIF & SLA Trend):* Confirmed ~37.1% delay rate and ~55.3% commercial OTIF rate across 30 monthly partitions with cancelled order denominator protection (`o.order_status != 'Cancelled'`).
  2. *Scenario 2 (Bottleneck Decomposition):* Proved that carrier delays account for >70% of fulfillment failures, with Seattle (59.7%) and LA (56.7%) suffering from coastal perimeter distance, while NY and LA spike in warehouse dispatch failure (~22%).
  3. *Scenario 3 (Category Unit Economics):* Confirmed $585.66M net revenue across 6 categories. Electronics drives 65% of volume ($380.5M), while Beauty & Health (46.87%) and Home & Kitchen (46.67%) lead in profit margins.
  4. *Scenario 4 (Top 3 SKUs with `DENSE_RANK()`):* Isolated 18 top revenue drivers, uncovering `Home & Kitchen Item #1079` as the star performer (62.67% gross margin).
  5. *Scenario 5 (MoM Growth with `LAG()`):* Demonstrated the 'February Trap' where calendar day reduction (28 vs 31 days) creates artificial -10% revenue dips despite steady daily run-rate ($637k/day).
  6. *Scenario 6 (Running Totals & Annual Milestones):* Verified YTD partition resets, tracked milestones ($100M in Jun 2022, $250M in Jan 2023, $500M in Feb 2024), and reconciled exactly 919,804 fulfilled orders and $585.66M lifetime revenue.
  7. *Scenario 7 (Customer Repeat Cohorts):* Segmented 50,000 customers into loyalty tiers; proved that 37,154 VIP Power Buyers (74.3%) drive 81.6% of company revenue ($478.1M) with 20.2 orders/customer.
  8. *Scenario 8 (Join Fan-Out Prevention):* Solved the 1:M join fan-out trap using pre-aggregated CTEs; proved how naive SQL would have inflated freight fees by 45% ($6.3M vs true $4.35M).
  9. *Scenario 9 (Pareto 80/20 SKU Profitability):* Proved that 150 SKUs (15%) generate 50% of total company profit, and 388 SKUs (38.8%) drive 80% of profit, justifying forward-deployment and long-tail centralization.
  10. *Scenario 10 (Geographic Shipping Lanes & SLA Correlation):* Discovered the 'Express Shipping Paradox' where 80.79% of $14.99 express orders on inter-state lanes breach SLAs due to standard warehouse dispatch latency (1.64 days) combined with highway transit (2.95 days).
* **Code Landed:** All 10 queries persisted with documentation to `notebooks/03_enterprise_sql_queries.sql`.

---



### Session 8: Applied Predictive Analytics with Spark MLlib (Complete)
* **Zero Data Leakage Contract Enforced:**
  * Joined `silver_orders`, `silver_order_items`, `silver_products`, and `silver_warehouses`.
  * Pre-aggregated parcel weight and cart complexity to the order grain; excluded 80,196 cancellations.
  * Strictly quarantined all post-checkout events (`ship_date`, `delivery_date`, `dispatch_days`, `transit_days`, `order_status`), training strictly on signals available at checkout (`promised_days`, `shipping_fee`, `warehouse_id`, `shipping_state`, `is_interstate_lane`, `total_order_weight_kg`).
* **Distributed Spark MLlib Pipeline:**
  * Categorical encoding via `StringIndexer`, unified dense vector assembly via `VectorAssembler`.
  * Distributed 50-tree `RandomForestClassifier` (`maxDepth=8`) trained on 735,787 orders (80%).
* **Model Evaluation on 184,017 Unseen Test Orders:**
  * **Classification Accuracy:** 81.07% (vs. 62.56% majority baseline, +18.5% lift).
  * **Delay Precision (Class 1):** 79.15% (when an alert is raised, it is correct ~8 out of 10 times).
  * **Delay Recall (Class 1):** 66.99% (catches 2 out of 3 delays before packing).
  * **Specificity:** 89.47% (on-time shipments pass without false alarm disruptions).
* **Feature Importance Ranking (Gini Impurity):**
  * Top 5 features account for **97.23%** of predictive power:
    1. `promised_days` (33.59%)
    2. `shipping_fee` (22.80%)
    3. `warehouse_id` (17.58%)
    4. `shipping_state` (16.06%)
    5. `is_interstate_lane` (7.20%)
  * Independently verified the "Express Shipping Paradox" discovered in Scenario 10 SQL.
* **Code Landed:** Complete PySpark script persisted to `notebooks/04_predictive_delay_modeling.py`.

---



### Session 9: Phase 5 Executive BI Dashboard & Strategic Decision Brief (Complete)
* **Gold Data Marts Landed (`data/gold/`):**
  * `gold_kpi_summary.json`: Top-level corporate KPI scorecard.
  * `gold_warehouse_bottlenecks.csv`: 10-hub decomposition table.
  * `gold_category_margin.csv`: 6-category unit economics and return rates.
  * `gold_top_skus.csv`: 18 top revenue SKUs with `DENSE_RANK()`.
  * `gold_monthly_financials.csv`: 30-month MoM growth and milestones.
  * `gold_customer_cohorts.csv`: Loyalty tiers and revenue share.
  * `gold_pareto_distribution.csv`: Pareto 80/20 catalog curve.
  * `gold_shipping_lanes.csv`: Intra- vs inter-state shipping lanes.
  * `gold_ml_feature_importance.csv`: Feature weights from Spark MLlib.
* **Interactive Streamlit BI Application Built (`src/dashboard.py`):**
  * Modern, responsive dark-mode executive interface with Plotly charts.
  * 5 KPI Cards: $585.66M Revenue, 919,804 Fulfilled Orders, 37.44% Delay Rate, 55.28% OTIF, 81.07% ML Accuracy.
  * 4 Modular Tabs:
    1. *Fulfillment Bottlenecks & Network Physics:* Hub decomposition and Express Shipping Paradox.
    2. *Merchandising Economics & Pareto:* Unit margins, top SKUs, and 80/20 profit breakdown.
    3. *Financial Trajectory & Customer Retention:* 30-month timeline, milestone flags ($100M, $250M, $500M), VIP cohorts.
    4. *Real-Time Checkout Delay Risk Simulator:* Interactive scoring engine with dynamic gauge and operational prescriptions.
* **Executive Strategic Decision Brief Authored (`docs/strategic_decision_brief.md`):**
  * Formal C-level memorandum outlining the 4 Core Strategic Directives:
    1. Forward-Deploy Tier 1 Inventory to Central Hubs (Kansas City / Chicago).
    2. Eliminate the Express Shipping Paradox via Priority WMS Queues & Dynamic SLA Checkout.
    3. Warehouse Slotting & Long-Tail Centralization (Golden Zone for 150 SKUs, de-stocking 612 SKUs).
    4. VIP Power Buyer Delivery Protection & White-Glove Upgrades.
  * Quantified ROI Model: **$14,200,000 in annualized EBITDA impact**.
  * Complete Big Data Analyst Technical Interview Defense Guide.

---

## 🏁 Project Summary & Graduation Status: 100% COMPLETE
* **Phase 1 (Data Generation):** 🟢 Complete (1.01M orders, 1.46M items across 4 CSVs)
* **Phase 2 (Bronze/Silver Lakehouse):** 🟢 Complete (6D Audit, Conformation, Delivery Cases, Delta MERGE)
* **Phase 3 (Enterprise SQL):** 🟢 Complete (10 Complex Scenarios, Window Functions, Fan-Out Prevention)
* **Phase 4 (Predictive MLlib):** 🟢 Complete (Zero-Leakage Random Forest, 81.1% Accuracy, 79.2% Precision)
* **Phase 5 (Executive BI & Strategic Brief):** 🟢 Complete (Streamlit Dashboard + C-Suite Brief)
