# 📦 Enterprise E-Commerce & Supply Chain Logistics Lakehouse
### Production-Grade Big Data Analytics, Dimensional Modeling & Applied Machine Learning
**Target Persona:** Big Data Analyst / Analytics Engineer / Applied BI Developer  
**Cloud Ecosystem:** Databricks (Delta Lake / Unity Catalog / PySpark / Spark MLlib / Spark SQL)  
**Scale:** 1,000,000 Orders | 1,449,520 Order Items | 10 Regional Distribution Centers | $585.66M Net Revenue  

---

## 🌟 Executive Summary & Impact
Built on **1,000,000+ real-world simulated e-commerce transactions**, this project models an end-to-end cloud analytics lakehouse solving complex supply chain, merchandising, and financial operations.

Through rigorous exploratory data auditing, dimensional conformation, 10 advanced Spark SQL business scenarios, and a distributed zero-data-leakage predictive classifier, this project identifies critical fulfillment bottlenecks and formulates **Four Strategic Directives projected to unlock \$14,200,000 in annualized EBITDA**.

```
┌────────────────────────────────────────────────────────────────────────────────────────┐
│                              MEDALLION LAKEHOUSE ARCHITECTURE                          │
├─────────────────────────┬──────────────────────────────┬───────────────────────────────┤
│      BRONZE LAYER       │         SILVER LAYER         │          GOLD LAYER           │
│   (Raw Ingestion &      │     (Cleaned, Conformed      │     (Aggregated Business      │
│     6D Data Audit)      │     & Deduplicated Delta)    │       Marts & BI Apps)        │
├─────────────────────────┼──────────────────────────────┼───────────────────────────────┤
│ • 4 Chunked CSV Volumes │ • 10k Replay Clones Removed  │ • 10-Hub Bottleneck Mart      │
│ • Unity Catalog Schema  │ • 2,564 Typo Reroutes        │ • Category Margin & COGS Mart │
│ • 6D Profiling Engine   │ • 4 Delivery Cases Enforced  │ • Top 3 SKUs (DENSE_RANK)     │
│ • Zero Orphan FK Checks │ • Delta MERGE INTO Upserts   │ • 30-Month MoM Growth Mart    │
│ • Automated Audit Suite │ • Partitioned by YYYY-MM     │ • Streamlit Executive Tower   │
└─────────────────────────┴──────────────────────────────┴───────────────────────────────┘
```

---

## 🏆 Core Business Scorecard

| Metric | Result | Analytical Significance |
| :--- | :---: | :--- |
| **Lifetime Net Revenue** | **\$585,658,057.70** | Across 30 continuous operating months (2022-01 to 2024-06) |
| **Fulfilled Order Volume** | **919,804** | Exactly reconciles with 1M total orders minus 80,196 cancellations |
| **Delivery Delay Rate** | **37.44%** | Protected by the Cancelled Order Denominator Rule (prevents 8% dilution) |
| **Commercial OTIF Rate** | **55.28%** | Decouples Carrier Delivery SLA from Commercial Retained Fulfillment |
| **Spark ML Model Accuracy** | **81.07%** | +18.5% lift over baseline with **Zero Data Leakage** at checkout |
| **Delay Prediction Precision**| **79.15%** | Flags high-risk orders at cart placement with ~80% accuracy |
| **Projected Annualized ROI** | **\$14,200,000** | EBITDA unlock from carrier contract, WMS pick speed, and inventory placement |

---

## 🧠 The 4 Delivery Cases (Data Modeling & Governance)

In modern logistics, naive metrics cause catastrophic decision errors. This lakehouse models four edge cases:

1. **Case 1: The Cancelled Order Denominator Trap**
   * *Problem:* When orders are cancelled prior to dispatch, setting `is_delayed = 0` falsely treats cancellations as on-time deliveries, diluting the corporate delay rate by 8.02%.
   * *Solution:* Explicitly set `is_delayed = NULL`, `is_transit_delayed = NULL`, and `is_dispatch_delayed = NULL` on cancelled orders.
2. **Case 2: Decoupling Carrier Logistics SLA from Commercial OTIF**
   * *Problem:* When a customer returns merchandise due to fit/color preference, logistics carriers should not be penalized.
   * *Solution:* Decoupled physical `is_on_time_delivery` (carrier transit compliance) from commercial `is_otif` (on-time and kept by customer).
3. **Case 3: Causal Delay Attribution**
   * *Problem:* Merging all delays into one bucket hides whether operational failures are internal or external.
   * *Solution:* Decomposed duration into **Warehouse Packing SLA (2 days)** vs. **Carrier Transit SLA** to isolate dock bottlenecks from highway transit delays.
4. **Case 4: Snapshot Shipping Address Grain**
   * *Problem:* Customers move over time. Linking historical deliveries to a customer's current address invalidates transit distance physics.
   * *Solution:* Preserved point-in-time `shipping_city` and `shipping_state` on the order fact, independent of changing customer dimension records.

---

## 📊 The 10 Enterprise SQL Scenarios

All queries are executed natively on Databricks Delta Lake and persisted in [`notebooks/03_enterprise_sql_queries.sql`](file:///notebooks/03_enterprise_sql_queries.sql):

| # | Scenario Title | Core SQL Technique | Key Business Finding |
| :-: | :--- | :--- | :--- |
| **1** | **Monthly OTIF & SLA Compliance** | Denominator Protection Filter | 37.1% carrier delay rate decoupled from 55.3% commercial OTIF |
| **2** | **Warehouse vs Carrier Bottlenecks** | Multi-condition Aggregation | Carrier delays account for >78% of failures at coastal hubs (Seattle/LA) |
| **3** | **Category Margin & Unit Economics** | `GROUP BY` & `HAVING` | Electronics drives \$380.5M GMV; Beauty (46.9%) leads profit margins |
| **4** | **Top 3 SKUs per Category** | `DENSE_RANK() OVER (PARTITION)` | Isolated 18 star products; Home Item #1079 delivers 62.7% margin |
| **5** | **MoM Financial & Customer Growth** | `LAG() OVER (ORDER BY month)` | Disproved "February churn" trap via steady daily run-rate (\$637k/day) |
| **6** | **Running Totals & Milestones** | `SUM() OVER (ROWS UNBOUNDED)` | Verified annual YTD resets; \$100M, \$250M, \$500M milestone crossings |
| **7** | **Repeat Customer Loyalty Cohorts** | Multi-stage CTE Segmentation | 37,154 VIP Power Buyers (74.3%) drive 81.6% of company revenue (\$478.1M) |
| **8** | **Join Fan-Out Prevention** | Safe Pre-aggregation CTE | Prevented 45% artificial freight fee inflation (\$6.3M naive vs \$4.35M true) |
| **9** | **Pareto 80/20 SKU Profitability** | Cumulative Running Profit % | 150 SKUs (15%) generate 50% of profit; 612 SKUs form the long-tail |
| **10** | **Shipping Lanes & SLA Correlation** | Origin-to-Destination Cross-Tab | Exposed "Express Paradox": 80.8% failure on \$14.99 cross-country orders |

---

## 🔮 Applied Machine Learning (Zero Data Leakage)
📁 Notebook: [`notebooks/04_predictive_delay_modeling.py`](file:///notebooks/04_predictive_delay_modeling.py)

To prevent delivery failures before boxes are packed, we trained a distributed **Random Forest Classifier (50 trees, maxDepth 8)** using Apache Spark MLlib on **735,787 training orders** and evaluated it on **184,017 unseen test orders**:

### Zero Data Leakage Rule:
Strictly excluded all post-checkout events (`ship_date`, `delivery_date`, `dispatch_days`, `transit_days`, `order_status`). Trained strictly on pre-dispatch attributes known at checkout.

### Model Evaluation Scorecard:
* **Accuracy:** **81.07%** (vs. 62.56% naive majority baseline, +18.5% lift)
* **Delay Precision (Class 1):** **79.15%** (8 out of 10 alerts are genuine risks)
* **Delay Recall (Class 1):** **66.99%** (catches 2 out of 3 delays before packing)
* **Specificity (Class 0):** **89.47%** (smooth on-time flow with minimal false alarms)

### Feature Importance (Top Failure Drivers):
1. `promised_days` (**33.59%**): SLA window tightness
2. `shipping_fee` (**22.80%**): Paid speed tier
3. `warehouse_id` (**17.58%**): Origin facility congestion
4. `shipping_state` (**16.06%**): Destination geography
5. `is_interstate_lane` (**7.20%**): Cross-country transit penalty
*(Top 5 features account for **97.23%** of predictive power)*

---

## 🖥️ Executive BI Dashboard & Control Tower
📁 Code: [`src/dashboard.py`](file:///src/dashboard.py)  
Built with **Streamlit** and **Plotly**, the dashboard translates complex queries into an executive decision portal across 4 persona-driven tabs:

1. **Tab 1: Fulfillment Bottlenecks & Network Physics** (Warehouse vs. Carrier breakdown + The Express Shipping Paradox).
2. **Tab 2: Merchandising Economics & Pareto 80/20** (Category unit economics + Top 3 SKUs per category explorer).
3. **Tab 3: Financial Trajectory & Customer Retention** (30-month timeline, milestone crossings, VIP Power Buyer share).
4. **Tab 4: Real-Time Checkout Delay Risk Simulator** (Live checkout scoring engine with dynamic gauge and operational prescriptions).

### Running the Dashboard Locally:
```powershell
pip install -r requirements.txt
streamlit run src/dashboard.py
```

---

## 📋 Strategic Decision Brief (\$14.2M ROI Recommendation)
📁 Full Report: [`docs/strategic_decision_brief.md`](file:///docs/strategic_decision_brief.md)

Based on data discoveries, four enterprise directives were presented to C-Suite leadership:
1. **Forward-Deploy Inventory to Central Hubs:** Pre-position high-velocity goods in Kansas City and Chicago to cut line-haul transit by 1.8 days.
2. **Eliminate the Express Shipping Paradox:** Reconfigure WMS queues to dispatch express orders within 4–6 hours (down from 1.64 days) and apply dynamic checkout SLA promising.
3. **Pareto Warehouse Slotting:** Slot Tier 1 SKUs in the "Golden Zone" (35% faster pick times); de-stock 612 long-tail SKUs from expensive metro facilities.
4. **VIP Loyalty White-Glove Guarantee:** Use the ML model to flag high-risk VIP parcels for automatic priority carrier upgrades.

---

## 📁 Repository Structure
```
ecommerce-supply-chain-analytics/
├── data/
│   ├── bronze/                   # Chunked raw CSVs (1M+ orders, line items, dimensions)
│   └── gold/                     # High-performance curated business marts (CSV/JSON)
├── docs/
│   ├── audit_findings/           # Automated 6-dimensional Bronze Data Audit Report
│   └── strategic_decision_brief.md # Executive C-Suite Decision Brief ($14.2M ROI)
├── notebooks/
│   ├── 00_data_generator.ipynb   # Physics-based high-volume data generator
│   ├── 01_bronze_ingestion_and_audit.py # Bronze ingestion & 6D profiling
│   ├── 02_silver_cleansing_and_conformation.py # Silver conformation, deduplication & Delta MERGE
│   ├── 03_enterprise_sql_queries.sql    # The 10 Enterprise SQL Scenarios
│   └── 04_predictive_delay_modeling.py  # Spark MLlib Zero-Leakage Pipeline
├── src/
│   ├── dashboard.py              # Streamlit Executive Control Tower Application
│   ├── generator.py              # Vectorized NumPy/Pandas synthetic data generator
│   └── model_inference.py        # Real-time delay risk inference engine (Spark ML calibrated)
├── PROJECT_BLUEPRINT.md          # Architectural specification & requirements
├── PROJECT_LOG.md                # Chronological milestone log across all 5 phases
├── SCHEMA_DOCUMENTATION.md       # Star Schema ERD & relational data dictionary
└── requirements.txt              # Production dashboard dependencies
```

---

## 🎙️ Technical Interview Defense Guide

### Q1: "How did you prevent metric distortion when aggregating multi-item orders with freight fees?"
> *"Transactions occur across two grains: Order Header (`orders`) and Line Items (`order_items`). If freight fees were joined directly to line items without pre-aggregation, multi-item orders would fan out, duplicating shipping fees by 45% (over \$6.3M reported instead of the true \$4.35M). We solved this using a pre-aggregation CTE to roll up line items to the order grain first before joining."*

### Q2: "Why did you decouple Carrier Logistics SLAs from Commercial OTIF?"
> *"Carrier logistics evaluates transit execution: did the truck arrive within the promised delivery window? Commercial OTIF evaluates customer retention: was the order delivered on time and kept by the customer? If a customer returns a shirt due to size preference, penalizing the freight carrier distorts carrier contract scorecards. By decoupling `is_on_time_delivery` from `is_otif`, we preserved objective logistics evaluation while giving commercial leadership true fulfillment visibility."*

### Q3: "How did you guarantee that your predictive model was free of data leakage?"
> *"We enforced a strict Zero Data Leakage Contract. Every single feature was evaluated against a temporal barrier: 'Was this attribute known when the customer clicked Purchase?' We strictly quarantined all fulfillment milestones—including dispatch date, actual delivery date, transit days, dispatch delay flags, and order status. We trained exclusively on pre-dispatch features (promised window, fee tier, catalog weights, origin hub, destination state, and seasonal month), ensuring 100% real-world inference validity."*
