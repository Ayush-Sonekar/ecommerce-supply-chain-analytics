# 📋 Executive Strategic Decision Brief
## Supply Chain Logistics & Fulfillment Optimization
**Target Audience:** Chief Operating Officer (COO), Chief Financial Officer (CFO), VP of Supply Chain  
**Author:** Big Data Analytics Engineer / Lead Big Data Analyst  
**Lakehouse Environment:** Databricks Cloud Lakehouse (Delta Lake / Unity Catalog / Spark MLlib)  
**Dataset Scale:** 1,000,000 Orders | 1,449,520 Line Items | 10 Regional Distribution Centers  

---

## 1. Executive Summary & Problem Framing

Over a 30-month operating period (January 2022 to June 2024), the enterprise generated **\$585,658,057.70 in Net Merchandise Revenue** across **919,804 fulfilled orders** (80,196 cancellations excluded via strict denominator protection).

While top-line gross margin is robust at **44.98% (\$263.46M gross profit)**, corporate fulfillment health is severely impaired:
* **Carrier Delivery Delay Rate:** **37.44%** (344,383 customer deliveries breached promised SLAs).
* **Commercial OTIF (On-Time In-Full) Rate:** **55.28%** (only 508,459 orders arrived completely on time without subsequent returns).
* **Estimated Annual Concession & Churn Loss:** **\$14.2M** in expedited freight re-ships, customer support ticket load, and refund concessions.

Through multi-dimensional SQL root-cause auditing and a distributed Zero-Data-Leakage Spark ML predictive model, we isolated the structural failure points and formulated **Four Strategic Directives** to recover fulfillment reliability and unlock \$14.2M in annualized EBITDA.

---

## 2. Key Analytical & Machine Learning Findings

### A. The Geographic Perimeter Decay vs. Central Hub Efficiency
* **Seattle (59.68% delay rate)** and **Los Angeles (56.74% delay rate)** suffer more than double the failure rates of central hubs like **Kansas City (24.37%)** and **Chicago (26.91%)**.
* **Carrier line-haul transit accounts for >78% of all fulfillment failures** at coastal facilities. Because inventory is concentrated on the coasts, cross-country truck shipments to Midwest/East Coast destinations face 2,000+ km of transit, structurally exceeding ground SLA windows.
* **Warehouse Packing Latency:** Most facilities operate at a predictable ~15.0% dispatch breach rate. However, **Los Angeles (22.09%)** and **New York (21.84%)** experience dock and pick-pack congestion due to mega-metro labor turnover and facility density.

### B. The "Express Shipping Paradox"
* Premium customers paying **\$14.99 for Express 2-Day Shipping** suffer an astonishing **80.79% SLA failure rate on Inter-State lanes** (compared to 19.67% on \$0.00 free shipping).
* **The Root Cause:** Warehouses process all shipments in a FIFO (First-In, First-Out) batch queue taking an average of **1.64 days to dispatch**. Adding **2.95 days of cross-country ground transit** equals **4.59 days**, guaranteeing an SLA breach on a 2-day promise.

### C. Pareto 80/20 SKU Working Capital Disparity
* **Tier 1 (Top 150 SKUs / 15% of Catalog):** Generates **49.88% of corporate gross profit (\$131.4M)** and \$286.6M in sales.
* **Tier 3 (Long-Tail 612 SKUs / 61.2% of Catalog):** Generates only **20.04% of gross profit** while consuming over 60% of regional warehouse bin capacity and working capital.

### D. Extreme Customer Concentration
* **VIP Power Buyers (16+ lifetime orders):** Account for **74.31% of the customer base (37,154 customers)** and generate **81.63% of total revenue (\$478.1M)**, averaging **20.2 orders** and **\$12,868 in spend**.
* Protecting this customer cohort from delivery delays is paramount; repeated SLA breaches risk driving our primary cash flow engine to competitors.

### E. Zero-Leakage Predictive Model Performance
* Built natively on Apache Spark MLlib using 12 pre-dispatch features known at checkout:
  * **Classification Accuracy:** **81.07%** (+18.5% lift over baseline).
  * **Delay Precision:** **79.15%** (8 out of 10 alerts are true breaches).
  * **Top 5 Risk Drivers (97.23% Feature Importance):** `promised_days` (33.6%), `shipping_fee` (22.8%), `warehouse_id` (17.6%), `shipping_state` (16.1%), and `is_interstate_lane` (7.2%).

---

## 3. Four Core Strategic Directives for Leadership

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                    FOUR PILLARS OF OPERATIONAL TRANSFORMATION                │
├──────────────────────────────┬───────────────────────────────────────────────┤
│ 1. Forward-Deploy Tier 1     │ Shift 150 high-velocity SKUs to Kansas City & │
│    Inventory to Central Hubs │ Chicago, reducing line-haul transit by 1.8 days│
├──────────────────────────────┼───────────────────────────────────────────────┤
│ 2. Eliminate Express Paradox │ Implement Priority WMS Pick Queues (<6h) and  │
│    via Dynamic SLA Checkout  │ restrict 2-day ground promises on long lanes  │
├──────────────────────────────┼───────────────────────────────────────────────┤
│ 3. Warehouse Slotting &      │ Consolidate 612 long-tail SKUs into 2 central │
│    Long-Tail Centralization  │ hubs; slot Tier 1 SKUs in the "Golden Zone"   │
├──────────────────────────────┼───────────────────────────────────────────────┤
│ 4. VIP Customer SLA          │ Automatically upgrade VIP high-value shipments│
│    White-Glove Guarantee     │ flagged by the ML model to priority carriers  │
└──────────────────────────────┴───────────────────────────────────────────────┘
```

### Directive 1: Multi-Node Inventory Rebalancing (Forward-Deployment)
* **Action:** Rebalance inventory placement using Kansas City (`Central Plains Hub`) and Chicago (`Midwest Logistics Center`) as national anchor facilities.
* **Mechanism:** Rather than shipping cross-country from Seattle/LA to the East Coast, pre-position high-velocity goods in the Central region. Kansas City can reach 80% of the continental US population within 48 hours via standard ground.
* **Impact:** Reduces average line-haul transit from 2.95 days to 1.80 days for 140,000+ annual orders.

### Directive 2: Eliminate the Express Shipping Paradox
* **Action A (Warehouse WMS Priority Routing):** Reconfigure Warehouse Management Systems (WMS) to split picking queues. Orders with \$14.99 express shipping must bypass batch picking and be dispatched within **4 to 6 hours** (down from 1.64 days).
* **Action B (Dynamic SLA Promising Engine at Checkout):** Integrate our trained ML model into the e-commerce checkout API. If an item is stocked exclusively in Seattle and shipping to New York:
  * Do NOT offer 2-day ground delivery.
  * Dynamically offer **4-day Ground Delivery** or charge an adjusted fee for **Next-Day Air Freight**.

### Directive 3: Pareto Catalog Slotting & Working Capital Rationalization
* **Action:** Re-slot regional warehouses based on the Pareto Tiering:
  * **Tier 1 (150 SKUs):** Forward-deploy in all 10 hubs in the **"Golden Zone"** (shelving within 20 meters of packing stations) to cut pick travel time by 35%.
  * **Tier 3 (612 Long-Tail SKUs):** De-stock from expensive metro warehouses (New York and Los Angeles). Consolidate into Kansas City or shift to direct vendor drop-shipping.
* **Impact:** Frees up **\$18.5M in tied-up working capital** and relieves dock congestion in NY and LA.

### Directive 4: VIP Power Buyer Delivery Protection
* **Action:** Embed real-time ML risk scoring into the VIP order processing pipeline.
* **Mechanism:** When our Spark ML model flags an order with `delay_probability > 0.60` placed by a customer in the VIP Cohort (16+ orders), the system automatically upgrades the parcel to expedited carrier priority at zero cost to the customer.
* **Impact:** Protects the 74% customer base driving 81.6% of enterprise revenue from churn.

---

## 4. Projected Financial & ROI Impact

| Initiative | Operational Lever | Projected Annual Benefit |
| :--- | :--- | :---: |
| **Carrier SLA Breach Reduction** | Reduce delay rate from 37.4% to <18% | **\$6,800,000** (Fewer refunds/re-ships) |
| **Express Order Concession Recovery** | Eliminate 80.8% failure on \$14.99 express | **\$2,400,000** (Saved freight claims) |
| **Warehouse Pick Speed (Golden Zone)** | 35% faster pick times on Tier 1 SKUs | **\$1,850,000** (Labor & overtime savings) |
| **VIP Cohort Churn Prevention** | 1.5% retention increase on 37k VIP buyers | **\$3,150,000** (Preserved customer LTV) |
| **Total Projected Net Financial Impact** | — | **\$14,200,000 / Year** |

---

## 5. Technical Architecture & Lakehouse Governance Appendix

### A. Medallion Lakehouse Flow
1. **Bronze Layer:** Raw multi-part CSV ingestion into Unity Catalog Delta tables with automated metadata tracking (`_ingested_at`, `_source_file`).
2. **Silver Layer:** 
   * Deduplicated 10,000 duplicate orders and 14,499 duplicate items using window `ROW_NUMBER()`.
   * Standardized 2,564 categorical typos in customer segments (`Consumr` $\rightarrow$ `Consumer`, `Corp` $\rightarrow$ `Corporate`).
   * Enforced the **4 Delivery Cases**: 80,196 cancellations assigned `NULL` SLA metrics to eliminate 8% metric dilution; decoupled Carrier Delivery SLA (`is_on_time_delivery`) from Commercial Retained SLA (`is_otif`).
   * Partitioned by `order_year_month` across 30 partitions.
3. **Gold Layer:** Business aggregation marts optimized for high-concurrency BI queries.

### B. Analytical & Machine Learning Interview Q&A Guide

#### Q1: "How did you prevent metric distortion when joining order headers to line items?"
> *"We decoupled transactions into an Order Header fact (`orders`) and Line Item fact (`order_items`). If freight fees were joined directly to line items without pre-aggregation, multi-item orders would fan out, duplicating shipping fees by 45% (over \$6.3M reported instead of the true \$4.35M). We solved this using a pre-aggregation CTE to roll up line items to the order grain first before joining."*

#### Q2: "How did you prove that your predictive model was free of data leakage?"
> *"We established a strict Zero Data Leakage Contract. Every single feature was evaluated against a temporal barrier: 'Was this attribute known when the customer clicked Purchase?' We strictly quarantined all fulfillment milestones—including dispatch date, actual delivery date, transit days, dispatch delay flags, and order status. We trained exclusively on pre-dispatch features (promised window, fee tier, catalog weights, origin hub, destination state, and seasonal month), ensuring 100% real-world inference validity."*

#### Q3: "Why did you decouple Carrier SLA from Commercial OTIF?"
> *"Carrier logistics performance evaluates physical transit: did the truck arrive within the promised delivery window? Commercial OTIF evaluates customer retention: was the order delivered on time and kept by the customer? If a customer returns a shirt due to size preference, penalizing the freight carrier distorts carrier contract scorecards. By decoupling `is_on_time_delivery` from `is_otif`, we preserved objective logistics evaluation while giving commercial leadership true fulfillment visibility."*

---
*Document approved for executive presentation to C-Suite and Board of Directors.*
