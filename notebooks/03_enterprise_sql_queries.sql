-- Databricks notebook source
-- ==============================================================================
-- Enterprise E-Commerce & Supply Chain Logistics Lakehouse
-- Notebook 3: Enterprise SQL Queries (Big Data Analyst / Analytics Engineer Track)
-- Target Catalog Tables: silver_orders, silver_order_items, silver_products, silver_customers, silver_warehouses
-- ==============================================================================

-- COMMAND ----------

-- ==============================================================================
-- Scenario 1: Monthly OTIF & Delivery SLA Compliance Trend
-- ==============================================================================
-- Business Objective:
-- Track corporate-wide delivery performance month-over-month. Decouple commercial 
-- On-Time In-Full (OTIF) fulfillment from pure logistics carrier transit delivery.
-- Enforce the Cancelled Order Denominator Protection rule (excluding cancellations 
-- from delay metrics so corporate delay rates are not falsely diluted by 8%).
-- ==============================================================================

%sql
SELECT 
    order_year_month,
    COUNT(order_id) AS total_orders,
    COUNT(order_id) FILTER (WHERE order_status != 'Cancelled') AS fulfilled_orders,
    COUNT(order_id) FILTER (WHERE is_delayed = 1) AS delayed_orders,
    ROUND(
        COUNT(order_id) FILTER (WHERE is_delayed = 1) * 100.0 / 
        COUNT(order_id) FILTER (WHERE order_status != 'Cancelled'), 
        2
    ) AS carrier_delay_rate_pct,
    COUNT(order_id) FILTER (WHERE is_otif = 1) AS otif_orders,
    ROUND(
        COUNT(order_id) FILTER (WHERE is_otif = 1) * 100.0 / 
        COUNT(order_id) FILTER (WHERE order_status != 'Cancelled'), 
        2
    ) AS otif_rate_pct
FROM silver_orders
GROUP BY order_year_month
ORDER BY order_year_month ASC;

-- COMMAND ----------

-- ==============================================================================
-- Scenario 2: Warehouse vs. Carrier Bottleneck Decomposition
-- ==============================================================================
-- Business Objective:
-- Decompose supply chain delivery failures across all 10 regional distribution hubs
-- into internal warehouse dispatch backlogs vs. external carrier transit delays.
-- Proves whether operational failures are facility-driven or transit/line-haul driven.
-- ==============================================================================

%sql
SELECT 
    w.warehouse_id,
    w.warehouse_name,
    w.city,
    w.state,
    COUNT(o.order_id) FILTER (WHERE o.order_status != 'Cancelled') AS fulfilled_orders,
    COUNT(o.order_id) FILTER (WHERE o.is_delayed = 1) AS total_delayed_orders,
    COUNT(o.order_id) FILTER (WHERE o.is_dispatch_delayed = 1 AND o.is_transit_delayed = 0) AS warehouse_delays,
    COUNT(o.order_id) FILTER (WHERE o.is_transit_delayed = 1 AND o.is_dispatch_delayed = 0) AS carrier_delays,
    COUNT(o.order_id) FILTER (WHERE o.is_dispatch_delayed = 1 AND o.is_transit_delayed = 1) AS compound_delays,
    ROUND(
        COUNT(o.order_id) FILTER (WHERE o.is_dispatch_delayed = 1) * 100.0 / 
        COUNT(o.order_id) FILTER (WHERE o.order_status != 'Cancelled'), 
        2
    ) AS warehouse_sla_breach_pct,
    ROUND(
        COUNT(o.order_id) FILTER (WHERE o.is_transit_delayed = 1) * 100.0 / 
        COUNT(o.order_id) FILTER (WHERE o.order_status != 'Cancelled'), 
        2
    ) AS carrier_sla_breach_pct,
    ROUND(
        COUNT(o.order_id) FILTER (WHERE o.is_delayed = 1) * 100.0 / 
        COUNT(o.order_id) FILTER (WHERE o.order_status != 'Cancelled'), 
        2
    ) AS total_delivery_delay_pct
FROM silver_orders o
JOIN silver_warehouses w ON o.warehouse_id = w.warehouse_id
GROUP BY 
    w.warehouse_id,
    w.warehouse_name,
    w.city,
    w.state
ORDER BY total_delayed_orders DESC;

-- COMMAND ----------

-- ==============================================================================
-- Scenario 3: Category Revenue Contribution & Margin Filtering (GROUP BY & HAVING)
-- ==============================================================================
-- Business Objective:
-- Measure unit economics, gross margins, promotional discount impact, and customer
-- return rates across product categories. Filter out non-strategic or low-volume tiers
-- using the HAVING clause post-aggregation.
-- ==============================================================================

%sql
SELECT 
    p.category,
    COUNT(DISTINCT oi.order_id) AS total_orders,
    SUM(oi.quantity) AS total_items_sold,
    ROUND(SUM(oi.quantity * oi.unit_price), 2) AS gross_revenue,
    ROUND(SUM(oi.quantity * oi.unit_price * oi.discount_pct), 2) AS total_discount_given,
    ROUND(SUM(oi.line_total), 2) AS net_revenue,
    ROUND(SUM(oi.quantity * p.unit_cost), 2) AS total_COGS,
    ROUND(SUM(oi.line_total) - SUM(oi.quantity * p.unit_cost), 2) AS net_profit,
    ROUND(
        (SUM(oi.line_total) - SUM(oi.quantity * p.unit_cost)) / SUM(oi.line_total) * 100, 
        2
    ) AS profit_margin_pct,
    ROUND(
        COUNT(CASE WHEN o.order_status = 'Returned' THEN 1 END) * 100.0 / COUNT(*), 
        2
    ) AS return_rate_pct
FROM silver_order_items oi
JOIN silver_products p ON oi.product_id = p.product_id
JOIN silver_orders o ON oi.order_id = o.order_id
WHERE o.order_status != 'Cancelled'
GROUP BY p.category
HAVING net_revenue > 5000000
ORDER BY net_profit DESC;

-- COMMAND ----------

-- ==============================================================================
-- Scenario 4: Top 3 Revenue SKUs per Category using DENSE_RANK()
-- ==============================================================================
-- Business Objective:
-- Identify the top 3 revenue-producing products in each category for warehouse slotting
-- and strategic vendor negotiations. Uses DENSE_RANK inside a CTE to guarantee exactly 
-- top 3 ranks per partition even when ties occur.
-- ==============================================================================

%sql
WITH sku_sales_ranked AS (
    SELECT 
        p.category,
        p.product_id,
        p.product_name,
        SUM(oi.quantity) AS total_units_sold,
        ROUND(SUM(oi.line_total), 2) AS total_net_revenue,
        ROUND(
            (SUM(oi.line_total) - SUM(oi.quantity * p.unit_cost)) / SUM(oi.line_total) * 100, 
            2
        ) AS profit_margin_pct,
        DENSE_RANK() OVER (
            PARTITION BY p.category 
            ORDER BY SUM(oi.line_total) DESC
        ) AS sku_rank
    FROM silver_order_items oi
    JOIN silver_products p ON oi.product_id = p.product_id
    JOIN silver_orders o ON oi.order_id = o.order_id
    WHERE o.order_status != 'Cancelled'
    GROUP BY 
        p.category,
        p.product_id,
        p.product_name,
        p.unit_cost
)
SELECT 
    category,
    sku_rank,
    product_id,
    product_name,
    total_units_sold,
    total_net_revenue,
    profit_margin_pct
FROM sku_sales_ranked
WHERE sku_rank <= 3
ORDER BY 
    category ASC, 
    sku_rank ASC;

-- COMMAND ----------

-- ==============================================================================
-- Scenario 5: Month-over-Month Financial & Customer Growth using LAG()
-- ==============================================================================
-- Business Objective:
-- Track corporate trajectory and customer stickiness across 30 continuous months.
-- Compares revenue and active customer counts to the immediate prior month, exposing
-- calendar day fluctuations (e.g. February dip) vs. actual sales velocity changes.
-- ==============================================================================

%sql
WITH monthly_aggregates AS (
    SELECT 
        o.order_year_month,
        ROUND(SUM(oi.line_total), 2) AS monthly_net_revenue,
        COUNT(DISTINCT o.customer_id) AS monthly_active_customers,
        COUNT(DISTINCT o.order_id) AS monthly_orders
    FROM silver_orders o
    JOIN silver_order_items oi ON o.order_id = oi.order_id
    WHERE o.order_status != 'Cancelled'
    GROUP BY o.order_year_month
),
monthly_lagged AS (
    SELECT 
        order_year_month,
        monthly_net_revenue,
        LAG(monthly_net_revenue, 1) OVER (ORDER BY order_year_month ASC) AS prior_month_revenue,
        monthly_active_customers,
        LAG(monthly_active_customers, 1) OVER (ORDER BY order_year_month ASC) AS prior_month_customers,
        monthly_orders,
        LAG(monthly_orders, 1) OVER (ORDER BY order_year_month ASC) AS prior_month_orders
    FROM monthly_aggregates
)
SELECT 
    order_year_month,
    monthly_net_revenue,
    prior_month_revenue,
    ROUND(monthly_net_revenue - prior_month_revenue, 2) AS mom_revenue_growth_dollars,
    ROUND((monthly_net_revenue - prior_month_revenue) / prior_month_revenue * 100, 2) AS mom_revenue_growth_pct,
    monthly_active_customers,
    prior_month_customers,
    ROUND((monthly_active_customers - prior_month_customers) / prior_month_customers * 100, 2) AS mom_customer_growth_pct,
    monthly_orders
FROM monthly_lagged
ORDER BY order_year_month ASC;

-- COMMAND ----------

-- ==============================================================================
-- Scenario 6: Cumulative Running Totals & Annual Milestones (SUM() OVER ())
-- ==============================================================================
-- Business Objective:
-- Compute Year-to-Date (YTD) running revenue resetting every January, all-time
-- lifetime cumulative revenue, running cumulative orders, and dynamic Running AOV.
-- ==============================================================================

%sql
WITH monthly_totals AS (
    SELECT 
        o.order_year_month,
        SUBSTRING(o.order_year_month, 1, 4) AS order_year,
        ROUND(SUM(oi.line_total), 2) AS monthly_net_revenue,
        COUNT(DISTINCT o.order_id) AS monthly_orders
    FROM silver_orders o
    JOIN silver_order_items oi ON o.order_id = oi.order_id
    WHERE o.order_status != 'Cancelled'
    GROUP BY o.order_year_month, SUBSTRING(o.order_year_month, 1, 4)
)
SELECT 
    order_year_month,
    order_year,
    monthly_net_revenue,
    -- YTD Running Revenue: Resets partition on January 1st of each calendar year
    ROUND(
        SUM(monthly_net_revenue) OVER (
            PARTITION BY order_year 
            ORDER BY order_year_month ASC 
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ), 
        2
    ) AS ytd_running_revenue,
    -- Lifetime Cumulative Revenue across entire 30-month corporate history
    ROUND(
        SUM(monthly_net_revenue) OVER (
            ORDER BY order_year_month ASC 
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ), 
        2
    ) AS lifetime_cumulative_revenue,
    -- Lifetime Cumulative Orders
    SUM(monthly_orders) OVER (
        ORDER BY order_year_month ASC 
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS lifetime_cumulative_orders,
    -- Dynamic Running Average Order Value (AOV)
    ROUND(
        SUM(monthly_net_revenue) OVER (
            ORDER BY order_year_month ASC 
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) / 
        SUM(monthly_orders) OVER (
            ORDER BY order_year_month ASC 
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ), 
        2
    ) AS running_aov
FROM monthly_totals
ORDER BY order_year_month ASC;

-- COMMAND ----------

-- ==============================================================================
-- Scenario 7: High-Value Customer Repeat Order Cohorts (Multi-stage CTEs)
-- ==============================================================================
-- Business Objective:
-- Segment the 50,000 customer base into behavioral tiers based on purchase frequency
-- (One-Time, Repeat, Frequent, VIP). Quantifies customer retention and revenue share.
-- ==============================================================================

%sql
WITH customer_order_summary AS (
    SELECT 
        o.customer_id,
        c.customer_segment,
        COUNT(DISTINCT o.order_id) AS lifetime_orders,
        ROUND(SUM(oi.line_total), 2) AS lifetime_spend,
        MIN(o.order_date) AS first_order_date,
        MAX(o.order_date) AS latest_order_date
    FROM silver_orders o
    JOIN silver_order_items oi ON o.order_id = oi.order_id
    JOIN silver_customers c ON o.customer_id = c.customer_id
    WHERE o.order_status != 'Cancelled'
    GROUP BY o.customer_id, c.customer_segment
),
customer_tiered AS (
    SELECT 
        customer_id,
        customer_segment,
        lifetime_orders,
        lifetime_spend,
        CASE 
            WHEN lifetime_orders = 1 THEN '1. One-Time Buyer (1 order)'
            WHEN lifetime_orders BETWEEN 2 AND 5 THEN '2. Repeat Buyer (2-5 orders)'
            WHEN lifetime_orders BETWEEN 6 AND 15 THEN '3. Frequent Buyer (6-15 orders)'
            ELSE '4. VIP Power Buyer (16+ orders)'
        END AS loyalty_cohort
    FROM customer_order_summary
)
SELECT 
    loyalty_cohort,
    COUNT(customer_id) AS total_customers,
    ROUND(COUNT(customer_id) * 100.0 / SUM(COUNT(customer_id)) OVER (), 2) AS customer_share_pct,
    ROUND(SUM(lifetime_spend), 2) AS cohort_total_revenue,
    ROUND(SUM(lifetime_spend) * 100.0 / SUM(SUM(lifetime_spend)) OVER (), 2) AS revenue_share_pct,
    ROUND(AVG(lifetime_spend), 2) AS avg_spend_per_customer,
    ROUND(AVG(lifetime_orders), 1) AS avg_orders_per_customer
FROM customer_tiered
GROUP BY loyalty_cohort
ORDER BY loyalty_cohort ASC;

-- COMMAND ----------

-- ==============================================================================
-- Scenario 8: Join Fan-Out Prevention (Header vs. Line-Item Metric Integrity)
-- ==============================================================================
-- Business Objective:
-- Prove technical mastery over SQL Join Fan-Out. When aggregating line items (SKU sales)
-- alongside order header metrics (shipping fees), a naive 1:M join duplicates shipping
-- fees for multi-item orders. This query demonstrates the safe pre-aggregation CTE pattern.
-- ==============================================================================

%sql
WITH order_level_merchandise AS (
    -- Pre-aggregate line items to the ORDER grain (1 row per order_id)
    SELECT 
        order_id,
        ROUND(SUM(line_total), 2) AS order_merchandise_subtotal,
        SUM(quantity) AS order_total_quantity,
        COUNT(order_item_id) AS order_item_count
    FROM silver_order_items
    GROUP BY order_id
)
SELECT 
    o.order_year_month,
    COUNT(DISTINCT o.order_id) AS total_fulfilled_orders,
    
    -- Merchandise Revenue aggregated cleanly from pre-aggregated line items:
    ROUND(SUM(olm.order_merchandise_subtotal), 2) AS net_merchandise_revenue,
    
    -- Shipping Fee Revenue summed safely from Order Header without fan-out duplication:
    ROUND(SUM(o.shipping_fee), 2) AS true_shipping_fee_collected,
    
    -- Total Invoiced Cash collected (Merchandise + Freight):
    ROUND(SUM(olm.order_merchandise_subtotal) + SUM(o.shipping_fee), 2) AS total_cash_collected,
    
    -- Freight fee contribution % to overall customer billing:
    ROUND(
        SUM(o.shipping_fee) * 100.0 / 
        (SUM(olm.order_merchandise_subtotal) + SUM(o.shipping_fee)), 
        2
    ) AS freight_revenue_share_pct
FROM silver_orders o
JOIN order_level_merchandise olm ON o.order_id = olm.order_id
WHERE o.order_status != 'Cancelled'
GROUP BY o.order_year_month
ORDER BY o.order_year_month ASC;

-- COMMAND ----------

-- ==============================================================================
-- Scenario 9: Pareto 80/20 SKU Profitability Analysis
-- ==============================================================================
-- Business Objective:
-- Test the Pareto Principle (80/20 Rule) across the catalog of 1,000 SKUs.
-- Calculates cumulative percentage of total corporate gross profit to classify
-- products into Top Tier (0-50%), Core Tier (50-80%), and Long Tail (80-100%).
-- ==============================================================================

%sql
WITH sku_profit_summary AS (
    SELECT 
        p.product_id,
        p.product_name,
        p.category,
        ROUND(SUM(oi.line_total), 2) AS net_revenue,
        ROUND(SUM(oi.line_total) - SUM(oi.quantity * p.unit_cost), 2) AS net_profit
    FROM silver_order_items oi
    JOIN silver_products p ON oi.product_id = p.product_id
    JOIN silver_orders o ON oi.order_id = o.order_id
    WHERE o.order_status != 'Cancelled'
    GROUP BY p.product_id, p.product_name, p.category
),
sku_cumulative_profit AS (
    SELECT 
        product_id,
        product_name,
        category,
        net_revenue,
        net_profit,
        -- Running cumulative profit ordered by highest profit generator down to lowest
        SUM(net_profit) OVER (
            ORDER BY net_profit DESC 
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS running_net_profit,
        -- Total profit across entire catalog
        SUM(net_profit) OVER () AS total_catalog_profit
    FROM sku_profit_summary
),
sku_pareto_classified AS (
    SELECT 
        product_id,
        product_name,
        category,
        net_revenue,
        net_profit,
        ROUND((running_net_profit / total_catalog_profit) * 100, 2) AS cumulative_profit_pct,
        CASE 
            WHEN (running_net_profit / total_catalog_profit) <= 0.50 THEN 'Tier 1: Top Profit Drivers (0-50%)'
            WHEN (running_net_profit / total_catalog_profit) <= 0.80 THEN 'Tier 2: Core Steady Performers (50-80%)'
            ELSE 'Tier 3: Long Tail / Low Margin (80-100%)'
        END AS pareto_tier
    FROM sku_cumulative_profit
)
SELECT 
    pareto_tier,
    COUNT(product_id) AS sku_count,
    ROUND(COUNT(product_id) * 100.0 / 1000.0, 2) AS pct_of_catalog_skus,
    ROUND(SUM(net_revenue), 2) AS tier_net_revenue,
    ROUND(SUM(net_profit), 2) AS tier_net_profit,
    ROUND(SUM(net_profit) * 100.0 / AVG(total_catalog_profit), 2) AS pct_of_total_profit
FROM (
    SELECT sp.*, sc.total_catalog_profit 
    FROM sku_pareto_classified sp 
    JOIN sku_cumulative_profit sc ON sp.product_id = sc.product_id
)
GROUP BY pareto_tier
ORDER BY pareto_tier ASC;

-- COMMAND ----------

-- ==============================================================================
-- Scenario 10: Geographic Shipping Lanes & Delivery SLA Correlation
-- ==============================================================================
-- Business Objective:
-- Analyze delivery performance across Local vs. Regional vs. Long-Haul shipping lanes
-- by comparing warehouse origin state to customer delivery state, coupled with shipping fee tier.
-- Evaluates whether premium paid shipping ($14.99 vs $0.00 Free) delivers faster, on-time service.
-- ==============================================================================

%sql
SELECT 
    CASE 
        WHEN w.state = o.shipping_state THEN 'Intra-State / Local Lane (Origin = Dest State)'
        ELSE 'Inter-State / Long-Haul Lane (Cross-Country)'
    END AS shipping_lane_type,
    o.shipping_fee,
    COUNT(o.order_id) AS total_orders,
    ROUND(AVG(o.promised_days), 1) AS avg_promised_days,
    ROUND(AVG(o.dispatch_days), 2) AS avg_dispatch_days,
    ROUND(AVG(o.transit_days), 2) AS avg_transit_days,
    ROUND(AVG(o.actual_delivery_days), 2) AS avg_actual_delivery_days,
    ROUND(AVG(o.is_delayed) * 100, 2) AS delay_rate_pct,
    ROUND(AVG(o.is_otif) * 100, 2) AS otif_rate_pct
FROM silver_orders o
JOIN silver_warehouses w ON o.warehouse_id = w.warehouse_id
WHERE o.order_status != 'Cancelled'
GROUP BY 
    CASE 
        WHEN w.state = o.shipping_state THEN 'Intra-State / Local Lane (Origin = Dest State)'
        ELSE 'Inter-State / Long-Haul Lane (Cross-Country)'
    END,
    o.shipping_fee
ORDER BY 
    shipping_lane_type ASC, 
    o.shipping_fee DESC;
