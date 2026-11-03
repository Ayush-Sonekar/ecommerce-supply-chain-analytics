# Databricks notebook source
# combine all the orders files into one
from pyspark.sql.functions import current_timestamp, col

path = "/Volumes/workspace/default/ecommerce"

df_orders_raw = spark.read.format("csv").option("header", "true").option("Inferschema", "true").load(f"{path}/orders_part_*.csv") \
    .withColumn("_ingested_at", current_timestamp()) \
        .withColumn("_source_file", col("_metadata.file_path"))

display(df_orders_raw.limit(5))

print("total rows: ", df_orders_raw.count())

# COMMAND ----------

# combine all the order items files into one
df_order_items_raw = spark.read.format("csv").option("header","true").option("Inferschema","true").load(f"{path}/order_items_part_*.csv") \
    .withColumn("_ingested_at", current_timestamp()) \
        .withColumn("_source_file", col("_metadata.file_path"))

display(df_order_items_raw.limit(5))

print("total rows: ", df_order_items_raw.count())


# COMMAND ----------

df_customers_raw = spark.read.format("csv").option("header","true").option("inferschema", "true").load(f"{path}/customers.csv") \
    .withColumn("_ingested_at", current_timestamp()) \
        .withColumn("_source_file", col("_metadata.file_path"))

display(df_customers_raw.limit(5))

print("total_rows: ", df_customers_raw.count())

# COMMAND ----------

df_products_raw = spark.read.format("csv").option("header","true").option("inferschema", "true").load(f"{path}/products.csv") \
    .withColumn("_ingested_at", current_timestamp()) \
        .withColumn("_source_file", col("_metadata.file_path"))

display(df_products_raw.limit(5))

print("total_rows: ", df_products_raw.count())

# COMMAND ----------

df_warehouses_raw = spark.read.format("csv").option("header", "true").option("inferschema", "true").load(f"{path}/warehouses.csv") \
    .withColumn("_ingested_at", current_timestamp()) \
        .withColumn("_source_file", col("_metadata.file_path"))

display(df_warehouses_raw.limit(5))

print("Total_rows: ", df_warehouses_raw.count())

# COMMAND ----------

# writing data to delta tables

df_orders_raw.write.format("delta").mode("overwrite").saveAsTable("bronze_orders")
df_customers_raw.write.format("delta").mode("overwrite").saveAsTable("bronze_customers")
df_order_items_raw.write.format("delta").mode("overwrite").saveAsTable("bronze_order_items")
df_products_raw.write.format("delta").mode("overwrite").saveAsTable("bronze_products")
df_warehouses_raw.write.format("delta").mode("overwrite").saveAsTable("bronze_warehouses")

# COMMAND ----------

# Completeness & Null Distribution Matrix

from pyspark.sql.functions import col, count, when, round

def audit_nulls(table_name):
    df = spark.read.table(table_name)
    exprs = [
        round((count(when(col(c).isNull(), c)) / count("*")) * 100 ,2).alias(c)
        for c in df.columns
    ]
    print(f"Null Audit: {table_name}")
    display(df.select(*exprs))

audit_nulls("bronze_orders")
audit_nulls("bronze_order_items")
audit_nulls("bronze_customers")
audit_nulls("bronze_products")
audit_nulls("bronze_warehouses")

# COMMAND ----------

# Investigating the Root Cause
from pyspark.sql.functions import col, count, when, round

orders_df = spark.read.table("bronze_orders")

null_by_status = (
    orders_df.groupBy("order_status")
    .agg(
        count("*").alias("total_orders"),
        count(when(col("ship_date").isNull(),1)).alias("null_ship_date_cnt"),
        count(when(col("delivery_date").isNull(),1)).alias("null_delivery_date_cnt"),
        round(
            count(when(col("ship_date").isNull(),1)) / count("*") * 100,2
            ).alias("null_ship_date_pct")
            ).orderBy(col("total_orders").desc())
)

display(null_by_status)

# COMMAND ----------

# Uniqueness & Key Granularity Profiling

from pyspark.sql.functions import col, count

# total records vs distinct primary keys
total_records = orders_df.count()
distinct_records = orders_df.select("order_id").distinct().count()
duplicate_count = total_records - distinct_records

print(f"Total_records: {total_records}\nDistinct_records: {distinct_records} \nDuplicates: {duplicate_count}")

# Identifying duplicate keys and inspect a sample

dup_keys_df = (
    orders_df.groupBy("order_id")
    .agg(count("*").alias("occurance"))
    .filter(col("occurance") > 1)
)

print(f"Number of unique order_ids affected by duplication: {dup_keys_df.count():,}")

sample_dup_id = dup_keys_df.limit(1).collect()[0]["order_id"]

print(f"\nInspecting duplicate rows for sample order_id: {sample_dup_id}")
display(orders_df.filter(col("order_id") == sample_dup_id))

# COMMAND ----------

# Geographic State & City Domain Conformance
from pyspark.sql.functions import col, count, length, trim, round as spark_round

orders_df = spark.read.table("bronze_orders")

print("=== Geographic State Domain & Formatting ===")
state_profile = (
    orders_df.groupBy("shipping_state")
    .agg(
        count("*").alias("order_count"),
        spark_round((count("*") / orders_df.count()) * 100, 2).alias("percentage")
    )
    .orderBy(col("order_count").desc())
)
display(state_profile)

invalid_states = orders_df.filter(
    (length(trim(col("shipping_state"))) != 2) | 
    (col("shipping_state") != trim(col("shipping_state")))
)
print(f"Total invalid or untrimmed state values: {invalid_states.count()}")
distinct_cities = orders_df.select("shipping_city").distinct().count()
distinct_states = orders_df.select("shipping_state").distinct().count()
print(f"Distinct States: {distinct_states} | Distinct Cities: {distinct_cities}")

# COMMAND ----------

from pyspark.sql.functions import (
    col, min, max, count, countDistinct, datediff, when
)

print("=== 1. Temporal Sequence Integrity (Chronological Paradox Checks) ===")

# Filter for fulfilled orders where timestamps exist
fulfilled_orders = orders_df.filter(col("ship_date").isNotNull() & col("delivery_date").isNotNull())

# Check 1: Ship date before Order date
ship_before_order_cnt = fulfilled_orders.filter(col("ship_date") < col("order_date")).count()

# Check 2: Delivery date before Ship date
delivery_before_ship_cnt = fulfilled_orders.filter(col("delivery_date") < col("ship_date")).count()

# Check 3: Delivery date before Order date
delivery_before_order_cnt = fulfilled_orders.filter(col("delivery_date") < col("order_date")).count()

print(f"Violations (ship_date < order_date):         {ship_before_order_cnt:,}")
print(f"Violations (delivery_date < ship_date):     {delivery_before_ship_cnt:,}")
print(f"Violations (delivery_date < order_date):    {delivery_before_order_cnt:,}")


print("\n=== 2. Calendar Timeline Horizon & Continuity ===")

# Compute overall date boundaries
date_bounds = orders_df.select(
    min("order_date").alias("earliest_order_date"),
    max("order_date").alias("latest_order_date"),
    countDistinct("order_date").alias("active_order_days"),
    datediff(max("order_date"), min("order_date")).alias("calendar_day_span")
)
display(date_bounds)

# COMMAND ----------

# Cross-Field Business Invariants & Math Logic Reconciliation
from pyspark.sql.functions import col, datediff

fulfilled = orders_df.filter(col("order_status") != "Cancelled")

# 1. Additivity Check
additivity_mismatch = fulfilled.filter(
    col("actual_delivery_days") != (col("dispatch_days") + col("transit_days"))
).count()

# 2. Timestamp datediff reconciliation
dispatch_diff_mismatch = fulfilled.filter(
    col("dispatch_days") != datediff(col("ship_date"), col("order_date"))
).count()
transit_diff_mismatch = fulfilled.filter(
    col("transit_days") != datediff(col("delivery_date"), col("ship_date"))
).count()

# 3. SLA Delay Logic
delay_flag_mismatch = fulfilled.filter(
    ((col("actual_delivery_days") > col("promised_days")) & (col("is_delayed") != 1)) |
    ((col("actual_delivery_days") <= col("promised_days")) & (col("is_delayed") != 0))
).count()

print(f"Additivity Mismatches (actual != dispatch + transit): {additivity_mismatch:,}")
print(f"Dispatch Days vs datediff(ship, order) mismatches:     {dispatch_diff_mismatch:,}")
print(f"Transit Days vs datediff(delivery, ship) mismatches:   {transit_diff_mismatch:,}")
print(f"Delay Flag Logic Inconsistencies:                      {delay_flag_mismatch:,}")

# COMMAND ----------

from pyspark.sql.functions import (
    col, expr, min, max, avg, stddev, round as spark_round
)

# 1. Ensure fulfilled orders DataFrame is referenced in this cell
fulfilled = orders_df.filter(col("order_status") != "Cancelled")

# 2. Target metrics to profile
target_metrics = [
    "dispatch_days", 
    "transit_days", 
    "actual_delivery_days", 
    "shipping_fee"
]

# 3. Build a single list of aggregation expressions for ALL metrics
# We explicitly cast min/max to double to prevent schema merge conflicts
agg_exprs = []
for m in target_metrics:
    agg_exprs.extend([
        min(col(m)).cast("double").alias(f"{m}_min"),
        expr(f"percentile_approx({m}, 0.25)").cast("double").alias(f"{m}_p25"),
        expr(f"percentile_approx({m}, 0.50)").alias(f"{m}_p50"),
        expr(f"percentile_approx({m}, 0.75)").alias(f"{m}_p75"),
        expr(f"percentile_approx({m}, 0.95)").alias(f"{m}_p95"),
        expr(f"percentile_approx({m}, 0.99)").alias(f"{m}_p99"),
        max(col(m)).cast("double").alias(f"{m}_max"),
        spark_round(avg(col(m)), 2).cast("double").alias(f"{m}_mean"),
        spark_round(stddev(col(m)), 2).cast("double").alias(f"{m}_stddev")
    ])

# 4. Execute in a SINGLE distributed pass over the 928,969 records
all_stats = fulfilled.select(*agg_exprs).collect()[0]

# 5. Restructure into rows for a clean summary presentation
summary_rows = []
for m in target_metrics:
    summary_rows.append((
        m,
        round(float(all_stats[f"{m}_min"]), 2),
        round(float(all_stats[f"{m}_p25"]), 2),
        round(float(all_stats[f"{m}_p50"]), 2),
        round(float(all_stats[f"{m}_p75"]), 2),
        round(float(all_stats[f"{m}_p95"]), 2),
        round(float(all_stats[f"{m}_p99"]), 2),
        round(float(all_stats[f"{m}_max"]), 2),
        round(float(all_stats[f"{m}_mean"]), 2),
        round(float(all_stats[f"{m}_stddev"]), 2)
    ))

# 6. Display as a structured DataFrame
schema = [
    "metric", "min", "p25", "median_p50", 
    "p75", "p95", "p99", "max", "mean", "std_dev"
]
stats_df = spark.createDataFrame(summary_rows, schema)
display(stats_df)

# COMMAND ----------

from pyspark.sql.functions import col, count, countDistinct, when

# 1. Reference bronze_order_items Delta table
items_df = spark.read.table("bronze_order_items")

# 2. Build expressions for a SINGLE distributed pass over 1.46M records
# Notice: we use 1 inside when() so count() increments on nulls
null_exprs = [
    count(when(col(c).isNull(), 1)).alias(f"{c}_nulls") 
    for c in items_df.columns
]

# Single distributed action computing total rows, distinct keys, and all nulls
audit_summary = items_df.select(
    count("*").alias("total_rows"),
    countDistinct("order_item_id").alias("distinct_item_ids"),
    countDistinct("order_id").alias("distinct_order_ids"),
    countDistinct("product_id").alias("distinct_product_ids"),
    *null_exprs
).collect()[0]

total_rows = audit_summary["total_rows"]
distinct_item_ids = audit_summary["distinct_item_ids"]
duplicate_rows = total_rows - distinct_item_ids

# 3. Print Key Cardinality Metrics
print("=== 1. Primary Key & Cardinality Metrics ===")
print(f"Total Line Items:          {total_rows:,}")
print(f"Distinct order_item_id:    {distinct_item_ids:,}")
print(f"Duplicate Line Items:      {duplicate_rows:,} ({round((duplicate_rows / total_rows) * 100, 2)}%)")
print(f"Distinct Parent Orders:    {audit_summary['distinct_order_ids']:,}")
print(f"Distinct Catalog SKUs:     {audit_summary['distinct_product_ids']:,}")

# 4. Print Field Completeness (Null Checks)
print("\n=== 2. Line Item Completeness (Null Counts) ===")
for c in items_df.columns:
    print(f"  - {c:20s}: {audit_summary[f'{c}_nulls']:,} nulls")

# 5. Product Collision Audit (Deduplicate order_item_id first to isolate true basket collisions)
deduped_items = items_df.dropDuplicates(["order_item_id"])
product_collisions = (
    deduped_items.groupBy("order_id", "product_id")
    .agg(count("*").alias("lines_per_sku"))
    .filter(col("lines_per_sku") > 1)
)
print(f"\n=== 3. Product Collisions within Same Basket ===")
print(f"Orders with duplicate product_id lines: {product_collisions.count():,}")

# COMMAND ----------

from pyspark.sql.functions import col, count, when, abs as spark_abs, round as spark_round

# Reference bronze_order_items Delta table
items_df = spark.read.table("bronze_order_items")

# 1.audit numeric bonds
bounds_audit = items_df.select(
    count(when(col("quantity") <= 0, 1)).alias("invalid_qty_zero_or_neg"),
    count(when(col("quantity") > 10, 1)).alias("invalid_qty_overflow"),
    count(when(col("unit_price") <= 0, 1)).alias("invalid_price_zero_or_neg"),
    count(when((col("discount_pct") < 0.0) | (col("discount_pct") > 0.30), 1)).alias("invalid_discount_range"),
    count(when(col("line_total") < 0, 1)).alias("negative_line_totals")
).collect()[0]

print("=== 1. Numeric Boundary Violations ===")
print(f"Quantities <= 0: {bounds_audit['invalid_qty_zero_or_neg']:,}")
print(f"Quantities > 10 (Overflow): {bounds_audit['invalid_qty_overflow']:,}")
print(f"Prices <= $0.00: {bounds_audit['invalid_price_zero_or_neg']:,}")
print(f"Discounts outside [0%, 30%]: {bounds_audit['invalid_discount_range']:,}")
print(f"Negative Line Totals: {bounds_audit['negative_line_totals']:,}")

# 2. Financial Arithmetic Reconciliation Check
# Calculate the expected revenue using standard commercial rounding
expected_line_calc = spark_round(
    col("quantity") * col("unit_price") * (1.0 - col("discount_pct")), 
    2
)
# Detect math drift greater than 1 cent ($0.01)
math_mismatches = items_df.filter(
    spark_abs(col("line_total") - expected_line_calc) > 0.01
)
math_drift_cnt = math_mismatches.count()
print("\n=== 2. Financial Math Reconciliation ===")
print(f"Formula: line_total == round(quantity * unit_price * (1 - discount), 2)")
print(f"Total Arithmetic Mismatches (> $0.01 drift): {math_drift_cnt:,}")
if math_drift_cnt > 0:
    print("\nInspecting sample math discrepancy:")
    display(math_mismatches.limit(5))


# COMMAND ----------

from pyspark.sql.functions import col, count, sum as spark_sum, expr, min, max, avg, round as spark_round

# 1. Aggregate to the Order Header level (Deduplicating order_item_id first to prevent replay inflation)
clean_items = items_df.dropDuplicates(["order_item_id"])

basket_metrics = (
    clean_items.groupBy("order_id")
    .agg(
        count("product_id").alias("distinct_lines_per_order"),
        spark_sum("quantity").alias("total_units_per_order"),
        spark_round(spark_sum("line_total"), 2).alias("order_merchandise_subtotal")
    )
)

# 2. Build multi-percentile profile for basket dimensions in a single pass
basket_cols = [
    "distinct_lines_per_order", 
    "total_units_per_order", 
    "order_merchandise_subtotal"
]

agg_exprs = []
for c in basket_cols:
    agg_exprs.extend([
        min(col(c)).cast("double").alias(f"{c}_min"),
        expr(f"percentile_approx({c}, 0.25)").cast("double").alias(f"{c}_p25"),
        expr(f"percentile_approx({c}, 0.50)").cast("double").alias(f"{c}_p50"),
        expr(f"percentile_approx({c}, 0.75)").cast("double").alias(f"{c}_p75"),
        expr(f"percentile_approx({c}, 0.95)").cast("double").alias(f"{c}_p95"),
        expr(f"percentile_approx({c}, 0.99)").cast("double").alias(f"{c}_p99"),
        max(col(c)).cast("double").alias(f"{c}_max"),
        spark_round(avg(col(c)), 2).cast("double").alias(f"{c}_mean")
    ])

stats = basket_metrics.select(*agg_exprs).collect()[0]

# 3. Format as a clean summary DataFrame
summary_rows = []
for c in basket_cols:
    summary_rows.append((
        c,
        round(float(stats[f"{c}_min"]), 2),
        round(float(stats[f"{c}_p25"]), 2),
        round(float(stats[f"{c}_p50"]), 2),
        round(float(stats[f"{c}_p75"]), 2),
        round(float(stats[f"{c}_p95"]), 2),
        round(float(stats[f"{c}_p99"]), 2),
        round(float(stats[f"{c}_max"]), 2),
        round(float(stats[f"{c}_mean"]), 2)
    ))

schema = ["dimension", "min", "p25", "median_p50", "p75", "p95", "p99", "max", "mean"]
basket_profile_df = spark.createDataFrame(summary_rows, schema)
display(basket_profile_df)

# COMMAND ----------

from pyspark.sql.functions import col, count, min, max, round as spark_round, countDistinct

# 1.Reference bronze customers 
cust_df = spark.read.table("bronze_customers")
total_customers = cust_df.count()

print(f"Total Customer Records: {total_customers:,}")

# 2. Audit customer_segment domain distribution and detect dirty strings/typos

print("\n=== 1. Customer Segments Frequency Distribution ===")
segmented_profile = (
    cust_df.groupBy("customer_segment")
    .agg(
        count("*").alias("customer_count"),
        spark_round(count("*") / total_customers * 100, 2).alias("pct")
    ).orderBy(col("customer_count").desc())
)
display(segmented_profile)

# 3. Audit customer seegment timeline
print("=== 2. Customer Registration Date Horizon")
signup_bounds = cust_df.select(
    min("signup_date").alias("earliest_signup"),
    max("signup_date").alias("latest_signup"),
    countDistinct("signup_date").alias("distinct_signup_days")
)
display(signup_bounds)

# COMMAND ----------

from pyspark.sql.functions import col, count, min, max, avg, round as spark_round

# reference bronze_products
prod_df = spark.read.table("bronze_products")
total_products = prod_df.count()

print(f"Total Catalog SKUs: {total_products:,}")

# 2. check for negative margins or loss-leader SKUs
negative_margin_count = prod_df.filter(col("unit_price") <= col("unit_cost")).count()
print(f"Products with Price <= Cost (Zero/Negative Margins): {negative_margin_count}")

# 3. Category-level Unit Economics and Weight Profiles
category_profile = (
    prod_df.groupBy("category")
    .agg(
        count("*").alias("sku_count"),
        spark_round(min("unit_price"), 2).alias("min_price"),
        spark_round(avg("unit_price"), 2).alias("avg_price"),
        spark_round(max("unit_price"), 2).alias("max_price"),
        spark_round(
            avg((col("unit_price") - col("unit_cost")) / col("unit_price")) * 100, 1
        ).alias("avg_gross_margin_pct"),
        spark_round(min("weight_kg"), 2).alias("min_weight_kg"),
        spark_round(avg("weight_kg"), 2).alias("avg_weight_kg"),
        spark_round(max("weight_kg"), 2).alias("max_weight_kg")
    )
    .orderBy(col("sku_count").desc())
)
display(category_profile)

# COMMAND ----------

from pyspark.sql.functions import col, min, max, avg, sum as spark_sum, count, countDistinct, round as spark_round, when

# refernce bronze_wareshouses
wh_df = spark.read.table("bronze_warehouses")

# comprehensive Quality and Capacity Intergrity audit
wh_audit = wh_df.select(
    count("*").alias("total_hubs"),
    countDistinct("warehouse_id").alias("distinct_hub_ids"),
    count(when(col("capacity_units").isNull(), 1)).alias("null_capacities"),
    count(when(col("capacity_units") <= 0, 1)).alias("invalid_capacities"),
    min("capacity_units").alias("min_capacity"),
    max("capacity_units").alias("max_capacity"),
    spark_round(avg("capacity_units"), 0).cast("long").alias("avg_capacity"),
    spark_sum("capacity_units").alias("total_network_capacity")
).collect()[0]

# Print the Quality & Capacity Audit Report
print("=== 1. Warehouse Integrity & Capacity Audit ===")
print(f"Total Hub Records: {wh_audit['total_hubs']}")
print(f"Distinct Warehouse IDs: {wh_audit['distinct_hub_ids']} (100% Unique PK)")
print(f"Null Capacities: {wh_audit['null_capacities']}")
print(f"Invalid Capacities (<= 0): {wh_audit['invalid_capacities']}")
print(f"Capacity Range (Min - Max): {wh_audit['min_capacity']:,} to {wh_audit['max_capacity']:,} units")
print(f"Average Hub Capacity: {wh_audit['avg_capacity']:,} units")
print(f"Total Network Capacity: {wh_audit['total_network_capacity']:,} units")

# 4. Single display() rendering the full census of all 10 hubs
print("\n=== 2. Warehouse Distribution Hubs Census ===")
display(
    wh_df.select(
        "warehouse_id", "warehouse_name", "city", "state", "capacity_units"
    ).orderBy("warehouse_id")
)

# COMMAND ----------

from pyspark.sql.functions import col, broadcast

# Load all tables
orders_df = spark.read.table("bronze_orders")
items_df = spark.read.table("bronze_order_items")
cust_df = spark.read.table("bronze_customers")
prod_df = spark.read.table("bronze_products")
wh_df = spark.read.table("bronze_warehouses")

print("=== 1. Star Schema Referential Integrity Anti-Join Matrix ===")

# Test 1: Orders without Customers (Foreign Key: customer_id)
# broadcast() hint optimizes performance by distributing the small dimension to all nodes
orphan_orders_cust = orders_df.join(broadcast(cust_df), on="customer_id", how="left_anti").count()

# Test 2: Orders without Warehouses (Foreign Key: warehouse_id)
orphan_orders_wh = orders_df.join(broadcast(wh_df), on="warehouse_id", how="left_anti").count()

# Test 3: Line Items without Products (Foreign Key: product_id)
orphan_items_prod = items_df.join(broadcast(prod_df), on="product_id", how="left_anti").count()

# Test 4: Line Items without Parent Orders (Foreign Key: order_id)
# Large table to large table anti-join (standard shuffle hash join)
orphan_items_order = items_df.join(
    orders_df.select("order_id").distinct(), 
    on="order_id", 
    how="left_anti"
).count()

print(f"Orphan Orders (Invalid customer_id): {orphan_orders_cust:,}")
print(f"Orphan Orders (Invalid warehouse_id): {orphan_orders_wh:,}")
print(f"Orphan Line Items (Invalid product_id): {orphan_items_prod:,}")
print(f"Orphan Line Items (Invalid order_id): {orphan_items_order:,}")
print("\n=== 2. Cross-Table Temporal Sequence Consistency ===")
# Test 5: Did any customer place an order before their signup_date?
order_before_signup_cnt = (
    orders_df.join(broadcast(cust_df), on="customer_id", how="inner")
    .filter(col("order_date") < col("signup_date"))
    .count()
)
print(f"Orders Placed Prior to Customer Signup: {order_before_signup_cnt:,}")  