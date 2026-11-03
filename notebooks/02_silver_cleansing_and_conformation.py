# Databricks notebook source
# ==============================================================================
# Enterprise E-Commerce & Supply Chain Logistics Lakehouse
# Notebook 2: Silver Conformation, Cleansing & Delta Lake MERGE
# Pipeline Layer: Silver Layer (Curated & Enriched Delta Tables)
# Enforces: Deduplication, The 4 Delivery Cases, Denominator Protection & Upserts
# ==============================================================================

# COMMAND ----------

from pyspark.sql.functions import (
    col, when, trim, round as spark_round, current_timestamp
)

print("=== 🚀 Starting Silver Master Dimensions Conformation ===")

# ==============================================================================
# 1. silver_customers: Standardize Segments, Trim Strings & Lineage Tracking
# ==============================================================================
raw_cust = spark.read.table("bronze_customers")

clean_segment_expr = trim(col("customer_segment"))

silver_customers = (
    raw_cust.withColumn(
        "customer_segment",
        when(clean_segment_expr == "Consumr", "Consumer")
        .when(clean_segment_expr == "Corp", "Corporate")
        .otherwise(clean_segment_expr)
    )
    .withColumn("_cleaned_at", current_timestamp())
)

(
    silver_customers.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("silver_customers")
)

# Read directly from conformed Delta table to verify storage registration
persisted_cust = spark.table("silver_customers")
print(f"✅ silver_customers registered: {persisted_cust.count():,} rows")
print("\nConformed Customer Segments Distribution:")
persisted_cust.groupBy("customer_segment").count().orderBy(col("count").desc()).show()


# ==============================================================================
# 2. silver_products: Materialize Unit Economics with Zero-Division Guard
# ==============================================================================
raw_prod = spark.read.table("bronze_products")

silver_products = (
    raw_prod.withColumn(
        "gross_margin_pct",
        when(
            col("unit_price") > 0,
            spark_round(((col("unit_price") - col("unit_cost")) / col("unit_price")) * 100, 2)
        ).otherwise(0.0)
    )
    .withColumn("_cleaned_at", current_timestamp())
)

(
    silver_products.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("silver_products")
)

persisted_prod = spark.table("silver_products")
print(f"✅ silver_products registered: {persisted_prod.count():,} SKUs")
print("\nSample Conformed Products with Margins:")
persisted_prod.select(
    "product_id", "product_name", "category", "unit_price", "unit_cost", "gross_margin_pct"
).show(5, truncate=False)


# ==============================================================================
# 3. silver_warehouses: Conformed Distribution Hubs
# ==============================================================================
raw_wh = spark.read.table("bronze_warehouses")

silver_warehouses = (
    raw_wh.withColumn("_cleaned_at", current_timestamp())
)

(
    silver_warehouses.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("silver_warehouses")
)

persisted_wh = spark.table("silver_warehouses")
print(f"✅ silver_warehouses registered: {persisted_wh.count():,} distribution hubs")
print("\n=== 🎉 All 3 Master Dimensions Successfully Conformed to Silver! ===")

# COMMAND ----------

from pyspark.sql.functions import (
    col, when, lit, datediff, date_format, year, month, 
    current_timestamp, broadcast, greatest, row_number
)
from pyspark.sql.window import Window

print("=== 🚀 Starting Fact Header (silver_orders) Conformation ===")

# ==============================================================================
# 1. Window Deduplication (Eliminating 10,000 Batch-Replay Exact Clones)
# ==============================================================================
raw_orders = spark.read.table("bronze_orders")

# Order by _ingested_at ASC to keep the first authoritative instance
order_window = Window.partitionBy("order_id").orderBy(col("_ingested_at").asc())

deduped_orders = (
    raw_orders
    .withColumn("row_num", row_number().over(order_window))
    .filter(col("row_num") == 1)
    .drop("row_num")
)

# ==============================================================================
# 2. Enforce the 4 Delivery Cases & SLA Denominator Protection
# ==============================================================================
orders_with_delivery = (
    deduped_orders
    # Case 1: Complete Denominator Protection (Set all SLAs to NULL for Cancellations)
    .withColumn(
        "is_delayed",
        when(col("order_status") == "Cancelled", lit(None).cast("integer"))
        .when(col("actual_delivery_days") > col("promised_days"), 1)
        .otherwise(0)
    )
    .withColumn(
        "is_dispatch_delayed",
        when(col("order_status") == "Cancelled", lit(None).cast("integer"))
        .otherwise(col("is_dispatch_delayed"))
    )
    .withColumn(
        "is_transit_delayed",
        when(col("order_status") == "Cancelled", lit(None).cast("integer"))
        .otherwise(col("is_transit_delayed"))
    )
    # Case 2: Carrier Logistics SLA (is_on_time_delivery covers Completed & Returned)
    .withColumn(
        "is_on_time_delivery",
        when(col("order_status") == "Cancelled", lit(None).cast("integer"))
        .when(col("actual_delivery_days") <= col("promised_days"), 1)
        .otherwise(0)
    )
    # Case 2: Commercial Fulfillment SLA (is_otif strictly requires Completed)
    .withColumn(
        "is_otif",
        when((col("is_on_time_delivery") == 1) & (col("order_status") == "Completed"), 1)
        .otherwise(0)
    )
    # Case 3: Root-Cause Delay Attribution Decomposition
    .withColumn(
        "delay_attribution",
        when(col("order_status") == "Cancelled", "Not Applicable (Cancelled)")
        .when(
            col("is_delayed") == 1,
            when((col("is_dispatch_delayed") == 1) & (col("is_transit_delayed") == 1), "Compound (Warehouse & Carrier)")
            .when(col("is_dispatch_delayed") == 1, "Warehouse Delay")
            .when(col("is_transit_delayed") == 1, "Carrier Delay")
            .otherwise("Carrier Delay")
        )
        .otherwise("None (On-Time)")
    )
)

# ==============================================================================
# 3. Enrich with Guest Checkout & Customer Tenure (Broadcast Join with silver_customers)
# ==============================================================================
silver_cust = spark.read.table("silver_customers").select("customer_id", "signup_date")

orders_enriched = (
    orders_with_delivery
    .join(broadcast(silver_cust), on="customer_id", how="inner")
    .withColumn(
        "is_guest_checkout",
        when(col("order_date") < col("signup_date"), 1).otherwise(0)
    )
    .withColumn(
        "customer_tenure_days",
        greatest(datediff(col("order_date"), col("signup_date")), lit(0))
    )
    .drop("signup_date") # Drop dimension column after feature engineering
    # Calendar Partition Attributes
    .withColumn("order_year", year(col("order_date")))
    .withColumn("order_month", month(col("order_date")))
    .withColumn("order_year_month", date_format(col("order_date"), "yyyy-MM"))
    .withColumn("_cleaned_at", current_timestamp())
)

# ==============================================================================
# 4. Materialize and Partition into Delta Lake
# ==============================================================================
(
    orders_enriched.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("order_year_month")
    .saveAsTable("silver_orders")
)

print("✅ silver_orders written successfully!")

# ==============================================================================
# 5. Silver Integrity Sanity Checks
# ==============================================================================
silver_orders = spark.read.table("silver_orders")
total_silver = silver_orders.count()

print(f"\nTotal Conformed Silver Orders: {total_silver:,} (Expected: exactly 1,000,000)")

print("\n=== Delay Attribution Breakdown ===")
silver_orders.groupBy("delay_attribution").count().orderBy(col("count").desc()).show(truncate=False)

print("\n=== Guest Checkout Reconciliation ===")
silver_orders.groupBy("is_guest_checkout").count().show()

print("\n=== Denominator Protection Check (Null SLA Counts on Cancellations) ===")
from pyspark.sql.functions import count as spark_cnt
silver_orders.filter(col("order_status") == "Cancelled").select(
    spark_cnt(when(col("is_delayed").isNull(), 1)).alias("null_is_delayed"),
    spark_cnt(when(col("is_transit_delayed").isNull(), 1)).alias("null_is_transit_delayed"),
    spark_cnt(when(col("is_dispatch_delayed").isNull(), 1)).alias("null_is_dispatch_delayed"),
    spark_cnt(when(col("is_on_time_delivery").isNull(), 1)).alias("null_is_on_time_delivery")
).show()

# COMMAND ----------

from pyspark.sql.functions import col, round as spark_round, current_timestamp

print("=== 🚀 Starting Fact Line Items (silver_order_items) Conformation ===")

# ==============================================================================
# 1. Primary Key Deduplication (Eliminating 14,499 Duplicate Line Items)
# ==============================================================================
raw_items = spark.read.table("bronze_order_items")

# Deduplicate strictly on the line item primary key
deduped_items = raw_items.dropDuplicates(["order_item_id"])

# ==============================================================================
# 2. Financial Precision & Revenue Feature Derivation
# ==============================================================================
# We preserve line_total (the authoritative raw OLTP invoice charge),
# while deriving gross_revenue and discount_amount for unit-economic analytics.
silver_order_items = (
    deduped_items
    .withColumn("gross_revenue", spark_round(col("quantity") * col("unit_price"), 2))
    .withColumn("discount_amount", spark_round(col("gross_revenue") - col("line_total"), 2))
    .withColumn("_cleaned_at", current_timestamp())
)

# ==============================================================================
# 3. Materialize Conformed Delta Table
# ==============================================================================
(
    silver_order_items.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("silver_order_items")
)

print("✅ silver_order_items written successfully!")

# ==============================================================================
# 4. Silver Line Items Integrity Sanity Checks
# ==============================================================================
conformed_items = spark.read.table("silver_order_items")
total_items = conformed_items.count()

print(f"\nTotal Conformed Silver Line Items: {total_items:,} (Expected: exactly 1,449,520)")

print("\nSample Conformed Line Items with Revenue Breakdown:")
conformed_items.select(
    "order_item_id", "order_id", "product_id", 
    "quantity", "unit_price", "gross_revenue", 
    "discount_pct", "discount_amount", "line_total"
).show(5)

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql.functions import col, lit, current_timestamp

print("=== 🚀 Demonstrating Idempotent Incremental Upsert (MERGE INTO) ===")

# 1. Target Delta Table reference
silver_orders_delta = DeltaTable.forName(spark, "silver_orders")
initial_count = spark.table("silver_orders").count()
print(f"Initial silver_orders Count: {initial_count:,}")

# 2. Extract 3 existing completed orders to simulate CDC status updates (Returns)
updates_df = (
    spark.table("silver_orders")
    .filter(col("order_status") == "Completed")
    .limit(3)
    # Simulate status update to Returned and adjust business flags
    .withColumn("order_status", lit("Returned"))
    .withColumn("is_otif", lit(0))
    .withColumn("_source_file", lit("cdc_stream_batch_01.json"))
    .withColumn("_cleaned_at", current_timestamp())
)

update_ids = [row["order_id"] for row in updates_df.select("order_id").collect()]
print(f"Simulating CDC Status Updates for existing order_ids: {update_ids}")

# Show their state BEFORE the merge
print("\nState BEFORE Merge:")
(
    spark.table("silver_orders")
    .filter(col("order_id").isin(update_ids))
    .select("order_id", "order_status", "is_otif", "_cleaned_at")
    .show(truncate=False)
)

# 3. Create 2 net-new orders using the EXACT schema of silver_orders
new_orders_df = (
    spark.table("silver_orders")
    .limit(2)
    .withColumn("order_id", col("order_id") + 88880000) # Assign brand new unique order_ids
    .withColumn("order_status", lit("Completed"))
    .withColumn("_source_file", lit("cdc_stream_batch_01.json"))
    .withColumn("_cleaned_at", current_timestamp())
)

# Combine updates + new inserts into a single incoming CDC batch (Exact Schema Match!)
cdc_batch = updates_df.unionByName(new_orders_df)

# 4. Execute ACID Delta MERGE INTO
(
    silver_orders_delta.alias("target")
    .merge(
        cdc_batch.alias("source"),
        "target.order_id = source.order_id"
    )
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
)

print("✅ Delta MERGE INTO executed successfully!")

# 5. Verify Results
new_count = spark.table("silver_orders").count()
print(f"\nFinal silver_orders Count: {new_count:,} (Expected: exactly {initial_count + 2:,})")

print("\nState AFTER Merge (Verified in-place updates without duplication):")
(
    spark.table("silver_orders")
    .filter(col("order_id").isin(update_ids))
    .select("order_id", "order_status", "is_otif", "_cleaned_at")
    .show(truncate=False)
)
