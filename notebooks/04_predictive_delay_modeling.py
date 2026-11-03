# Databricks notebook source
# ==============================================================================
# Enterprise E-Commerce & Supply Chain Logistics Lakehouse
# Notebook 4: Applied Predictive Analytics (Delivery Delay Risk Classification)
# Target Model: Zero-Data-Leakage Spark MLlib Pipeline (Random Forest Classifier)
# ==============================================================================

# COMMAND ----------

# ==============================================================================
# Cell 1: Zero-Leakage Pre-Dispatch Feature Engineering
# ==============================================================================
# Business Goal:
# Formulate the training dataset strictly using signals available at the moment 
# of order checkout. Any post-checkout attributes (ship_date, dispatch_days, 
# delivery_date, transit_days, order_status) are strictly excluded to prevent 
# data leakage.
# ==============================================================================

from pyspark.sql.functions import (
    col, sum as _sum, count, countDistinct, when, month, dayofweek, round as _round
)

print("=== Step 1: Pre-Aggregating Line Items to Order-Level Features ===")

# 1. Pre-aggregate order items with product catalog to compute total weight and cart size
df_order_cart = spark.table("silver_order_items").alias("oi") \
    .join(spark.table("silver_products").alias("p"), col("oi.product_id") == col("p.product_id"), "inner") \
    .groupBy("oi.order_id") \
    .agg(
        _round(_sum(col("oi.quantity") * col("p.weight_kg")), 2).alias("total_order_weight_kg"),
        _sum(col("oi.quantity")).cast("integer").alias("total_units_ordered"),
        count("oi.order_item_id").cast("integer").alias("distinct_item_count")
    )

print("=== Step 2: Joining with Order Header and Warehouse Master Data ===")

# 2. Join with Silver Orders (filtering out Cancelled orders as they have no delivery outcome)
df_orders = spark.table("silver_orders").alias("o") \
    .filter(col("o.order_status") != "Cancelled") \
    .filter(col("o.is_delayed").isNotNull())

df_warehouses = spark.table("silver_warehouses").alias("w") \
    .select("warehouse_id", col("state").alias("wh_state"))

# 3. Assemble the conformed ML training table with zero data leakage
df_ml_raw = df_orders \
    .join(df_order_cart.alias("c"), col("o.order_id") == col("c.order_id"), "inner") \
    .join(df_warehouses.alias("w"), col("o.warehouse_id") == col("w.warehouse_id"), "inner") \
    .select(
        col("o.order_id"),
        # Numeric Features (Available at checkout)
        col("o.promised_days").cast("double").alias("promised_days"),
        col("o.shipping_fee").cast("double").alias("shipping_fee"),
        col("c.total_order_weight_kg"),
        col("c.total_units_ordered").cast("double").alias("total_units_ordered"),
        col("c.distinct_item_count").cast("double").alias("distinct_item_count"),
        col("o.customer_tenure_days").cast("double").alias("customer_tenure_days"),
        
        # Temporal Features (Extracted from purchase order_date)
        month(col("o.order_date")).cast("double").alias("order_month"),
        dayofweek(col("o.order_date")).cast("double").alias("order_day_of_week"),
        when(month(col("o.order_date")).isin(11, 12), 1.0).otherwise(0.0).alias("is_q4_holiday_surge"),
        
        # Categorical / Spatial Lane Features
        col("o.warehouse_id").cast("string").alias("warehouse_id_str"),
        col("o.shipping_state").alias("shipping_state"),
        when(col("w.wh_state") != col("o.shipping_state"), 1.0).otherwise(0.0).alias("is_interstate_lane"),
        
        # Target Label (Binary: 1 = Late Delivery, 0 = On-Time Delivery)
        col("o.is_delayed").cast("integer").alias("label")
    )

df_ml_raw.cache()
total_records = df_ml_raw.count()
print(f" Conformed ML Training Dataset Materialized: {total_records:,} orders (Zero Data Leakage)")
display(df_ml_raw.limit(5))

# COMMAND ----------

# ==============================================================================
# Cell 2: Target Class Balance & Feature Profiling
# ==============================================================================

print("=== Class Balance Profile ===")
df_ml_raw.groupBy("label").agg(
    count("order_id").alias("order_count"),
    _round(count("order_id") * 100.0 / total_records, 2).alias("percentage")
).orderBy("label").show()

# COMMAND ----------

# ==============================================================================
# Cell 3: Spark MLlib Pipeline (VectorAssembler & RandomForestClassifier)
# ==============================================================================

from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.classification import RandomForestClassifier

print("=== Step 1: Feature Transformation & Pipeline Assembly ===")

# Index categorical features
wh_indexer = StringIndexer(inputCol="warehouse_id_str", outputCol="wh_idx", handleInvalid="keep")
state_indexer = StringIndexer(inputCol="shipping_state", outputCol="state_idx", handleInvalid="keep")

# Numeric + encoded feature vector list
feature_cols = [
    "promised_days",
    "shipping_fee",
    "total_order_weight_kg",
    "total_units_ordered",
    "distinct_item_count",
    "customer_tenure_days",
    "order_month",
    "order_day_of_week",
    "is_q4_holiday_surge",
    "is_interstate_lane",
    "wh_idx",
    "state_idx"
]

assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")

# Initialize Random Forest Classifier
# Using 50 trees, maxDepth 8 for strong non-linear pattern capture without cluster memory strain
rf = RandomForestClassifier(
    featuresCol="features",
    labelCol="label",
    numTrees=50,
    maxDepth=8,
    seed=42
)

ml_pipeline = Pipeline(stages=[wh_indexer, state_indexer, assembler, rf])

print("=== Step 2: 80/20 Train-Test Split ===")
train_df, test_df = df_ml_raw.randomSplit([0.80, 0.20], seed=42)

train_count = train_df.count()
test_count = test_df.count()
print(f"Training Set : {train_count:,} orders (80%)")
print(f"Testing Set  : {test_count:,} orders (20%)")

print("=== Step 3: Training Spark MLlib Random Forest Model ===")
model = ml_pipeline.fit(train_df)
print(" Random Forest Classifier Trained Successfully!")

# COMMAND ----------

# ==============================================================================
# Cell 4: Model Evaluation & Feature Importance Ranking
# ==============================================================================

from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
import pandas as pd

print("=== Step 1: Model Inference on Unseen Test Dataset ===")
predictions = model.transform(test_df)
predictions.cache()

# 1. ROC-AUC and PR-AUC Evaluators
roc_evaluator = BinaryClassificationEvaluator(labelCol="label", metricName="areaUnderROC")
pr_evaluator = BinaryClassificationEvaluator(labelCol="label", metricName="areaUnderPR")

roc_auc = roc_evaluator.evaluate(predictions)
pr_auc = pr_evaluator.evaluate(predictions)

# 2. Multi-class classification metrics (Accuracy, Precision, Recall, F1)
multi_evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction")
accuracy = multi_evaluator.evaluate(predictions, {multi_evaluator.metricName: "accuracy"})
precision = multi_evaluator.evaluate(predictions, {multi_evaluator.metricName: "weightedPrecision"})
recall = multi_evaluator.evaluate(predictions, {multi_evaluator.metricName: "weightedRecall"})
f1 = multi_evaluator.evaluate(predictions, {multi_evaluator.metricName: "f1"})

print("\n" + "="*50)
print("     PREDICTIVE MODEL PERFORMANCE BENCHMARKS")
print("="*50)
print(f" Area Under ROC (ROC-AUC) : {roc_auc:.4f}  (Target: > 0.75)")
print(f" Area Under PR (PR-AUC)   : {pr_auc:.4f}")
print(f" Classification Accuracy   : {accuracy*100:.2f}%")
print(f" Weighted Precision        : {precision*100:.2f}%")
print(f" Weighted Recall           : {recall*100:.2f}%")
print(f" F1-Score                  : {f1:.4f}")
print("="*50)

# 3. Confusion Matrix Breakdown
print("\n=== Confusion Matrix Breakdown ===")
predictions.groupBy("label", "prediction").agg(count("*").alias("count")) \
    .withColumn("classification", 
        when((col("label") == 1) & (col("prediction") == 1.0), "True Positive (Late Detected)")
        .when((col("label") == 0) & (col("prediction") == 0.0), "True Negative (On-Time Confirmed)")
        .when((col("label") == 0) & (col("prediction") == 1.0), "False Positive (False Alarm)")
        .otherwise("False Negative (Missed Delay)")
    ).select("label", "prediction", "classification", "count").show(truncate=False)

# 4. Feature Importance Extraction
print("=== Feature Importance Analysis ===")
rf_stage = model.stages[-1]
importances = rf_stage.featureImportances.toArray()

fi_df = pd.DataFrame({
    "Feature": feature_cols,
    "Importance": importances
}).sort_values(by="Importance", ascending=False).reset_index(drop=True)

fi_df["Importance_Pct"] = (fi_df["Importance"] * 100).round(2)
print(fi_df.to_string(index=False))
