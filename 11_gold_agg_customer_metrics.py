# Databricks notebook source
from pyspark.sql.functions import col, countDistinct, sum, avg, min, max, round, current_timestamp

# 1. Load Gold Sales Fact and Silver Reviews
df_sales = spark.table("olist_data_engineering.gold.fct_sales")
df_reviews = spark.table("olist_data_engineering.silver.order_reviews")

# 2. Pre-aggregate reviews per order (handles multiple reviews per order)
df_order_ratings = (
    df_reviews
    .groupBy("order_id")
    .agg(avg("review_score").alias("order_review_score"))
)

# 3. Join and calculate Customer Lifetime & Behavioral Metrics
df_customer_metrics = (
    df_sales
    .filter(col("order_status") == "delivered")
    .join(df_order_ratings, on="order_id", how="left")
    .groupBy(
        "customer_id",
        "customer_city",
        "customer_state"
    )
    .agg(
        countDistinct("order_id").alias("lifetime_orders"),
        countDistinct("order_item_key").alias("lifetime_items_bought"),
        round(sum("total_item_value"), 2).alias("lifetime_spend"),
        round(avg("total_item_value"), 2).alias("avg_order_value"),
        min("order_date").alias("first_purchase_date"),
        max("order_date").alias("latest_purchase_date"),
        round(avg("order_review_score"), 1).alias("avg_satisfaction_score")
    )
    .orderBy(col("lifetime_spend").desc())
    .withColumn("_created_at", current_timestamp())
)

# 4. Save to Gold Delta Table
(
    df_customer_metrics.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("olist_data_engineering.gold.agg_customer_metrics")
)

# 5. Preview Output
display(spark.table("olist_data_engineering.gold.agg_customer_metrics").limit(10))