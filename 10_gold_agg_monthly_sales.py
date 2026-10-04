# Databricks notebook source
from pyspark.sql.functions import col, countDistinct, sum, avg, round, current_timestamp

# 1. Read from the Gold Sales Fact Table
df_sales = spark.table("olist_data_engineering.gold.fct_sales")

# 2. Compute aggregated monthly category performance
df_monthly_sales = (
    df_sales
    # Filter for valid, completed business orders
    .filter(col("order_status") == "delivered")
    .groupBy("order_year", "order_month", "category_name")
    .agg(
        countDistinct("order_id").alias("total_orders"),
        countDistinct("customer_id").alias("total_unique_customers"),
        round(sum("price"), 2).alias("total_merchandise_revenue"),
        round(sum("freight_value"), 2).alias("total_freight_revenue"),
        round(sum("total_item_value"), 2).alias("total_gross_revenue"),
        round(avg("price"), 2).alias("avg_item_price")
    )
    .orderBy("order_year", "order_month", col("total_gross_revenue").desc())
    .withColumn("_created_at", current_timestamp())
)

# 3. Write to Gold Delta Table
(
    df_monthly_sales.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("olist_data_engineering.gold.agg_monthly_sales")
)

# 4. Preview Output
display(spark.table("olist_data_engineering.gold.agg_monthly_sales").limit(10))