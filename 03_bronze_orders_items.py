# Databricks notebook source
df = spark.table("olist_data_engineering.bronze.olist_order_items")
display(df.limit(5))

# COMMAND ----------

df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import *
display (
    df.select([
               count(when(col(c).isNull(), c)).alias(c) 
               for c in df.columns 
               ])
)

# COMMAND ----------

from pyspark.sql.functions import col, concat_ws, current_timestamp
from pyspark.sql.types import DecimalType

# 1. Transform and clean
df_silver_order_items = (
    df
    # Safeguard against corrupt key records
    .filter(col("order_id").isNotNull() & col("order_item_id").isNotNull())
    .select(
        # Unique row key
        concat_ws("_", col("order_id"), col("order_item_id")).alias("order_item_key"),
        col("order_id"),
        col("order_item_id").cast("int").alias("order_item_id"),
        col("product_id"),
        col("seller_id"),
        col("shipping_limit_date"),
        # Exact monetary precision
        col("price").cast(DecimalType(10, 2)).alias("price"),
        col("freight_value").cast(DecimalType(10, 2)).alias("freight_value"),
        # Total item spend calculation
        (col("price") + col("freight_value")).cast(DecimalType(10, 2)).alias("total_item_value")
    )
    # Pipeline audit timestamp
    .withColumn("_transformed_at", current_timestamp())
)

# 2. Persist to Silver Delta table
(
    df_silver_order_items.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("olist_data_engineering.silver.order_items")
)

# 3. Preview output
display(spark.table("olist_data_engineering.silver.order_items").limit(5))