# Databricks notebook source
df = spark.table("olist_data_engineering.bronze.olist_sellers")

display(df.limit(10))

# COMMAND ----------

df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import *

display(
    df.select([
        count(when(col(c).isNull() , c )).alias(c)
        for c in df.columns
    ])
)

# COMMAND ----------

from pyspark.sql.functions import col, lpad, initcap, upper, trim, current_timestamp

# 1. Clean, format postal codes, standardize text, and add audit timestamp
df_silver_sellers = (
    df
    .filter(col("seller_id").isNotNull())
    .select(
        col("seller_id"),
        # Restore leading zeros to ensure exact 5-digit zip code prefix
        lpad(col("seller_zip_code_prefix").cast("string"), 5, "0").alias("seller_zip_code_prefix"),
        initcap(trim(col("seller_city"))).alias("seller_city"),
        upper(trim(col("seller_state"))).alias("seller_state")
    )
    .withColumn("_transformed_at", current_timestamp())
)

# 2. Persist to Silver Delta table
(
    df_silver_sellers.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("olist_data_engineering.silver.dim_sellers")
)

# 3. Preview output
display(spark.table("olist_data_engineering.silver.dim_sellers").limit(5))