# Databricks notebook source
df = spark.table("olist_data_engineering.bronze.olist_products")

display(df.limit(10))

# COMMAND ----------

df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import *

display(
    df.select([
        count(when(col(c).isNull() , c)).alias(c)
        for c in df.columns
    ])
)

# COMMAND ----------

# Before writing to Silver, verify whether product_id is 100% unique.

total_products = df.count()
distinct_products = df.select("product_id").distinct().count()

print(f"Total rows: {total_products}")
print(f"Distinct product IDs: {distinct_products}")


# COMMAND ----------

from pyspark.sql.functions import col, coalesce, lit, current_timestamp

# 1. Clean, rename misspelled columns, fill null categories, downcast types
df_silver_products = (
    df
    .filter(col("product_id").isNotNull())
    .select(
        col("product_id"),
        coalesce(col("product_category_name"), lit("unknown")).alias("product_category_name"),
        col("product_name_lenght").cast("int").alias("product_name_length"),
        col("product_description_lenght").cast("int").alias("product_description_length"),
        col("product_photos_qty").cast("int").alias("product_photos_qty"),
        col("product_weight_g").cast("int").alias("product_weight_g"),
        col("product_length_cm").cast("int").alias("product_length_cm"),
        col("product_height_cm").cast("int").alias("product_height_cm"),
        col("product_width_cm").cast("int").alias("product_width_cm")
    )
    .withColumn("_transformed_at", current_timestamp())
)

# 2. Persist to Silver Delta table
(
    df_silver_products.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("olist_data_engineering.silver.dim_products")
)

# 3. Preview output
display(spark.table("olist_data_engineering.silver.dim_products").limit(5))