# Databricks notebook source
df = spark.table("olist_data_engineering.bronze.product_category_name")
display(df.limit(5))

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

from pyspark.sql.functions import col, trim, initcap, regexp_replace, current_timestamp

# 1. Clean, format, and standardize category names
df_silver_category_name = (
    df
    .filter(
        col("product_category_name").isNotNull() & 
        col("product_category_name_english").isNotNull()
    )
    .select(
        trim(col("product_category_name")).alias("product_category_name"),
        trim(col("product_category_name_english")).alias("product_category_name_english"),
        # Formatted readable label (e.g., "bed_bath_table" -> "Bed Bath Table")
        initcap(regexp_replace(trim(col("product_category_name_english")), "_", " ")).alias("category_display_name")
    )
    .distinct()
    .withColumn("_transformed_at", current_timestamp())
)

# 2. Persist to Silver Delta table
(
    df_silver_category_name.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("olist_data_engineering.silver.product_category_name_translation")
)

# 3. Preview output
display(spark.table("olist_data_engineering.silver.product_category_name_translation").limit(5))