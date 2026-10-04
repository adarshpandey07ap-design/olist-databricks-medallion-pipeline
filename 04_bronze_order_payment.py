# Databricks notebook source
df = spark.table("olist_data_engineering.bronze.olist_order_payments")
display(df.limit(10))

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# 1. Check for null values in payment columns
from pyspark.sql.functions import col, when, count

display(
    df.select([
        count(when(col(c).isNull(), c)).alias(c) 
        for c in df.columns
    ])
)



# COMMAND ----------

# 2. Check the distinct payment types
display(df.groupBy("payment_type").count().orderBy(col("count").desc()))

# COMMAND ----------

# to find not defined payment type

display(df.filter(col("payment_type") == "not_defined"))

# COMMAND ----------

from pyspark.sql.functions import col, concat_ws, current_timestamp
from pyspark.sql.types import DecimalType

# 1. Transform, clean, and filter out bad rows
df_silver_payments = (
    df
    # Drop records with null keys, unknown types, or zero value
    .filter(
        col("order_id").isNotNull() & 
        col("payment_sequential").isNotNull() & 
        (col("payment_type") != "not_defined") & 
        (col("payment_value") > 0)
    )
    .select(
        # Surrogate primary key
        concat_ws("_", col("order_id"), col("payment_sequential")).alias("payment_key"),
        col("order_id"),
        col("payment_sequential").cast("int").alias("payment_sequential"),
        col("payment_type"),
        col("payment_installments").cast("int").alias("payment_installments"),
        # Exact monetary precision
        col("payment_value").cast(DecimalType(10, 2)).alias("payment_value")
    )
    .withColumn("_transformed_at", current_timestamp())
)

# 2. Persist to Silver Delta Table
(
    df_silver_payments.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("olist_data_engineering.silver.order_payments")
)

# 3. Preview output
display(spark.table("olist_data_engineering.silver.order_payments").limit(5))