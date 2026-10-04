# Databricks notebook source
df_order = spark.table("olist_data_engineering.bronze.olist_orders")
display(df_order.limit(5))

# COMMAND ----------

df_order.printSchema()

# COMMAND ----------

# checking null
from pyspark.sql.functions import col , when , count

df_order.select([
   
    count(when(col(c).isNull(), c )).alias(c)
    for c in df_order.columns
                 
]).show()

# COMMAND ----------

df_order.groupBy("order_status").count().distinct().show()
df_order.count()

# COMMAND ----------

# Checking Duplicates by row
print("Total rows:", df_order.count())
print("Distinct rows:", df_order.distinct().count())

# COMMAND ----------

# checking Duplicates by columns 
df_order.groupBy("order_id").count().filter(col("count")> 1).show()
df_order.groupBy("customer_id").count().filter(col("count")> 1).show()

# COMMAND ----------

from pyspark.sql.functions import col, datediff, current_timestamp

# 1. Clean and add audit metadata + delivery delay metric
df_silver_orders = (
    df_order
    # Protect against corrupt records with missing primary/foreign keys
    .filter(col("order_id").isNotNull() & col("customer_id").isNotNull())
    # Add business calculation: days difference between actual and estimated delivery
    .withColumn(
        "delivery_delay_days", 
        datediff(col("order_delivered_customer_date"), col("order_estimated_delivery_date"))
    )
    # Add audit timestamp
    .withColumn("_transformed_at", current_timestamp())
)

# 2. Persist to Silver Delta table
(
    df_silver_orders.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("olist_data_engineering.silver.orders")
)

# 3. Preview output
display(spark.table("olist_data_engineering.silver.orders").limit(5))