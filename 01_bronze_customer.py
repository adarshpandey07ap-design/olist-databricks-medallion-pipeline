# Databricks notebook source
df = spark.table("olist_data_engineering.bronze.olist_customers")

df.display()

# COMMAND ----------

df.count()

# COMMAND ----------

df.show()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# Finding Null Values 
from pyspark.sql.functions import col, count, when

df.select([
    count(when(col(c).isNull(), c)).alias(c)
    for c in df.columns
]).show()

# COMMAND ----------

# Checking Duplicates by row
print("Total rows:", df.count())
print("Distinct rows:", df.distinct().count())

# COMMAND ----------

# by column
df.groupBy("customer_id").count().filter(col("count")> 1).show()
df.groupBy("customer_unique_id").count().filter(col("count")> 1).show()



# COMMAND ----------

df.select("customer_state").distinct().show()
df.select("customer_state").distinct().count()

# COMMAND ----------

df.select("customer_city").distinct().show(50,truncate=False)
df.select("customer_city").distinct().count()


# COMMAND ----------

from pyspark.sql.functions import col, trim, initcap, upper, lpad, current_timestamp

# 2. Apply transformations
df_silver_customers = (
    df
    .select(
        col("customer_id"),
        col("customer_unique_id"),
        lpad(col("customer_zip_code_prefix").cast("string"), 5, "0").alias("customer_zip_code_prefix"),
        initcap(trim(col("customer_city"))).alias("customer_city"),
        upper(trim(col("customer_state"))).alias("customer_state")
    )
    .withColumn("_transformed_at", current_timestamp())
)

# 3. Write into Silver Delta table
(
    df_silver_customers.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("olist_data_engineering.silver.dim_customers")
)

display(spark.table("olist_data_engineering.silver.dim_customers").limit(5))