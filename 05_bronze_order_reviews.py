# Databricks notebook source
df = spark.table("olist_data_engineering.bronze.olist_order_reviews")

display(df.limit(10))

# COMMAND ----------

df.printSchema

# COMMAND ----------

from pyspark.sql.functions import *

display(
    df.select([
        count(when(col(c).isNull(), c )).alias(c)
        for c in df.columns
    ])
)

# COMMAND ----------

display(df.groupBy("review_score").count().orderBy("review_score"))

# COMMAND ----------

display(df.groupBy("review_id").count().filter(col("count") > 1).orderBy(col("count").desc()))

# COMMAND ----------

from pyspark.sql.functions import col, length, current_timestamp
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

# 1. Filter out corrupted rows and invalid scores
df_clean_reviews = (
    df
    .filter(
        (col("review_id").isNotNull()) &
        (length(col("review_id")) == 32) &          # Strips all the broken text lines
        (col("order_id").isNotNull()) &
        (col("review_score").between(1, 5))         # Ensures scores are strictly 1 to 5
    )
)

# 2. Deduplicate on review_id keeping the latest review_answer_timestamp
window_spec = Window.partitionBy("review_id").orderBy(col("review_answer_timestamp").desc_nulls_last())

df_silver_reviews = (
    df_clean_reviews
    .withColumn("row_num", row_number().over(window_spec))
    .filter(col("row_num") == 1)
    .drop("row_num")
    .select(
        col("review_id"),
        col("order_id"),
        col("review_score").cast("int").alias("review_score"),
        col("review_comment_title"),
        col("review_comment_message"),
        col("review_creation_date"),
        col("review_answer_timestamp")
    )
    .withColumn("_transformed_at", current_timestamp())
)

# 3. Persist to Silver Delta table
(
    df_silver_reviews.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("olist_data_engineering.silver.order_reviews")
)

# 4. Preview output
display(spark.table("olist_data_engineering.silver.order_reviews").limit(5))