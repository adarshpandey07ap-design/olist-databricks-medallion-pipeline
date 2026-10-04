# Databricks notebook source
# Load all required Silver tables
df_items = spark.table("olist_data_engineering.silver.order_items")
df_orders = spark.table("olist_data_engineering.silver.orders")
df_products = spark.table("olist_data_engineering.silver.dim_products")
df_translation = spark.table("olist_data_engineering.silver.product_category_name_translation")
df_customers = spark.table("olist_data_engineering.silver.dim_customers")
df_sellers = spark.table("olist_data_engineering.silver.dim_sellers")

# COMMAND ----------

from pyspark.sql.functions import col, coalesce, lit, to_date, year, month, current_timestamp

# 1. Enrich products with translated English category names
df_products_enriched = (
    df_products.join(
        df_translation,
        on="product_category_name",
        how="left"
    )
    .select(
        col("product_id"),
        col("product_category_name"),
        # Use English display name if mapped, otherwise default back or to 'Unknown'
        coalesce(col("category_display_name"), col("product_category_name"), lit("Unknown")).alias("category_name"),
        col("product_weight_g")
    )
)

# 2. Join into Gold Sales Fact Table
df_gold_fct_sales = (
    df_items.alias("items")
    # Join Orders (Status & Timestamps)
    .join(
        df_orders.alias("ord"),
        col("items.order_id") == col("ord.order_id"),
        how="inner"
    )
    # Join Customers (Buyer Geography)
    .join(
        df_customers.alias("cust"),
        col("ord.customer_id") == col("cust.customer_id"),
        how="left"
    )
    # Join Sellers (Seller Geography)
    .join(
        df_sellers.alias("sell"),
        col("items.seller_id") == col("sell.seller_id"),
        how="left"
    )
    # Join Enriched Products (Category Metadata)
    .join(
        df_products_enriched.alias("prod"),
        col("items.product_id") == col("prod.product_id"),
        how="left"
    )
    # Select clean dimensional attributes and business metrics
    .select(
        col("items.order_item_key"),
        col("items.order_id"),
        col("items.order_item_id"),
        col("ord.customer_id"),
        col("items.seller_id"),
        col("items.product_id"),
        col("prod.category_name"),
        col("ord.order_status"),
        col("ord.order_purchase_timestamp"),
        to_date(col("ord.order_purchase_timestamp")).alias("order_date"),
        year(col("ord.order_purchase_timestamp")).alias("order_year"),
        month(col("ord.order_purchase_timestamp")).alias("order_month"),
        col("cust.customer_city"),
        col("cust.customer_state"),
        col("sell.seller_city"),
        col("sell.seller_state"),
        col("items.price"),
        col("items.freight_value"),
        col("items.total_item_value")
    )
    .withColumn("_created_at", current_timestamp())
)

# 3. Write to Gold Delta Table
(
    df_gold_fct_sales.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("olist_data_engineering.gold.fct_sales")
)

# 4. Preview Gold Fact Table
display(spark.table("olist_data_engineering.gold.fct_sales").limit(10))