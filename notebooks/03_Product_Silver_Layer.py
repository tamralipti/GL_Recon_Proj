# Databricks notebook source
# MAGIC %md
# MAGIC ##### Silver Layer data load for Product

# COMMAND ----------

from pyspark.sql import functions as F
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Check Silver layer product table if that exists

# COMMAND ----------

silver_table_name = "glrecon.silver.silver_dim_products"

# COMMAND ----------

# MAGIC %md
# MAGIC ###### CRITICAL FIX: Ensure the schema container exists inside the catalog

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS glrecon.silver")

# COMMAND ----------

# MAGIC %md
# MAGIC #####Check if the Silver table already exists in Unity Catalog

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Read the product data from bronze

# COMMAND ----------


df_product = spark.table("glrecon.bronze.product_master")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 1. Clean Bronze data (strip leading 'P' from product_id)

# COMMAND ----------

df_product = df_product.withColumn(
    "product_id", 
    F.regexp_replace(F.col("product_id"), "^P", "")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ###### 2. Check if the Silver table already exists

# COMMAND ----------


# Check if the table exists in your Unity Catalog
if not spark.catalog.tableExists(silver_table_name):
    print(f"Table {silver_table_name} does not exist. Initializing Silver table...")
    
    # FIRST RUN: Create and populate the table for the first time
    df_initial = (
        df_product
        .withColumn("is_active", F.lit(1))
        .withColumnRenamed("ingestion_date", "create_date")
        .withColumn("update_date", F.current_timestamp())
    )
    
    df_initial.write.format("delta").mode("overwrite").saveAsTable(silver_table_name)
    print("Silver table successfully created and populated.")

else:
    print(f"Table {silver_table_name} exists. Proceeding with MERGE pipeline...")
    
    # SUBSEQUENT RUNS: Safely reference the existing table to execute the merge
    silver_table = DeltaTable.forName(spark, silver_table_name)

    (
        silver_table.alias("target")
        .merge(
            df_product.alias("source"),
            "target.product_id = source.product_id"
        )
        # 1. In Bronze & Silver + changes detected -> Update & ensure active status
        .whenMatchedUpdate(
            condition="""
                NOT (target.product_name <=> source.product_name) OR 
                NOT (target.category <=> source.category) OR 
                target.is_active = 0
            """,
            set={
                "product_name": "source.product_name",
                "category": "source.category",
                "is_active": F.lit(1),
                "update_date": F.current_timestamp()
            }
        )
        # 2. In Bronze & NOT in Silver -> Insert new product as active
        .whenNotMatchedInsert(
            values={
                "product_id": "source.product_id",
                "product_name": "source.product_name",
                "category": "source.category",
                "is_active": F.lit(1),
                "create_date": "source.ingestion_date",
                "update_date": F.current_timestamp()
            }
        )
        # 3. NOT in Bronze & in Silver -> Mark as inactive (soft delete)
        .whenNotMatchedBySourceUpdate(
            condition="target.is_active = 1",
            set={
                "is_active": F.lit(0),
                "update_date": F.current_timestamp()
            }
        )
        .execute()
    )
    print("Merge execution completed successfully.")