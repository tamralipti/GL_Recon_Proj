# Databricks notebook source
# Import required libraries
from pyspark.sql import functions as F
from delta.tables import DeltaTable
from pyspark.sql import types as T

# COMMAND ----------

# Stock Transactions in Silver layer 
silver_table_name = "glrecon.silver.silver_stock_transactions"

# COMMAND ----------

#CRITICAL FIX: Ensure the schema container exists inside the catalog

spark.sql("CREATE SCHEMA IF NOT EXISTS glrecon.silver")

# COMMAND ----------

# Read the cash transactions from bronze
df_stockTrans = spark.table("glrecon.bronze.stock_transactions")
display(df_stockTrans)

# COMMAND ----------

df_stockTrans = (
    df_stockTrans
    .withColumn("StockTransactionId", (F.regexp_replace(F.col("transaction_id"), "^STK", "")).cast("long")+10000)
    .drop("transaction_id")
    .withColumn("ProductId", (F.regexp_replace(F.col("product_id"), "^P", "")))
    .withColumnRenamed("transaction_type", "TransDescription")
    .withColumnRenamed("quantity", "Quantity")
    .withColumnRenamed("ingestion_date", "CreateDate")
    .withColumnRenamed("unit_price", "UnitPrice")
    .drop("transaction_id")
    .drop("amount")
    .drop("transaction_type")
    .drop("product_id")
    .withColumn("UpdateDate", F.current_timestamp())
    .withColumnRenamed("transaction_date", "TransactionDate")
)

df_stockTrans = df_stockTrans.select(
    "StockTransactionId",
    "ProductId",
    "TransDescription",
    "Quantity",
    "UnitPrice",
    "TransactionDate",
    "CreateDate",
    "UpdateDate"
) 

display(df_stockTrans)

# COMMAND ----------


# Check if the table exists in your Unity Catalog
if not spark.catalog.tableExists(silver_table_name):
    print(f"Table {silver_table_name} does not exist. Initializing Silver table...")    
    df_stockTrans.write.format("delta").mode("append").saveAsTable(silver_table_name)
    print("Silver table successfully created and populated.")

else:
    print(f"Table {silver_table_name} exists. Proceeding with MERGE pipeline...")

    # SUBSEQUENT RUNS: Safely reference the existing table to execute the merge
    silver_table = DeltaTable.forName(spark, silver_table_name)

    (
        silver_table.alias("target")
        .merge(
        df_stockTrans.alias("source"),
        """
        target.StockTransactionId = source.StockTransactionId
        """
    )
    .whenMatchedUpdate(
        condition="""
                NOT (target.ProductId <=> source.ProductId) OR
                NOT (target.TransDescription <=> source.TransDescription) OR
                NOT (target.Quantity <=> source.Quantity) OR
                NOT (target.TransactionDate <=> source.TransactionDate) OR
                NOT (target.CreateDate <=> source.CreateDate) OR
                NOT (target.UpdateDate <=> source.UpdateDate)
            """,
            set={

                "UpdateDate": F.current_timestamp()
            }
        )
    .whenNotMatchedInsert(
        values={
            "StockTransactionId": "source.StockTransactionId",
            "ProductId": "source.ProductId",
            "TransDescription": "source.TransDescription",
            "Quantity": "source.Quantity",
            "Unit"
            "TransactionDate": "source.TransactionDate",
            "CreateDate": "source.CreateDate",
            "UpdateDate": F.current_timestamp()

        }
    )
    .execute()
    )
        
print("Merge execution completed successfully.")

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from glrecon.silver.silver_stock_transactions