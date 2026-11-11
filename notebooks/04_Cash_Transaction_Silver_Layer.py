# Databricks notebook source
# MAGIC %md
# MAGIC ##### Silver Layer data load for Cash Transactions

# COMMAND ----------

# Import required libraries
from pyspark.sql import functions as F
from delta.tables import DeltaTable
from pyspark.sql import types as T

# COMMAND ----------

# Cash Transactions in Silver layer 
silver_table_name = "glrecon.silver.silver_cash_transactions"

# COMMAND ----------

#CRITICAL FIX: Ensure the schema container exists inside the catalog

spark.sql("CREATE SCHEMA IF NOT EXISTS glrecon.silver")

# COMMAND ----------

# Read the cash transactions from bronze
df_cashTrans = spark.table("glrecon.bronze.cash_transactions")

# COMMAND ----------

display(df_cashTrans)

# COMMAND ----------

# MAGIC %md
# MAGIC ####### Clean Bronze data 
# MAGIC 1. strip leading 'CASH' from cash transaction id 
# MAGIC 2. put cash in a separate column
# MAGIC 3. strip leading stk from reference_id
# MAGIC 4. rename reference id to stock_transaction_id
# MAGIC 5. rename transaction_type to TransDescription 
# MAGIC 6. the value should not have '_'
# MAGIC 7. amount is renamed to CashTransAmount 
# MAGIC 8. covert the value to decimal)

# COMMAND ----------

df_cashTrans = (
    df_cashTrans
    .withColumn("CashTransactionId", (F.regexp_replace(F.col("cash_transaction_id"), "^CASH", "")).cast("long")+1000)
    .drop("cash_transaction_id")
    .withColumn("StockTransactionId", (F.regexp_replace(F.col("reference_id"), "^STK", "")).cast("long")+10000)
    .withColumn("amount", F.col("amount").cast(T.DecimalType(10, 2)))
    .withColumnRenamed("ingestion_date", "CreateDate")
    .drop("reference_id")
    .withColumn("TransDescription", (F.regexp_replace(F.col("transaction_type"), "_", " ")))
    .drop("transaction_type")
    .withColumnRenamed("amount", "CashTransAmount")
    .drop("amount")
    .withColumn("UpdateDate", F.current_timestamp())
    .withColumnRenamed("transaction_date", "TransactionDate")
)

df_cashTrans = df_cashTrans.select(
    "CashTransactionId",
    "StockTransactionId",
    "TransDescription",
    "CashTransAmount",
    "TransactionDate",
    "CreateDate",
    "UpdateDate"
) 

# COMMAND ----------


# Check if the table exists in your Unity Catalog
if not spark.catalog.tableExists(silver_table_name):
    print(f"Table {silver_table_name} does not exist. Initializing Silver table...")    
    df_cashTrans.write.format("delta").mode("append").saveAsTable(silver_table_name)
    print("Silver table successfully created and populated.")

else:
    print(f"Table {silver_table_name} exists. Proceeding with MERGE pipeline...")

    # SUBSEQUENT RUNS: Safely reference the existing table to execute the merge
    silver_table = DeltaTable.forName(spark, silver_table_name)

    (
        silver_table.alias("target")
        .merge(
        df_cashTrans.alias("source"),
        """
        target.CashTransactionId = source.CashTransactionId AND
        target.StockTransactionId = source.StockTransactionId
        """
    )
        
    .whenMatchedUpdate(
        condition="""
                NOT (target.TransDescription <=> source.TransDescription) OR
                NOT (target.CashTransAmount <=> source.CashTransAmount) OR
                NOT (target.TransactionDate <=> source.TransactionDate) OR
                NOT (target.CreateDate <=> source.CreateDate) 
            """,
            set={

                "UpdateDate": F.current_timestamp()
            }
        )
    .whenNotMatchedInsert(
        values={
            "CashTransactionId": "source.CashTransactionId",
            "StockTransactionId": "source.StockTransactionId",
            "TransDescription": "source.TransDescription",
            "CashTransAmount": "source.CashTransAmount",
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
# MAGIC
# MAGIC SELECT * FROM glrecon.silver.silver_cash_transactions