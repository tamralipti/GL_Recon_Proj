# Databricks notebook source
# Import required libraries
from pyspark.sql import functions as F
from delta.tables import DeltaTable
from pyspark.sql import types as T

# COMMAND ----------

# Stock Transactions in Silver layer 
silver_table_name = "glrecon.silver.silver_overhead_costs"

# COMMAND ----------

#CRITICAL FIX: Ensure the schema container exists inside the catalog

spark.sql("CREATE SCHEMA IF NOT EXISTS glrecon.silver")

# COMMAND ----------

# Read the overhead costs from bronze
df_overheadCosts = spark.table("glrecon.bronze.overhead_costs")
display(df_overheadCosts)

# COMMAND ----------

df_overheadCosts = (
    df_overheadCosts
    .withColumn("OverheadId", (F.regexp_replace(F.col("overhead_id"), "^OVH", "")).cast("long")+30000)
    .drop("overhead_id")
    .withColumnRenamed("category", "Category")
    .withColumnRenamed("amount", "Amount")
    .withColumnRenamed("cost_date", "CostDate")
    .withColumnRenamed("ingestion_date", "CreateDate")
    .withColumn("UpdateDate", F.current_timestamp())
)

df_overheadCosts = df_overheadCosts.select(
    "OverheadId",
    "Category",
    "Amount",
    "CostDate",
    "CreateDate",
    "UpdateDate"
) 

display(df_overheadCosts)

# COMMAND ----------


# Check if the table exists in your Unity Catalog
if not spark.catalog.tableExists(silver_table_name):
    print(f"Table {silver_table_name} does not exist. Initializing Silver table...")    
    df_overheadCosts.write.format("delta").mode("append").saveAsTable(silver_table_name)
    print("Silver table successfully created and populated.")

else:
    print(f"Table {silver_table_name} exists. Proceeding with MERGE pipeline...")

    # SUBSEQUENT RUNS: Safely reference the existing table to execute the merge
    silver_table = DeltaTable.forName(spark, silver_table_name)

    (
        silver_table.alias("target")
        .merge(
        df_overheadCosts.alias("source"),
        """
        target.OverheadId = source.OverheadId
        """
    )

    .whenNotMatchedInsert(
        values={
            "OverheadId": "source.OverheadId",
            "Category": "source.Category",
            "Amount": "source.Amount",
            "CostDate": "source.CostDate",
            "CreateDate": "source.CreateDate",
            "UpdateDate": F.current_timestamp()

        }
    )
    .execute()
    )
        
print("Merge execution completed successfully.")