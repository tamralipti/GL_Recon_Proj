# Databricks notebook source
# MAGIC %md
# MAGIC ### Setup the files to be regularly loaded 

# COMMAND ----------

datasets = [
    {
        "file_name": "stock_transactions.csv"
    },
    {
        "file_name": "cash_transactions.csv"
    },
    {
        "file_name": "overhead_costs.csv"
    },
    {   
        "file_name": "product_master.csv"
    }
]

# COMMAND ----------

dbutils.jobs.taskValues.set("Output_Datasets", datasets)