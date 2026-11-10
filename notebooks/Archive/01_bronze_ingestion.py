# Databricks notebook source
# MAGIC %md
# MAGIC ## Making it dynamic

# COMMAND ----------

dbutils.widgets.text("file_name","")
p_file_name = dbutils.widgets.get("file_name")

# COMMAND ----------

p_file_name = dbutils.widgets.get("file_name")
df = spark.read.format("csv").option("ignoreCorruptFiles", "true").load(f"abfss://source@glreconstorageaaccount.dfs.core.windows.net/{p_file_name}")
df.count()


# COMMAND ----------

# MAGIC %md
# MAGIC ### Test path is working

# COMMAND ----------

display(
    dbutils.fs.ls(
        f"abfss://source@glreconstorageaaccount.dfs.core.windows.net/{p_file_name}"
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Check data can be read from the file

# COMMAND ----------

test_df = spark.read.format("csv") \
    .option("header", "true") \
    .load(
        f"abfss://source@glreconstorageaaccount.dfs.core.windows.net/{p_file_name}"
    )

display(test_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create folder name

# COMMAND ----------

import pyspark
filename = p_file_name
foldername = filename.removesuffix(".csv")
print(f"Calculated: {foldername}")

# Use the widget API instead of spark.conf.set
dbutils.widgets.text("my_sql_folder", foldername)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Reading And Writing

# COMMAND ----------

import pyspark
from pyspark.sql import SparkSession

df = (
    spark.read
    .format("csv")
    .option("header", "true")
    .load(f"abfss://source@glreconstorageaaccount.dfs.core.windows.net/{p_file_name}")
)
(
df.write \
    .format("delta") \
    .mode("overwrite") \
    .save(
        f"abfss://bronze@glreconstorageaaccount.dfs.core.windows.net/{foldername}"
    )
)


# COMMAND ----------

# MAGIC %md
# MAGIC - ### Query the dataset stored in the Bronze folder verifying that the ingestion pipeline worked

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT *
# MAGIC FROM delta.`abfss://bronze@glreconstorageaaccount.dfs.core.windows.net/${my_sql_folder}`
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Check for the schema

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG glrecon;
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS bronze;

# COMMAND ----------

display(
  dbutils.fs.ls(
    f"abfss://bronze@glreconstorageaaccount.dfs.core.windows.net/{foldername}"
  )
)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS glrecon.bronze.${my_sql_folder}
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://bronze@glreconstorageaaccount.dfs.core.windows.net/${my_sql_folder}';

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read from the table

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM glrecon.bronze.${my_sql_folder}