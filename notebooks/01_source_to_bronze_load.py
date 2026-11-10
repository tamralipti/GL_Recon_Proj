# Databricks notebook source
# MAGIC %md
# MAGIC ##### Read the CSV files and Loads in the bronze folder

# COMMAND ----------

from pyspark.sql import functions as F
#Get File nae parameterised
dbutils.widgets.text("file_name","")
p_file_name = dbutils.widgets.get("file_name")

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Setting filepath and filename

# COMMAND ----------

p_file_name = dbutils.widgets.get("file_name")
##df = spark.read.format("csv").option("ignoreCorruptFiles", "true").load(f"abfss://source@glreconstorageaaccount.dfs.core.windows.net/{p_file_name}") ## When file placed in external location of Azure Storage Account
filepath = f"dbfs:/Volumes/workspace/default/source/{p_file_name}.csv" 


# COMMAND ----------

# MAGIC %md
# MAGIC ######Read the data

# COMMAND ----------

df = spark.read.format("csv")\
    .option("header","true")\
    .option("inferSchema","true")\
    .load(filepath) 

df = df.withColumn("ingestion_date", localtimestamp())

df.write.format("delta")\
    .mode("overwrite")\
    .save(
        f"/Volumes/workspace/default/bronze/checkpoint_{p_file_name}"
    )



# COMMAND ----------

from delta.tables import DeltaTable

folder_path = f"/Volumes/workspace/default/bronze/checkpoint_{p_file_name}"
delta_table = DeltaTable.forPath(spark, folder_path)

# Get the latest version number
latest_version = delta_table.history(1).select("version").collect()[0][0]

# Read the table at that exact version
df = spark.read.format("delta").option("versionAsOf", latest_version).load(folder_path)
display(df)
