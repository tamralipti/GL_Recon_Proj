# Databricks notebook source
# MAGIC %md
# MAGIC ##### 1. Define your source Volume path and target table name

# COMMAND ----------

#Get File nae parameterised
dbutils.widgets.text("file_name","")
p_file_name = dbutils.widgets.get("file_name")

# COMMAND ----------

folder_path = f"/Volumes/workspace/default/bronze/checkpoint_{p_file_name}"
target_table = f"glrecon.bronze.{p_file_name}"

# 2. Ensure the bronze schema exists in Unity Catalog
spark.sql("CREATE SCHEMA IF NOT EXISTS glrecon.bronze")

# 3. Read the Delta data from the Volume
df = spark.read.format("delta").load(folder_path)

# 4. Write the data into a managed Bronze Delta table
df.write.format("delta").mode("overwrite").saveAsTable(target_table)

# 5. Verify the table was created
display(spark.table(target_table))
