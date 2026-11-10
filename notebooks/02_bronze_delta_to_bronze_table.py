# Databricks notebook source
# MAGIC %md
# MAGIC ##### 1. Define your source Volume path and target table name

# COMMAND ----------

#Get File nae parameterised
dbutils.widgets.text("file_name","")
p_file_name = dbutils.widgets.get("file_name")

# COMMAND ----------

table_name = f"glrecon.bronze.{p_file_name}"
# Ensure this points to the actual Delta data folder, not a streaming checkpoint folder
delta_path = f"/Volumes/workspace/default/bronze/checkpoint_{p_file_name}" 

if spark.catalog.tableExists(table_name):
    spark.sql(f"""
        ALTER TABLE {table_name}
        SET TBLPROPERTIES ('delta.feature.timestampNtz' = 'supported')
    """)
else:
    df = spark.read.format("delta").load(delta_path)
    (
        df.write.format("delta")
        .option("delta.feature.timestampNtz", "supported")
        .saveAsTable(table_name)
    )

# COMMAND ----------

# MAGIC %sql
# MAGIC --SHOW TBLPROPERTIES glrecon.bronze.stock_transactions

# COMMAND ----------

from pyspark.sql import DataFrame

def write_to_bronze_generic(df: DataFrame, target_table: str, mode: str = "overwrite"):
    """
    Safely writes a DataFrame to a Unity Catalog Delta table, dynamically adding 
    any new columns via ALTER TABLE to bypass Table ACL schema migration limits.
    """
    # 1. Check if the target table already exists
    if spark.catalog.tableExists(target_table):
        target_cols = {field.name.lower() for field in spark.table(target_table).schema}
        
        # 2. Identify columns present in the DataFrame but missing from the target table
        missing_cols = [
            f"{field.name} {field.dataType.simpleString()}"
            for field in df.schema
            if field.name.lower() not in target_cols
        ]
        
        # 3. Explicitly add missing columns via ALTER TABLE
        if missing_cols:
            cols_sql = ", ".join(missing_cols)
            print(f"Adding new columns to {target_table}: {cols_sql}")
            spark.sql(f"ALTER TABLE {target_table} ADD COLUMNS ({cols_sql})")

 
    # 4. Write the DataFrame to the Bronze table
    df.write.format("delta").mode(mode).saveAsTable(target_table)
    print(f"Successfully wrote data to {target_table}")


# COMMAND ----------

# MAGIC %sql
# MAGIC -- select * from glrecon.bronze.stock_transactions order by product_id