# Databricks notebook source
# MAGIC %md 
# MAGIC ## Initial Setup

# COMMAND ----------

# MAGIC %pip install https://github.com/databrickslabs/dbldatagen/releases/download/v.0.2.0-rc1-master/dbldatagen-0.2.0rc1-py3-none-any.whl

# COMMAND ----------

dbutils.library.restartPython() 

# COMMAND ----------

import dbldatagen as dg
import numpy as np
import pyspark.sql.functions as F
from datetime import timedelta, datetime

# spark.catalog.clearCache()
shuffle_partitions_requested = 8
partitions_requested = 8

spark.conf.set("spark.sql.shuffle.partitions", shuffle_partitions_requested)
#spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
#spark.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", 20000)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Set Base Path

# COMMAND ----------

username = spark.sql("SELECT regexp_replace(current_user(), '[^a-zA-Z0-9]', '_')").first()[0].split('_databricks')[0]
spark.sql(f"CREATE CATALOG IF NOT EXISTS {username}_quickstart")
spark.sql(f"USE CATALOG {username}_quickstart")
spark.sql(f'CREATE SCHEMA IF NOT EXISTS structured_streaming_meta COMMENT "A new Unity Catalog schema called structured_streaming_meta for a quickstart"')
spark.sql(f"USE SCHEMA structured_streaming_meta")
spark.sql(f'CREATE VOLUME IF NOT EXISTS {username}_quickstart.structured_streaming_meta.raw_data COMMENT "This is my example managed volume to store sample raw data"')
spark.sql(f'CREATE VOLUME IF NOT EXISTS {username}_quickstart.structured_streaming_meta.checkpointlocations COMMENT "This is my example managed volume to store offsets and schema changes for autoloader"')
spark.sql(f'CREATE VOLUME IF NOT EXISTS {username}_quickstart.structured_streaming_meta.ssm_data')

# COMMAND ----------

# CLEANING RAW DATA VOLUME AND PAST CHECKPOINTS FROM PREVIOUS RUNS
BASE_PATH = f'/Volumes/{username}_quickstart/structured_streaming_meta/raw_data'
dbutils.fs.rm(BASE_PATH, recurse= True)
dbutils.fs.rm(f'/Volumes/{username}_quickstart/structured_streaming_meta/checkpointlocations/', recurse= True)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Set Database

# COMMAND ----------

DB_NAME = "data_generator"
spark.sql(f"DROP DATABASE IF EXISTS {DB_NAME} CASCADE")
spark.sql(f"CREATE DATABASE {DB_NAME}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Set Tables Size

# COMMAND ----------

tables = {}
tables_size = {}
tables_size["customers"] = 10 * 100
tables_size["products"] = tables_size["customers"] * 2

interval = timedelta(days=1, hours=1)
start = datetime(2017,10,1,0,0,0)
end = datetime(2022,10,1,6,0,0)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tables Definition
# MAGIC #### customers

# COMMAND ----------

customers = tables["customers"] = {}
customers["customer_id"] = {"colType": "long", "uniqueValues": tables_size["customers"]}
customers["name"] = {"percentNulls": 0.01, "template": r'\\w \\w|\\w a. \\w'}
customers["alias"] = {"percentNulls": 0.01, "template": r'\\w \\w|\\w a. \\w'}
customers["customer_notes"] = {"text": dg.ILText(words=(1,8))}
customers["created_at"] = {"colType": "timestamp", "expr": "now()"}
customers["modified_at"] = {"colType": "timestamp", "expr": "now()"}
customers["op"] = {"colType": "string", "values": ["full_load"]}

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Products

# COMMAND ----------



products = tables["products"] = {}
products["product_id"] = {"colType": "long", "uniqueValues": tables_size["products"]}
products["product_name"] = {"percentNulls": 0.01, "template": r'\\w \\w|\\w a. \\w'}
products["alias"] = {"percentNulls": 0.01, "template": r'\\w \\w|\\w a. \\w'}
products["price"] = {"colType": "float", "values": list(np.arange(1, 200, 0.5)), "random": True, "distribution": "normal"}
products["product_notes"] = {"text": dg.ILText(words=(1,8))}
products["created_at"] = {"colType": "timestamp", "begin": start, "end": end, "interval":interval, "random": True}
products["modified_at"] = {"colType": "timestamp", "expr": "now()"}
products["op"] = {"colType": "string", "values": ["full_load"]}

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ## Generate and Write The Datasets 

# COMMAND ----------


df_tbls = {}
datascpec_tbls = {}
for tbl_name, tbl_columns in tables.items():
  dataspec = (dg.DataGenerator(spark, rows=tables_size[tbl_name], partitions=partitions_requested))
  for column_name, params in tbl_columns.items():
    dataspec.withColumn(column_name, **params)              
  df_tbls[tbl_name] = dataspec.build()
  datascpec_tbls[tbl_name] = dataspec
  print("Table:", tbl_name)
  df_tbls[tbl_name].printSchema()  
  display(df_tbls[tbl_name])
  print(f"Number of records {tbl_name}:", df_tbls[tbl_name].count())
  df_tbls[tbl_name].write.mode('overwrite').format("csv").save(BASE_PATH + "/" + tbl_name,header=True)
  #spark.sql(f"CREATE TABLE {DB_NAME}.{tbl_name} USING CSV LOCATION '{BASE_PATH}/{tbl_name}'")
  
  

# COMMAND ----------


for tbl_name, df_tbl in df_tbls.items():
  df_tbl.registerTempTable(tbl_name)

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ## Generate Inserts and Updates

# COMMAND ----------

df_tbls_changes = {}
for tbl_name, df_tbl in df_tbls.items():
  name_id = tbl_name[0: -1] + "_id"
          
  df_tbls_updates = (df_tbl.sample(False, 0.1)
              .limit(50 * 1000)
              .withColumn("modified_at",F.expr('now()'))
              .withColumn("op", F.lit("update")))
  
  tables_size[tbl_name] = int(tables_size[tbl_name] + 10 * 1000)
  df_tbls_inserts = (datascpec_tbls[tbl_name].clone()
                  .option("startingId", tables_size[tbl_name])
                  .withRowCount(10 * 1000)
                  .build()
                  .withColumn("op", F.lit("insert"))
                  .withColumn(name_id, F.expr(f"{name_id} + {tables_size[tbl_name]}")))
  
  df_tbls_deletes = (df_tbl.sample(False, 0.1)
              .limit(50 * 1000)
              .withColumn("modified_at", F.expr("cast(current_timestamp as TIMESTAMP) - INTERVAL 1 minute"))
              .withColumn("op", F.lit("delete")))
  
  
  df_tbls_changes[tbl_name] = df_tbls_inserts.union(df_tbls_updates).union(df_tbls_deletes).dropDuplicates([name_id])
  #df_tbls_changes[tbl_name] = df_tbls_updates.dropDuplicates([name_id])
  print(BASE_PATH + "/" + tbl_name)
  print("Saving table:", tbl_name, "Records:", df_tbls_changes[tbl_name].count())
  df_tbls_changes[tbl_name].write.format("csv").mode("append").save(BASE_PATH + "/" + tbl_name,header=True)
  
  display(df_tbls_changes[tbl_name].where("op = 'delete'"))

# COMMAND ----------

df_tbls_changes["customers"].registerTempTable("customers_changes")


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM customers_changes ;
# MAGIC
# MAGIC

# COMMAND ----------


