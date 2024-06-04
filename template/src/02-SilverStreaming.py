# Databricks notebook source
# MAGIC %md
# MAGIC #Silver streaming pipeline setup
# MAGIC
# MAGIC Reads the list of data pipelines from a YAML configuration file. Read the raw data from the previously defined bronze Delta table, unpack nested columns, apply basic transformations such as renaming columns and write the data to a Silver Delta table.

# COMMAND ----------

dbutils.widgets.text("config_path", f"./pipeline_config/pipeline_config.yml")
config_path = dbutils.widgets.get("config_path")
dbutils.widgets.text("template_param_file", "")
template_param_file = dbutils.widgets.get("template_param_file")
dbutils.widgets.dropdown("template_type", "default", ["default", "jinja2"])
template_type = dbutils.widgets.get("template_type")

# COMMAND ----------

# Import necessary libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import json
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from delta.tables import *
from functools import partial
from pyspark.sql.types import *
from pprint import pprint

# COMMAND ----------

# MAGIC %md
# MAGIC ### Allow schema evolution during merges

# COMMAND ----------

spark.sql("SET spark.databricks.delta.schema.autoMerge.enabled = true") 

# COMMAND ----------

# MAGIC %run ./util

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read YAML config File

# COMMAND ----------

# MAGIC %md
# MAGIC Below cell reads the list of data pipelines from a YAML configuration file.

# COMMAND ----------

pipelines = load_config_jinja2(config_path, template_param_file=template_param_file) if template_type == "jinja2" else load_config(config_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Starting Streaming silver layer

# COMMAND ----------

# MAGIC %md
# MAGIC This section defines four functions: read_silver_pipeline(), process_silver_pipeline() and write_table_silver_append() and write_table_silver_cdc() to read raw data from a Delta table, apply some simple tranformations and unpack nested columns and write the data to a Silver Delta table.
# MAGIC
# MAGIC The read_silver_pipeline() function reads data from a specified Delta table using spark.readStream.table().
# MAGIC
# MAGIC The process_silver_pipeline() function applies a schema to the data, parses the payload column using the schema. The function returns the transformed data as the output.
# MAGIC
# MAGIC The write_table_silver_append() function writes the transformed output to a specified silver Delta table using silver_streaming.toTable()
# MAGIC
# MAGIC The write_table_silver_cdc() function enables CDC. The function contains the upsert logic for the Delta table using the Delta merge operation. The upsert logic is used to merge the batch output with the Delta table data.

# COMMAND ----------

# Define an empty dictionary to hold silver data pipelines
silver_pipelines = {}

def read_silver_pipeline(pipeline, name, input_table):
    print("Starting silver streaming:", name)
    
    silver_pipeline = spark.readStream.table(input_table)
    
    if "read_options" in pipeline:
        silver_pipeline = silver_pipeline.options(**pipeline["read_options"])
    
    return silver_pipeline

# Function to process silver data pipeline
def process_silver_pipeline(pipeline, name, silver_pipeline):
    silver_pipeline = (silver_pipeline
                       .select(expr(pipeline.get("select_expression", "*")))
                       .filter(pipeline.get("filters", "1 = 1")))
    
    # Unpack any nested columns in the input data
    if "unpack_columns" in pipeline:        
        for col_obj in pipeline["unpack_columns"]:
            # Use getItem to extract values from a nested column
            col_name = col_obj["name"]
            col_schema = col_obj["schema"]

            # Create a schema for the output data
            fields = []
            for col_name2, col_value in col_schema.items():
                fields.append(f"{col_name2} {col_value}")
            schema = ",".join(fields)

            silver_pipeline = silver_pipeline.withColumn(col_name, from_json(col(col_name), schema)).select("*", col(col_name + ".*")).drop(col_name)

    if "rename_columns" in pipeline:
        rename_map = pipeline["rename_columns"]
        select_expr = [col(old_name).alias(new_name) for old_name, new_name in rename_map.items()]
        silver_pipeline = silver_pipeline.select(*select_expr)
        
    return silver_pipeline


# COMMAND ----------

# MAGIC %md 
# MAGIC ### Function to write processed silver data to a Delta table 

# COMMAND ----------

def write_table_silver_append(pipeline, name, silver_streaming):
    print("Writing to silver Table:", pipeline["toTable"])

    silver_streaming = silver_streaming.writeStream

    if "write_options" in pipeline:
        silver_streaming = silver_streaming.options(**pipeline["write_options"])    

    silver_streaming = (silver_streaming.outputMode(pipeline["outputMode"])
                                        .trigger(**pipeline.get("trigger", {"availableNow": True}))    
                                        .queryName(pipeline["queryName"])) 
    
    silver_streaming = silver_streaming.toTable(pipeline["toTable"])    
    
    return silver_streaming

# COMMAND ----------

def write_table_silver_cdc(pipeline, name, silver_streaming):
    print("Writing silver Table:", pipeline["toTable"])
    
    input_schema = silver_streaming.schema
    silver_streaming = silver_streaming.writeStream

    if "write_options" in pipeline:
        silver_streaming = silver_streaming.options(**pipeline["write_options"])    
        
    silver_streaming = (silver_streaming.outputMode(pipeline.get("outputMode", "append"))
                                    .trigger(**pipeline.get("trigger", {"availableNow": True}))    
                                    .queryName(pipeline["queryName"]))
    
    (DeltaTable.createIfNotExists(spark)
            .tableName(pipeline["toTable"])
            .addColumns(input_schema)
            .execute())
    scd1_table_df_local = DeltaTable.forName(spark, pipeline["toTable"])
   
    # Define the upsert logic for the Delta table 
    def upsert_to_delta(microBatchOutputDF, batchId, scd1_table_df_local, key_col, sequence_col):
    
        windowSpec  = Window.partitionBy(key_col).orderBy(desc(sequence_col))
        microBatchOutputDF = (microBatchOutputDF.withColumn("row_number", row_number().over(windowSpec))
                                                .where("row_number = 1")
                                                .drop("row_number"))
        
                
        merge_df = (scd1_table_df_local.alias("scd1_table")
                    .merge(microBatchOutputDF.alias("new_op_table"), 
                            f"new_op_table.{key_col} = scd1_table.{key_col}")        
        )        
        merge_df =  merge_df if "delete" not in pipeline else merge_df.whenMatchedDelete("new_op_table.op = '{delete}'".format(**pipeline))        
        merge_df =  (merge_df.whenMatchedUpdateAll(f"new_op_table.{sequence_col} > scd1_table.{sequence_col}")
                    .whenNotMatchedInsertAll()
                    .execute())

    partial_upsert_to_delta = partial(upsert_to_delta, scd1_table_df_local = scd1_table_df_local, key_col = pipeline["key_col"], sequence_col = pipeline["sequence_col"])
    silver_streaming = silver_streaming.foreachBatch(partial_upsert_to_delta)    
    silver_streaming.start()       
    
    return silver_streaming

# COMMAND ----------

for pipeline in pipelines:
    
    name = pipeline["pipeline"]

    if "silver" in pipeline:
        pipeline_silver = pipeline["silver"]
        silver_pipelines[name] = read_silver_pipeline(pipeline_silver, name, pipeline["bronze"]["toTable"])
        silver_pipelines[name] = process_silver_pipeline(pipeline_silver, name, silver_pipelines[name])
        
        if pipeline_silver.get("cdc", False):
            print("CDC Enabled")
            silver_pipelines[name] = write_table_silver_cdc(pipeline_silver, name, silver_pipelines[name])
        else:
            print("Append Only - CDC Disabled")
            silver_pipelines[name] = write_table_silver_append(pipeline_silver, name, silver_pipelines[name])
            
        
