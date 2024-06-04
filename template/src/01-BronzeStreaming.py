# Databricks notebook source
# MAGIC %md
# MAGIC #Bronze streaming pipeline setup
# MAGIC
# MAGIC Reads a YAML config file and creates a readStream object for the input source for any number of pipelines defined in the configuration file and lands the raw data in a Bronze Delta table

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

# MAGIC %run ./util

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read YAML config File

# COMMAND ----------

# MAGIC %md
# MAGIC Below cell reads the list of data pipelines from a YAML configuration file.

# COMMAND ----------
print("Template type:", template_type)
pipelines = load_config_jinja2(config_path, template_param_file=template_param_file) if template_type == "jinja2" else load_config(config_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Starting streaming of the bronze layer

# COMMAND ----------

# MAGIC %md
# MAGIC This section defines two functions: process_bronze_pipeline() and write_table_bronze() to read raw data from Google Pub/Sub and write the data to a Delta table.
# MAGIC
# MAGIC The process_bronze_pipeline() function creates a readStream object for the input source, sets the read options, loads the input data as a stream. Afterward, the function returns the bronze stream.
# MAGIC
# MAGIC The write_table_bronze() function creates a writeStream object for the output, sets the write options, output mode, queryName, and trigger for the output. The function writes the output stream to a Delta table using the toTable() function and then returns the bronze stream as the output.

# COMMAND ----------

username = spark.sql("SELECT regexp_replace(current_user(), '[^a-zA-Z0-9]', '_')").first()[0].split('_databricks')[0]
spark.sql(f"USE CATALOG {username}_quickstart")
spark.sql("USE SCHEMA structured_streaming_meta")

# COMMAND ----------

bronze_pipelines = {}

# Function to process bronze data pipeline
def process_bronze_pipeline(pipeline, name):
    print("Starting bronze streaming:", name)

    bronze_streaming = spark.readStream.format(pipeline['format'])

    if "read_options" in pipeline:        
        bronze_streaming = bronze_streaming.options(**pipeline["read_options"])   
    
    bronze_streaming = bronze_streaming.load(pipeline['source_path']) if "source_path" in pipeline else bronze_streaming.load()

    return bronze_streaming

# Function to write raw data to Delta table
def write_table_bronze(pipeline, name, bronze_streaming):
    print(bronze_streaming.columns)
    print("Writing Bronze Table:", pipeline["toTable"])

    bronze_streaming = bronze_streaming.writeStream

    if "write_options" in pipeline:
        bronze_streaming = bronze_streaming.options(**pipeline["write_options"])    

    bronze_streaming = (bronze_streaming.outputMode(pipeline["outputMode"])
                                    .trigger(**pipeline.get("trigger", {"availableNow": True}))    
                                    .queryName(pipeline["queryName"])
                                    .toTable(pipeline["toTable"]))   
                                        
    return bronze_streaming

# COMMAND ----------

for pipeline in pipelines:

    name = pipeline["pipeline"]
    if "bronze" in pipeline:
        bronze_pipelines[name] = process_bronze_pipeline(pipeline["bronze"], name)
        bronze_pipelines[name] = write_table_bronze(pipeline["bronze"], name, bronze_pipelines[name])
