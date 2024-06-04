# Databricks notebook source
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

dbutils.widgets.text("config_path", f"../pipeline_config.yml")
config_path = dbutils.widgets.get("config_path")
print("Config path:", config_path)

# COMMAND ----------

# MAGIC %run ../util

# COMMAND ----------

pipelines = load_config(config_path)

# COMMAND ----------

for pipeline in pipelines:
  dbutils.fs.rm(pipeline["silver"]["write_options"]["checkpointLocation"], recurse=True)

# COMMAND ----------

for pipeline in pipelines:
  dbutils.fs.rm(pipeline["bronze"]["write_options"]["checkpointLocation"], recurse=True)
