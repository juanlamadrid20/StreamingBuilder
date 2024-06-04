# Databricks notebook source
from pyspark.sql.session import SparkSession
from urllib.request import urlretrieve
import time

def stop_all_streams() -> bool:
    stopped = True

    for stream in spark.streams.active:
      original = spark.conf.get("spark.sql.streaming.stopTimeout")
      spark.conf.set("spark.sql.streaming.stopTimeout", 0)
      try:
          stream.stop()
          stream.awaitTermination(300)
          
      except Exception as e:
          stopped = False
          print("Ignoring exception while stopping streams")
          print(e)
      
      finally:
          spark.conf.set("spark.sql.streaming.stopTimeout", original)
          
    return stopped


def stop_named_stream(spark: SparkSession, namedStream: str) -> bool:

    for stream in spark.streams.active:
      original = spark.conf.get("spark.sql.streaming.stopTimeout")
      spark.conf.set("spark.sql.streaming.stopTimeout", 0)
      try:
          if stream.name == namedStream:
              stream.stop()
              stream.awaitTermination(300)
              return True
          
      except Exception as e:
          print("Ignoring exception while stopping streams")
          print(e)
      
      finally:
          spark.conf.set("spark.sql.streaming.stopTimeout", original)
    
    return False

def untilStreamIsReady(namedStream, progressions = 3) -> bool:
    queries = []
    for stream in spark.streams.active:
      if stream.name == namedStream:
        queries.append(stream)
      
#     queries = list(filter(lambda query: query.name == namedStream, spark.streams.active))
    while len(queries) == 0 or len(queries[0].recentProgress) < progressions:
        time.sleep(5)
        queries = []
        for stream in spark.streams.active:
          if stream.name == namedStream:
            queries.append(stream)
    print("The stream {} is active and ready.".format(namedStream))
    return True


# COMMAND ----------

def pprint(dict_to_print):
    return json.dumps(dict_to_print, indent = 4)


# COMMAND ----------

# install pyyaml for reading yaml files
%pip install pyyaml

# COMMAND ----------

import yaml
import json
def load_config(path, verbose=True):
    # read config yaml file
    with open(path, 'r') as stream:
        config = yaml.safe_load(stream)
        if verbose:
            print("Config:", pprint(config))
        return config


# COMMAND ----------

%pip install jinja2

# COMMAND ----------

%pip install mergedeep

# COMMAND ----------

import yaml
import json
import jinja2
import mergedeep

def load_config_jinja2(path, verbose=True, template_param_file=None):
    env = jinja2.Environment()
    base_path = "/".join(path.split("/")[0:-1])
    # read config yaml file
    processed_configs = []
    with open(path, 'r') as stream:
        config_file_content = stream.read()
        if template_param_file:
            with open(base_path + "/" + template_param_file, 'r') as f_param:
                template_params = yaml.safe_load(f_param)                
                base_template = env.from_string(config_file_content)
                config_file_content = base_template.render(**template_params)
        
        config = yaml.safe_load(config_file_content)
        for pipeline in config:
            if "inherit" in pipeline:                
                with open(base_path + "/" + pipeline["inherit"], 'r') as f_template:
                    template = env.from_string(f_template.read())
                    rendered_template = template.render(**pipeline)
                    # print("Rendered template:", rendered_template)
                    inherit_config = yaml.safe_load(rendered_template)

                    print("inherit_config:", pprint(inherit_config))
                    print("pipeline:", pprint(pipeline))
                    
                    new_pipeline_config = mergedeep.merge(inherit_config, pipeline)
                    processed_configs.append(new_pipeline_config)
            else:
                processed_configs.append(pipeline)
                    # print("inherit_config:", pprint(new_pipeline_config))
        if verbose:
            print("Config:", pprint(processed_configs))
        return processed_configs
    
# cfg = load_config_jinja2("/Workspace//Users/alex.lopes@databricks.com/.bundle/Project_StructuredStreamingMeta/dev/files/resources/pipeline_config/csv_pipeline.yml")



