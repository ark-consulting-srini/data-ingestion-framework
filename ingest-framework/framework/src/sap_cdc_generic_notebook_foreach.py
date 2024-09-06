# Databricks notebook source
dbutils.widgets.text("data_product_name", "", "Data Product Name is a Campbell's specific variable for their audit system")
dbutils.widgets.text("table_name", "", "The table name for ingestion")
dbutils.widgets.text("config_database", "", "The table name with configurations")

# COMMAND ----------

import time
import argparse
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.connect.dataframe import DataFrame
import sys
import os

from sparkbuilder.utils.config_handler import ConfigHandler
from sparkbuilder.builder.engine import PipelineBuilder
import re
import json

# COMMAND ----------

dbutils.widgets.text("data_product_name", "", "Data Product Name is a Campbell's specific variable for their audit system")
dbutils.widgets.text("table_name", "", "The table name for ingestion")
dbutils.widgets.text("config_database", "", "The table name with configurations")

# Retrieve the values from the widgets
data_product_name = dbutils.widgets.get("data_product_name")
table_name = dbutils.widgets.get("table_name")
config_database = dbutils.widgets.get("config_database")
print(data_product_name)
print(table_name)
print(config_database)

# COMMAND ----------

class Args:
    def __init__(self, config_path, data_product_name,table_name,initial_filepath,delta_filepath,external_location,is_initial_completed,source_data_type, source_table_type, source_reader_options, audit_write, verbose, run_dq_rules,source_filepath,cast_column,pkeys, streaming, trigger, writes, transformations):
        self.config_path = config_path
        self.data_product_name = data_product_name
        self.table_name = table_name
        self.initial_filepath = initial_filepath
        self.delta_filepath = delta_filepath
        self.external_location=external_location
        self.is_initial_completed=is_initial_completed
        self.source_data_type = source_data_type
        self.source_table_type = source_table_type
        self.source_reader_options = source_reader_options
        self.audit_write = audit_write
        self.verbose = verbose
        self.run_dq_rules = run_dq_rules
        self.source_filepath = source_filepath
        self.cast_column = cast_column
        self.pkeys = pkeys
        self.streaming = streaming
        self.trigger = trigger
        self.writes=writes
        self.transformations=transformations
#/End of Class        

# COMMAND ----------

def get_user_args():

  
    # Retrieve full configurations from {config_database} to bypass the forarch task context limit of 1KB
    config_raw_df = spark.sql(f"""select * except(rec_created_date) from {config_database} where upper(is_table_enabled)='TRUE' and upper(data_product_name) = upper('{data_product_name}') and upper(table_name) = upper('{table_name}')""")

    config_raw_df_pd = config_raw_df.toPandas()
    task_list = config_raw_df_pd.to_dict(orient='records')[0]
    
    return Args(config_path=None,
                data_product_name=task_list["data_product_name"],
                table_name=task_list["table_name"],
                initial_filepath=task_list["initial_filepath"],
                delta_filepath=task_list["delta_filepath"],
                external_location=task_list["external_location"],
                is_initial_completed=task_list["is_initial_completed"],
                source_data_type=task_list["source_data_type"],
                source_table_type=task_list["source_table_type"],
                source_reader_options=json.loads(task_list["source_reader_options"]),
                audit_write=task_list["audit_write"],
                verbose=task_list["verbose"],
                run_dq_rules=task_list["run_dq_rules"],
                source_filepath=task_list["source_filepath"],
                cast_column=json.loads(task_list["cast_column"]),
                pkeys=task_list["pkeys"].split(","),
                streaming=task_list["streaming"],
                trigger=task_list["trigger"],
                writes=json.loads(task_list["writes"]),
                transformations=json.loads(task_list["transformations"])
                )

# COMMAND ----------

def main():
    # Get User Arguments
    args = get_user_args()
    table_name = args.table_name

    source_location:str=None
    #checkpoint_location:str=None


    # check if initial load is completed, get delta location
    if not (args.is_initial_completed).upper() == "TRUE":
        # The file source in ADLS
        source_location=args.initial_filepath
    else:
        # The file source in ADLS
        source_location=args.delta_filepath
    
    pipeline_config = {
        # Data Product Name is a Campbell's specific variable for their audit system
        # for keeping track of their data subsets
        "data_product_name": args.data_product_name,

        # Source data type -> spark.read.format(source_data_type)
        "source_data_type": args.source_data_type,

        # Streaming boolean for whether to use spark.read or the streaming reader
        "streaming" : args.streaming,

        # Source table type is for when using streaming to specify the cloudFiles + source_table_type
        "source_table_type" : args.source_table_type,
      
        # The file source in ADLS
        "source_filepath":source_location,

        # Columns that are needed to be cast (data type conversion)
        "cast_column":args.cast_column,

        # Any custom source reading options i.e. spark.read.option(**source_reader_options)
        "source_reader_options": args.source_reader_options,

        # Streaming trigger value (currently only supports trigger once or continuous)
        "trigger": args.trigger,

        #External file location
        "external_location":args.external_location,

        # Writes would be a list of write values, this allows for multiple writes per source
        "writes": args.writes
        #"transformations": args.transformations
    }

    # Process the configuration (either path or dictionary)
    config = ConfigHandler(config_path=args.config_path, config=pipeline_config).get_config()
    # TO DO: Add functions to fncs dinamically
    pb = PipelineBuilder(spark, config, verbose=args.verbose, 
                        fncs=[])
   
    pb.create_delta_table_if_not_exists()
    
    # Read the data
    df = pb.read()
 
    # cast string data types based on cast_column in the config table 
    df = pb.datatype_conversion(df)

    # Write output
    pb.write(df)

    # set initial Load is completed for the given table.
    spark.sql(f"""UPDATE {config_database} SET is_initial_completed=true where upper(is_table_enabled)='TRUE' and upper(is_initial_completed) = 'FALSE' and upper(data_product_name) = upper('{data_product_name}') and upper(table_name) = upper('{table_name}')""")

  

if __name__ == "__main__":
    main()

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC
