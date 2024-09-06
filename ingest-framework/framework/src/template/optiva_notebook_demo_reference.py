# Databricks notebook source
import time
import argparse
from pyspark.sql import functions as F
import sys
import os

# data pipeline imports
from sparkbuilder.utils.config_handler import ConfigHandler
from sparkbuilder.builder.engine import PipelineBuilder
import re

# dq imports
from sparkbuilder.dq.rules import DataQualityChecks
from sparkbuilder.dq.builder import get_dq_config

# audit imports
from sparkbuilder.audit.utils import AuditTableHandler


def add_timestamp_cols(input_df):
    input_df = (input_df
                .withColumn("create_date", F.current_timestamp())
                .withColumn("change_date", F.current_timestamp())
                .withColumn("src_file_process_time", F.current_timestamp())
                .withColumn("file_modification_time", F.expr("_metadata.file_modification_time"))
                .withColumn("hkey", F.lit(""))
                .withColumn("hdiff", F.lit(""))
                )
    return input_df


def rename_cols(df, table_name, col_mapping_config_table):
    config_df = spark.table(col_mapping_config_table)
    config_df = config_df.filter(config_df.table_name == table_name)
    config = config_df.collect()

    for row in config:
        old_col = row.source_col_name
        new_col = row.source_target_name
        df = df.withColumnRenamed(old_col, new_col)
    return df

def display_count(df):
    print(f"Batch count: {df.count()}")
    return df


def get_distinct_vals(df):
    return df.distinct()


def handle_deletes(df):
    deletes_df = df.filter(col("ACTION")=="DELETE")
    non_deletes_df = df.filter(col("ACTION")!="DELETE")
    # Delete sql goes here
    deletes_df.createOrReplaceTempView("DELETES")
    df.sparkSession.sql(f"delete * from target_table where id in (select id from {deletes_df})")

    return non_deletes_df


class Args:
    def __init__(self, config_path, table_name, audit_write, verbose, run_dq_rules):
        self.config_path = config_path
        self.table_name = table_name
        self.audit_write = audit_write
        self.verbose = verbose
        self.run_dq_rules = run_dq_rules


def get_user_args():
    # parser = argparse.ArgumentParser()
    # parser.add_argument("--config_path", help="The path to the config file", default=None, required=False)
    # parser.add_argument("--table_name", help="The table name for the config files", default=None, required=False)
    # parser.add_argument("--audit_write", help="Whether to write to the audit tables", default=True, required=False)
    # parser.add_argument("--verbose", help="Whether to write out the SQL queries", default=True, type=bool,
    #                     required=False)
    # args = parser.parse_args()
    # return args

    dbutils.widgets.text("config_path", "", "The path to the config file")
    dbutils.widgets.text("table_name", "FSACTIONSETDEFAULTPARAM", "The table name for the config files")
    dbutils.widgets.dropdown("audit_write", "False", ["True", "False"], "Whether to write to the audit tables")
    dbutils.widgets.dropdown("run_dq_rules", "False", ["True", "False"], "Whether to run data quality rules")
    dbutils.widgets.dropdown("verbose", "True", ["True", "False"], "Whether to write out the SQL queries")

    # Retrieve the values from the widgets
    config_path = dbutils.widgets.get("config_path")
    table_name = dbutils.widgets.get("table_name")
    audit_write = dbutils.widgets.get("audit_write") == "True"  # Convert to boolean
    verbose = dbutils.widgets.get("verbose") == "True"  # Convert to boolean
    run_dq_rules = dbutils.widgets.get("run_dq_rules") == "True"

    return Args(config_path=config_path,
                table_name=table_name,
                audit_write=audit_write,
                verbose=verbose,
                run_dq_rules=run_dq_rules)

def main():
    # Get User Arguments
    args = get_user_args()

    audit_write = args.audit_write

    table_name = args.table_name

    #pkeys = ["USER_CODE","ACTIONSET_CODE","PARAM_CODE", "PVALUE"] #FSACTIONSETDEFAULTPARAM
    #pkeys = ["USER_CODE","ACTIONSET_CODE","PARAM_CODE"] #FSACTIONSETDEFAULTPARAM official
    pkeys = ['ACTION_CODE'] # FSACTION
    
    pipeline_config = {
        # Data Product Name is a Campbell's specific variable for their audit system
        # for keeping track of their data subsets
        "data_product_name": "optiva",

        # Source data type -> spark.read.format(source_data_type)
        "source_data_type": "parquet",

        # Streaming boolean for whether to use spark.read or the streaming reader
        "streaming" : "false",

        # Source table type is for when using streaming to specify the cloudFiles + source_table_type
        "source_table_type" : "parquet",

        # The file source in ADLS
        "source_filepath": f"abfss://landing@sacscdnausedlhdev.dfs.core.windows.net/optiva/{table_name}",

        # Any custom source reading options i.e. spark.read.option(**source_reader_options)
        "source_reader_options": {
            #"cloudFiles.format": "parquet",
            "recursiveFileLookup" : "true",
            # "pathGlobFilter": "*.parquet",
            #"cloudFiles.schemaLocation": f"/dbfs/mnt/landing/{table_name}",
        },
        # Streaming trigger value (currently only supports trigger once or continuous)
        #"trigger" : "once",

        # Writes would be a list of write values, this allows for multiple writes per source
        "writes": [
            {
                # UC Catalog (HMS would be "database" : "myDatabase")
                "catalog": "sandbox",
                # UC Schema
                "schema": "dbx_demo",
                # UC Table
                "table": table_name,
                # Data Type (not really needed for UC)
                "data_type": "delta",
                # Write mode (append, overwrite, merge)
                "mode": "overwrite",
                # Merge requires a keys for primary keys
                "keys": pkeys,
                # Not needed, but a timestamp column for ordering records
                #"timestamp_col": "change_date",
                "timestamp_col": "file_modification_time",
                # SCD Type (defaults to 1), supports 1 and 2
                "scd_type": 1,
            },
            {
                "catalog": "sandbox",
                "schema": "dbx_demo",
                "table": f"{table_name}_HISTORY",
                "data_type": "delta",
                "mode": "merge",
                "keys": pkeys,
                #"timestamp_col": "change_date",
                "timestamp_col": "file_modification_time",
                
                "scd_type": 2,
            }
        ],
        "transformations": [
            # {
            #     "py": "rename_cols",
            #     "args": {
            #         "table_name": "FSACTIONSETDEFAULTPARAM",
            #         "col_mapping_config_table": "sandbox.dbx_demo_config.col_mapping_config"
            #         #"col_mapping_config_table": "dev.raw.col_mapping_config"
            #     }
            # },
            {
                # Custom Python functions are set as "py" : python function object
                # They also need be added to the PipelineBuilder fncs arguments as a 
                # list of functions
                "py" : "get_distinct_vals"
            },
            # {
            #     "py": "add_timestamp_cols"
            # },
            {
                # Common functions
                "normalize_cols": None
            },
            # {
            #     "brute_force_subtract" : {
            #         "table": f"sandbox.dbx_demo.{table_name}",
            #         "pkeys": pkeys
            #     }
            # },
            {
                "py": "display_count"
            }
            # {
            #     "py":"order_transactions"
            # }
            # {
            #     "py":"handle_deletes"
            # }
        ]
    }

    # Process the configuration (either path or dictionary)
    config_handler = ConfigHandler(config_path=args.config_path, config=pipeline_config)
    config = config_handler.get_config()

    pb = PipelineBuilder(spark, config, verbose=args.verbose, 
                         fncs=[add_timestamp_cols, display_count, get_distinct_vals, rename_cols])
    

    if audit_write is True:
        audit_table_config = {
            "audit_dim_id": "run-001"

        }
        data_product_name = config["data_product_name"]
        target_table = config_handler.get_target_table_name()
        source_table = config_handler.get_source_table_name()

        ath = AuditTableHandler(audit_table_config)

        ath.write_audit_dim_table(target_table, data_product_name, source_table)
        ath.signal_pipeline_start()
    
    if args.run_dq_rules is True:
        dq = DataQualityChecks()

    # Read the data
    df = pb.read()
    
    # Perform transformations
    df, _ = pb.run(df)

    # Run Data Quality Checks
    if args.run_dq_rules is True:
        dq_config = get_dq_config(table="dq_database_eval_quality") # Table must be a delta table in Databricks
        active_valid_df, active_invalid_df, passive_valid_df, passive_invalid_df = dq.run(df, dq_config)

    # Write output
    pb.write(df)

    if audit_write:
        # Write the type 1 updates and the target table count to the audit table
        ath.write_type1_updates(df)
        ath.write_target_table_count(target_table)
        ath.signal_pipeline_end()


if __name__ == "__main__":
    main()



# COMMAND ----------

# MAGIC %sql
# MAGIC --select count(*) from sandbox.dbx_demo.FSACTION

# COMMAND ----------

# MAGIC  %sql
# MAGIC -- config tables
# MAGIC --dev.config.dq_rule_dim 
# MAGIC --dev.config.dq_rule_object_rel
# MAGIC
# MAGIC --select * from dev.config.dq_rule_object_rel
# MAGIC --select * from dev.config.control_raw_source_to_target_col_map_v0 --where table_name  'FSACTIONSETDEFAULTPARAM'

# COMMAND ----------

# create sandbox.dbx_demo_config.col_mapping_config

# data = [
#     {"table_name": "FSACTIONSETDEFAULTPARAM", "source_col_name": "ACTIONSET_CODE", "source_target_name":"ACTIONSET_CODE_RENAMED"}
# ]
# spark.createDataFrame(data).writeTo("sandbox.dbx_demo_config.col_mapping_config").createOrReplace()


# COMMAND ----------

# Pkeys location: https://docs.google.com/spreadsheets/d/1uDWDJ8-TiFyxzDIOMVBHyjMLOXQBWrri/edit?gid=1427409845#gid=1427409845
# Tables included: https://docs.google.com/spreadsheets/d/1BrMJMGTaPKyKL24Xr0lCIzaKecrKq88n/edit?gid=1464889253#gid=1464889253

# COMMAND ----------

# data = [
#     {
#         "stream_name": "optiva",
#         "schema_name": "raw",
#         "table_name": "FSACTION",
#         "landing_file_path": "abfss://landing@sacscdnausedlhdev.dfs.core.windows.net/optiva/FSACTION",
#         "landing_file_format": "parquet",
#         "landing_file_name_format": "*.parquet",
#         "landing_file_header_flag": "true",
#         "load_type": "update",
#         "check_point_path": "landing/optiva/chkpoint",
#         "rec_created_date": "2024-06-27T00:00:00.099+00:00",
#         "schema": "ACTION_CODE,MODIFY_DATE"
#     },
#     {
#         "stream_name": "optiva",
#         "schema_name": "raw",
#         "table_name": "FSACTIONSETDEFAULTASSIGN",
#         "landing_file_path": "abfss://landing@sacscdnausedlhdev.dfs.core.windows.net/optiva/FSACTIONSETDEFAULTASSIGN",
#         "landing_file_format": "parquet",
#         "landing_file_name_format": "*.parquet",
#         "landing_file_header_flag": "true",
#         "load_type": "update",
#         "check_point_path": "landing/optiva/chkpoint",
#         "rec_created_date": "2024-06-27T00:00:00.099+00:01",
#         "schema": "USER_CODE,ACTIONSET_CODE,LINE_ID"
#     },
#     {
#         "stream_name": "optiva",
#         "schema_name": "raw",
#         "table_name": "FSACTIONSETDEFAULTPARAM",
#         "landing_file_path": "abfss://landing@sacscdnausedlhdev.dfs.core.windows.net/optiva/FSACTIONSETDEFAULTPARAM",
#         "landing_file_format": "parquet",
#         "landing_file_name_format": "*.parquet",
#         "landing_file_header_flag": "true",
#         "load_type": "update",
#         "check_point_path": "landing/optiva/chkpoint",
#         "rec_created_date": "2024-06-27T00:00:00.099+00:04",
#         "schema": "USER_CODE,ACTIONSET_CODE_RENAMED,PARAM_CODE"
#     },
#     {
#         "stream_name": "optiva",
#         "schema_name": "raw",
#         "table_name": "FSACTIONSETPARAM",
#         "landing_file_path": "abfss://landing@sacscdnausedlhdev.dfs.core.windows.net/optiva/FSACTIONSETPARAM",
#         "landing_file_format": "parquet",
#         "landing_file_name_format": "*.parquet",
#         "landing_file_header_flag": "true",
#         "load_type": "update",
#         "check_point_path": "landing/optiva/chkpoint",
#         "rec_created_date": "2024-06-27T00:00:00.099+00:07",
#         "schema": "USER_CODE,ACTIONSET_CODE,PARAM_CODE"
#     }
# ]

# spark.createDataFrame(data).writeTo("sandbox.dbx_demo_config.optiva_config_raw_tables").createOrReplace()

# COMMAND ----------

# spark.sql("DROP schema sandbox.dbx_demo cascade")
# spark.sql("create schema sandbox.dbx_demo")


# COMMAND ----------

# MAGIC %sql
# MAGIC select table_name, schema, landing_file_path from sandbox.dbx_demo_config.optiva_config_raw_tables

# COMMAND ----------

# MAGIC %sql
# MAGIC select table_name, pkeys, source_filepath from sandbox.dbx_demo_config.optiva_config_raw_tables_v3

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from sandbox.dbx_demo_config.col_mapping_config
