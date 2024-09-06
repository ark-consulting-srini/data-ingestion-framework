# Databricks notebook source
import time
import argparse
from pyspark.sql import functions as F
import sys
import os

from sparkbuilder.utils.config_handler import ConfigHandler
from sparkbuilder.builder.engine import PipelineBuilder
import re


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
    def __init__(self, config_path, table_name, audit_write, verbose, run_dq_rules,source_filepath,pkeys):
        self.config_path = config_path
        self.table_name = table_name
        self.audit_write = audit_write
        self.verbose = verbose
        self.run_dq_rules = run_dq_rules
        self.source_filepath = source_filepath
        self.pkeys = pkeys


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
    dbutils.widgets.text("source_filepath","","The source path for loading data")
    dbutils.widgets.text("pkeys","","Primary Keys of target table")

    # Retrieve the values from the widgets
    config_path = dbutils.widgets.get("config_path")
    table_name = dbutils.widgets.get("table_name")
    audit_write = dbutils.widgets.get("audit_write") == "True"  # Convert to boolean
    verbose = dbutils.widgets.get("verbose") == "True"  # Convert to boolean
    run_dq_rules = dbutils.widgets.get("run_dq_rules") == "True"
    source_filepath = dbutils.widgets.get("source_filepath")
    pkeys = dbutils.widgets.get("pkeys")

    return Args(config_path=config_path,
                table_name=table_name,
                audit_write=audit_write,
                verbose=verbose,
                run_dq_rules=run_dq_rules,
                source_filepath=source_filepath,
                pkeys=pkeys)

def main():
    # Get User Arguments
    args = get_user_args()

    audit_write = args.audit_write

    if audit_write is True:
        audit_table_config = {}
        data_product_name = config["data_product_name"]
        target_table = config.get_target_table_name()
        source_table = config.get_source_table_name()

        ath = AuditTableHandler(audit_table_config)

        ath.write_audit_dim_table(target_table, data_product_name, source_table)
        ath.signal_pipeline_start()

    table_name = args.table_name

    #pkeys = ["USER_CODE", "ACTIONSET_CODE", "PARAM_CODE", "PVALUE"]
    pkeys = args.pkeys.split(",")


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
        #"source_filepath": f"abfss://landing@sacscdnausedlhdev.dfs.core.windows.net/optiva/{table_name}",
        "source_filepath":args.source_filepath,

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
                "timestamp_col": "change_date",
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
                "timestamp_col": "change_date",
                "scd_type": 2,
            }
        ],
        "transformations": [
            {
                "py": "rename_cols",
                "args": {
                    "table_name": "FSACTIONSETDEFAULTPARAM",
                    "col_mapping_config_table": "sandbox.dbx_demo_config.col_mapping_config"
                }
            },
            {
                # Custom Python functions are set as "py" : python function object
                # They also need be added to the PipelineBuilder fncs arguments as a 
                # list of functions
                "py" : "get_distinct_vals"
            },
            {
                "py": "add_timestamp_cols",
            },
            {
                # Common functions
                "normalize_cols": None,
            },
            # {
            #     "brute_force_subtract" : {
            #         "table": f"sandbox.dbx_dev.{table_name}",
            #         "pkeys": pkeys,
            #     }
            # },
            {
                "py": "display_count",
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
    config = ConfigHandler(config_path=args.config_path, config=pipeline_config).get_config()

    pb = PipelineBuilder(spark, config, verbose=args.verbose, 
                         fncs=[add_timestamp_cols, display_count, get_distinct_vals,rename_cols])
    
    if args.run_dq_rules is True:
        dq = DataQualityChecks()

    # Read the data
    df = pb.read()
    
    # Perform transformations
    df, _ = pb.run(df)

    # Run Data Quality Checks
    if args.run_dq_rules is True:
        dq_config = get_dq_config(table="dq_database_eval_quality")
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


