# Databricks notebook source
import time
import argparse
from pyspark.sql import functions as F
import sys
import os

from sparkbuilder.utils.config_handler import ConfigHandler
from sparkbuilder.builder.engine import PipelineBuilder
import re
import json
from datetime import datetime

# #Databricks Connect specific: To delete
from databricks.connect import DatabricksSession
spark = DatabricksSession.builder.getOrCreate()

# COMMAND ----------

# Local pipeline functions

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
    deletes_df = df.filter(F.col("ACTION")=="DELETE")
    non_deletes_df = df.filter(F.col("ACTION")!="DELETE")
    # Delete sql goes here
    deletes_df.createOrReplaceTempView("DELETES")
    df.sparkSession.sql(f"delete * from target_table where id in (select id from {deletes_df})")

    return non_deletes_df

# COMMAND ----------

class Args:
    def __init__(self, config_path, data_product_name,table_name,source_data_type, source_table_type, source_reader_options, audit_write, audit_config, verbose, run_dq_rules, dq_config, source_filepath,pkeys, streaming, trigger, writes, transformations, source_orderBy_column, source_extraction_type, source_workload_type):
        self.config_path = config_path
        self.data_product_name = data_product_name
        self.source_extraction_type = source_extraction_type
        self.source_workload_type = source_workload_type
        self.table_name = table_name
        self.source_data_type = source_data_type
        self.source_table_type = source_table_type
        self.source_reader_options = source_reader_options
        self.audit_write = audit_write
        self.audit_config = audit_config
        self.verbose = verbose
        self.run_dq_rules = run_dq_rules
        self.dq_config=dq_config
        self.source_filepath = source_filepath
        self.pkeys = pkeys
        self.streaming = streaming
        self.trigger = trigger
        self.writes=writes
        self.transformations=transformations
        self.source_orderBy_column=source_orderBy_column


def get_user_args():

    # dbutils.widgets.text("data_product_name", "", "Data Product Name is a Campbell's specific variable for their audit system")
    # dbutils.widgets.text("table_name", "", "The table name for ingestion")
    # dbutils.widgets.text("config_database", "", "The table name with configurations")

    # Retrieve the values from the widgets
    # data_product_name = dbutils.widgets.get("data_product_name")
    # table_name = dbutils.widgets.get("table_name")
    # config_database = dbutils.widgets.get("config_database")
    data_product_name = "optiva_ie"
    table_name = "FSVIEWFILTERSYMBOLSET"
    # config_database = "dev.config.optiva_config_raw_tables_v8_fnr"
    config_database = "dev.config.non_cdc_framework_config"

    # Retrieve full configurations from {config_database} to bypass the forarch task context limit of 1KB
    # config_raw_df = spark.sql(f"""select * except(rec_created_date) from {config_database} where data_product_name = '{data_product_name}' and table_name = '{table_name}' """)
    # config_raw_df_pd = config_raw_df.toPandas()
    # task_list = config_raw_df_pd.to_dict(orient='records')[0]
    task_list = {
        "data_product_name": "optiva_ie",
        "source_extraction_type": "IE",
        "source_workload_type": "NON_CDC",
        "table_name": "SAP_MSEG",
        "source_data_type": "parquet",
        "source_table_type": "cloudFiles",
        "source_reader_options": """{
            "recursiveFileLookup": "True",
            "cloudFiles.schemaEvolutionMode": "rescue",
            "cloudFiles.format": "parquet"
        }""",
        "audit_write": "True",
        "audit_config": """{
            "audit_table_name": "dev.do_dest.non_cdc_audit_logs",
            "external_location": "abfss://dodest@sacscdnausedlhdev.dfs.core.windows.net/ecc/ingestion_framework/non_cdc/tables/non_cdc_audit-logs"
        }""",
        "verbose": "True",
        "run_dq_rules": "False",
        "dq_config": """{
            "dq_config_table": "dev.config.non_cdc_framework_data_quality_rules_config",
            "dq_log_table": "dev.do_dest.non_cdc_data_quality_logs",
            "external_location": "abfss://dodest@sacscdnausedlhdev.dfs.core.windows.net/ecc/ingestion_framework/non_cdc/tables/non_cdc_data-quality-logs"
        }""",
        "source_filepath": "abfss://landing@sacscdnausedlhdev.dfs.core.windows.net/sap_dwc/V_MSEG",
        "pkeys": "MANDT,MBLNR,MJAHR,ZEILE",
        "streaming": "True",
        "trigger": "once",
        "writes": """[
            {
                "table_medallion_layer": "bronze",
                "catalog": "dev",
                "schema": "do_dest",
                "table": "ie_SAP_MSEG_bronze",
                "data_type": "delta",
                "mode": "append",
                "scd_type": "2",
                "external_location": "abfss://dodest@sacscdnausedlhdev.dfs.core.windows.net/ecc/ingestion_framework/non_cdc/tables/ie_SAP_MSEG_bronze",
                "checkpointLocation": "abfss://dodest@sacscdnausedlhdev.dfs.core.windows.net/ecc/ingestion_framework/non_cdc/checkpoint/ie_SAP_MSEG_bronze"
            },
            {
                "table_medallion_layer": "silver",
                "catalog": "dev",
                "schema": "do_dest",
                "table": "ie_SAP_MSEG_history",
                "data_type": "delta",
                "mode": "merge",
                "scd_type": "2",
                "external_location": "abfss://dodest@sacscdnausedlhdev.dfs.core.windows.net/ecc/ingestion_framework/non_cdc/tables/ie_SAP_MSEG_history",
                "delete_source_filepath": "abfss://landing@sacscdnausedlhdev.dfs.core.windows.net/delete/sap_dwc/V_MSEG"
            },
            {
                "table_medallion_layer": "silver",
                "catalog": "dev",
                "schema": "do_dest",
                "table": "ie_SAP_MSEG",
                "data_type": "delta",
                "mode": "merge",
                "scd_type": "1",
                "external_location": "abfss://dodest@sacscdnausedlhdev.dfs.core.windows.net/ecc/ingestion_framework/non_cdc/tables/ie_SAP_MSEG",
                "delete_source_filepath": "abfss://landing@sacscdnausedlhdev.dfs.core.windows.net/delete/sap_dwc/V_MSEG"
            }
        ]""",
        "transformations": """[]""",
        "source_orderBy_column": "file_modification_time"
    }
    
    return Args(config_path=None,
                table_name=task_list["table_name"],
                data_product_name=task_list["data_product_name"],
                source_data_type=task_list["source_data_type"],
                source_table_type=task_list["source_table_type"],
                source_reader_options=json.loads(task_list["source_reader_options"]),
                audit_write=task_list["audit_write"],
                audit_config=json.loads(task_list["audit_config"]),
                verbose=task_list["verbose"],
                run_dq_rules=task_list["run_dq_rules"],
                dq_config=json.loads(task_list["dq_config"]),
                source_filepath=task_list["source_filepath"],
                pkeys=task_list["pkeys"].split(","),
                streaming=task_list["streaming"],
                trigger=task_list["trigger"],
                writes=json.loads(task_list["writes"]),
                transformations=json.loads(task_list["transformations"]),
                source_orderBy_column=task_list["source_orderBy_column"],
                source_extraction_type=task_list["source_extraction_type"],
                source_workload_type=task_list["source_workload_type"]
                )

# COMMAND ----------

print(json.dumps(get_user_args().__dict__,indent=4))

# COMMAND ----------

def main():
    # Get User Arguments
    args = get_user_args()
    table_name = args.table_name

    # Append extra pkeys key
    writes = []
    for write in args.writes:
        write["keys"] = args.pkeys
        writes.append(write.copy())

    # use notebook environment to retrieve the job_id
    try:
        context = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
        job_id = context.jobRunId().get()
    except Exception as e:
        now = datetime.now()
        job_id = f"INTERACTIVE_RUN_AT_{now}"

    pipeline_config = {
        # Data Product Name is a Campbell's specific variable for their audit system
        # for keeping track of their data subsets
        "data_product_name": args.data_product_name,

        #Source table name
        "table_name": args.table_name,

        #table keys
        "keys": args.pkeys,
        
        "source_orderBy_column": args.source_orderBy_column,

        # Source data type -> spark.read.format(source_data_type)
        "source_data_type": args.source_data_type,

        # Streaming boolean for whether to use spark.read or the streaming reader
        "streaming" : args.streaming,
        
        "audit_write": args.audit_write,
        "audit_config": args.audit_config,

        "run_dq_rules": args.run_dq_rules,
        "dq_config": args.dq_config,
        
        "source_workload_type": args.source_workload_type,
        "source_extraction_type": args.source_extraction_type,

        # Source table type is for when using streaming to specify the cloudFiles + source_table_type
        "source_table_type" : args.source_table_type,

        # The file source in ADLS
        "source_filepath":args.source_filepath,

        # Any custom source reading options i.e. spark.read.option(**source_reader_options)
        "source_reader_options": args.source_reader_options,

        # Streaming trigger value (currently only supports trigger once or continuous)
        "trigger": args.trigger,

        # Writes would be a list of write values, this allows for multiple writes per source
        "writes": writes,
        "transformations": args.transformations,
        "transformations": [
        #     {
        #         "rename_and_cast_columns": [
        #             {
        #                 'source_column_name': 'integer',
        #                 'targe_column_name': 'int',
        #                 'target_column_type':'integer'
        #             },
        #             {
        #                 'source_column_name': 'timestamp',
        #                 'targe_column_name': 'start_time',
        #                 'target_column_type':'timestamp'
        #             },
        #         ]
        #     }
        # {
        #     "column_names_to_lower":{}
        # }
        ],
        "job_id": job_id
    }

    # Process the configuration (either path or dictionary)
    config = ConfigHandler(config_path=args.config_path, config=pipeline_config).get_config()

    # TO DO: Add functions to fncs dinamically
    pb = PipelineBuilder(spark, config, verbose=args.verbose, 
                        fncs=[add_timestamp_cols, display_count, get_distinct_vals,rename_cols])
    

    # run the medallion pipeline.
    #pb.run_medallion()
    
    #test transformation
    # df = spark.createDataFrame([{
    #     "ID": 1,
    #     "INTEGER": "10",
    #     "TIMESTAMP": "2024-08-06T15:35:03.055+00:00"
    # }])
    # df.display()
    # df, _ = pb.run(df)
    # df.display()
    # print(df.schema)


if __name__ == "__main__":
    main()

