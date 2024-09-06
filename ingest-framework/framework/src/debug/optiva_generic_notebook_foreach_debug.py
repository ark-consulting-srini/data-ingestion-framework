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
    deletes_df = df.filter(col("ACTION")=="DELETE")
    non_deletes_df = df.filter(col("ACTION")!="DELETE")
    # Delete sql goes here
    deletes_df.createOrReplaceTempView("DELETES")
    df.sparkSession.sql(f"delete * from target_table where id in (select id from {deletes_df})")

    return non_deletes_df

# COMMAND ----------

class Args:
    def __init__(self, config_path, data_product_name,table_name,source_data_type, source_table_type, source_reader_options, audit_write, verbose, run_dq_rules,source_filepath,pkeys, streaming, trigger, writes, transformations, source_orderBy_column, dq_config_table, source_extraction_type, source_workload_type):
        self.config_path = config_path
        self.data_product_name = data_product_name
        self.source_extraction_type = source_extraction_type
        self.source_workload_type = source_workload_type
        self.table_name = table_name
        self.source_data_type = source_data_type
        self.source_table_type = source_table_type
        self.source_reader_options = source_reader_options
        self.audit_write = audit_write
        self.verbose = verbose
        self.run_dq_rules = run_dq_rules
        self.source_filepath = source_filepath
        self.pkeys = pkeys
        self.streaming = streaming
        self.trigger = trigger
        self.writes=writes
        self.transformations=transformations
        self.source_orderBy_column=source_orderBy_column
        self.dq_config_table=dq_config_table


def get_user_args():

    dbutils.widgets.text("data_product_name", "", "Data Product Name is a Campbell's specific variable for their audit system")
    dbutils.widgets.text("table_name", "", "The table name for ingestion")
    dbutils.widgets.text("config_database", "", "The table name with configurations")

    # Retrieve the values from the widgets
    data_product_name = dbutils.widgets.get("data_product_name")
    table_name = dbutils.widgets.get("table_name")
    config_database = dbutils.widgets.get("config_database")

    # Retrieve full configurations from {config_database} to bypass the forarch task context limit of 1KB
    config_raw_df = spark.sql(f"""select * except(rec_created_date) from {config_database} where data_product_name = '{data_product_name}' and table_name = '{table_name}' """)
    config_raw_df_pd = config_raw_df.toPandas()
    task_list = config_raw_df_pd.to_dict(orient='records')[0]
    
    return Args(config_path=None,
                table_name=task_list["table_name"],
                data_product_name=task_list["data_product_name"],
                source_data_type=task_list["source_data_type"],
                source_table_type=task_list["source_table_type"],
                source_reader_options=json.loads(task_list["source_reader_options"]),
                audit_write=task_list["audit_write"],
                verbose=task_list["verbose"],
                run_dq_rules=task_list["run_dq_rules"],
                dq_config_table=task_list["dq_config_table"],
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

        "run_dq_rules": args.run_dq_rules,
        "dq_config_table": args.dq_config_table,
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
        "transformations": args.transformations
    }

    # Process the configuration (either path or dictionary)
    config = ConfigHandler(config_path=args.config_path, config=pipeline_config).get_config()

    # TO DO: Add functions to fncs dinamically
    pb = PipelineBuilder(spark, config, verbose=args.verbose, 
                        fncs=[add_timestamp_cols, display_count, get_distinct_vals,rename_cols])
    
    # Read the data
    df = pb.read()
    return df
    # # Perform transformations
    #df, _ = pb.run(df)

    # # Write output
    #pb.write(df)


if __name__ == "__main__":
    df = main()
    

# COMMAND ----------

df_with_date = df.withColumn("date", F.to_timestamp(F.to_date(F.col("file_modification_time"))))

# COMMAND ----------

windowed_df = df_with_date.withWatermark("date", "1 day") \
                .groupBy(F.window("date", "1 day"))

# COMMAND ----------

latest_records = windowed_df.agg(F.max("date").alias("max_date")).sort(F.desc("max_date")).limit(1)

# COMMAND ----------

latest_records.display()

# COMMAND ----------

result_df = df_with_date.join(latest_records, 
                    (df_with_date.date == latest_records.max_date),
                    "inner")

# COMMAND ----------

target_table = "dev.do_dest.optiva_testfnr_fsactionwipparam"
checkpointLocation = "abfss://dodest@sacscdnausedlhdev.dfs.core.windows.net/ecc/ingestion_framework/checkpoints/optiva_testfnr_fsactionwipparam"
query = result_df.writeStream \
    .format("delta") \
    .outputMode("append") \
    .trigger(availableNow=True) \
    .option("checkpointLocation", checkpointLocation) \
    .toTable(target_table)
query.awaitTermination()
