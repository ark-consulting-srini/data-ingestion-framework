# Databricks notebook source
from pyspark.sql import functions as F

from sparkbuilder.utils.config_handler import ConfigHandler
from sparkbuilder.builder.engine import PipelineBuilder

#Databricks Connect specific: To delete
# from databricks.connect import DatabricksSession
#spark = DatabricksSession.builder.serverless().getOrCreate()
# spark = DatabricksSession.builder.getOrCreate()

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
    def __init__(self, config_path, data_product_name,table_name,source_data_type, source_table_type, source_reader_options, audit_write, verbose, run_dq_rules,source_filepath,pkeys, streaming, trigger, writes, transformations, source_orderBy_column,checkpoint_filepath,dq_config_table):
        self.config_path = config_path
        self.data_product_name = data_product_name
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
        self.checkpoint_filepath=checkpoint_filepath
        self.dq_config_table=dq_config_table



def get_user_args():
    #Databricks Connect specific: To delete
    data_product_name = "optiva"
    table_name = "ingestion"
    pkeys = ["pkey"] 
    source_data_type = "parquet"
    source_table_type = "parquet"
    
    # TODO: Add this column to the config database
    source_orderBy_column = "file_modification_time"
    
    # TODO: Remember to verify if this columnn is needed in the config database
    checkpoint_filepath=f'dbfs:/FileStore/dbx_demo_checkpointLocation/{table_name}',

    source_reader_options = {
            #"cloudFiles.format": "parquet",
            "recursiveFileLookup" : "true",
            "pathGlobFilter": "*.parquet",
            #"maxFilesPerTrigger": 1,
            #"cloudFiles.schemaLocation": f"/dbfs/mnt/landing/{table_name}",
        }
    audit_write= "False"
    verbose = "False"
    run_dq_rules = "False"
    dq_config_table = "dev.config.ingestion_framework_dq_rules_config"
    source_filepath = f"dbfs:/FileStore/dbx_demo/{table_name}"
    streaming = "False"
    trigger = "once"
    writes =[
            # {
            #     "catalog": "dev",
            #     "schema": "tempjobs",
            #     "table": f"optiva_test_{table_name}",
            #     "data_type": "delta",
            #     "mode": "merge",
            #     "keys": pkeys,
            #     "scd_type": 1,
            #     "checkpointLocation": f"dbfs:/FileStore/dbx_demo_checkpointLocation/optiva_test_{table_name}/"
            # },
            {
                "catalog": "dev",
                "schema": "tempjobs",
                "table": f"optiva_test_{table_name}_history",
                "data_type": "delta",
                "mode": "merge",
                "keys": pkeys,
                "scd_type": 2,
                "checkpointLocation": f"dbfs:/FileStore/dbx_demo_checkpointLocation/optiva_test_{table_name}_history/"
            }
        ]
    transformations = [
        ]

    return Args(config_path=None,
            table_name=table_name,
            data_product_name=data_product_name,
            source_data_type=source_data_type,
            source_table_type=source_table_type,
            source_reader_options=source_reader_options,
            audit_write=audit_write,
            verbose=verbose,
            run_dq_rules=run_dq_rules,
            source_filepath=source_filepath,
            pkeys=pkeys,
            streaming=streaming,
            trigger=trigger,
            writes=writes,
            transformations=transformations,
            source_orderBy_column = source_orderBy_column,
            checkpoint_filepath=checkpoint_filepath,
            dq_config_table=dq_config_table
        )


# COMMAND ----------

def main():
    # Get User Arguments
    args = get_user_args()

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

        "keys": args.pkeys,

        "source_orderBy_column": args.source_orderBy_column,

        # Source data type -> spark.read.format(source_data_type)
        "source_data_type": args.source_data_type,

        # Streaming boolean for whether to use spark.read or the streaming reader
        "streaming" : args.streaming,

        #Dq rules
        # TODO: add dq_config_table column to the config table
        "run_dq_rules": args.run_dq_rules,
        "dq_config_table": args.dq_config_table,

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
    config = ConfigHandler(config_path=args.config_path, config=pipeline_config, spark=spark).get_config()

    # TO DO: Add functions to fncs dinamically
    pb = PipelineBuilder(spark, config, verbose=args.verbose, 
                        fncs=[add_timestamp_cols, display_count, get_distinct_vals,rename_cols])
    
    # Read the data
    df = pb.read()
    return df

    # # Perform transformations
    df, _ = pb.run(df)

    # # Write output
    pb.write(df)


if __name__ == "__main__":
    df = main()

# COMMAND ----------

# MAGIC %md
# MAGIC # To Delete

# COMMAND ----------

df = df.where("price = 2").withColumn("delete_time", F.lit(None).cast("timestamp"))

# COMMAND ----------

join_statement = "target.pkey = source.pkey"
target_table = "dev.tempjobs.optiva_test_ingestion"

# COMMAND ----------

df.createOrReplaceTempView('SourceData')
df.display()

# COMMAND ----------

# DBTITLE 1,ANTES
spark.sql(f"select * from {target_table}").display()

# COMMAND ----------

update_query = f"""


"""

spark.sql(spark.sql(update_query).display()).display()

# COMMAND ----------

# update_query = f"""
# SELECT t.* FROM PreprocessedSource as s
# JOIN {target_table} as t
# ON s.pkey = t.pkey
# AND t.is_current = 1
# UNION ALL
# SELECT * FROM PreprocessedSource
# """
# deleted rows
update_query = f"""
SELECT target.* EXCEPT(end_time, is_current, delete_time), 
    current_timestamp() AS end_time, 
    0 AS is_current, 
    current_timestamp() AS delete_time 
FROM {target_table} AS target
LEFT JOIN SourceData as source
    ON target.pkey = source.pkey
    WHERE 
        source.pkey is NULL
        AND
        target.is_current = 1
"""

update_query = f"""
                SELECT target.* EXCEPT(end_time, is_current, delete_time),
                    current_timestamp() AS end_time, 
                    0 AS is_current, 
                    null AS delete_time  
                FROM SourceData as source
                    JOIN {target_table} as target
                    ON {join_statement}
                    AND target.is_current = 1
                UNION ALL
                    SELECT * FROM SourceData
"""
# UNION ALL
#                     SELECT * FROM SourceData

spark.sql(update_query).display()

# COMMAND ----------

update_query = f"""
SELECT * except(is_current, end_time),
                    ROW_NUMBER() OVER (PARTITION BY pKey ORDER BY file_modification_time DESC) AS rn,
                    CASE
                    WHEN rn != 1 THEN current_timestamp() 
                    ELSE null
                    END AS end_time,
                    CASE 
                    WHEN rn != 1 THEN 0 
                    ELSE 1 
                    END AS is_current
                FROM PreprocessedSource

"""
spark.sql(update_query).display()

# COMMAND ----------


# update_query = f"""
# WITH target as (
#                         SELECT * FROM {target_table}
#                     )
#                         SELECT source.* from PreprocessedSource AS source
#                         left JOIN target
#                         ON {join_statement} and source.file_modification_time = target.file_modification_time
#                         and target.end_time is null

#                 """

# Working
# update_query = f"""
# MERGE INTO {target_table} AS target
# USING
#     (SELECT * FROM PreprocessedSource) AS source
# ON {join_statement} AND source.file_modification_time = target.file_modification_time
# WHEN MATCHED
#   THEN
#   UPDATE SET
#     is_current = 0,
#     end_time = current_timestamp()
# WHEN NOT MATCHED THEN
#   INSERT *
# """

# Working merge
# update_query = f"""
# MERGE INTO {target_table} AS target
# USING

#   (SELECT * except(is_current, end_time),
#     ROW_NUMBER() OVER (PARTITION BY pKey ORDER BY file_modification_time DESC) AS rn,
#     CASE
#       WHEN rn != 1 THEN current_timestamp() 
#       ELSE null
#     END AS end_time,
#     CASE 
#       WHEN rn != 1 THEN 0 
#       ELSE 1 
#     END AS is_current
#   FROM PreprocessedSource) AS source

# ON {join_statement} 
# AND source.file_modification_time = target.file_modification_time
# AND target.end_time is null 
# WHEN MATCHED 
#   THEN
#   UPDATE SET
#     target.is_current = 0,
#     target.end_time = current_timestamp()
# WHEN NOT MATCHED THEN
#   INSERT *
# """


# update_query = f"""
#       SELECT * except(is_current, end_time),
#         ROW_NUMBER() OVER (PARTITION BY pKey ORDER BY file_modification_time DESC) AS rn,
#         CASE
#           WHEN rn != 1 THEN current_timestamp() 
#           ELSE null
#         END AS end_time,
#         CASE 
#           WHEN rn != 1 THEN 0 
#           ELSE 1 
#         END AS is_current
#       FROM PreprocessedSource
# """


spark.sql(update_query)
# updated_df.display()

# COMMAND ----------

# DBTITLE 1,Depois
spark.sql(f"select * from {target_table}").display()

# COMMAND ----------

if full_test_df.isEmpty
    print("empty")

# COMMAND ----------

full_test_df.write.mode("append").format("delta").saveAs

# COMMAND ----------

keys = ["pkey","price"]
left_join_nulls_pkey_list = " AND ".join([f"source.{k} is null" for k in keys])
print(nulls_pkey_list)
