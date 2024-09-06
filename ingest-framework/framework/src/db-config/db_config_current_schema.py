# Databricks notebook source
import datetime
import json
from pyspark.sql import functions as F

# #Databricks Connect specific: To delete
# from databricks.connect import DatabricksSession
# spark = DatabricksSession.builder.getOrCreate()

# COMMAND ----------

current_db_config_table = "dev.config.optiva_config_raw_tables_v7_1"
new_db_config_table = "dev.config.non_cdc_framework_config"

# COMMAND ----------

#spark.sql(f"select * from {current_db_config_table}").display()

# COMMAND ----------

current_db_config_df = spark.read.table(current_db_config_table)
#current_db_config_list = current_db_config_df.collect()

# COMMAND ----------


current_db_config_df = current_db_config_df.where(F.col("table_name").isin(['FSGLOBALCALCRELATIONSHIP','FSGLOBALCALCFORMULARELATION','FSGLOBALCALCFORMULASEARCHLIST','FSGLOBALCALCPARENTFORMULA','FSGLOBALCALCFORMULAMESSAGE','FSGLOBALCALCTEMPFORMULALEVEL','FSGLOBALCALCCHILDFORMULA','FSGLOBALCALCFORMULALEVEL']))
#current_db_config_df = current_db_config_df.where(F.col("table_name").isin(["FSGLOBALCALCFORMULARELATION"]))



# COMMAND ----------

data = []
current_db_config_list = current_db_config_df.collect()
for row in current_db_config_list:
    pkeys = ",".join(spark.read.option("recursiveFileLookup", "true").parquet(row.source_filepath).limit(1).columns)
    data.append({
        "data_product_name": row.data_product_name, # optiva or sap_cdc
        "table_name": row.table_name,
        "pkeys": pkeys, # optiva primary keys to be used on merge
        "source_filepath": row.source_filepath,
        "is_table_enabled": row.is_table_enabled # Enable or disable the datapipeline for this table
    }.copy())

new_df = spark.createDataFrame(data)

# COMMAND ----------

new_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Create new table for testing 

# COMMAND ----------

current_db_config_list=new_df.collect()

# COMMAND ----------

# DBTITLE 1,New working template
data = []
target_catalog = "dev"
target_schema = "do_dest"
target_checkpointLocation = "abfss://dodest@sacscdnausedlhdev.dfs.core.windows.net/ecc/ingestion_framework/non_cdc/checkpoints"

# External Locations are disabled - ask for a schema where we can create external tables
targetexternal_location = "abfss://dodest@sacscdnausedlhdev.dfs.core.windows.net/ecc/ingestion_framework/non_cdc/tables"

for row in current_db_config_list:

    db_config_data = {
        "data_product_name": row.data_product_name, # optiva or sap_cdc
        "table_name": row.table_name,
        "pkeys": row.pkeys, # optiva primary keys to be used on merge
        "source_filepath": row.source_filepath,
        "source_orderBy_column": "file_modification_time", # default column request to order records
        "audit_write": "True",
        "audit_config": json.dumps({"audit_table_name":"dev.do_dest.non_cdc_audit_logs", "external_location": f"{targetexternal_location}/non_cdc_audit-logs"}), #does support external_location
        "run_dq_rules": "True", 
        "dq_config": json.dumps({'dq_config_table':'dev.config.non_cdc_framework_data_quality_rules_config', 'dq_log_table':'dev.do_dest.non_cdc_data_quality_logs', "external_location": f"{targetexternal_location}/non_cdc_data-quality-logs"}),
        "source_workload_type": "NON_CDC", # NON_CDC or CDC
        "source_extraction_type": "FE", # FE for Full Extract or IE for Incremental Extract
        "source_data_type": "parquet",
        "cast_column": "", # sap_cdc only
        "initial_filepath": "", # sap_cdc only
        "delta_filepath": "", # sap_cdc only
        "external_location": "", # sap_cdc only
        "is_initial_completed": "False", # sap_cdc only
        "source_is_streaming": "False", # Not implemented yet
        "source_reader_options": '{"recursiveFileLookup":"True","cloudFiles.schemaEvolutionMode":"rescue","cloudFiles.format":"parquet"}', # spark read or readstream supported parameters
        "source_table_type": "cloudFiles", # It needs to be 'cloudfiles' for autoloader. In batch mode, it must match table type (parquet, csv, json)
        "streaming": "True", # False == batch mode / True == autoloader
        "transformations": "[]",
        "trigger": "once", # Not implemented yet
        "verbose": "True", 
        "writes":json.dumps([
                {
                    "table_medallion_layer": "bronze",
                    "catalog": f"{target_catalog}",
                    "schema": f"{target_schema}",
                    "table": f"noncdc_{row.table_name}_bronze",
                    "data_type": "delta",
                    "mode": "append",
                    "scd_type": "2",
                    "checkpointLocation": f"{target_checkpointLocation}/noncdc_{row.table_name}_bronze",
                    "external_location": f"{targetexternal_location}/noncdc_{row.table_name}_bronze"
                },
                {
                    "table_medallion_layer": "silver",
                    "catalog": f"{target_catalog}",
                    "schema": f"{target_schema}",
                    "table": f"noncdc_{row.table_name}_history",
                    "data_type": "delta",
                    "mode": "merge",
                    "scd_type": "2",
                    "checkpointLocation": f"{target_checkpointLocation}/noncdc_{row.table_name}_history",
                    "external_location": f"{targetexternal_location}/noncdc_{row.table_name}_history"
                },
                {
                    "table_medallion_layer": "silver",
                    "catalog": f"{target_catalog}",
                    "schema": f"{target_schema}",
                    "table": f"noncdc_{row.table_name}",
                    "data_type": "delta",
                    "mode": "merge",
                    "scd_type": "1",
                    "checkpointLocation": f"{target_checkpointLocation}/noncdc_{row.table_name}",
                    "external_location": f"{targetexternal_location}/noncdc_{row.table_name}"
                }
                ]),
        "is_table_enabled": row.is_table_enabled, # Enable or disable the datapipeline for this table
        "rec_created_date": datetime.datetime.now() #Timestamp("2024-07-17 00:29:42.141000")
    }

    data.append(db_config_data.copy())

# COMMAND ----------

df = spark.createDataFrame(data)
df.display()

# COMMAND ----------

df.write.format("delta").mode("append").saveAsTable(new_db_config_table)

# COMMAND ----------

spark.sql(f"select * from {new_db_config_table} where is_table_enabled = 'True' and data_product_name = 'optiva' ").display()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dev.config.non_cdc_framework_data_quality_rules_config
