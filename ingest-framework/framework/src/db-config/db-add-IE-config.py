# Databricks notebook source
from datetime import datetime
from pyspark.sql import functions as F

# COMMAND ----------

config_database = "dev.config.non_cdc_framework_config_v2"

task_list = {
    "data_product_name": "optiva_ie_delete",
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
    "pkeys": "mandt,mblnr,mjahr,zeile",
    "streaming": "True",
    "trigger": "once",
    "writes": """[
        {
            "table_medallion_layer": "bronze",
            "catalog": "dev",
            "schema": "do_dest",
            "table": "noncdc_SAP_MSEG_bronze",
            "data_type": "delta",
            "mode": "append",
            "scd_type": "2",
            "checkpointLocation": "abfss://dodest@sacscdnausedlhdev.dfs.core.windows.net/ecc/ingestion_framework/non_cdc/checkpoints/noncdc_SAP_MSEG_bronze",
            "external_location": "abfss://dodest@sacscdnausedlhdev.dfs.core.windows.net/ecc/ingestion_framework/non_cdc/tables/noncdc_SAP_MSEG_bronze"
        },
        {
            "table_medallion_layer": "silver",
            "catalog": "dev",
            "schema": "do_dest",
            "table": "noncdc_SAP_MSEG_history",
            "data_type": "delta",
            "mode": "merge",
            "scd_type": "2",
            "external_location": "abfss://dodest@sacscdnausedlhdev.dfs.core.windows.net/ecc/ingestion_framework/non_cdc/tables/noncdc_SAP_MSEG_history",
            "delete_source_filepath": "abfss://landing@sacscdnausedlhdev.dfs.core.windows.net/delete/sap_dwc/V_MSEG"

        },
        {
            "table_medallion_layer": "silver",
            "catalog": "dev",
            "schema": "do_dest",
            "table": "noncdc_SAP_MSEG",
            "data_type": "delta",
            "mode": "merge",
            "scd_type": "1",
            "external_location": "abfss://dodest@sacscdnausedlhdev.dfs.core.windows.net/ecc/ingestion_framework/non_cdc/tables/noncdc_SAP_MSEG",
            "delete_source_filepath": "abfss://landing@sacscdnausedlhdev.dfs.core.windows.net/delete/sap_dwc/V_MSEG"

        }
    ]""",
    "transformations": """[
        {
            "column_names_to_lower":{}
        }
    ]""",
    "source_orderBy_column": "file_modification_time",
    "is_table_enabled": "True", # Enable or disable the datapipeline for this table
    "rec_created_date": datetime.now() #Timestamp("2024-07-17 00:29:42.141000")
}

# COMMAND ----------

# spark.createDataFrame([task_list]).write.format("delta").mode("append").saveAsTable(config_database)

# COMMAND ----------

#spark.sql(f"select * from {config_database} where data_product_name = 'optiva_ie_delete'").display()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dev.config.non_cdc_framework_config_v2
# MAGIC where data_product_name = 'optiva_ie_delete'
# MAGIC -- delete from dev.config.non_cdc_framework_config_v2
# MAGIC -- where data_product_name = 'optiva_ie_delete'
# MAGIC

# COMMAND ----------

df = spark.sql(f"select * from {config_database} where table_name = 'FSACTION' and data_product_name = 'optiva'")
df.display()

# COMMAND ----------

df = df.union(_sqldf)

# COMMAND ----------

df = df.withColumn("data_product_name", F.lit("optiva_demo"))

# COMMAND ----------

df.display()

# COMMAND ----------

#df.write.format("delta").mode("append").saveAsTable(config_database)
