# Databricks notebook source
# MAGIC %sql
# MAGIC select * from dev.config.optiva_config_raw_tables_v6
# MAGIC --delete from dev.config.optiva_config_raw_tables_v6 where table_name ='dc_test'

# COMMAND ----------

# MAGIC  %sql
# MAGIC   INSERT INTO dev.config.optiva_config_raw_tables_v6
# MAGIC   select 
# MAGIC     'sap_cdc' as data_product_name,
# MAGIC     'dc_test' as table_name,
# MAGIC      '' pkeys, 
# MAGIC   Null as source_filepath,
# MAGIC   'False'audit_write,
# MAGIC   'False' as run_dq_rules,
# MAGIC   'parquet' source_data_type,
# MAGIC   '[{"BRGEW":"DECIMAL(13,3)","NTGEW":"DECIMAL(13,3)","__timestamp":"TIMESTAMP_NTZ"}]' as cast_column,
# MAGIC   "abfss://poc@sacscdnausedlhdev.dfs.core.windows.net/ecc/sapcdc/test/dc_test" as initial_filepath, 
# MAGIC   "" as delta_filepath,
# MAGIC   "abfss://users@sacscdnausedlhdev.dfs.core.windows.net/ecc/external/sapcdc/dc_test/" as external_location,
# MAGIC   "False" as is_initial_completed,
# MAGIC   "False" source_is_streaming,
# MAGIC '{"recursiveFileLookup":"True","cloudFiles.schemaEvolutionMode":"rescue","cloudFiles.format":"parquet","cloudFiles.schemaLocation":"abfss://poc@sacscdnausedlhdev.dfs.core.windows.net/ecc/schema_path/sapcdc/dc_test/"}' as source_reader_options,
# MAGIC   "cloudFiles" as source_table_type,
# MAGIC   "True" as streaming,
# MAGIC   '[]' as transformations,
# MAGIC   "once" as trigger,
# MAGIC   "True" as verbose,
# MAGIC   '[
# MAGIC             {
# MAGIC                     "catalog":"sandbox",
# MAGIC                     "schema":"dbx_demo",
# MAGIC                     "table":"dc_test",
# MAGIC                     "target_table_type":"delta",
# MAGIC                     "mode":"append",
# MAGIC                     "mergeSchema":"true",
# MAGIC                     "trigger":"availableNow=True",
# MAGIC                     "checkpoint_filepath": "abfss://poc@sacscdnausedlhdev.dfs.core.windows.net/ecc/checkpoint_path/sapcdc/dc_test/"
# MAGIC             }
# MAGIC         ]'  as writes,
# MAGIC   "True" as is_table_enabled,
# MAGIC   current_timestamp() as rec_created_date

# COMMAND ----------

from pyspark.sql import functions as F
df = spark.sql("""select * from parquet.`abfss://poc@sacscdnausedlhdev.dfs.core.windows.net/ecc/MARA_ECC_ADLS/MARA_ECC_ADLS/initial/` limit 5""")
df1=(df
     .withColumn("__timestamp",F.col("__timestamp").cast('string'))
     .withColumn("BRGEW",F.col('BRGEW').cast('string'))
)

df1.write.format("parquet").save("abfss://poc@sacscdnausedlhdev.dfs.core.windows.net/ecc/sapcdc/test/dc_test")


# COMMAND ----------

# MAGIC %sql
# MAGIC show create table  sandbox.dbx_demo.srini_test
