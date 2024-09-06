# Databricks notebook source
# MAGIC %sql
# MAGIC drop  table dev.config.optiva_config_raw_tables_v6

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE dev.config.optiva_config_raw_tables_v6 (
# MAGIC   data_product_name STRING,
# MAGIC   table_name STRING,
# MAGIC   pkeys STRING,
# MAGIC   source_filepath STRING,
# MAGIC   audit_write STRING,
# MAGIC   run_dq_rules STRING,
# MAGIC   source_data_type STRING,
# MAGIC   cast_column STRING,
# MAGIC   initial_filepath STRING,
# MAGIC   delta_filepath STRING,
# MAGIC   external_location STRING,
# MAGIC   is_initial_completed STRING,
# MAGIC   source_is_streaming STRING,
# MAGIC   source_reader_options STRING,
# MAGIC   source_table_type STRING,
# MAGIC   streaming STRING,
# MAGIC   transformations STRING,
# MAGIC   trigger STRING,
# MAGIC   verbose STRING,
# MAGIC   writes STRING,
# MAGIC   is_table_enabled STRING,
# MAGIC   rec_created_date TIMESTAMP DEFAULT current_timestamp)
# MAGIC USING delta
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.feature.allowColumnDefaults' = 'supported')
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC   INSERT INTO dev.config.optiva_config_raw_tables_v6
# MAGIC   select 
# MAGIC   'sap_cdc' as data_product_name,
# MAGIC   "mara" table_name,
# MAGIC   "mandt,matnr" as pkeys,
# MAGIC   Null as source_filepath,
# MAGIC   "False" as audit_write,
# MAGIC   'False' as run_dq_rules,
# MAGIC   'parquet' source_data_type,
# MAGIC   '[]' as cast_column,
# MAGIC   "abfss://poc@sacscdnausedlhdev.dfs.core.windows.net/ecc/MARA_ECC_ADLS/MARA_ECC_ADLS/initial/" as initial_filepath, 
# MAGIC   "abfss://poc@sacscdnausedlhdev.dfs.core.windows.net/ecc/MARA_ECC_ADLS/MARA_ECC_ADLS/delta/" as delta_filepath,
# MAGIC   "abfss://users@sacscdnausedlhdev.dfs.core.windows.net/ecc/external/sapcdc/mara/" as external_location,
# MAGIC   "False" as is_initial_completed,
# MAGIC   "False" source_is_streaming,
# MAGIC '{"recursiveFileLookup":"True","cloudFiles.schemaEvolutionMode":"rescue", "cloudFiles.format":"parquet","cloudFiles.schemaLocation":"abfss://users@sacscdnausedlhdev.dfs.core.windows.net/ecc/schema_path/sapcdc/mara/"}' as source_reader_options,
# MAGIC   "cloudFiles" as source_table_type,
# MAGIC   "True" as streaming,
# MAGIC   '[]' as transformations,
# MAGIC   "availableNow=True" as trigger,
# MAGIC   "True" as verbose,
# MAGIC   '[
# MAGIC             {
# MAGIC                     "catalog":"sandbox",
# MAGIC                     "schema":"dbx_demo",
# MAGIC                     "table":"MARA",
# MAGIC                     "target_table_type":"delta",
# MAGIC                     "mode": "append",
# MAGIC                     "mergeSchema":"true",
# MAGIC                     "trigger":"availableNow=True",
# MAGIC                     "checkpoint_filepath": "abfss://users@sacscdnausedlhdev.dfs.core.windows.net/ecc/checkpoint_path/sapcdc/mara/"
# MAGIC             }
# MAGIC         ]'  as writes,
# MAGIC   "True" as is_table_enabled
# MAGIC   ,current_Timestamp() as rec_created_date
# MAGIC
# MAGIC

# COMMAND ----------

#df=spark.read.parquet('abfss://landing@sacscdnausedlhdev.dfs.core.windows.net/optiva/FSACTION/2024/07/14/')
from pyspark.sql import functions as F
reader_config={"recursiveFileLookup":"True"}
path='abfss://landing@sacscdnausedlhdev.dfs.core.windows.net/optiva/FSACTION/2024/07/14/'
df = (spark.read
            .options(**reader_config)
            .parquet(path)
            .withColumn("file_modification_time", F.col("_metadata.file_modification_time"))
            .withColumn("file_path", F.col("_metadata.file_path"))
)
df.createOrReplaceTempView("fsaction_vw")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from fsaction_vw  order by action_code

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dev.config.optiva_config_raw_tables_v6
# MAGIC --delete from dev.config.optiva_config_raw_tables_v6
# MAGIC --where table_name='employee'
# MAGIC -- update dev.config.optiva_config_raw_tables_v6
# MAGIC -- set is_table_enabled="False"
# MAGIC -- where table_name='mara'
# MAGIC select * from 

# COMMAND ----------

# MAGIC %sql
# MAGIC   INSERT INTO dev.config.optiva_config_raw_tables_v6
# MAGIC   select 
# MAGIC   'optiva' as data_product_name,
# MAGIC   "FSACTION" table_name,
# MAGIC   "ACTION_CODE" as pkeys,
# MAGIC   "abfss://landing@sacscdnausedlhdev.dfs.core.windows.net/optiva/FSACTION/2024/07/14/" as source_filepath,
# MAGIC   "False" as audit_write,
# MAGIC   'False' as run_dq_rules,
# MAGIC   'parquet' source_data_type,
# MAGIC   '[]' as cast_column,
# MAGIC   '' as initial_filepath, 
# MAGIC   '' as delta_filepath,
# MAGIC    "abfss://users@sacscdnausedlhdev.dfs.core.windows.net/ecc/external/optiva/fsaction/" as external_location,
# MAGIC   '' as is_initial_completed,
# MAGIC   "False" source_is_streaming,
# MAGIC '{"recursiveFileLookup":"True"}' as source_reader_options,
# MAGIC   "parquet" as source_table_type,
# MAGIC   "False" as streaming,
# MAGIC   '[]' as transformations,
# MAGIC   "once" as trigger,
# MAGIC   "True" as verbose,
# MAGIC  '[{"catalog": "sandbox", "schema": "dbx_demo", "table": "FSACTION", "data_type": "delta", "mode": "overwrite", "scd_type": 1}, {"catalog": "sandbox", "schema": "dbx_demo", "table": "FSACTION_HISTORY", "data_type": "delta", "mode": "merge", "scd_type": 2}]' as writes,
# MAGIC   "True" as is_table_enabled
# MAGIC   ,current_Timestamp() as rec_created_date
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC   INSERT INTO dev.config.optiva_config_raw_tables_v6 select 
# MAGIC   'optiva' as data_product_name,
# MAGIC       'employee' as table_name,
# MAGIC   "name" as pkeys,
# MAGIC   'abfss://poc@sacscdnausedlhdev.dfs.core.windows.net/ecc/srini/employee/' source_filepath,
# MAGIC   'False'audit_write,
# MAGIC
# MAGIC
# MAGIC   'False' as run_dq_rules,
# MAGIC   'parquet' source_data_type,
# MAGIC  
# MAGIC    '[]' as cast_column,
# MAGIC   "" as initial_filepath, 
# MAGIC   "" as delta_filepath,
# MAGIC   "abfss://users@sacscdnausedlhdev.dfs.core.windows.net/ecc/external/srini/employee/" as external_location,
# MAGIC   "" as is_initial_completed,
# MAGIC   "False" source_is_streaming,
# MAGIC '{"recursiveFileLookup":"True"}' as source_reader_options,
# MAGIC   "parquet" as source_table_type,
# MAGIC   "False" as streaming,
# MAGIC   '[]' as transformations,
# MAGIC   "" as trigger,
# MAGIC   "True" as verbose,
# MAGIC   '[{"catalog": "sandbox", "schema": "dbx_demo", "table": "employee", "data_type": "delta", "mode": "merge", "scd_type": 1}, {"catalog": "sandbox", "schema": "dbx_demo", "table": "employee_history", "data_type": "delta", "mode": "merge", "scd_type": 2}]' as writes,
# MAGIC     "True" as is_table_enabled
# MAGIC   ,current_Timestamp() as rec_created_date
# MAGIC   
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS sandbox.dbx_demo_config.optiva_config_raw_tables_v4 (
# MAGIC   active STRING comment 'when it is active then only SAP table is read from source and written into target for CDC',
# MAGIC   table_name STRING comment 'target table name into which source data is ingested',
# MAGIC   audit_write STRING,
# MAGIC   data_product_name STRING comment 'product name such as SAP-CDC,OPTIVA, NON-OPTIVA', 
# MAGIC   pkeys STRING comment 'primary keys of the table',
# MAGIC   run_dq_rules STRING comment'represents if data quality rules should be run for this table',
# MAGIC   source_data_type STRING comment 'source files type such as parquet, csv, json,text etc.',
# MAGIC   source_filepath STRING comment 'source files location in azure storage account',
# MAGIC   cast_column STRING comment 'Columns whose data types need to be converted or cast', 
# MAGIC   initial_filepath string comment 'location of the initial (history) SAP CDC source files in parquet file format',
# MAGIC   delta_filepath string comment 'location of the SAP CDC delta (cdc) files in parquet file format',
# MAGIC   external_location string comment "table's external location to have the delta table created",
# MAGIC   is_initial_completed BOOLEAN comment 'Represents if loading initial (history) files is completed for the SAP CDC table',
# MAGIC   source_is_streaming STRING comment 'if reading the source files as a stream', 
# MAGIC   source_reader_options STRING comment 'mention all the options that are using to read the source files',
# MAGIC   source_table_type STRING comment 'it represents how are we reading the source such as cloudFiles using autoloadder',
# MAGIC   streaming STRING comment 'represents True/False if it is streaming source or not',
# MAGIC   transformations STRING  comment 'list of transformations that needs to be applied on the source data',
# MAGIC   trigger STRING comment 'Represents how often reading the stream happens. valid values are: "processingTime=N seconds/minutes" or  "availablenow=True"',
# MAGIC   verbose STRING,
# MAGIC   writes STRING comment 'mention all the values that needs to be used in spark.writeStream syntax in the key,value pairs ',
# MAGIC   rec_created_date TIMESTAMP comment 'it is just a timestamp to represent the row creation datatime')
# MAGIC USING delta

# COMMAND ----------

# MAGIC %sql
# MAGIC --select * from sandbox.dbx_demo_config.optiva_config_raw_tables_v4
# MAGIC update sandbox.dbx_demo_config.optiva_config_raw_tables_v4
# MAGIC set active='True' , is_initial_completed=true
# MAGIC where table_name='MARA'
# MAGIC
# MAGIC --select count(*) From sandbox.dbx_demo.mara 
# MAGIC --1,293,407
# MAGIC --1,293,415

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct __operation_type from sandbox.dbx_demo.mara  

# COMMAND ----------

# MAGIC   %sql
# MAGIC   INSERT INTO sandbox.dbx_demo_config.optiva_config_raw_tables_v4 
# MAGIC   select 'False' as active, 
# MAGIC   'AENR' as table_name,
# MAGIC   'False'audit_write,
# MAGIC   'sap-cdc' as data_product_name,
# MAGIC   "mandt,aennr" as pkeys,
# MAGIC   'False' as run_dq_rules,
# MAGIC   'parquet' source_data_type,
# MAGIC   Null as source_filepath,
# MAGIC  '[]' as cast_column,
# MAGIC   "abfss://poc@sacscdnausedlhdev.dfs.core.windows.net/ecc/AENR_ECC_ADLS/initial/" as initial_filepath, 
# MAGIC   "abfss://poc@sacscdnausedlhdev.dfs.core.windows.net/ecc/AENR_ECC_ADLS/delta/" as delta_filepath,
# MAGIC   "abfss://users@sacscdnausedlhdev.dfs.core.windows.net/ecc/external/aenr/" as external_location,
# MAGIC   'False' as is_initial_completed,
# MAGIC   'False' source_is_streaming,
# MAGIC   '{"recursiveFileLookup" : "True","cloudFiles.schemaEvolutionMode":"rescue","cloudFiles.format" : "parquet", "cloudFiles.schemaLocation" : "abfss://poc@sacscdnausedlhdev.dfs.core.windows.net/ecc/schema_path/aenr/"}' as source_reader_options,
# MAGIC   "cloudFiles" as source_table_type,
# MAGIC   "True" as streaming,
# MAGIC   '[]' as transformations,
# MAGIC   "availableNow=True" as trigger,
# MAGIC   "True" as verbose,
# MAGIC   '[
# MAGIC             {
# MAGIC                     "catalog": "sandbox",
# MAGIC                     "schema": "dbx_demo",
# MAGIC                     "table": "AENR",
# MAGIC                     "target_table_type": "delta",
# MAGIC                     "mode": "append",
# MAGIC                     "trigger" : "availableNow=True"
# MAGIC                     "mergeSchema":"true",
# MAGIC                     "checkpoint_filepath": "abfss://poc@sacscdnausedlhdev.dfs.core.windows.net/ecc/checkpoint_path/aenr" 
# MAGIC             }
# MAGIC         ]'  as writes,
# MAGIC   current_timestamp() as rec_created_date
# MAGIC

# COMMAND ----------

# MAGIC  %sql
# MAGIC   INSERT INTO sandbox.dbx_demo_config.optiva_config_raw_tables_v4 
# MAGIC   select 'False' as active, 
# MAGIC     'MARA' as table_name,
# MAGIC   'False'audit_write,
# MAGIC   'sap-cdc' as data_product_name,
# MAGIC   "mandt,matnr" as pkeys,
# MAGIC   'False' as run_dq_rules,
# MAGIC   'parquet' source_data_type,
# MAGIC   Null as source_filepath,
# MAGIC    '[]' as cast_column,
# MAGIC   "abfss://poc@sacscdnausedlhdev.dfs.core.windows.net/ecc/MARA_ECC_ADLS/MARA_ECC_ADLS/initial/" as initial_filepath, 
# MAGIC   "abfss://poc@sacscdnausedlhdev.dfs.core.windows.net/ecc/MARA_ECC_ADLS/MARA_ECC_ADLS/delta/" as delta_filepath,
# MAGIC   "abfss://users@sacscdnausedlhdev.dfs.core.windows.net/ecc/external/sapcdc/mara/" as external_location,
# MAGIC   "False" as is_initial_completed,
# MAGIC   "False" source_is_streaming,
# MAGIC '{"recursiveFileLookup":"True","cloudFiles.schemaEvolutionMode":"rescue", "cloudFiles.format":"parquet","cloudFiles.schemaLocation":"abfss://poc@sacscdnausedlhdev.dfs.core.windows.net/ecc/schema_path/sapcdc/mara/"}' as source_reader_options,
# MAGIC   "cloudFiles" as source_table_type,
# MAGIC   "True" as streaming,
# MAGIC   '[]' as transformations,
# MAGIC   "availableNow=True" as trigger,
# MAGIC   "True" as verbose,
# MAGIC   '[
# MAGIC             {
# MAGIC                     "catalog": "sandbox",
# MAGIC                     "schema": "dbx_demo",
# MAGIC                     "table": "MARA",
# MAGIC                     "target_table_type": "delta",
# MAGIC                     "mode": "append",
# MAGIC                     "mergeSchema":"true",
# MAGIC                     "trigger" : "availableNow=True",
# MAGIC                     "checkpoint_filepath": "abfss://poc@sacscdnausedlhdev.dfs.core.windows.net/ecc/checkpoint_path/mara/"
# MAGIC             }
# MAGIC         ]'  as writes,
# MAGIC   current_timestamp() as rec_created_date

# COMMAND ----------

# MAGIC %sql
# MAGIC   INSERT INTO dev.config.optiva_config_raw_tables_v6
# MAGIC   select 
# MAGIC   'optiva' as data_product_name,
# MAGIC   "FSACTION" table_name,
# MAGIC   "ACTION_CODE" as pkeys,
# MAGIC   "abfss://landing@sacscdnausedlhdev.dfs.core.windows.net/optiva/FSACTION/2024/07/14/" as source_filepath,
# MAGIC   "False" as audit_write,
# MAGIC   'False' as run_dq_rules,
# MAGIC   'parquet' source_data_type,
# MAGIC   '[]' as cast_column,
# MAGIC   '' as initial_filepath, 
# MAGIC   '' as delta_filepath,
# MAGIC    "abfss://users@sacscdnausedlhdev.dfs.core.windows.net/ecc/external/optiva/fsaction/" as external_location,
# MAGIC   '' as is_initial_completed,
# MAGIC   "False" source_is_streaming,
# MAGIC '{"recursiveFileLookup":"True"}' as source_reader_options,
# MAGIC   "parquet" as source_table_type,
# MAGIC   "False" as streaming,
# MAGIC   '[]' as transformations,
# MAGIC   "once" as trigger,
# MAGIC   "True" as verbose,
# MAGIC  '[{"catalog": "sandbox", "schema": "dbx_demo", "table": "FSACTION", "data_type": "delta", "mode": "overwrite", "scd_type": 1}, {"catalog": "sandbox", "schema": "dbx_demo", "table": "FSACTION_HISTORY", "data_type": "delta", "mode": "merge", "scd_type": 2}]' as writes,
# MAGIC   "True" as is_table_enabled
# MAGIC   ,current_Timestamp() as rec_created_date

# COMMAND ----------

# MAGIC  %sql
# MAGIC   INSERT INTO sandbox.dbx_demo_config.optiva_config_raw_tables_v4 
# MAGIC   select 'True' as active, 
# MAGIC     'SRINI_TEST' as table_name,
# MAGIC   'False'audit_write,
# MAGIC   'sap-cdc' as data_product_name,
# MAGIC   "mandt,matnr" as pkeys,
# MAGIC   'False' as run_dq_rules,
# MAGIC   'parquet' source_data_type,
# MAGIC   Null as source_filepath,
# MAGIC   '[{"BRGEW":"DECIMAL(13,3)","NTGEW":"DECIMAL(13,3)","__timestamp":"TIMESTAMP_NTZ"}]' as cast_column,
# MAGIC   "abfss://poc@sacscdnausedlhdev.dfs.core.windows.net/ecc/srini_test/" as initial_filepath, 
# MAGIC   "abfss://poc@sacscdnausedlhdev.dfs.core.windows.net/ecc/MARA_ECC_ADLS/srini_test/delta/" as delta_filepath,
# MAGIC   "abfss://users@sacscdnausedlhdev.dfs.core.windows.net/ecc/external/srini_test/" as external_location,
# MAGIC   "False" as is_initial_completed,
# MAGIC   "False" source_is_streaming,
# MAGIC '{"recursiveFileLookup":"True","cloudFiles.schemaEvolutionMode":"rescue","cloudFiles.format":"parquet","cloudFiles.schemaLocation":"abfss://poc@sacscdnausedlhdev.dfs.core.windows.net/ecc/schema_path/srini_test/"}' as source_reader_options,
# MAGIC   "cloudFiles" as source_table_type,
# MAGIC   "True" as streaming,
# MAGIC   '[]' as transformations,
# MAGIC   "availableNow=True" as trigger,
# MAGIC   "True" as verbose,
# MAGIC   '[
# MAGIC             {
# MAGIC                     "catalog":"sandbox",
# MAGIC                     "schema":"dbx_demo",
# MAGIC                     "table":"srini_test",
# MAGIC                     "target_table_type":"delta",
# MAGIC                     "mode":"append",
# MAGIC                     "mergeSchema":"true",
# MAGIC                     "trigger":"availableNow=True",
# MAGIC                     "checkpoint_filepath": "abfss://poc@sacscdnausedlhdev.dfs.core.windows.net/ecc/checkpoint_path/srini_test/"
# MAGIC             }
# MAGIC         ]'  as writes,
# MAGIC   current_timestamp() as rec_created_date

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from sandbox.dbx_demo_config.optiva_config_raw_tables_v4
# MAGIC --update sandbox.dbx_demo_config.optiva_config_raw_tables_v4 set active='False' where table_name='AENR'
# MAGIC --1,293,407
# MAGIC --1293412

# COMMAND ----------

from pyspark.sql import functions as F
df = spark.sql("""select * from parquet.`abfss://poc@sacscdnausedlhdev.dfs.core.windows.net/ecc/MARA_ECC_ADLS/MARA_ECC_ADLS/initial/` limit 5""")
df1=(df
     .withColumn("__timestamp",F.col("__timestamp").cast('string'))
     .withColumn("BRGEW",F.col('BRGEW').cast('string'))
)

df1.write.format("parquet").save("abfss://poc@sacscdnausedlhdev.dfs.core.windows.net/ecc/sapcdc/test/dc_test")


# COMMAND ----------

dbutils.fs.ls('abfss://landing@sacscdnausedlhdev.dfs.core.windows.net/optiva/FSACTION')

# COMMAND ----------

# MAGIC %sql
# MAGIC create table dev.raw.srini_test (
# MAGIC   name string,
# MAGIC   job string,
# MAGIC   salary double,
# MAGIC   creation_date timestamp,
# MAGIC   modify_date timestamp
# MAGIC )
# MAGIC using delta
# MAGIC location 'abfss://users@sacscdnausedlhdev.dfs.core.windows.net/ecc/optiva/srini_test'
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into dev.raw.srini_test(name, job, salary, creation_date, modify_date) 
# MAGIC values('John', 'Marketing', 2333.00, current_timestamp(), current_timestamp())

# COMMAND ----------

df = spark.sql("select * from dev.raw.srini_test")
df.write.format("parquet").save("abfss://poc@sacscdnausedlhdev.dfs.core.windows.net/ecc/srini/employee/01/")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from sandbox.dbx_demo.dc_test
