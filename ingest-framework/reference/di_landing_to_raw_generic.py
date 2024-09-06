# Databricks notebook source
# MAGIC %md
# MAGIC ## Data from Landing to Raw Layer

# COMMAND ----------

# MAGIC %md 
# MAGIC #####Define Widgets

# COMMAND ----------

import os, re
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType
from pyspark.sql.functions import concat,lit,col,concat_ws,sum, row_number, split, explode
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark import sql

# COMMAND ----------

dbutils.widgets.text("schema_name", "raw")
dbutils.widgets.text("table_name", "")
dbutils.widgets.text("data_product_name", "")
dbutils.widgets.text("catalog_name", "dev")
dbutils.widgets.text("post_run", "Y")

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Phase 1: Load

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog '${catalog_name}'

# COMMAND ----------

# MAGIC %sql
# MAGIC use database ${schema_name}

# COMMAND ----------

# MAGIC %sql
# MAGIC SET TIME ZONE 'America/New_York';

# COMMAND ----------

# MAGIC %md
# MAGIC #####--ABC Set up SQL server connection for ABC and DQ

# COMMAND ----------

# MAGIC %run "./SQLConfigNotebook"

# COMMAND ----------

# MAGIC %md 
# MAGIC #####Check if configs exist. If not, Exit with Exception

# COMMAND ----------

val_table_name = dbutils.widgets.get("table_name")
val_data_product_name = dbutils.widgets.get("data_product_name")
val_schema_name = dbutils.widgets.get("schema_name")
val_catalog_name = dbutils.widgets.get("catalog_name")
val_post_run = dbutils.widgets.get("post_run")

# COMMAND ----------

# ctl_query_pre = (spark.read
#   .format("sqlserver")
#   .option("host", server)
#   .option("port", "1433")
#   .option("user", username)
#   .option("password", password)
#   .option("database", database)
#   .option("dbtable", "dbo.control_raw")
#   .load()
#  )
#
# df_ctl = ctl_query_pre[(ctl_query_pre['table_name'] == val_table_name) & (ctl_query_pre['schema_name'] == val_schema_name)]
#
# # COMMAND ----------
#
# if df_ctl.count() == 0:
#     raise Exception("No ingest configured for table "+ val_schema_name + "." + val_table_name +". Please configure and try again.")

# COMMAND ----------

#breadcrumbs for debugging #1
df_ctl.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #####--ABC Initialize audit
# MAGIC

# COMMAND ----------

# values = [(val_table_name,val_data_product_name)]
# columns = ['table_name','data_product_name']
# df = spark.createDataFrame(values, columns)
#
# try:
#     df.write \
#         .format("jdbc") \
#         .mode("Append") \
#         .option("url", jdbc_url) \
#         .option("dbtable", "dq_audit_dim") \
#         .option("user", username) \
#         .option("password", password) \
#         .option("SaveMode", "APPEND") \
#         .save()
# except Exception as e:
#     print(e)


# # COMMAND ----------
#
# audit_dim_id_variable = 0
# sla_dim_id_variable = 0
#
# query = "(SELECT max(audit_dim_id) audit_dim_id FROM dbo.dq_audit_dim WHERE table_name = '" + val_table_name + "') AS audit_dim_id"
# df_tables = spark.read.jdbc(url=jdbc_url, table=query, properties=connection_properties)
# audit_dim_id_variable  = df_tables.first()[0]
#
# query2 = "(Select max(sla_dim_id) sla_dim_id From dbo.dq_sla_dim where table_name ='" + val_table_name + "') AS sla_dim_id"
# df_tables2 = spark.read.jdbc(url=jdbc_url, table=query2, properties=connection_properties)
# sla_dim_id_variable = df_tables2.first()[0]
#
# # COMMAND ----------
#
# print(audit_dim_id_variable)
#
# # COMMAND ----------
#
# if sla_dim_id_variable is None:
#     sla_dim_id_variable = 0

# COMMAND ----------

# MAGIC %md
# MAGIC #####--ABC Log Pipeline Start

# COMMAND ----------

from datetime import datetime

columns1 = ['sla_dim_id', 'audit_record_type_desc', 'audit_timestamp', 'audit_value', 'valid_flag', 'audit_dim_id']
values1 = [(str(sla_dim_id_variable),  "pipeline_start" , datetime.now(), 0, 'Y',  str(audit_dim_id_variable))]
df_audit_insert = spark.createDataFrame(values1, columns1)

try:
    df_audit_insert.write \
        .format("jdbc") \
        .mode("Append") \
        .option("url", jdbc_url) \
        .option("dbtable", "dbo.dq_audit_fact") \
        .option("user", username) \
        .option("password", password) \
        .option("SaveMode", "APPEND") \
        .save()
except Exception as e:
    print(e) 
    

# COMMAND ----------

# MAGIC %md 
# MAGIC #####Import pyspark SQL Functions

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Define Temp View Name

# COMMAND ----------

# Target table name with schema and table name together
tgt_table_name = val_schema_name + "." + val_table_name

# Temp view for autoloader upsert only
temp_view_name = "TEMP_" + val_table_name 

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Reading parameter values from Control Table.

# COMMAND ----------


# Landing File Path
val_path = df_ctl.head()[3]

# Landing File Format (csv, parquet, json etc)
val_file_frmt = df_ctl.head()[4]

# Landing File Name Format
val_file_nm_frmt = df_ctl.head()[5]

# Landing File header Flag
val_file_header = df_ctl.head()[6]

# Landing File Load Type (update ,append ,complete)
val_load_type = df_ctl.head()[7]

# Check Point Path
checkpoint_path = df_ctl.head()[8] + "_" + val_table_name

val_schema = df_ctl.head()[10]

# Input File with Path
Input_file = val_path + val_file_nm_frmt

case_statement = "" 

# COMMAND ----------

print(val_path)
print(val_file_frmt)
print(val_file_nm_frmt)
print(val_file_header)
print(val_load_type)
print(checkpoint_path)
print(val_schema)
print(Input_file)


# COMMAND ----------

# MAGIC %md
# MAGIC #####Read Data file Schema from most recent file

# COMMAND ----------

# get most recently loaded file


df_schema= spark.read.option("recursiveFileLookup", "true").format(val_file_frmt).load(val_path).select("_metadata").distinct()
if df_schema.count() == 0:
    print("Empty File")
    dbutils.notebook.exit("Empty Files Provided")
else:
    df_schema=df_schema.withColumn("Filepath",F.expr("_metadata.file_path")).withColumn("file_modification_time",F.expr("_metadata.file_modification_time")).withColumn("rnk",F.row_number().over(Window.orderBy(F.col("file_modification_time").desc()))).filter("rnk==1").select('Filepath')
    #display(df_schema)
    val_most_recent=df_schema.first()['Filepath']
    if(val_file_header==False):
        query = "(select source_col_name,target_col_order from dbo.control_raw_source_to_target_col_map(nolock) where schema_name = '"+val_schema_name+"' and table_name='"+val_table_name+"') as source_quey"
        col_lst_query = spark.read.jdbc(url=jdbc_url, table=query, properties=connection_properties).orderBy(col("target_col_order")).select("source_col_name").withColumn("source_col_name",F.expr('concat_ws(",",collect_list(source_col_name))'))
        #display(col_lst_query)

        df_col_lst = col_lst_query.head()[0].split(",")
        schema = (
        spark.read.format(val_file_frmt)
        .option("header", val_file_header)
        .load(val_most_recent)
        .toDF(*df_col_lst)
        .withColumn("file_modification_time",F.expr("_metadata.file_modification_time"))
        .schema
        )
        #print(schema)
    else:
        schema = (
        spark.read.format(val_file_frmt)
        .option("header", val_file_header)
        .load(val_most_recent)
        .withColumn("file_modification_time",F.expr("_metadata.file_modification_time"))
        .schema
        )

# COMMAND ----------

# MAGIC %md
# MAGIC #####Read Data files using Autoloader Stream and add audit columns (file name being processed and processing Time)

# COMMAND ----------

input_df = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", val_file_frmt)
    .option("header", val_file_header)
    .schema(schema)
    .option("pathGlobfilter", val_file_nm_frmt)
    .load(val_path)
    .select(
        "*",
        F.current_timestamp().alias("create_date"),
        F.current_timestamp().alias("change_date"),
        F.current_timestamp().alias("src_file_process_time"),
        F.lit("true").alias("data_quality_valid_flag"),
        F.array([]).alias("data_quality_result_array")

    )
    .withColumn("file_modification_time",F.expr("_metadata.file_modification_time"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC #####--ABC Get Target Before count

# COMMAND ----------

trg_val = spark.sql(
    "Select count(*) as row_count from "
    + dbutils.widgets.get("schema_name")
    + "."
    + dbutils.widgets.get("table_name")
).first()["row_count"]

columns1 = ['sla_dim_id', 'audit_record_type_desc', 'audit_timestamp', 'audit_value', 'valid_flag', 'audit_dim_id']
values1 = [(str(sla_dim_id_variable),  "Target_Object_Pre_Count" , datetime.now(), str(trg_val), 'Y',  str(audit_dim_id_variable))]

df_audit_insert = spark.createDataFrame(values1, columns1)

try:
    df_audit_insert.write \
        .format("jdbc") \
        .mode("Append") \
        .option("url", jdbc_url) \
        .option("dbtable", "dbo.dq_audit_fact") \
        .option("user", username) \
        .option("password", password) \
        .option("SaveMode", "APPEND") \
        .save()
except Exception as e:
    print(e)
    

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Replace spaces with Underscore in Dataframe column names

# COMMAND ----------

for each in input_df.schema.names:
    input_df = input_df.withColumnRenamed(
        each, re.sub(r"\s+([a-zA-Z_][a-zA-Z_0-9]*)\s*", "", each.replace(" ", "_"))
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Read Source target Column Mapping from framework table

# COMMAND ----------

#check_column = spark.catalog.listColumns("dev."+val_table_name+"."+val_schema_name)
check_column = spark.sql("select count(*) cnt from " + val_catalog_name + ".information_schema.columns where table_name = '" + val_table_name + "' and column_name in ('data_quality_valid_flag','data_quality_result_array');  ").first()["cnt"]
if check_column >= 2:
   val_dq_Codes = "Y"
else:
   val_dq_Codes = "N" 
    

# COMMAND ----------

if val_dq_Codes == "N":
    columns1 = ['sla_dim_id', 'audit_record_type_desc', 'audit_timestamp', 'audit_value', 'valid_flag', 'audit_dim_id']
    values1 = [(str(sla_dim_id_variable),  "valid_flag or dq_Codes Missing" + val_table_name , datetime.now(), 0, 'Y',  str(audit_dim_id_variable))]
    df_audit_insert = spark.createDataFrame(values1, columns1)

    try:
        df_audit_insert.write \
            .format("jdbc") \
            .mode("Append") \
            .option("url", jdbc_url) \
            .option("dbtable", "dbo.dq_audit_fact") \
            .option("user", username) \
            .option("password", password) \
            .option("SaveMode", "APPEND") \
            .save()
    except Exception as e:
        print(e) 


# COMMAND ----------

# Rename the Source column names into Organization Stnadards and apply any transformation from config

col_map_query = "(select case when target_col_expression is not null then target_col_expression else source_col_name end as source_col_name,target_col_name,target_col_order from dbo.control_raw_source_to_target_col_map(nolock) where schema_name = '"+val_schema_name+"' and table_name='"+val_table_name+"') as source_quey"
df_col_map_1 = spark.read.jdbc(url=jdbc_url, table=col_map_query, properties=connection_properties).orderBy(col("target_col_order"))
df_col_map=df_col_map_1.selectExpr("replace(source_col_name,' ','_')||' AS '||target_col_name as col_name").withColumn("col_name",F.expr("concat_ws(':', collect_list(col_name))")).select('col_name')
df_col_map_update=df_col_map_1.selectExpr("replace(concat('t.',target_col_name),' ','_')||' = '||concat('s.',target_col_name) as col_name").withColumn("col_name",F.expr("concat_ws(',', collect_list(col_name))")).select('col_name')
df_col_map_insert=df_col_map_1.selectExpr("target_col_name as col_name").withColumn("col_name",F.expr("concat_ws(',', collect_list(col_name))")).select('col_name')

# Parameter with source to target column alias
val_col_map = df_col_map.head()[0]+':create_date AS create_date:change_date AS change_date:file_modification_time AS file_modification_time:data_quality_valid_flag as data_quality_valid_flag:data_quality_result_array as data_quality_result_array'

if val_dq_Codes == "Y":
   val_col_map_insert = df_col_map_insert.head()[0]+',create_date,change_date,data_quality_valid_flag,data_quality_result_array'
   val_col_map_update = df_col_map_update.head()[0]+',t.change_date=s.change_date,t.data_quality_valid_flag=s.data_quality_valid_flag,t.data_quality_result_array=s.data_quality_result_array'
else:
   val_col_map_insert = df_col_map_insert.head()[0]+',create_date,change_date' 
   val_col_map_update = df_col_map_update.head()[0]+',t.change_date=s.change_date'

# creating List from the string of columns from above paramter
col_list = list(val_col_map.split(":"))
#print(col_list)

# Rename the column header in dataframe with the columns as per standards
input_df = input_df.selectExpr(
    col_list
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Preparing Join condition for Update Mode Batch Processing

# COMMAND ----------

if val_load_type == "update" or  val_load_type == "complete" :

    #col_join_query = f"""select concat_ws(' and ', collect_list('s.'||target_col_name||'=t.'||target_col_name)) FROM (select target_col_name from config.control_raw_source_to_target_col_map where table_name='{val_table_name}' and target_key_col_order is not null ORDER BY target_key_col_order)"""

    #df_join_cond = spark.sql(col_join_query)

    col_join_query = "(select target_col_name,target_key_col_order from dbo.control_raw_source_to_target_col_map(nolock) where table_name='"+val_table_name+"' and target_key_col_order is not null) as source_quey"
    df_join_cond_1 = spark.read.jdbc(url=jdbc_url, table=col_join_query, properties=connection_properties).orderBy(col("target_key_col_order"))
    df_join_cond=df_join_cond_1.selectExpr("'s.'||target_col_name||'=t.'||target_col_name as col_name").withColumn("col_name",F.expr("concat_ws(' and ', collect_list(col_name))")).select('col_name')

    # string parameter contains join condition
    val_join_cond = df_join_cond.head()[0]
    # print(val_join_cond)
else:
    print("Not update Mode")

# COMMAND ----------

display(val_join_cond)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Prepare Merge SQL Query for the write stream (Update Mode Batch Processing only)

# COMMAND ----------

#if val_load_type == "update" or val_load_type == "complete" :
#    merge_query = f"""MERGE INTO raw.{val_table_name} t USING {temp_view_name} s ON {val_join_cond} WHEN MATCHED THEN UPDATE SET * #WHEN NOT MATCHED THEN INSERT *"""dbuti
#else:
#    print("Not Update Mode")
if val_load_type == "update" or val_load_type == "complete" :
    merge_query = f"""MERGE INTO raw.{val_table_name} t USING {temp_view_name} s ON {val_join_cond} WHEN MATCHED THEN UPDATE SET {val_col_map_update} WHEN NOT MATCHED THEN INSERT ({val_col_map_insert}) VALUES({val_col_map_insert})"""
else:
    print("Not Update Mode")

# COMMAND ----------

print(val_table_name)

# COMMAND ----------

# MAGIC %run "../../dq/transformations/dq_rule_builder_common" $audit_dim_id=audit_dim_id_variable,$catalog_name=val_catalog_name,$table_name=val_table_name,$database_name=val_schema_name,$post_run=val_post_run

# COMMAND ----------

# MAGIC %md
# MAGIC ##### UDF for Update Mode batch processing
# MAGIC Note that at least 1 pkey and 1 'latest row' column are required for upsert. Configure in config.control_source_to_target_col_map

# COMMAND ----------

if val_load_type == "update" or val_load_type == "complete":

    def upsertToDelta(microBatchOutputDF, batch_id):
        print("start")
        src_rowcount = 0
        # get pkey of the table
        #col_pkey_query = f"""select concat_ws(',',collect_set( pkeys )) as pkeys from (select source_col_name as pkeys from config.control_raw_source_to_target_col_map where table_name='{val_table_name}' and target_key_col_order is not null ORDER BY target_key_col_order)"""
        #col_pkey_query.display() 
        #df_pkey_cond = spark.sql(col_pkey_query)
        #val_pkey = df_pkey_cond.head()[0].split(",")

        col_pkey_query = "(select target_col_name as pkeys,target_key_col_order from dbo.control_raw_source_to_target_col_map(nolock) where table_name='"+val_table_name+"' and target_key_col_order is not null) as source_quey"
        df_pkey_cond_1 = spark.read.jdbc(url=jdbc_url, table=col_pkey_query, properties=connection_properties).orderBy(col("target_key_col_order"))
        df_pkey_cond=df_pkey_cond_1.selectExpr("pkeys as col_name").withColumn("col_name",F.expr("concat_ws(',', collect_set(col_name))")).select('col_name')   
        val_pkey = df_pkey_cond.head()[0].split(",")

        # Get column to set most latest record
        # print(merge_query)
        # get most recent row per the window function
        df_cte = microBatchOutputDF.withColumn(
            "row_number", row_number().over(Window.partitionBy(val_pkey).orderBy(col('file_modification_time').desc()))
        )

        df_cte.cache()
        src_rowcount_raw = df_cte.count()

        if src_rowcount_raw > 0:
            delete_full_load()

        # capture the raw dataframe count

        columns2 = ['sla_dim_id', 'audit_record_type_desc', 'audit_timestamp', 'audit_value', 'valid_flag', 'audit_dim_id']
        values2 = [(str(sla_dim_id_variable),  "Source_Object_Raw_Count" , datetime.now(), str(src_rowcount_raw), 'Y',  str(audit_dim_id_variable))]
        df_audit_insert = spark.createDataFrame(values2, columns2)

        try:
            df_audit_insert.write \
            .format("jdbc") \
            .mode("Append") \
            .option("url", jdbc_url) \
            .option("dbtable", "dbo.dq_audit_fact") \
            .option("user", username) \
            .option("password", password) \
            .option("SaveMode", "APPEND") \
            .save()
        except Exception as e:
            print(e)


        df_cte_result = df_cte.filter(col("row_number") == 1)
        final_df = df_cte_result.drop(col("row_number")).drop(col("file_modification_time"))

        # create temp view with microbatch
        #final_df.cache()


        if case_statement != "":
            #final_df._jdf.sparkSession().createOrReplaceTempView("temp_view_name_stage")
            final_df.createOrReplaceTempView("temp_view_name_stage")
            sparkSession = microBatchOutputDF._jdf.sparkSession()
            select_stmt = "select *," + case_statement + " from temp_view_name_stage" 

            if table_join != "":
               select_stmt += table_join

            #+ temp_view_name  + "_stage"
            #else:
            #select_stmt = "*," + case_statement 
        
            #new_result_df = final_df.selectExpr(select_stmt)

            df = sparkSession.sql(select_stmt)
            new_result_df = sql.DataFrame(df,microBatchOutputDF.sql_ctx)
            #print(new_result_df.count())

            selected_columns = [column for column in new_result_df.columns if column.startswith("case")]

            #new_result_df = new_result_df.withColumn("dq_codes",concat_ws(",",*selected_columns))
            #new_result_df = new_result_df.withColumn("valid_flag",when(new_result_df.dq_codes != "", 'false').otherwise('true'))

            new_result_df = new_result_df.withColumn("data_quality_result_array",array(concat_ws(",",*selected_columns)))
            new_result_df = new_result_df.withColumn("data_quality_valid_flag",when(size(new_result_df.data_quality_result_array) > 0, 'false').otherwise('true'))

            selected_columns_2 = [sum(when(isnull(column), lit("0")).otherwise(lit("1"))).alias(column) for column in new_result_df.columns if column.startswith("case")]

            audit_df = new_result_df.agg(*selected_columns_2)
            audit_df = audit_df.withColumn("key",lit("rules"))

            df1 = audit_df.unpivot("key", selected_columns, "column_to_split", "val")

            # Assuming you have a DataFrame called 'df' with a column called 'column_to_split'
            # Split the column into multiple columns using a delimiter
            df = df1.withColumn("rule_id", split(df1.column_to_split, "-")[2])
            df = df.withColumn("rel_id", split(df1.column_to_split, "-")[3])
            df = df.withColumn("audit_dim_id",lit(audit_dim_id_variable))

            new_df = df.withColumn("audit_dim_id", col("audit_dim_id")) \
                .withColumn("rule_dim_id", col("rule_id")) \
                .withColumn("rule_object_rel_id", col("rel_id")) \
                    .withColumn("dq_sample_result", col("val")) \
                        .withColumn("dq_sample_pass", lit('N')) \
                            .withColumn("dq_notification_required", lit('N')) \
                .withColumn("dq_action_taken", lit('flaged')) \
                .withColumn("dq_sample_timestamp", lit(datetime.utcnow()))

            insert_data_df = new_df.select( 
                (col("audit_dim_id")), 
                    (col("rule_dim_id")),
                        (col("rule_object_rel_id")),
                        (col("dq_sample_timestamp")), 
                            (col("dq_sample_result")), 
                                (col("dq_sample_pass")), 
                                    (col("dq_notification_required")), 
                                        (col("dq_action_taken"))
                                            )

            insert_data_df.write \
                .format("jdbc") \
                .mode("Append") \
                .option("url", jdbc_url) \
                .option("dbtable", "dq_sample_fact") \
                .option("user", username) \
                .option("password", password) \
                .option("SaveMode", "append") \
                .save()
    

            #new_result_df.display()

            new_result_df = new_result_df.drop(*selected_columns)

            final_df = new_result_df



        # capture the deduped dataframe count
        final_df.cache()
        final_df.createOrReplaceTempView(temp_view_name)

        src_rowcount += final_df.count()

        columns3 = ['sla_dim_id', 'audit_record_type_desc', 'audit_timestamp', 'audit_value', 'valid_flag', 'audit_dim_id']
        values3 = [(str(sla_dim_id_variable),  "Source_Object_DeDupe_Count" , datetime.now(), str(src_rowcount), 'Y',  str(audit_dim_id_variable))]
        df_audit_insert = spark.createDataFrame(values3, columns3)

        try:
            df_audit_insert.write \
            .format("jdbc") \
            .mode("Append") \
            .option("url", jdbc_url) \
            .option("dbtable", "dbo.dq_audit_fact") \
            .option("user", username) \
            .option("password", password) \
            .option("SaveMode", "APPEND") \
            .save()
        except Exception as e:
            print(e)

        # execute merge 
        #print(merge_query)
        try:
            microBatchOutputDF._jdf.sparkSession().sql(merge_query)
        except Exception as e: 
            print("Exception " + str(e) +" ocurred")
            dbutils.notebook.exit("Error in function upsertToDelta: " + str(e))
else:
    print("Not Update Mode")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Write Stream based on Load Type

# COMMAND ----------

# MAGIC %md 
# MAGIC if loadtype is 'complete', then clear out target table via delete

# COMMAND ----------

def delete_full_load():
    if val_load_type == "complete":
        try:
            spark.sql("DELETE FROM "+val_schema_name+"."+val_table_name)
        except Exception as e: 
            print("Exception " + str(e) + " ocurred")
    else:
        print("Not Complete Mode")
        #dbutils.notebook.exit("Error in Cell 45: " + str(e))

# COMMAND ----------

val_queryname = 'raw_load_'+val_table_name
#display(val_queryname)



# COMMAND ----------

if val_load_type == "update":
    autoload = (
        input_df.writeStream.format("delta")
        .foreachBatch(upsertToDelta)
        .outputMode("update")
        .queryName(val_queryname)
        .trigger(availableNow=True)
        .option("checkpointLocation", checkpoint_path)
        .start()
    )
elif val_load_type == "append":
    autoload = (
        input_df.writeStream.format("delta")
        .outputMode("append")
        .queryName(val_queryname)
        .trigger(availableNow=True)
        .option("checkpointLocation", checkpoint_path)
        .table(tgt_table_name)
    )
elif val_load_type == "complete":
    autoload = (
        input_df.writeStream.format("delta")
        .foreachBatch(upsertToDelta)
        .queryName(val_queryname)
        .outputMode("update")
        .trigger(availableNow=True)
        .option("checkpointLocation", checkpoint_path)
        .start()
    )
else:
    print("Write Stream Outputmode not found")

# don't run anything else until stream is complete
try: 
    autoload.awaitTermination()
except Exception as e:
    print("Error: " + str(e))
    dbutils.notebook.exit("Error in Cell 47: " + str(e))

# COMMAND ----------

print(case_statement)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### --ABC Get Source count -captured during merge

# COMMAND ----------

# MAGIC %md
# MAGIC #####--ABC Get update counts and target after counts 

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP VIEW IF EXISTS temp_describe;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TEMP VIEW temp_describe 
# MAGIC as 
# MAGIC DESCRIBE HISTORY ${schema_name}.${table_name};

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from temp_describe

# COMMAND ----------


new_df = spark.sql(
    "Select * from temp_describe WHERE operation in ('DELETE','MERGE', 'WRITE', 'UPDATE','STREAMING UPDATE') order by version desc limit 1"
)
try: 
    df2 = new_df.select(explode("operationMetrics"))
    """None Object Error if operation is not there"""
    operation = new_df.select(col("operation")).first()["operation"]
except Exception as e:
    print("Error: "+ str(e))
    dbutils.notebook.exit("Error in Cell 54: "+ str(e))


if operation == "WRITE":
    num_inserted = (
        df2.select(col("value")).filter(df2.key == "numOutputRows").first()["value"]
    )
    num_updated = "0"
    num_deleted = "0"

if operation == "STREAMING UPDATE":
    num_inserted = (
        df2.select(col("value")).filter(df2.key == "numOutputRows").first()["value"]
    )
    num_updated = "0"
    num_deleted = "0"

if operation == "UPDATE":
    num_inserted = "0"
    num_updated = (
        df2.select(col("value")).filter(df2.key == "numUpdatedRows").first()["value"]
    )
    num_deleted = "0"

del_test = df2.select(col("key")).filter(df2.key == "numDeletedRows").display()

if operation == "DELETE":
    if del_test is None:
        num_inserted = "0"
        num_updated = "0"
        num_deleted = "0"
    else:
        num_inserted = "0"
        num_updated = "0"
        num_deleted = (
            df2.select(col("value"))
            .filter(df2.key == "numDeletedRows")
            .first()["value"]
        )

if operation == "MERGE":
    num_inserted = (
        df2.select(col("value"))
        .filter(df2.key == "numTargetRowsInserted")
        .first()["value"]
    )
    num_updated = (
        df2.select(col("value"))
        .filter(df2.key == "numTargetRowsUpdated")
        .first()["value"]
    )
    num_deleted = (
        df2.select(col("value"))
        .filter(df2.key == "numTargetRowsDeleted")
        .first()["value"]
    )

num_inserted_int = int(num_inserted)
num_updated_int = int(num_updated)
num_deleted_int = int(num_deleted)

if num_updated_int > 0:
    try:
        columns1 = ['sla_dim_id', 'audit_record_type_desc', 'audit_timestamp', 'audit_value', 'valid_flag', 'audit_dim_id']
        values1 = [(str(sla_dim_id_variable),  "Target_Update_Record_Count" , datetime.now(), str(num_updated), 'Y',  str(audit_dim_id_variable))]
        df_audit_insert = spark.createDataFrame(values1, columns1)

        df_audit_insert.write \
            .format("jdbc") \
            .mode("Append") \
            .option("url", jdbc_url) \
            .option("dbtable", "dbo.dq_audit_fact") \
            .option("user", username) \
            .option("password", password) \
            .option("SaveMode", "APPEND") \
            .save()
    except Exception as e:
        print("Error"+ str(e))
        dbutils.notebook.exit("Error in Cell 59"+ str(e))

if num_inserted_int > 0:
    try: 
        
        columns1 = ['sla_dim_id', 'audit_record_type_desc', 'audit_timestamp', 'audit_value', 'valid_flag', 'audit_dim_id']
        values1 = [(str(sla_dim_id_variable),  "Target_Insert_Record_Count" , datetime.now(), str(num_inserted_int), 'Y',  str(audit_dim_id_variable))]
        df_audit_insert = spark.createDataFrame(values1, columns1)

        df_audit_insert.write \
            .format("jdbc") \
            .mode("Append") \
            .option("url", jdbc_url) \
            .option("dbtable", "dbo.dq_audit_fact") \
            .option("user", username) \
            .option("password", password) \
            .option("SaveMode", "APPEND") \
            .save()
    except Exception as e:
        print("Error"+ str(e))
        dbutils.notebook.exit("Error in Cell 59" + str(e))

if num_deleted_int > 0:
    try:
        
        columns1 = ['sla_dim_id', 'audit_record_type_desc', 'audit_timestamp', 'audit_value', 'valid_flag', 'audit_dim_id']
        values1 = [(str(sla_dim_id_variable),  "Target_Delete_Record_Count" , datetime.now(), str(num_deleted_int), 'Y',  str(audit_dim_id_variable))]
        df_audit_insert = spark.createDataFrame(values1, columns1)

        df_audit_insert.write \
            .format("jdbc") \
            .mode("Append") \
            .option("url", jdbc_url) \
            .option("dbtable", "dbo.dq_audit_fact") \
            .option("user", username) \
            .option("password", password) \
            .option("SaveMode", "APPEND") \
            .save()  
    except Exception as e:
        print("Error"+ str(e))
        dbutils.notebook.exit("Error in Cell 59"+ str(e))

if (num_deleted_int + num_inserted_int + num_updated_int) > 0:
    try: 
        trg_val = spark.sql(
        "Select count(*) as row_count from "
        + dbutils.widgets.get("schema_name")
        + "."
        + dbutils.widgets.get("table_name")
        ).first()["row_count"]
        
        columns1 = ['sla_dim_id', 'audit_record_type_desc', 'audit_timestamp', 'audit_value', 'valid_flag', 'audit_dim_id']
        values1 = [(str(sla_dim_id_variable),  "Target_Object_Post_Count" , datetime.now(), str(trg_val), 'Y',  str(audit_dim_id_variable))]
        df_audit_insert = spark.createDataFrame(values1, columns1)

        df_audit_insert.write \
            .format("jdbc") \
            .mode("Append") \
            .option("url",jdbc_url) \
            .option("dbtable", "dbo.dq_audit_fact") \
            .option("user", username) \
            .option("password", password) \
            .option("SaveMode", "APPEND") \
            .save()
    except Exception as e:
        print("Error"+str(e))
        dbutils.notebook.exit("Error in Cell 59"+ str(e))

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Phase 2: Validate

# COMMAND ----------

# MAGIC %md
# MAGIC ##### -optional- Verify Data Loaded in Target Table

# COMMAND ----------

#%sql
# select distinct source_file_name, count(*) from ${schema_name}.${table_name} group by source_file_name;

# COMMAND ----------



# COMMAND ----------

columns1 = ['sla_dim_id', 'audit_record_type_desc', 'audit_timestamp', 'audit_value', 'valid_flag', 'audit_dim_id']

if val_post_run == "Y":
    values1 = [(str(sla_dim_id_variable),  "pipelines_load_phase_end" , datetime.now(), 0, 'Y',  str(audit_dim_id_variable)), \
            (str(sla_dim_id_variable),  "pipelines_dq_phase_start" , datetime.now(), 0, 'Y',  str(audit_dim_id_variable)) ]
else:
    values1 = [(str(sla_dim_id_variable),  "pipelines_load_phase_end" , datetime.now(), 0, 'Y',  str(audit_dim_id_variable))]

df_audit_insert = spark.createDataFrame(values1, columns1)

#df_audit_insert.display()
try:
    df_audit_insert.write \
        .format("jdbc") \
        .mode("Append") \
        .option("url",jdbc_url) \
        .option("dbtable", "dbo.dq_audit_fact") \
        .option("user", username) \
        .option("password", password) \
        .option("SaveMode", "APPEND") \
        .save()
except Exception as e:
    print("Error: " + str(e))
    dbutils.notebook.exit("Error in Cell 65: " + str(e))


# COMMAND ----------

# MAGIC %md
# MAGIC ##### --DQ Do immediate and queued checks
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC immediate

# COMMAND ----------

if val_post_run == "Y":
    dbutils.notebook.run(
        "../../dq/transformations/dq_rule_builder_post_update_immediate_par_sql",
        6000,
        {
            "table_name": val_table_name,
            "audit_dim_id": audit_dim_id_variable,
            "sla_dim_id": sla_dim_id_variable,
            "database_name": val_schema_name,
            "catalog_name": val_catalog_name
        },
    )

# COMMAND ----------

print(audit_dim_id_variable,sla_dim_id_variable,val_post_run)

# COMMAND ----------

# MAGIC %md 
# MAGIC Log DQ phase finished, pipeline end

# COMMAND ----------

# MAGIC %md
# MAGIC #####--ABC log pipeline finish

# COMMAND ----------

if val_post_run == "Y":
    columns1 = ['sla_dim_id', 'audit_record_type_desc', 'audit_timestamp', 'audit_value', 'valid_flag', 'audit_dim_id']
    values1 = [(str(sla_dim_id_variable),  "pipelines_dq_phase_end" , datetime.now(), 0, 'Y',  str(audit_dim_id_variable)) ]
    df_audit_insert = spark.createDataFrame(values1, columns1)

    #df_audit_insert.display()
    try:
        df_audit_insert.write \
            .format("jdbc") \
            .mode("Append") \
            .option("url", jdbc_url) \
            .option("dbtable", "dbo.dq_audit_fact") \
            .option("user", username) \
            .option("password", password) \
            .option("SaveMode", "append") \
            .save()
    except Exception as e:
        print("Error: " + str(e))
        dbutils.notebook.exit("Error in Cell 74: " + str(e))

# COMMAND ----------

# MAGIC %md
# MAGIC <b>Changelog</b>: 
# MAGIC
# MAGIC
# MAGIC
# MAGIC 2023-04-05 swheeler - created script
# MAGIC
# MAGIC 2023-04-06 swheeler - updated script to have inserts to audit tables directly in code rather than dbutils calls. Speeds up the overall processing. Also Added data product name widget to use in dq_audit_dim inserts. 
# MAGIC
# MAGIC 2023-04-12 swheeler - added changelog
# MAGIC
# MAGIC 2023-05-08 swheeler - added try/except with exit if exception was caught to exit with proper messages. 
# MAGIC
# MAGIC 2023-05-08 yoganasu - corrected the program line by adding concat + the exception handling value ("exception" + str(e))
# MAGIC
# MAGIC 2023-05-15 swheeler - added check at begining of script for configs by moving ctrl query and widget pulls to top. If job is not configured in control tables, raise exception to halt execution. 
# MAGIC
# MAGIC 2023-06-01 Balaji_Raja - modified data_product_name to pick from variable from hard coded value(in ABC Initialize audit)
# MAGIC
# MAGIC 2023-06-06 Balaji_Raja - modified upsertToDelta function in cmd 44 to handle for composite keys in performing upserts.
# MAGIC
# MAGIC 2023-06-12 Balaji Raja - commented get_dir_content & get_latest_modified_file_from_directoryin CMD 28 and used spark approach to pick the latest file schema. Updated code is available in cmd 29. Also, code is updated to pick the latest record(if multiple records available in landing layer for a given batch) using file modification time.
# MAGIC
# MAGIC 2023-06-21 Balaji Raja - Enhanced the code to pick the headers from mapping table if header flag is set to False. refer Schema definition section
# MAGIC
# MAGIC 2024-01-04 Balaji Raja - Enhanced code to handle for create date & change addition for raw tables and to perfrom any simple transformation/datatype conversions from config table(changes were made to CMD 33,40,44)
# MAGIC
# MAGIC 2024-01-08 Shawn Wheeler: Updated code to use SQL server connection. Put placeholder targets for DQ notebooks. TODO: Properly place DQ notebooks and SQL config notebook and update paths. 
# MAGIC
# MAGIC 2024-01-11 Balaji Raja : have updated the positions to adjust for schema change in control_raw table(cmd 27).
# MAGIC
# MAGIC 2024-01-26 swheeler: move to repo
# MAGIC
# MAGIC 2024-02-23 swheeler: removed deprecated code to assist with troubleshooting. 
# MAGIC
