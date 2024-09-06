-- Databricks notebook source
USE DATABASE config;
USE CATALOG '${catalog_name}'

-- COMMAND ----------

SET TIME ZONE 'America/New_York';

-- COMMAND ----------

 %python
 from pyspark.sql import SparkSession
 from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType
 from pyspark.sql.functions import concat,lit,col,concat_ws,sum
 from pyspark.sql.functions import split
 from pyspark.sql.functions import *
 from pyspark.sql.types import *
 from datetime import datetime

-- COMMAND ----------

CREATE WIDGET TEXT database_name DEFAULT 'default';
CREATE WIDGET TEXT table_name DEFAULT 'test';
CREATE WIDGET TEXT catalog_name DEFAULT 'dev';
CREATE WIDGET TEXT rule_operator DEFAULT '';
CREATE WIDGET TEXT rule_type_desc  DEFAULT '';
CREATE WIDGET TEXT rule_desc  DEFAULT '';
CREATE WIDGET TEXT threshold_low  DEFAULT '';
CREATE WIDGET TEXT threshold_high  DEFAULT '';
CREATE WIDGET TEXT expected_datatype  DEFAULT '';
CREATE WIDGET TEXT expected_date_format  DEFAULT 'MM-dd-yyyy';
CREATE WIDGET TEXT threshold_pct  DEFAULT '';
CREATE WIDGET TEXT database_name  DEFAULT '';
CREATE WIDGET TEXT table_name  DEFAULT '';
CREATE WIDGET TEXT column_name  DEFAULT '';
CREATE WIDGET TEXT pkey DEFAULT '';
CREATE WIDGET TEXT grouping_columns  DEFAULT '';
CREATE WIDGET TEXT related_obj_database_name  DEFAULT '';
CREATE WIDGET TEXT related_obj_table_name  DEFAULT '';
CREATE WIDGET TEXT related_obj_column_name  DEFAULT '';
CREATE WIDGET TEXT implement_type  DEFAULT '';
CREATE WIDGET TEXT audit_dim_id DEFAULT '0';
CREATE WIDGET TEXT rule_id DEFAULT '0';
CREATE WIDGET TEXT rule_object_rel_id  DEFAULT '0';
CREATE WIDGET TEXT regex_string DEFAULT '[^a-zA-Z0-9_]';

-- COMMAND ----------

 %run "/Repos/DA_ADB_DLH_DE/DA_ADB_DLH_DE/common/config/transformations/SQLConfigNotebook"

-- COMMAND ----------

 %md
 ##Pull all rules for the object which as update_post type

-- COMMAND ----------

 %md
 ### Pull rules from SQL server

-- COMMAND ----------

 %python
 ctl_query_rel = (spark.read
   .format("sqlserver")
   .option("host", server)
   .option("port", "1433") 
   .option("user", username)
   .option("password", password)
   .option("database", database)
   .option("dbtable", "dbo.dq_rule_object_rel")
   .load()
  )

 ctl_query_rel.createOrReplaceTempView("temp_dq_rule_object_rel")

 ctl_query_rules = (spark.read
   .format("sqlserver")
   .option("host", server)
   .option("port", "1433") 
   .option("user", username)
   .option("password", password)
   .option("database", database)
   .option("dbtable", "dbo.dq_rule_dim")
   .load()
  )

 ctl_query_rules.createOrReplaceTempView("temp_dq_rule_dim")

 ctl_query_rules = (spark.read
   .format("sqlserver")
   .option("host", server)
   .option("port", "1433") 
   .option("user", username)
   .option("password", password)
   .option("database", database)
   .option("dbtable", "dbo.dq_rule_valid_value_dim")
   .load()
  )

 ctl_query_rules.createOrReplaceTempView("temp_dq_rule_valid_value_dim_pre")

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW  temp_dq_rule_valid_value_dim
as
SELECT rule_dim_id, list_id, list_desc,  concat('\'', column_value, '\'') column_value
from temp_dq_rule_valid_value_dim_pre

-- COMMAND ----------

 %python
 df_valid = spark.sql("select rule_dim_id, list_id, list_desc, array_join(collect_list(concat(column_value)),', ') valid_list from temp_dq_rule_valid_value_dim group by rule_dim_id, list_id, list_desc")

 df_valid.createOrReplaceTempView("temp_dq_rule_valid_value_dim_post")

-- COMMAND ----------

Select * from temp_dq_rule_valid_value_dim

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW dq_database_eval_quality as 
Select row_number() over (partition by rel.database_name, rel.table_name Order by rel.rule_dim_id) rownum,
    rule_type_desc, 
    rule_desc,
    operator,  
    threshold_low,
    threshold_high,
    expected_datatype,
    expected_date_format,
    threshold_pct,
    rel.database_name,
    rel.table_name,
    rel.column_name,
    pkey,
    REPLACE(grouping_columns,"|",",") grouping_columns,
    related_obj_database_name,
    related_obj_table_name,
    related_obj_column_name, 
    implement_type, 
    rel.rule_dim_id, 
    rule_object_rel_id, 
    regex_string, 
    vl.valid_list
From temp_dq_rule_object_rel rel 
join temp_dq_rule_dim dim on rel.rule_dim_id = dim.rule_dim_id 
left join temp_dq_rule_valid_value_dim_post vl on rel.rule_dim_id = vl.rule_dim_id and rel.list_id = vl.list_id
where rel.database_name = '${database_name}' and rel.table_name = '${table_name}'
and implement_type = 'Post_Update'
and rel.rule_enabled = 'Y'



-- COMMAND ----------

Select * from dq_database_eval_quality

-- COMMAND ----------

 %md
 #### Set up empty Dataframe 

-- COMMAND ----------

 %python

 schema = StructType([ \
     StructField("sql_statement", StringType(), True) , \
     StructField("audit_dim_id", StringType(), True) , \
     StructField("rule_id", StringType(), True) , \
     StructField("rule_object_rel_id", StringType(), True), \
     StructField("col_statement", StringType(), True),  \
     StructField("where_statement", StringType(), True)  \
       ])

 schema2 = StructType([ \
     StructField("notebook_path", StringType(), True), \
     StructField("timeout", IntegerType(), True) , \
     StructField("args", schema, True) \
       ])

-- COMMAND ----------

 %python
 #This change is made to address the error we received if emptyRDD is used in dev
 #rules_df = spark.createDataFrame(sc.emptyRDD(), schema)
 rules_df = spark.createDataFrame([], schema)

-- COMMAND ----------

 %md 
 ###Evaluate checks in Loop

-- COMMAND ----------

 %python 
 rownum = 1 
 limit = spark.sql("Select count(*) as row_count from dq_database_eval_quality").first()["row_count"]
 where_statement=""
 case_statement=""
 col_statement = ""

 for rownum in range (1, limit+1):
     '''
     debugging
     print("Rownum is "+str(rownum))
     print("Limit is "+str(limit))
     print("Select operator from dq_database_eval_quality where rownum = "+str(rownum))
     '''
     #Get the current row's rules
     a = spark.sql("Select operator, rule_type_desc, rule_desc, threshold_low, threshold_high,expected_datatype, expected_date_format, threshold_pct,database_name, table_name, column_name , pkey , grouping_columns, related_obj_database_name, related_obj_table_name, related_obj_column_name,implement_type, rule_dim_id , rule_object_rel_id, regex_string, valid_list  from dq_database_eval_quality where rownum = "+str(rownum))
     
     #Fill Nulls so that the Dict can be built  
     a2 = a.na.fill("na").collect()
     #convert to Dictionary 
     b = a2[0].asDict()
     #convert dictionary to tuple so that we can set all variables at once 
     c= tuple(b.values())
     
     #set variables 
     rule_operator , rule_type_desc, rule_desc, threshold_low, threshold_high, expected_datatype, expected_date_format, threshold_pct ,database_name, table_name, column_name , pkey , grouping_columns, related_obj_database_name, related_obj_table_name, related_obj_column_name,implement_type, rule_id , rule_object_rel_id, regex_string, valid_list   = c
     
     audit_dim_id = dbutils.widgets.get("audit_dim_id")
    
     #Logging 
     print("Prepping rule " + str(rownum) + " of " + str(limit) + ":" + rule_desc)
     
     ######## Build The SQL Statements ##########
     
     
     
     #Null Check SQL
     if rule_type_desc == 'null_check' and implement_type == 'Post_Update':
         sql_statement = "UPDATE " + database_name + "." + table_name + " SET valid_flag = 'n' WHERE " + column_name + " " + rule_operator 
         col_statement = ""
         where_statement += "("+column_name+"  "+ rule_operator + ") OR "
         
         case_statement += "case when ("+column_name+" " +rule_operator + ") then 1 else 0 end as `case-" + column_name + "-" + str(rule_id) + "-" + str(rule_object_rel_id) + "`,"


     #Range Check SQL, rules read as "Flag if This Condition Exists"
     if rule_type_desc == 'range_check' and implement_type == 'Post_Update':
         #*** Column is lower than that low threshold ***
         if rule_operator == '<': 
             sql_statement = "UPDATE " + database_name + "." + table_name + " SET valid_flag = 'n' WHERE " + column_name + " " + rule_operator + " " + threshold_low
             where_statement += "("+column_name+"  "+ rule_operator + " " + threshold_low + ") OR "
             case_statement += "case when ("+column_name+"  "+ rule_operator + " " + threshold_low + ") then 1 else 0 end as `case-" + column_name + "-" + str(rule_id) + "-" + str(rule_object_rel_id) + "`,"

         #*** Column is higher than the high threshold ***
         if rule_operator == '>': 
             sql_statement = "UPDATE " + database_name + "." + table_name + " SET valid_flag = 'n' WHERE " + column_name + " " + rule_operator + " " + threshold_high 
             where_statement += "("+column_name+"  "+ rule_operator + " " + threshold_high + ") OR "
             case_statement += "case when ("+column_name+"  "+ rule_operator + " " + threshold_high + ") then 1 else 0 end as `case-" + column_name + "-" + str(rule_id) + "-" + str(rule_object_rel_id) + "`,"

         #*** For this column, this range is invalid ***
         if rule_operator == 'between': 
             sql_statement = "UPDATE " + database_name + "." + table_name + " SET valid_flag = 'n' WHERE " + column_name + " " + rule_operator + " " + threshold_low + " and " + threshold_high 
             where_statement += "("+column_name+"  "+ rule_operator + " " + threshold_low + " and " + threshold_high + ") OR "
             case_statement += "case when ("+column_name+"  "+ rule_operator + " " + threshold_low + " and " + threshold_high + ") then 1 else 0 end as `case-" + column_name + "-" + str(rule_id) + "-" + str(rule_object_rel_id) + "`,"

         #*** For this column, this is the only valid range ***
         if rule_operator == 'not between': 
             sql_statement = "UPDATE " + database_name + "." + table_name + " SET valid_flag = 'n' WHERE NOT(" + column_name + " " + rule_operator + " " + threshold_low + " and " + threshold_high +")"  
             where_statement += "NOT("+column_name+"  "+ rule_operator + " " + threshold_low + " and " + threshold_high + ") OR "
             case_statement += "case when NOT("+column_name+"  "+ rule_operator + " " + threshold_low + " and " + threshold_high + ") then 1 else 0 end as `case-" + column_name + "-" + str(rule_id) + "-" + str(rule_object_rel_id) + "`,"

         #*** This is a mandatory value for this column ***
         if rule_operator == '<>': 
             sql_statement = "UPDATE " + database_name + "." + table_name + " SET valid_flag = 'n' WHERE " + column_name + " = " + threshold_low  
             where_statement += "("+column_name+" =  "+ threshold_low + ") OR "
             case_statement += "case when ("+column_name+" =  "+ threshold_low + ") then 1 else 0 end as `case-" + column_name + "-" + str(rule_id) + "-" + str(rule_object_rel_id) + "`,"

         #*** This value is always an error ***
         if rule_operator == '=': 
             sql_statement = "UPDATE " + database_name + "." + table_name + " SET valid_flag = 'n' WHERE " + column_name + " <> " + threshold_low  
             where_statement += "("+column_name+" <> "+ threshold_low + ") OR "
             case_statement += "case when ("+column_name+" <> "+ threshold_low + ") then 1 else 0 end as `case-" + column_name + "-" + str(rule_id) + "-" + str(rule_object_rel_id) + "`,"
             
     #Orphan Check
     # *** Left join to the foreign table on related_obj_database_name.related_obj_table_name.related_obj_column_name. If the FK table is NULL, flag the record ***
     if rule_type_desc == 'orphan_check' and implement_type == 'Post_Update':
         sql_statement = "merge into "+ database_name + "." + table_name + " a using "+related_obj_database_name+"."+related_obj_table_name+" b on a."+related_obj_column_name+" = b."+related_obj_column_name  + " WHEN NOT MATCHED THEN UPDATE set a.valid_flag = 'n'"


     #Unique Check SQL
     # *** Check that the value of this row is always unique. If not, flag the row ***
     if rule_type_desc == 'unique_check' and implement_type == 'Post_Update':
         sql_statement = "MERGE INTO "+ database_name + "." + table_name + " a USING  (Select "+ pkey +" from "+ database_name + "." + table_name + " where "+column_name+" in (Select COALESCE("+column_name+",0) from "+ database_name + "." + table_name + "  group by "+ grouping_columns +" having count(*) > 1)) b on a."+column_name+" = b."+column_name+" WHEN MATCHED THEN UPDATE SET a.valid_flag = 'n'"
     
     #Datatype Check SQL
     # *** Check that the datatype of this row matches the expectation. If not, flag the row ***
     if rule_type_desc == 'datatype_check' and implement_type == 'Post_Update':
         sql_statement = "Update "+ database_name + "." + table_name + " set valid_flag = 'n' WHERE typeof("+column_name+") <> lower('"+expected_datatype+"') "  
         where_statement += "(typeof("+column_name+") <> lower('"+ expected_datatype + "')) OR "
         case_statement += "case when (typeof("+column_name+") <> lower('"+ expected_datatype + "')) then 1 else 0 end as `case-" + column_name + "-" + str(rule_id) + "-" + str(rule_object_rel_id) + "`,"
         col_statement += "case when (typeof("+column_name+") <> lower('" +expected_datatype + "')) then 'Failed_" + column_name + "' else 'Passed_" + column_name + "' end as `rule-" + column_name + "-" + str(rule_id) + "-" + str(rule_object_rel_id) + "`,"

     
     #Special Character Check SQL
     # *** Check that the string does not have any special characters. If any are found, flag the row ***
     if rule_type_desc == 'special_char_check' and implement_type == 'Post_Update':
         sql_statement = "Update "+ database_name + "." + table_name + " set valid_flag = 'n'  WHERE regexp_replace("+ column_name +", "+ regex_string + ", "") <> "+ column_name 
         where_statement += "(regexp_replace("+column_name+", "+ regex_string + ", "") <> " + column_name + ") OR "
         case_statement += "case when regexp_replace("+column_name+", "+ regex_string + ", "") <> " + column_name + " then 1 else 0 end as `case-" + column_name + "-" + str(rule_id) + "-" + str(rule_object_rel_id) + "`,"
     
     #Date Format Check SQL
     # *** Check that the string does not have any special characters. If any are found, flag the row ***
     if rule_type_desc == 'date_format_check' and implement_type == 'Post_Update':
         sql_statement = "Update "+ database_name + "." + table_name + " set valid_flag = 'n'  WHERE to_date('"+column_name+"', '"+expected_date_format+"') is null "
         where_statement += "(to_date('"+column_name+"', '"+ expected_date_format + ")' is null)  OR "
         case_statement += "case when to_date('"+column_name+"', '"+ expected_date_format + "') is null then 1 else 0 end as `case-" + column_name + "-" + str(rule_id) + "-" + str(rule_object_rel_id) + "`,"

     #Date Format Check SQL
     # *** Check that the string should have fixed lenght characters. If any are found, flag the row ***
     if rule_type_desc == 'string_fixed_length' and implement_type == 'Post_Update':
         # sql_statement = "Update "+ database_name + "." + table_name + " set valid_flag = 'n'  WHERE valid_flag = 'y' and len("+column_name+") > " +valid_list
         sql_statement = ""
         where_statement += "(len("+column_name+") > "+ valid_list + ") OR "
         case_statement += "case when (len("+column_name+") > " +valid_list + ") then 1 else 0 end as `case-" + column_name + "-" + str(rule_id) + "-" + str(rule_object_rel_id) + "`,"

         col_statement = "case when (len("+column_name+") > " +valid_list + ") then 'Failed_" + column_name + "' else 'Passed_" + column_name + "' end as `rule-" + column_name + "-" + str(rule_id) + "-" + str(rule_object_rel_id) + "`,"
         #sql_statement = "Update "+ database_name + "." + table_name + " set valid_flag = 'n'  WHERE valid_flag = 'y' and len("+column_name+") > 0 and len("+column_name+") < " ##+valid_list
         #col_statement = "case when (len("+column_name+") > 0 and len("+column_name+") < " +valid_list + ") then '" + str(rule_object_rel_id) + "' else '0' end as rule_" + str(rownum) + "_" + str(rule_object_rel_id)

     #Data in valid values Check SQL
     # check if the values in the column are in the list of valid values. If not, Flag row. 
     if rule_type_desc == 'valid_values_check' and implement_type == 'Post_Update':
         sql_statement = "Update "+ database_name + "." + table_name + " set valid_flag = 'n'  WHERE valid_flag = 'y' and "+ column_name +" not in (" + valid_list + ") "
         col_statment = "(" + column_name +" not in (" + valid_list + ") ) as rule_" + str(rule_id) + "_" + str(rule_object_rel_id)
         where_statement += "(("+column_name+" not in ("+ valid_list + ")) OR "
         case_statement += "case when ("+column_name+" not in (" +valid_list + ")) then 1 else 0 end as `case-" + column_name + "-" + str(rule_id) + "-" + str(rule_object_rel_id) + "`,"
    
     data = [(sql_statement, str(audit_dim_id), str(rule_id), str(rule_object_rel_id),col_statement,where_statement)]
     df2 = spark.createDataFrame(data)
     rules_df = rules_df.unionAll(df2)
     
     ############       Reset variables     ############
     rule_operator , rule_type_desc, rule_desc, threshold_low, threshold_high, expected_datatype, threshold_pct ,database_name, table_name, column_name , pkey , grouping_columns, related_obj_database_name, related_obj_table_name, related_obj_column_name,implement_type, rule_dim_id , rule_object_rel_id ,audit_dim_id, expected_date_format, regex_string, valid_values   = "", "", "", "", "", "","", "", "", "", "", "", "", "", "", "", 0 , 0,0, 'MM-dd-yyyy', '[^a-zA-Z0-9_]',''

 where_statement = "(upper(valid_flag) = 'Y') and (" + where_statement[:-3] + ")"
 case_statement = case_statement[:-1]
 col_statement = col_statement[:-1]


-- COMMAND ----------

 %md
 ### Getting SUM of invalid records

-- COMMAND ----------

 %python


 var_table_name = dbutils.widgets.get("table_name")
 var_database_name = dbutils.widgets.get("database_name")


 if case_statement == "":
    select_stmt = "select * from dev.raw." + var_table_name
 else:
     select_stmt = "select *," + case_statement + " from " + var_database_name + "." + var_table_name

 new_result_df = spark.sql(select_stmt)

 selected_columns = [column for column in new_result_df.columns if column.startswith("case")]

 selected_columns_2 = [sum(column).alias(column) for column in new_result_df.columns if column.startswith("case")]

 print(selected_columns_2)

 final_df = new_result_df[selected_columns]
 final_df = new_result_df.agg(*selected_columns_2)
 final_df = final_df.withColumn("key",lit("rules"))




-- COMMAND ----------

 %python
 display(final_df)

-- COMMAND ----------

 %md
 ### Generating audit dataframe

-- COMMAND ----------

 %python

 var_audit_dim_id = dbutils.widgets.get("audit_dim_id")

 df1 = final_df.unpivot("key", selected_columns, "column_to_split", "val")

 # Assuming you have a DataFrame called 'df' with a column called 'column_to_split'
 # Split the column into multiple columns using a delimiter
 df = df1.withColumn("rule_id", split(df1.column_to_split, "-")[2])
 df = df.withColumn("rel_id", split(df1.column_to_split, "-")[3])
 df = df.withColumn("audit_dim_id",lit(var_audit_dim_id))



 new_df = df.withColumn("audit_dim_id", col("audit_dim_id")) \
     .withColumn("rule_dim_id", col("rule_id")) \
     .withColumn("rule_object_rel_id", col("rel_id")) \
         .withColumn("dq_sample_result", col("val")) \
             .withColumn("dq_sample_pass", lit('N')) \
                    .withColumn("dq_notification_required", lit('N')) \
     .withColumn("dq_action_taken", lit('flaged')) \
     .withColumn("dq_sample_timestamp", lit(datetime.utcnow()))


-- COMMAND ----------

 %md
 ### Writing dataframe to Audit fact table

-- COMMAND ----------

 %python
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


-- COMMAND ----------

 %md
 ### Generating Single Update Statement

-- COMMAND ----------

 %python


 var_table_name_update = dbutils.widgets.get("table_name")

 var_database_name_update = dbutils.widgets.get("database_name")


 var_audit_id = dbutils.widgets.get("audit_dim_id")


 sql_statement_update = "Update " +var_database_name_update + "." + var_table_name_update + " set valid_flag = 'n' WHERE " + where_statement
 print(sql_statement_update)


 args = {
         "sql_statement": sql_statement_update,
         "audit_dim_id": var_audit_id,
         "rule_id": 0,
         "rule_object_rel_id": 0,
         }

 result_df = spark.sql(sql_statement_update)
 print("...Done")
 affected_rows = result_df.first()["num_affected_rows"]
 #affected_rows = 0
 print(str(affected_rows) + " row(s) affected" )        


-- COMMAND ----------

 %md
 Redeploy 2024-03-15



