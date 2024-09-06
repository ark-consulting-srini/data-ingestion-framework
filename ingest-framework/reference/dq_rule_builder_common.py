-- Databricks notebook source
CREATE WIDGET TEXT database_name DEFAULT 'default';
CREATE WIDGET TEXT table_name DEFAULT 'test';
CREATE WIDGET TEXT catalog_name DEFAULT 'dev';
CREATE WIDGET TEXT audit_dim_id DEFAULT '0';
CREATE WIDGET TEXT post_run DEFAULT 'N';

-- COMMAND ----------

-- MAGIC %python
-- MAGIC if dbutils.widgets.get("post_run") == "Y":
-- MAGIC     case_statement = ""
-- MAGIC     #dbutils.notebook.exit("Only Post Run ")

-- COMMAND ----------

USE CATALOG '${catalog_name}'

-- COMMAND ----------

SET TIME ZONE 'America/New_York';

-- COMMAND ----------

-- MAGIC %python
-- MAGIC val_table_name = dbutils.widgets.get("table_name")
-- MAGIC var_database_name = dbutils.widgets.get("database_name")
-- MAGIC audit_dim_id = dbutils.widgets.get("audit_dim_id")
-- MAGIC
-- MAGIC
-- MAGIC

-- COMMAND ----------

set var_table_name= ${table_name}

-- COMMAND ----------

-- MAGIC %python
-- MAGIC print(val_table_name)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql import SparkSession
-- MAGIC from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType
-- MAGIC from pyspark.sql.functions import concat,lit,col,concat_ws,sum, sha2
-- MAGIC from pyspark.sql.functions import split
-- MAGIC from pyspark.sql.functions import *
-- MAGIC from pyspark.sql.types import *
-- MAGIC from datetime import datetime
-- MAGIC import pyspark.sql.functions as F
-- MAGIC #test

-- COMMAND ----------

-- MAGIC %run "../../config/transformations/SQLConfigNotebook"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Pull all rules for the object which as update_post type

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Pull rules from SQL server

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC ctl_query_rel = (spark.read
-- MAGIC   .format("sqlserver")
-- MAGIC   .option("host", server)
-- MAGIC   .option("port", "1433") 
-- MAGIC   .option("user", username)
-- MAGIC   .option("password", password)
-- MAGIC   .option("database", database)
-- MAGIC   .option("dbtable", "dbo.dq_rule_object_rel")
-- MAGIC   .load()
-- MAGIC  )
-- MAGIC
-- MAGIC ctl_query_rel.createOrReplaceTempView("temp_dq_rule_object_rel")
-- MAGIC
-- MAGIC ctl_query_rules = (spark.read
-- MAGIC   .format("sqlserver")
-- MAGIC   .option("host", server)
-- MAGIC   .option("port", "1433") 
-- MAGIC   .option("user", username)
-- MAGIC   .option("password", password)
-- MAGIC   .option("database", database)
-- MAGIC   .option("dbtable", "dbo.dq_rule_dim")
-- MAGIC   .load()
-- MAGIC  )
-- MAGIC
-- MAGIC ctl_query_rules.createOrReplaceTempView("temp_dq_rule_dim")
-- MAGIC
-- MAGIC ctl_query_rules = (spark.read
-- MAGIC   .format("sqlserver")
-- MAGIC   .option("host", server)
-- MAGIC   .option("port", "1433") 
-- MAGIC   .option("user", username)
-- MAGIC   .option("password", password)
-- MAGIC   .option("database", database)
-- MAGIC   .option("dbtable", "dbo.dq_rule_valid_value_dim")
-- MAGIC   .load()
-- MAGIC  )
-- MAGIC
-- MAGIC ctl_query_rules.createOrReplaceTempView("temp_dq_rule_valid_value_dim_pre")

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW  temp_dq_rule_valid_value_dim
as
SELECT rule_dim_id, list_id, list_desc,  concat('\'', column_value, '\'') column_value
from temp_dq_rule_valid_value_dim_pre

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df_valid = spark.sql("select rule_dim_id, list_id, list_desc, array_join(collect_list(concat(column_value)),', ') valid_list from temp_dq_rule_valid_value_dim group by rule_dim_id, list_id, list_desc")
-- MAGIC
-- MAGIC df_valid.createOrReplaceTempView("temp_dq_rule_valid_value_dim_post")

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

-- MAGIC %md
-- MAGIC #### Set up empty Dataframe 

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC schema = StructType([ \
-- MAGIC     StructField("sql_statement", StringType(), True) , \
-- MAGIC     StructField("audit_dim_id", StringType(), True) , \
-- MAGIC     StructField("rule_id", StringType(), True) , \
-- MAGIC     StructField("rule_object_rel_id", StringType(), True), \
-- MAGIC     StructField("col_statement", StringType(), True),  \
-- MAGIC     StructField("where_statement", StringType(), True)  \
-- MAGIC       ])

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #This change is made to address the error we received if emptyRDD is used in dev
-- MAGIC #rules_df = spark.createDataFrame(sc.emptyRDD(), schema)
-- MAGIC rules_df = spark.createDataFrame([], schema)

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ###Evaluate checks in Loop

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC where_statement=""
-- MAGIC case_statement=""
-- MAGIC col_statement = ""
-- MAGIC table_join=""
-- MAGIC val_join_cond = ""
-- MAGIC
-- MAGIC
-- MAGIC data_collect = spark.sql("Select operator, rule_type_desc, rule_desc, threshold_low, threshold_high,expected_datatype, expected_date_format, threshold_pct,database_name, table_name, column_name , pkey , grouping_columns, related_obj_database_name, related_obj_table_name, related_obj_column_name,implement_type, rule_dim_id , rule_object_rel_id, regex_string, valid_list from dq_database_eval_quality")
-- MAGIC
-- MAGIC data_collect = data_collect.fillna("na")
-- MAGIC data_collect = data_collect.collect() 
-- MAGIC # looping thorough each row of the dataframe
-- MAGIC rownum = 0
-- MAGIC for row in data_collect:
-- MAGIC     #d = [row.asDict() for row in data_collect.collect()]
-- MAGIC     rownum = rownum + 1
-- MAGIC     b = row.asDict() 
-- MAGIC     c = list(b.items())
-- MAGIC     c= tuple(b.values())
-- MAGIC     #c= tuple(b.values())
-- MAGIC
-- MAGIC     rule_operator , rule_type_desc, rule_desc, threshold_low, threshold_high, expected_datatype, expected_date_format, threshold_pct ,database_name, table_name, column_name , pkey , grouping_columns, related_obj_database_name, related_obj_table_name, related_obj_column_name,implement_type, rule_id , rule_object_rel_id, regex_string, valid_list   = c
-- MAGIC     #print(c)
-- MAGIC
-- MAGIC     #print("Prepping rule " + str(rownum) +  ":" + rule_desc + ":" + column_name)
-- MAGIC     #Null Check SQL
-- MAGIC     if rule_type_desc == 'null_check' and implement_type == 'Post_Update':
-- MAGIC         sql_statement = "UPDATE " + database_name + "." + table_name + " SET data_quality_valid_flag = 'n' WHERE " + column_name + " " + rule_operator 
-- MAGIC         col_statement = ""
-- MAGIC         where_statement += "("+column_name+"  "+ rule_operator + ") OR "
-- MAGIC         
-- MAGIC         case_statement += "case when ("+column_name+" " +rule_operator + ") then " + str(rule_object_rel_id) +" else null end as `case-" + column_name + "-" + str(rule_id) + "-" + str(rule_object_rel_id) + "`,"
-- MAGIC
-- MAGIC
-- MAGIC     #Range Check SQL, rules read as "Flag if This Condition Exists"
-- MAGIC     if rule_type_desc == 'range_check' and implement_type == 'Post_Update':
-- MAGIC         #*** Column is lower than that low threshold ***
-- MAGIC         if rule_operator == '<': 
-- MAGIC             sql_statement = "UPDATE " + database_name + "." + table_name + " SET data_quality_valid_flag = 'n' WHERE " + column_name + " > " + threshold_low
-- MAGIC             where_statement += "("+column_name+"  > " + threshold_low + ") OR "
-- MAGIC             case_statement += "case when ("+column_name+"  > '" + threshold_low + "') then " + str(rule_object_rel_id) +" else null end as `case-" + column_name + "-" + str(rule_id) + "-" + str(rule_object_rel_id) + "`,"
-- MAGIC
-- MAGIC         #*** Column is higher than the high threshold ***
-- MAGIC         if rule_operator == '>': 
-- MAGIC             sql_statement = "UPDATE " + database_name + "." + table_name + " SET data_quality_valid_flag = 'n' WHERE " + column_name + " < " + threshold_high 
-- MAGIC             where_statement += "("+column_name+"  < " + threshold_high + ") OR "
-- MAGIC             case_statement += "case when ("+column_name+"  < '" + threshold_high + "') then " + str(rule_object_rel_id) +" else null end as `case-" + column_name + "-" + str(rule_id) + "-" + str(rule_object_rel_id) + "`,"
-- MAGIC
-- MAGIC         #*** For this column, this range is invalid ***
-- MAGIC         if rule_operator == 'between': 
-- MAGIC             sql_statement = "UPDATE " + database_name + "." + table_name + " SET data_quality_valid_flag = 'n' WHERE " + column_name + " " + rule_operator + " " + threshold_low + " and " + threshold_high 
-- MAGIC             where_statement += "("+column_name+" not  "+ rule_operator + "  '" + threshold_low + "' and '" + threshold_high + "') OR "
-- MAGIC             case_statement += "case when ("+column_name+" not  "+ rule_operator + " '" + threshold_low + "' and '" + threshold_high + "') then " + str(rule_object_rel_id) +" else null end as `case-" + column_name + "-" + str(rule_id) + "-" + str(rule_object_rel_id) + "`,"
-- MAGIC
-- MAGIC         #*** For this column, this is the only valid range ***
-- MAGIC         if rule_operator == 'not between': 
-- MAGIC             sql_statement = "UPDATE " + database_name + "." + table_name + " SET data_quality_valid_flag = 'n' WHERE NOT(" + column_name + " " + rule_operator + " " + threshold_low + " and " + threshold_high +")"  
-- MAGIC             where_statement += "("+column_name+"  "+ rule_operator + " '" + threshold_low + "' and '" + threshold_high + "') OR "
-- MAGIC             case_statement += "case when ("+column_name+"  "+ rule_operator + " '" + threshold_low + "' and '" + threshold_high + "') then " + str(rule_object_rel_id) +" else null end as `case-" + column_name + "-" + str(rule_id) + "-" + str(rule_object_rel_id) + "`,"
-- MAGIC
-- MAGIC         #*** This is a mandatory value for this column ***
-- MAGIC         if rule_operator == '<>': 
-- MAGIC             sql_statement = "UPDATE " + database_name + "." + table_name + " SET data_quality_valid_flag = 'n' WHERE " + column_name + " = " + threshold_low  
-- MAGIC             where_statement += "("+column_name+" =  "+ threshold_low + ") OR "
-- MAGIC             case_statement += "case when ("+column_name+" =  '"+ threshold_low + "') then " + str(rule_object_rel_id) +" else null end as `case-" + column_name + "-" + str(rule_id) + "-" + str(rule_object_rel_id) + "`,"
-- MAGIC
-- MAGIC         #*** This value is always an error ***
-- MAGIC         if rule_operator == '=': 
-- MAGIC             sql_statement = "UPDATE " + database_name + "." + table_name + " SET data_quality_valid_flag = 'n' WHERE " + column_name + " <> " + threshold_low  
-- MAGIC             where_statement += "("+column_name+" <> "+ threshold_low + ") OR "
-- MAGIC             case_statement += "case when ("+column_name+" <> '"+ threshold_low + "') then " + str(rule_object_rel_id) +" else null end as `case-" + column_name + "-" + str(rule_id) + "-" + str(rule_object_rel_id) + "`,"
-- MAGIC             
-- MAGIC     #Orphan Check
-- MAGIC     # *** Left join to the foreign table on related_obj_database_name.related_obj_table_name.related_obj_column_name. If the FK table is NULL, flag the record ***
-- MAGIC     if rule_type_desc == 'orphan_check' and implement_type == 'Post_Update':
-- MAGIC         sql_statement = "merge into "+ database_name + "." + table_name + " a using "+related_obj_database_name+"."+related_obj_table_name+" b on a."+related_obj_column_name+" = b."+related_obj_column_name  + " WHEN NOT MATCHED THEN UPDATE set a.data_quality_valid_flag = 'n'"
-- MAGIC         case_statement += "case when rule_r_" + related_obj_column_name +" IS NULL then "+ str(rule_object_rel_id) +" else null end as `case-" + column_name + "-" + str(rule_id) + "-" + str(rule_object_rel_id) + "`,"
-- MAGIC         where_statement += "(" + related_obj_table_name + ".rule_r_" + related_obj_column_name +"IS NULL) OR "
-- MAGIC         table_join += "  left join (select "+related_obj_column_name +" as rule_r_" + related_obj_column_name +" from "+ related_obj_database_name + "." + related_obj_table_name +") as r_"+str(rule_object_rel_id) +" on "+ table_name + "."+column_name+ " = r_"+str(rule_object_rel_id) +".rule_r_"+related_obj_column_name
-- MAGIC
-- MAGIC
-- MAGIC     #Unique Check SQL
-- MAGIC     # *** Check that the value of this row is always unique. If not, flag the row ***
-- MAGIC     # if rule_type_desc == 'unique_check' and implement_type == 'Post_Update':
-- MAGIC     #     sql_statement = "MERGE INTO "+ database_name + "." + table_name + " a USING  (Select "+ pkey +" from "+ database_name + "." + table_name + " where "+column_name+" in (Select COALESCE("+column_name+",0) from "+ database_name + "." + table_name + "  group by "+ grouping_columns +" having count(*) > 1)) b on a."+column_name+" = b."+column_name+" WHEN MATCHED THEN UPDATE SET a.valid_flag = 'n'"
-- MAGIC     if rule_type_desc == 'unique_check' and implement_type == 'Post_Update':
-- MAGIC         sql_statement = "MERGE INTO "+ database_name + "." + table_name + " a USING  (Select "+ pkey +" from "+ database_name + "." + table_name + " where "+column_name+" in (Select COALESCE("+column_name+",0) from "+ database_name + "." + table_name + "  group by "+ grouping_columns +" having count(*) > 1)) b on a."+column_name+" = b."+column_name+" WHEN MATCHED THEN UPDATE SET a.data_quality_valid_flag = 'n'"
-- MAGIC         case_statement += "case when t_" + str(rule_object_rel_id) +".count_of_values > 1 then "+ str(rule_object_rel_id) +" else null end as `case-" + column_name + "-" + str(rule_id) + "-" + str(rule_object_rel_id) + "`,"
-- MAGIC         #where_statement += "(t_"+str(rule_object_rel_id) +".t_" + str(rule_object_rel_id) +".count_of_values > 1) OR "
-- MAGIC         where_statement +=  "(t_"+str(rule_object_rel_id) + ".count_of_values > 1) OR "
-- MAGIC         table_join += "  join (select "+column_name +" as crule_c_" + column_name +", count(*) as count_of_values from "+ database_name + "." + table_name +" group by "+column_name +") as t_"+str(rule_object_rel_id) +" on "+ table_name + "."+column_name+ " = t_"+str(rule_object_rel_id) +".crule_c_"+column_name
-- MAGIC     
-- MAGIC     #Datatype Check SQL
-- MAGIC     # *** Check that the datatype of this row matches the expectation. If not, flag the row ***
-- MAGIC     if rule_type_desc == 'datatype_check' and implement_type == 'Post_Update':
-- MAGIC         sql_statement = "Update "+ database_name + "." + table_name + " set data_quality_valid_flag = 'n' WHERE typeof("+column_name+") <> lower('"+expected_datatype+"') "  
-- MAGIC         where_statement += "(typeof("+column_name+") <> lower('"+ expected_datatype + "')) OR "
-- MAGIC         case_statement += "case when (substr(typeof("+column_name+"),1,instr(typeof(" + column_name + "),'(')-1) <> lower('"+ expected_datatype + "')) then " + str(rule_object_rel_id) +" else null end as `case-" + column_name + "-" + str(rule_id) + "-" + str(rule_object_rel_id) + "`,"
-- MAGIC         col_statement += "case when (typeof("+column_name+") <> lower('" +expected_datatype + "')) then 'Failed_" + column_name + "' else 'Passed_" + column_name + "' end as `rule-" + column_name + "-" + str(rule_id) + "-" + str(rule_object_rel_id) + "`,"
-- MAGIC
-- MAGIC     
-- MAGIC     #Special Character Check SQL
-- MAGIC     # *** Check that the string does not have any special characters. If any are found, flag the row ***
-- MAGIC     if rule_type_desc == 'special_char_check' and implement_type == 'Post_Update':
-- MAGIC         #sql_statement = "Update "+ database_name + "." + table_name + " set valid_flag = 'n'  WHERE regexp_replace("+ column_name +", "+ regex_string + ", "") <> "+ column_name 
-- MAGIC         #where_statement += "(regexp_replace("+column_name+", "+ regex_string + ", "") <> " + column_name + ") OR "
-- MAGIC         #case_statement += "case when regexp_replace("+column_name+", "+ regex_string + ", "") <> " + column_name + " then 1 else 0 end as `case-" + column_name + "-" + str(rule_id) + "-" + str#(rule_object_rel_id) + "`,"
-- MAGIC         sql_statement = "Update "+ database_name + "." + table_name + " set data_quality_valid_flag = 'n'  WHERE regexp_replace("+ column_name +", "+ valid_list + ", '') <> "+ column_name 
-- MAGIC         where_statement += "(regexp_replace("+column_name+", "+ valid_list + ", '') <> " + column_name + ") OR "
-- MAGIC         case_statement += "case when regexp_replace("+column_name+", "+ valid_list + ", '') <> " + column_name + " then " + str(rule_object_rel_id) + " else null end as `case-" + column_name + "-" + str(rule_id) + "-" + str(rule_object_rel_id) + "`,"
-- MAGIC
-- MAGIC     
-- MAGIC     #Date Format Check SQL
-- MAGIC     # *** Check that the string does not have any special characters. If any are found, flag the row ***
-- MAGIC     if rule_type_desc == 'date_format_check' and implement_type == 'Post_Update':
-- MAGIC         sql_statement = "Update "+ database_name + "." + table_name + " set data_quality_valid_flag = 'n'  WHERE to_date('"+column_name+"', "+valid_list+") is null "
-- MAGIC         where_statement += "(to_date(coalesce("+column_name+",'1900-01-01'), "+ valid_list + ") is null)  OR "
-- MAGIC         case_statement += "case when to_date(coalesce("+column_name+",'1900-01-01'), "+ valid_list + ") is null then " + str(rule_object_rel_id) + " else null end as `case-" + column_name + "-" + str(rule_id) + "-" + str(rule_object_rel_id) + "`,"
-- MAGIC
-- MAGIC     #String Length Check SQL
-- MAGIC     # *** Check that the string should have fixed lenght characters. If any are found, flag the row ***
-- MAGIC     if rule_type_desc == 'string_fixed_length' and implement_type == 'Post_Update':
-- MAGIC         # sql_statement = "Update "+ database_name + "." + table_name + " set valid_flag = 'n'  WHERE valid_flag = 'y' and len("+column_name+") > " +valid_list
-- MAGIC         sql_statement = ""
-- MAGIC         where_statement += "(len("+column_name+") > "+ valid_list + ") OR "
-- MAGIC         case_statement += "case when (len("+column_name+") > " +valid_list + ") then " + str(rule_object_rel_id) + " else null end as `case-" + column_name + "-" + str(rule_id) + "-" + str(rule_object_rel_id) + "`,"
-- MAGIC
-- MAGIC         col_statement = "case when (len("+column_name+") > " +valid_list + ") then 'Failed_" + column_name + "' else 'Passed_" + column_name + "' end as `rule-" + column_name + "-" + str(rule_id) + "-" + str(rule_object_rel_id) + "`,"
-- MAGIC         #sql_statement = "Update "+ database_name + "." + table_name + " set valid_flag = 'n'  WHERE valid_flag = 'y' and len("+column_name+") > 0 and len("+column_name+") < " ##+valid_list
-- MAGIC         #col_statement = "case when (len("+column_name+") > 0 and len("+column_name+") < " +valid_list + ") then '" + str(rule_object_rel_id) + "' else '0' end as rule_" + str(rownum) + "_" + str(rule_object_rel_id)
-- MAGIC
-- MAGIC     #Data in valid values Check SQL
-- MAGIC     # check if the values in the column are in the list of valid values. If not, Flag row. 
-- MAGIC     if rule_type_desc == 'valid_values_check' and implement_type == 'Post_Update':
-- MAGIC         sql_statement = "Update "+ database_name + "." + table_name + " set data_quality_valid_flag = 'n'  WHERE data_quality_valid_flag = 'y' and "+ column_name +" not in (" + valid_list + ") "
-- MAGIC         col_statment = "(" + column_name +" not in (" + valid_list + ") ) as rule_" + str(rule_id) + "_" + str(rule_object_rel_id)
-- MAGIC         where_statement += "("+column_name+" not in ("+ valid_list + ")) OR "
-- MAGIC         case_statement += "case when ("+column_name+" not in (" +valid_list + ")) then " + str(rule_object_rel_id) +" else null end as `case-" + column_name + "-" + str(rule_id) + "-" + str(rule_object_rel_id) + "`,"
-- MAGIC    
-- MAGIC     data = [(sql_statement, str(audit_dim_id), str(rule_id), str(rule_object_rel_id),col_statement,where_statement)]
-- MAGIC     df2 = spark.createDataFrame(data)
-- MAGIC     rules_df = rules_df.unionAll(df2)
-- MAGIC     
-- MAGIC     ############       Reset variables     ############
-- MAGIC     rule_operator , rule_type_desc, rule_desc, threshold_low, threshold_high, expected_datatype, threshold_pct ,database_name, table_name, column_name , pkey , grouping_columns, related_obj_database_name, related_obj_table_name, related_obj_column_name,implement_type, rule_dim_id , rule_object_rel_id ,audit_dim_id, expected_date_format, regex_string, valid_values   = "", "", "", "", "", "","", "", "", "", "", "", "", "", "", "", 0 , 0,0, 'MM-dd-yyyy', '[^a-zA-Z0-9_]',''
-- MAGIC
-- MAGIC where_statement = where_statement[:-3]
-- MAGIC case_statement = case_statement[:-1]
-- MAGIC col_statement = col_statement[:-1]
-- MAGIC

-- COMMAND ----------

-- %python 
-- rownum = 1 
-- limit = spark.sql("Select count(*) as row_count from dq_database_eval_quality").first()["row_count"]
-- where_statement=""
-- case_statement=""
-- col_statement = ""
-- table_join=""

-- for rownum in range (1, limit+1):
--     '''
--     debugging
--     print("Rownum is "+str(rownum))
--     print("Limit is "+str(limit))
--     print("Select operator from dq_database_eval_quality where rownum = "+str(rownum))
--     '''
--     #Get the current row's rules
--     a = spark.sql("Select operator, rule_type_desc, rule_desc, threshold_low, threshold_high,expected_datatype, expected_date_format, threshold_pct,database_name, table_name, column_name , pkey , grouping_columns, related_obj_database_name, related_obj_table_name, related_obj_column_name,implement_type, rule_dim_id , rule_object_rel_id, regex_string, valid_list  from dq_database_eval_quality where rownum = "+str(rownum))
    
--     #Fill Nulls so that the Dict can be built  
--     a2 = a.na.fill("na").collect()
--     #convert to Dictionary 
--     b = a2[0].asDict()
--     #convert dictionary to tuple so that we can set all variables at once 
--     c= tuple(b.values())
--     print(c)
    
--     #set variables 
--     rule_operator , rule_type_desc, rule_desc, threshold_low, threshold_high, expected_datatype, expected_date_format, threshold_pct ,database_name, table_name, column_name , pkey , grouping_columns, related_obj_database_name, related_obj_table_name, related_obj_column_name,implement_type, rule_id , rule_object_rel_id, regex_string, valid_list   = c
    
   
--     #Logging 
--     print("Prepping rule " + str(rownum) + " of " + str(limit) + ":" + rule_desc + ":" + column_name)
    
--     ######## Build The SQL Statements ##########
    
    
    
--     #Null Check SQL
--     if rule_type_desc == 'null_check' and implement_type == 'Post_Update':
--         sql_statement = "UPDATE " + database_name + "." + table_name + " SET valid_flag = 'n' WHERE " + column_name + " " + rule_operator 
--         col_statement = ""
--         where_statement += "("+column_name+"  "+ rule_operator + ") OR "
        
--         case_statement += "case when ("+column_name+" " +rule_operator + ") then " + str(rule_object_rel_id) +" else null end as `case-" + column_name + "-" + str(rule_id) + "-" + str(rule_object_rel_id) + "`,"


--     #Range Check SQL, rules read as "Flag if This Condition Exists"
--     if rule_type_desc == 'range_check' and implement_type == 'Post_Update':
--         #*** Column is lower than that low threshold ***
--         if rule_operator == '<': 
--             sql_statement = "UPDATE " + database_name + "." + table_name + " SET valid_flag = 'n' WHERE " + column_name + " > " + threshold_low
--             where_statement += "("+column_name+"  > " + threshold_low + ") OR "
--             case_statement += "case when ("+column_name+"  > '" + threshold_low + "') then " + str(rule_object_rel_id) +" else null end as `case-" + column_name + "-" + str(rule_id) + "-" + str(rule_object_rel_id) + "`,"

--         #*** Column is higher than the high threshold ***
--         if rule_operator == '>': 
--             sql_statement = "UPDATE " + database_name + "." + table_name + " SET valid_flag = 'n' WHERE " + column_name + " < " + threshold_high 
--             where_statement += "("+column_name+"  < " + threshold_high + ") OR "
--             case_statement += "case when ("+column_name+"  < '" + threshold_high + "') then " + str(rule_object_rel_id) +" else null end as `case-" + column_name + "-" + str(rule_id) + "-" + str(rule_object_rel_id) + "`,"

--         #*** For this column, this range is invalid ***
--         if rule_operator == 'between': 
--             sql_statement = "UPDATE " + database_name + "." + table_name + " SET valid_flag = 'n' WHERE " + column_name + " " + rule_operator + " " + threshold_low + " and " + threshold_high 
--             where_statement += "("+column_name+" not  "+ rule_operator + "  '" + threshold_low + "' and '" + threshold_high + "') OR "
--             case_statement += "case when ("+column_name+" not  "+ rule_operator + " '" + threshold_low + "' and '" + threshold_high + "') then " + str(rule_object_rel_id) +" else null end as `case-" + column_name + "-" + str(rule_id) + "-" + str(rule_object_rel_id) + "`,"

--         #*** For this column, this is the only valid range ***
--         if rule_operator == 'not between': 
--             sql_statement = "UPDATE " + database_name + "." + table_name + " SET valid_flag = 'n' WHERE NOT(" + column_name + " " + rule_operator + " " + threshold_low + " and " + threshold_high +")"  
--             where_statement += "("+column_name+"  "+ rule_operator + " '" + threshold_low + "' and '" + threshold_high + "') OR "
--             case_statement += "case when ("+column_name+"  "+ rule_operator + " '" + threshold_low + "' and '" + threshold_high + "') then " + str(rule_object_rel_id) +" else null end as `case-" + column_name + "-" + str(rule_id) + "-" + str(rule_object_rel_id) + "`,"

--         #*** This is a mandatory value for this column ***
--         if rule_operator == '<>': 
--             sql_statement = "UPDATE " + database_name + "." + table_name + " SET valid_flag = 'n' WHERE " + column_name + " = " + threshold_low  
--             where_statement += "("+column_name+" =  "+ threshold_low + ") OR "
--             case_statement += "case when ("+column_name+" =  '"+ threshold_low + "') then " + str(rule_object_rel_id) +" else null end as `case-" + column_name + "-" + str(rule_id) + "-" + str(rule_object_rel_id) + "`,"

--         #*** This value is always an error ***
--         if rule_operator == '=': 
--             sql_statement = "UPDATE " + database_name + "." + table_name + " SET valid_flag = 'n' WHERE " + column_name + " <> " + threshold_low  
--             where_statement += "("+column_name+" <> "+ threshold_low + ") OR "
--             case_statement += "case when ("+column_name+" <> '"+ threshold_low + "') then " + str(rule_object_rel_id) +" else null end as `case-" + column_name + "-" + str(rule_id) + "-" + str(rule_object_rel_id) + "`,"
            
--     #Orphan Check
--     # *** Left join to the foreign table on related_obj_database_name.related_obj_table_name.related_obj_column_name. If the FK table is NULL, flag the record ***
--     if rule_type_desc == 'orphan_check' and implement_type == 'Post_Update':
--         sql_statement = "merge into "+ database_name + "." + table_name + " a using "+related_obj_database_name+"."+related_obj_table_name+" b on a."+related_obj_column_name+" = b."+related_obj_column_name  + " WHEN NOT MATCHED THEN UPDATE set a.valid_flag = 'n'"
--         case_statement += "case when rule_r_" + related_obj_column_name +" IS NULL then "+ str(rule_object_rel_id) +" else null end as `case-" + column_name + "-" + str(rule_id) + "-" + str(rule_object_rel_id) + "`,"
--         where_statement += "(" + related_obj_table_name + ".rule_r_" + related_obj_column_name +"IS NULL) OR "
--         table_join += "  left join (select "+related_obj_column_name +" as rule_r_" + related_obj_column_name +" from "+ related_obj_database_name + "." + related_obj_table_name +") as r_"+str(rule_object_rel_id) +" on "+ table_name + "."+column_name+ " = r_"+str(rule_object_rel_id) +".rule_r_"+related_obj_column_name


--     #Unique Check SQL
--     # *** Check that the value of this row is always unique. If not, flag the row ***
--     # if rule_type_desc == 'unique_check' and implement_type == 'Post_Update':
--     #     sql_statement = "MERGE INTO "+ database_name + "." + table_name + " a USING  (Select "+ pkey +" from "+ database_name + "." + table_name + " where "+column_name+" in (Select COALESCE("+column_name+",0) from "+ database_name + "." + table_name + "  group by "+ grouping_columns +" having count(*) > 1)) b on a."+column_name+" = b."+column_name+" WHEN MATCHED THEN UPDATE SET a.valid_flag = 'n'"
--     if rule_type_desc == 'unique_check' and implement_type == 'Post_Update':
--         sql_statement = "MERGE INTO "+ database_name + "." + table_name + " a USING  (Select "+ pkey +" from "+ database_name + "." + table_name + " where "+column_name+" in (Select COALESCE("+column_name+",0) from "+ database_name + "." + table_name + "  group by "+ grouping_columns +" having count(*) > 1)) b on a."+column_name+" = b."+column_name+" WHEN MATCHED THEN UPDATE SET a.valid_flag = 'n'"
--         case_statement += "case when t_" + str(rule_object_rel_id) +".count_of_values > 1 then "+ str(rule_object_rel_id) +" else null end as `case-" + column_name + "-" + str(rule_id) + "-" + str(rule_object_rel_id) + "`,"
--         #where_statement += "(t_"+str(rule_object_rel_id) +".t_" + str(rule_object_rel_id) +".count_of_values > 1) OR "
--         where_statement +=  "(t_"+str(rule_object_rel_id) + ".count_of_values > 1) OR "
--         table_join += "  join (select "+column_name +" as crule_c_" + column_name +", count(*) as count_of_values from "+ database_name + "." + table_name +" group by "+column_name +") as t_"+str(rule_object_rel_id) +" on "+ table_name + "."+column_name+ " = t_"+str(rule_object_rel_id) +".crule_c_"+column_name
    
--     #Datatype Check SQL
--     # *** Check that the datatype of this row matches the expectation. If not, flag the row ***
--     if rule_type_desc == 'datatype_check' and implement_type == 'Post_Update':
--         sql_statement = "Update "+ database_name + "." + table_name + " set valid_flag = 'n' WHERE typeof("+column_name+") <> lower('"+expected_datatype+"') "  
--         where_statement += "(typeof("+column_name+") <> lower('"+ expected_datatype + "')) OR "
--         case_statement += "case when (substr(typeof("+column_name+"),1,instr(typeof(" + column_name + "),'(')-1) <> lower('"+ expected_datatype + "')) then " + str(rule_object_rel_id) +" else null end as `case-" + column_name + "-" + str(rule_id) + "-" + str(rule_object_rel_id) + "`,"
--         col_statement += "case when (typeof("+column_name+") <> lower('" +expected_datatype + "')) then 'Failed_" + column_name + "' else 'Passed_" + column_name + "' end as `rule-" + column_name + "-" + str(rule_id) + "-" + str(rule_object_rel_id) + "`,"

    
--     #Special Character Check SQL
--     # *** Check that the string does not have any special characters. If any are found, flag the row ***
--     if rule_type_desc == 'special_char_check' and implement_type == 'Post_Update':
--         #sql_statement = "Update "+ database_name + "." + table_name + " set valid_flag = 'n'  WHERE regexp_replace("+ column_name +", "+ regex_string + ", "") <> "+ column_name 
--         #where_statement += "(regexp_replace("+column_name+", "+ regex_string + ", "") <> " + column_name + ") OR "
--         #case_statement += "case when regexp_replace("+column_name+", "+ regex_string + ", "") <> " + column_name + " then 1 else 0 end as `case-" + column_name + "-" + str(rule_id) + "-" + str#(rule_object_rel_id) + "`,"
--         sql_statement = "Update "+ database_name + "." + table_name + " set valid_flag = 'n'  WHERE regexp_replace("+ column_name +", "+ valid_list + ", '') <> "+ column_name 
--         where_statement += "(regexp_replace("+column_name+", "+ valid_list + ", '') <> " + column_name + ") OR "
--         case_statement += "case when regexp_replace("+column_name+", "+ valid_list + ", '') <> " + column_name + " then " + str(rule_object_rel_id) + " else null end as `case-" + column_name + "-" + str(rule_id) + "-" + str(rule_object_rel_id) + "`,"

    
--     #Date Format Check SQL
--     # *** Check that the string does not have any special characters. If any are found, flag the row ***
--     if rule_type_desc == 'date_format_check' and implement_type == 'Post_Update':
--         sql_statement = "Update "+ database_name + "." + table_name + " set valid_flag = 'n'  WHERE to_date('"+column_name+"', "+valid_list+") is null "
--         where_statement += "(to_date(coalesce("+column_name+",'1900-01-01'), "+ valid_list + ") is null)  OR "
--         case_statement += "case when to_date(coalesce("+column_name+",'1900-01-01'), "+ valid_list + ") is null then " + str(rule_object_rel_id) + " else null end as `case-" + column_name + "-" + str(rule_id) + "-" + str(rule_object_rel_id) + "`,"

--     #String Length Check SQL
--     # *** Check that the string should have fixed lenght characters. If any are found, flag the row ***
--     if rule_type_desc == 'string_fixed_length' and implement_type == 'Post_Update':
--         # sql_statement = "Update "+ database_name + "." + table_name + " set valid_flag = 'n'  WHERE valid_flag = 'y' and len("+column_name+") > " +valid_list
--         sql_statement = ""
--         where_statement += "(len("+column_name+") > "+ valid_list + ") OR "
--         case_statement += "case when (len("+column_name+") > " +valid_list + ") then " + str(rule_object_rel_id) + " else null end as `case-" + column_name + "-" + str(rule_id) + "-" + str(rule_object_rel_id) + "`,"

--         col_statement = "case when (len("+column_name+") > " +valid_list + ") then 'Failed_" + column_name + "' else 'Passed_" + column_name + "' end as `rule-" + column_name + "-" + str(rule_id) + "-" + str(rule_object_rel_id) + "`,"
--         #sql_statement = "Update "+ database_name + "." + table_name + " set valid_flag = 'n'  WHERE valid_flag = 'y' and len("+column_name+") > 0 and len("+column_name+") < " ##+valid_list
--         #col_statement = "case when (len("+column_name+") > 0 and len("+column_name+") < " +valid_list + ") then '" + str(rule_object_rel_id) + "' else '0' end as rule_" + str(rownum) + "_" + str(rule_object_rel_id)

--     #Data in valid values Check SQL
--     # check if the values in the column are in the list of valid values. If not, Flag row. 
--     if rule_type_desc == 'valid_values_check' and implement_type == 'Post_Update':
--         sql_statement = "Update "+ database_name + "." + table_name + " set valid_flag = 'n'  WHERE valid_flag = 'y' and "+ column_name +" not in (" + valid_list + ") "
--         col_statment = "(" + column_name +" not in (" + valid_list + ") ) as rule_" + str(rule_id) + "_" + str(rule_object_rel_id)
--         where_statement += "("+column_name+" not in ("+ valid_list + ")) OR "
--         case_statement += "case when ("+column_name+" not in (" +valid_list + ")) then " + str(rule_object_rel_id) +" else null end as `case-" + column_name + "-" + str(rule_id) + "-" + str(rule_object_rel_id) + "`,"
   
--     data = [(sql_statement, str(audit_dim_id), str(rule_id), str(rule_object_rel_id),col_statement,where_statement)]
--     df2 = spark.createDataFrame(data)
--     rules_df = rules_df.unionAll(df2)
    
--     ############       Reset variables     ############
--     rule_operator , rule_type_desc, rule_desc, threshold_low, threshold_high, expected_datatype, threshold_pct ,database_name, table_name, column_name , pkey , grouping_columns, related_obj_database_name, related_obj_table_name, related_obj_column_name,implement_type, rule_dim_id , rule_object_rel_id ,audit_dim_id, expected_date_format, regex_string, valid_values   = "", "", "", "", "", "","", "", "", "", "", "", "", "", "", "", 0 , 0,0, 'MM-dd-yyyy', '[^a-zA-Z0-9_]',''

-- where_statement = where_statement[:-3]
-- case_statement = case_statement[:-1]
-- col_statement = col_statement[:-1]


-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC col_join_query = "(select target_col_name,target_key_col_order from dbo.control_raw_source_to_target_col_map(nolock) where table_name='"+ val_table_name + "' and target_key_col_order is not null) as source_quey"
-- MAGIC df_join_cond_1 = spark.read.jdbc(url=jdbc_url, table=col_join_query, properties=connection_properties).orderBy(col("target_key_col_order"))
-- MAGIC df_join_cond=df_join_cond_1.selectExpr("'s.'||target_col_name||'=t.'||target_col_name as col_name").withColumn("col_name",F.expr("concat_ws(' and ', collect_list(col_name))")).select('col_name')
-- MAGIC
-- MAGIC # string parameter contains join condition
-- MAGIC val_join_cond = df_join_cond.head()[0]
-- MAGIC
-- MAGIC #df_join_cond.display()
-- MAGIC print(val_join_cond)
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df_join_cond.display()

-- COMMAND ----------

-- %python
-- if case_statement != "":
--     #final_df._jdf.sparkSession().createOrReplaceTempView("temp_view_name_stage")

--     select_stmt = "select *," + case_statement + " from " + var_database_name + "." + val_table_name
--     if table_join != "":
--         select_stmt += table_join

--     #print(select_stmt)
--     new_result_df = spark.sql(select_stmt)
--     #new_result_df = sql.DataFrame(df,microBatchOutputDF.sql_ctx)
--     #print(new_result_df.count())

--     selected_columns = [column for column in new_result_df.columns if column.startswith("case")]

--     selected_columns_hash_target = ",".join(["t."+col for col in new_result_df.columns if ((col not in {'dq_codes', 'valid_flag', 'create_date','change_date','count_of_values'}) and (col.startswith("case") == False ) and (col.startswith("crule") == False ) and (col.startswith("rule") == False ) ) ])
--     #print(selected_columns_hash_target)
--     selected_columns_hash_source = ",".join(["s."+col for col in new_result_df.columns if ((col not in {'dq_codes', 'valid_flag', 'create_date','change_date','count_of_values'}) and (col.startswith("case") == False ) and (col.startswith("crule") == False ) and (col.startswith("rule") == False ) ) ])
--     #print(selected_columns_hash_source)

--     new_result_df = new_result_df.withColumn("dq_codes",concat_ws(",",*selected_columns))
--     new_result_df = new_result_df.withColumn("valid_flag",when(new_result_df.dq_codes != "", 'n').otherwise('y'))
--     #new_result_df = new_result_df.withColumn("row_sha2", sha2(concat_ws("||", *selected_columns_hash), 256))

--     #selected_columns = [column.split('-')[1].strip() for column in selected_columns]
--     #selected_columns = [column for column in selected_columns if column != 'valid_flag']
--     #selected_columns.append("dq_codes")
--     #selected_columns.append("valid_flag")
--     temp_view_name = "TEMP_" + val_table_name 

--     new_result_df.createOrReplaceTempView(temp_view_name)

--     val_col_map_update = 't.dq_codes=ARRAY(s.dq_codes),t.valid_flag=s.valid_flag'
--     merge_query = ""
--     if val_join_cond == "":
--        val_join_cond = f"""sha2(concat_ws("||", {selected_columns_hash_target}), 256)=sha2(concat_ws("||", {selected_columns_hash_source}), 256)""" 
--     #else:
--     #  sha2(concat_ws("||", *selected_columns_hash), 256)     

--     merge_query = f"""MERGE INTO raw.{val_table_name} t USING {temp_view_name} s ON {val_join_cond} WHEN MATCHED THEN UPDATE SET {val_col_map_update} """
--     #print(merge_query)

--     #merge_query = ""
--     try:
--         if merge_query != "":
--            spark.sql(merge_query)
--     except Exception as e: 
--         print("Exception " + str(e) +" ocurred")
--         dbutils.notebook.exit("Error in function : " + str(e))

--     #valid_list_df = new_result_df.select(selected_columns)

--     #valid_list_df.display()
--     #new_result_df.display()


--     #new_result_df = new_result_df.drop(*selected_columns)

--     final_df = new_result_df


-- COMMAND ----------

-- MAGIC %python
-- MAGIC if dbutils.widgets.get("post_run") == "Y":
-- MAGIC     case_statement = ""
-- MAGIC     where_statement=""
-- MAGIC     val_join_cond = ""
-- MAGIC     col_statement = ""
-- MAGIC     table_join=""
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC %md
-- MAGIC Redeploy 2024-03-15
