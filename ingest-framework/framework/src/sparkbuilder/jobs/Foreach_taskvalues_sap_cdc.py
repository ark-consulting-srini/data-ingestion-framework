# Databricks notebook source
import json

# COMMAND ----------

 dbutils.widgets.text("data_product_name", "", "Data Product Name is a Campbell's specific variable for their audit system")
dbutils.widgets.text("config_database", "", "The table name with configurations")


# COMMAND ----------

# Retrieve the values from the widgets
data_product_name = dbutils.widgets.get("data_product_name")
config_database = dbutils.widgets.get("config_database")
#data_product_name='sap-cdc'
#config_database='sandbox.dbx_demo_config.optiva_config_raw_tables_v4'
print(data_product_name)
print(config_database)

# COMMAND ----------

# List to store the objects
job_task_list = []
##table_name,data_product_name,config_database
# List of sap-cdc tables
sap_cdc_tables_df = spark.sql(f"""select table_name,data_product_name,'{config_database}' as config_database from  {config_database} where upper(active)="TRUE" and data_product_name='{data_product_name}' order by table_name""")

# Generate objects with table names
for row in sap_cdc_tables_df.collect():
    
    table_name = row["table_name"]
    data_product_name=row["data_product_name"]
    config_database=row["config_database"] 
    dict_obj = {"table_name": table_name,"data_product_name":data_product_name, "config_database": config_database}
    job_task_list.append(dict_obj)


# # Set a task value using dbutils
dbutils.jobs.taskValues.set(key = "sap_cdc_tables", value = job_task_list)
