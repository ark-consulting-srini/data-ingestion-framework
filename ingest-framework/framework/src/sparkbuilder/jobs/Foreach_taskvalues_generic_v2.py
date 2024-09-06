# Databricks notebook source
import json

# COMMAND ----------

dbutils.widgets.text("config_database", "", "The 3 level config table name")
dbutils.widgets.text("data_product_name", "optiva", "Campbells product name. Defaults to optiva")

# COMMAND ----------

config_database = dbutils.widgets.get("config_database")
data_product_name = dbutils.widgets.get("data_product_name")

# COMMAND ----------

config_raw_df = spark.sql(f"""select data_product_name, table_name, '{config_database}' as config_database, is_table_enabled  from {config_database} where data_product_name = '{data_product_name}' and lower(is_table_enabled) = 'true'""")
config_raw_df_pd = config_raw_df.toPandas()
task_list = config_raw_df_pd.to_dict(orient='records')
task_json_list = [eval(json.dumps(d)) for d in task_list]

# COMMAND ----------

config_raw_df.display()

# COMMAND ----------

len(task_json_list), task_json_list

# COMMAND ----------

# Creating Job task Values
dbutils.jobs.taskValues.set(key = 'raw_load', value = task_json_list)
