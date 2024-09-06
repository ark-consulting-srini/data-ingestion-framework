# Databricks notebook source
def get_environment():
    """

    :return:
    """
    #environment_id = spark.conf.get("spark.databricks.clusterUsageTags.clusterOwnerOrgId")
    environment_id = "3411009296921520"

    val_cat = 'UNKNOWN'
    if environment_id == '3411009296921520':
        val_cat = 'dev'
        workspace = 'DEV'
    if environment_id == '7632849253561708':
        val_cat = 'qa'
        workspace = 'QA'
    if environment_id == '7595596043971971':
        val_cat == 'prd'
        workspace = 'PRD'
    if environment_id == '0000000000000000':
        val_cat = 'dr'
        workspace = 'DR'
    print(f"env:{environment_id} --> catalog:{val_cat}, ws:{workspace}")
    return val_cat, workspace


def get_connection_properties(workspace: str,
                              scope_prefix="ITDA_KEY_VAULT_",
                              server_key="ADFSQLCONFIGSERVER",
                              database_key="ADFSQLCONFIGDB",
                              username_key="ADFSQLCONFIGUID",
                              password_key="ADFSQLCONFIGPWD"):
    """

    :param workspace:
    :param scope_prefix:
    :param server_key:
    :param database_key:
    :param username_key:
    :param password_key:
    :return:
    """
    if not isinstance(workspace, str):
        raise ValueError("Workspace must be a string")
    if not workspace.upper() in ["DEV", "QA", "PRD", "DR"]:
        raise ValueError("Workspace must be one of DEV, QA, PRD, DR")
    if not isinstance(scope_prefix, str):
        raise ValueError("Scope prefix must be a string")
    if not isinstance(server_key, str):
        raise ValueError("Server key must be a string")
    if not isinstance(database_key, str):
        raise ValueError("Database key must be a string")
    if not isinstance(username_key, str):
        raise ValueError("Username key must be a string")
    if not isinstance(password_key, str):
        raise ValueError("Password key must be a string")

    server = dbutils.secrets.get(scope=f"{scope_prefix}{workspace}", key=server_key)
    database = dbutils.secrets.get(scope=f"{scope_prefix}{workspace}", key=database_key)
    username = dbutils.secrets.get(scope=f"{scope_prefix}{workspace}", key=username_key)
    password = dbutils.secrets.get(scope=f"{scope_prefix}{workspace}", key=password_key)

    return server, database, username, password


val_cat, workspace = get_environment()
server, database, username, password = get_connection_properties(workspace)

# COMMAND ----------

ctl_query_pre = (spark.read
  .format("sqlserver")
  .option("host", server)
  .option("port", "1433") 
  .option("user", username)
  .option("password", password)
  .option("database", database)
  .option("dbtable", "dbo.control_raw")
  .load()
 )

display(ctl_query_pre)

# COMMAND ----------

dq_rule_object_rel_df = (spark.read
  .format("sqlserver")
  .option("host", server)
  .option("port", "1433") 
  .option("user", username)
  .option("password", password)
  .option("database", database)
  .option("dbtable", "dbo.dq_rule_object_rel")
  .load()
 )

display(dq_rule_object_rel_df)

# COMMAND ----------

dq_rule_dim_df = (spark.read
  .format("sqlserver")
  .option("host", server)
  .option("port", "1433") 
  .option("user", username)
  .option("password", password)
  .option("database", database)
  .option("dbtable", "dbo.dq_rule_dim")
  .load()
 )

display(dq_rule_dim_df)

# COMMAND ----------

dq_rule_valid_value_dim_df = (spark.read
  .format("sqlserver")
  .option("host", server)
  .option("port", "1433") 
  .option("user", username)
  .option("password", password)
  .option("database", database)
  .option("dbtable", "dbo.dq_rule_valid_value_dim")
  .load()
 )

display(dq_rule_valid_value_dim_df)
