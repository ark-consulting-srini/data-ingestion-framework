from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.dbutils import DBUtils
import re


def sql_server_write(df, _table_name, _jdbc_url, _user, _password):
    """

    :param df:
    :param _table_name:
    :param _jdbc_url:
    :param _user:
    :param _password:
    :return:
    """
    try:
        (df.write
         .format("jdbc")
         .mode("Append")
         .option("url", _jdbc_url)
         .option("dbtable", _table_name)
         .option("user", _user)
         .option("password", _password)
         .option("SaveMode", "APPEND")
         .save()
         )
    except Exception as e:
        print(e)


def get_environment():
    """

    :return:
    """
    # TODO: Make this dynamic?
    spark = SparkSession.builder.getOrCreate()
    environment_id = spark.conf.get("spark.databricks.clusterUsageTags.clusterOwnerOrgId")

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

    spark = SparkSession.builder.getOrCreate()
    dbutils = DBUtils(spark)

    server = dbutils.secrets.get(scope=f"{scope_prefix}{workspace}", key=server_key)
    database = dbutils.secrets.get(scope=f"{scope_prefix}{workspace}", key=database_key)
    username = dbutils.secrets.get(scope=f"{scope_prefix}{workspace}", key=username_key)
    password = dbutils.secrets.get(scope=f"{scope_prefix}{workspace}", key=password_key)

    return server, database, username, password


def create_jdbc(server, database, username, password):
    """

    :param server:
    :param database:
    :param username:
    :param password:
    :return:
    """
    # TODO: Verify args
    jdbc_url = f"jdbc:sqlserver://{server};database={database}"
    connection_properties = {
        "user": username,
        "password": password,
        "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    }
    return jdbc_url, connection_properties


def write_change_date_df(catalog,
                         schema_raw,
                         data_product_name,
                         jdbc_url,
                         connection_properties,
                         tbl_name="dbo.control_curated"):
    """

    :param catalog:
    :param schema_raw:
    :param data_product_name:
    :param jdbc_url:
    :param connection_properties:
    :param tbl_name:
    :return:
    """
    table_name = f"{catalog}.{schema_raw}.{data_product_name}"
    spark = SparkSession.builder.getOrCreate()
    try:
        if not spark.catalog._jsparkSession.catalog().tableExists(table_name):
            raise ValueError(f"Table {table_name} does not exist")
    except Exception as e:
        print(f"Warning: {e}")

    df = spark.sql(f"select change_date from {table_name} limit 1")
    change_date = df.head()[0]

    data = [(data_product_name, 'curated', 'material_dim', 'Raw_2_curated_Material_dim', 'B', change_date)]
    schema = "stream_name:string, schema_name:string, table_name:string, job_name:string, manual_or_batch_load_flag:string, change_date:timestamp"

    df_sql_server = spark.createDataFrame(data, schema)

    (df_sql_server.write
     .format("jdbc")
     .option("url", jdbc_url)
     .option("dbtable", tbl_name)
     .option("user", connection_properties["user"])
     .option("password", connection_properties["password"])
     .mode("append")
     .save()
     )


class AuditTableHandler(object):

    def __init__(self, audit_table_config: dict):
        """

        :param audit_table_config:
        """
        self.spark = SparkSession.builder.getOrCreate()
        self.config = audit_table_config
        # Debug
        # self._verify_config()
        env, workspace = get_environment()
        server, database, username, password = get_connection_properties(env)
        self.username = username
        self.password = password
        self.jdbc_url = f"jdbc:sqlserver://{server};database={database}"
        if not isinstance(username, str):
            raise ValueError("Username must be a string")
        if not isinstance(password, str):
            raise ValueError("Password must be a string")
        self._verify_jdbc_url(self.jdbc_url)
        self.connection_properties = ""

    def _verify_config(self):
        """

        :return:
        """
        req_keys = ['audit_dim_id', 'rec_created_date', 'source_table_name', 'target_table_name']
        for key in req_keys:
            if key not in self.config:
                raise ValueError(f"Key {key} not found in the audit table config")

    def _verify_jdbc_url(self, jdbc_url):
        """

        :param jdbc_url:
        :return:
        """
        if not isinstance(jdbc_url, str):
            raise ValueError("JDBC URL must be a string")
        if not re.match(r'^jdbc:sqlserver://', jdbc_url):
            raise ValueError("JDBC URL must be a string and start with 'jdbc:sqlserver://'")

    def sql_server_write_fact_msg(self, msg):
        """

        :param msg:
        :return:
        """
        columns = ['sla_dim_id', 'audit_record_type_desc', 'audit_timestamp', 'audit_dim_id']
        # TODO: Fix dim id and audit ids
        values = [(str(val_sla_dim_id), msg, datetime.now(), str(val_audit_dim_id))]
        df = self.spark.createDataFrame(values, columns)
        val_sql_tbl_name = "dq_audit_fact"
        sql_server_write(df, val_sql_tbl_name, self.jdbc_url, self.username, self.password)

    def sql_server_write_fact_count(self, msg, cnt):
        """

        :param msg:
        :param cnt:
        :return:
        """
        columns = ['sla_dim_id', 'audit_record_type_desc', 'audit_timestamp', 'audit_value', 'valid_flag',
                   'audit_dim_id']
        # TODO: Fix dim id and audit ids
        values = [(str(val_sla_dim_id), msg, datetime.now(), str(cnt), 'Y', str(val_audit_dim_id))]
        df = self.spark.createDataFrame(values, columns)
        val_sql_tbl_name = "dq_audit_fact"
        sql_server_write(df, val_sql_tbl_name, self.jdbc_url, self.username, self.password)

    def write_audit_dim_table(self,
                              target_table,
                              data_product_name,
                              src_table_name,
                              sql_tbl_name="dq_audit_dim"):
        """

        :param target_table:
        :param data_product_name:
        :param src_table_name:
        :param sql_tbl_name:
        :return:
        """
        values = [(target_table[4:], data_product_name, src_table_name[4:], target_table[4:])]
        columns = ['table_name', 'data_product_name', 'source_table_name', 'target_table_name']
        df = self.spark.createDataFrame(values, columns)
        sql_server_write(df, sql_tbl_name, self.jdbc_url, self.username, self.password)

    def get_audit_dim_id(self,
                         target_table,
                         sql_tbl_name="dq_audit_dim",
                         sql_db_name="dbo"):
        """

        :param target_table:
        :param sql_tbl_name:
        :param sql_db_name:
        :return:
        """
        query = f"(SELECT max(audit_dim_id) audit_dim_id FROM {sql_db_name}.{sql_tbl_name} WHERE target_table_name = '{target_table[4:]}') AS audit_dim_id"
        df = self.spark.read.jdbc(url=self.jdbc_url, table=query, properties=self.connection_properties)
        audit_dim_id = df.first()['audit_dim_id']
        sla_dim_id = 0
        return audit_dim_id, sla_dim_id

    def signal_pipeline_start(self):
        """

        :return:
        """
        self.sql_server_write_fact_msg("pipeline start")

    def signal_pipeline_end(self):
        """

        :return:
        """
        self.sql_server_write_fact_msg("pipeline end")

    def write_source_counts(self, cnt_query, target_table):
        """

        :param cnt_query:
        :param target_table:
        :return:
        """
        # write audit fact source_row_count
        self.sql_server_write_fact_count("source_row_count", cnt_query)

        # write audit fact target_row_count
        cnt_target = self.spark.sql(f"select count(*) cnt from {target_table}").first()['cnt']
        self.sql_server_write_fact_count("target_row_count", cnt_target)

    def write_type2_updates(self, df):
        """

        :param df:
        :return:
        """
        if not "num_updated_rows" in df.columns:
            raise ValueError("num_updated_rows not found in the dataframe")
        if not "num_inserted_rows" in df.columns:
            raise ValueError("num_inserted_rows not found in the dataframe")
        num_rows_updated_t2 = df.first()['num_updated_rows']
        num_rows_inserted = df.first()['num_inserted_rows']

        self.sql_server_write_fact_count("target_insert_count", num_rows_inserted)
        self.sql_server_write_fact_count("target_update_count_type2", num_rows_updated_t2)

    def write_type1_updates(self, df):
        """

        :param df:
        :return:
        """
        if not "num_updated_rows" in df.columns:
            raise ValueError("num_updated_rows not found in the dataframe")
        num_rows_updated_t1 = df.first()['num_updated_rows']
        self.sql_server_write_fact_count("target_update_count_type1", num_rows_updated_t1)

    def write_target_table_count(self, target_table):
        """

        :param target_table:
        :return:
        """
        cnt_target = self.spark.sql(f"select * from {target_table}").count()
        self.sql_server_write_fact_count("target_row_count", cnt_target)
