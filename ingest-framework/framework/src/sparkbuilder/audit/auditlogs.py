from pyspark.sql import DataFrame
from datetime import datetime
from pyspark.sql import functions as F

class AuditLogs(object):
    def __init__(self, spark, config,audit_table_name, external_location=None) -> None:
        self.spark = spark
        self.audit_table_name = audit_table_name
        self.config = config
        self.job_id = self.config["job_id"]

        if not self._table_exists(table_name=self.audit_table_name):
            self._create_audit_table(table_name=self.audit_table_name, external_location=external_location) 


    def audit_log(self, tableName:str, operation:str,audit_df:DataFrame, base_file_path):
        if self.config["audit_write"].lower() == "true":
            audit_log_df = (
                audit_df.selectExpr(f"'{tableName}' as table_name",f"'{operation}' as audit_operation","current_timestamp() as audit_timestamp","current_user() as audit_user","to_json(struct(*)) AS audit_info")
                .withColumn("job_id", F.lit(self.job_id))
                .withColumn("base_file_path", F.lit(base_file_path))
                            )
            
            audit_log_df.write.format("delta").mode("append").saveAsTable(f"{self.audit_table_name}")
    
    def _table_exists(self, table_name):
        return self.spark.catalog.tableExists(table_name)
    
    def _create_audit_table(self, table_name, external_location=None):
        external_location_sql = ""
        if external_location:
            external_location_sql = f"LOCATION '{external_location}'"
        self.spark.sql(f"""
            CREATE TABLE {table_name} (
                table_name STRING,
                audit_operation STRING,
                audit_timestamp TIMESTAMP,
                audit_user STRING,
                audit_info STRING,
                job_id STRING,
                base_file_path STRING
            )
            {external_location_sql}
        """)