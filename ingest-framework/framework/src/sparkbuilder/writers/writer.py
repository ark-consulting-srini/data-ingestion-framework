from pyspark.sql import functions as F
from sparkbuilder.writers.batch_writers import merge_write #, overwrite_write, append_write 
from sparkbuilder.writers.streaming_writers import streaming_write_table_append #,streaming_write_file_overwrite, streaming_write_file_append, 
import time
from sparkbuilder.audit.auditlogs import AuditLogs
from sparkbuilder.readers.reader import read_uc_table, read_parquet

class Writer(object):

    def __init__(self, spark, config):
        """
        Writer class for writing dataframes to a target location
        :param config:
        """
        if not isinstance(config, dict):
            raise ValueError("config must be a dictionary")
        self.config = config
        self.spark = spark
        self.control_columns_list = ["row_creation_time", "file_modification_time", "file_path", "start_time", "end_time", "is_current", "delete_time"]
        
        if "audit_config" in self.config and "audit_table_name" in self.config["audit_config"]:
            if "audit_write" in self.config and self.config["audit_write"].lower() == "true":
                audit_table_name = self.config["audit_config"]["audit_table_name"]
                print(f"Audit logs ENABLED: Sending logs to {audit_table_name}")

            external_location = None
            if "external_location" in self.config["audit_config"]:
                external_location = self.config["audit_config"]["external_location"]

            self.audit = AuditLogs(spark=self.spark, config=self.config, audit_table_name=self.config["audit_config"]["audit_table_name"], external_location=external_location)
        else: 
            raise Exception("audit_config must be configured even though the audit_write is False.")
    
    def _overwrite_delta(self, batch_df, batch_id, keys, source_orderBy_column, target_table, scd_type):
        batch_df.createOrReplaceTempView("updates")
        over_partition_columns = ", ".join([ k for k in keys])
        control_columns = ", ".join([ k for k in self.control_columns_list])

        base_df = batch_df.sparkSession.sql(f"""
            SELECT *, 
                ROW_NUMBER() OVER(PARTITION BY {over_partition_columns} ORDER BY {source_orderBy_column} DESC) as rn
            FROM updates
        """)

        preprocessed_df = base_df.where(f"rn = 1").withColumn(
            "end_time", F.when(F.col("rn") != 1, F.current_timestamp()).otherwise(None)
        ).withColumn(
            "is_current", F.when(F.col("rn") != 1, 0).otherwise(1)
        )

        if scd_type == 1:
            audit_df = preprocessed_df.drop(*self.control_columns_list).drop('rn').mode("overwrite").save(target_table)
            self.audit.audit_log(tableName=target_table, operation="overwrite", audit_df=audit_df, base_file_path="")

        elif scd_type == 2:
            audit_df = preprocessed_df.drop('rn').mode("overwrite").save(target_table)
            self.audit.audit_log(tableName=target_table, operation="overwrite", audit_df=audit_df, base_file_path="")

        else:
            raise Exception("Missing scd_type in function _first_insert_to_delta")

    def _first_insert_to_delta(self, batch_df, batch_id, keys, source_orderBy_column, target_table, scd_type):
        batch_df = batch_df.orderBy([source_orderBy_column], ascending = [True])
        batch_df.createOrReplaceTempView("updates")
        over_partition_columns = ", ".join([ k for k in keys])
        control_columns = ", ".join([ k for k in self.control_columns_list])

        if scd_type == 1:
            except_columns = ", ".join([ k for k in self.control_columns_list])
            update_query = f"""
                WITH BasePreprocessedSource AS (
                SELECT *,
                    ROW_NUMBER() OVER(PARTITION BY {over_partition_columns} ORDER BY {source_orderBy_column} DESC) as rn
                FROM updates
                )
                INSERT INTO {target_table}
                    SELECT * EXCEPT({control_columns}, rn) from BasePreprocessedSource where rn = 1
            """

        elif scd_type == 2:
            update_query = f"""
                WITH BasePreprocessedSource AS (
                SELECT * except(end_time, is_current, delete_time),
                    ROW_NUMBER() OVER(PARTITION BY {over_partition_columns} ORDER BY {source_orderBy_column} DESC) as rn
                FROM updates
                ),
                PreprocessedSource AS (
                    SELECT *,
                        CASE
                            WHEN rn != 1 THEN current_timestamp() 
                            ELSE null
                        END AS end_time,
                        CASE 
                            WHEN rn != 1 THEN 0 
                            ELSE 1 
                        END AS is_current, 
                        null AS delete_time
                    FROM BasePreprocessedSource
                )
                INSERT INTO {target_table}
                    SELECT * EXCEPT(rn) from PreprocessedSource
            """
        else:
            raise Exception("Missing scd_type in function _first_insert_to_delta")
        
        audit_df = batch_df.sparkSession.sql(update_query)
        self.audit.audit_log(tableName=target_table, operation="merge", audit_df=audit_df, base_file_path="")


        #if batch_id % 101 == 0:
            # this is called inline optimization and it happens here because the streaming is continuous
            # whitout row-level concurrency enabled, merge and optmize could conflict
            # https://docs.databricks.com/en/optimizations/isolation-level.html#enable-row-level-concurrency
            #self.spark.sql(f"optimize {target_table}")
            #print(f"Optimizing {target_table} based on batch_id")


    def _table_exists(self, table_name):
        return self.spark.catalog.tableExists(table_name)


    def _create_external_table_from_df(self, df, table_name, location):
        schema = df.schema
        columns = []
        
        for field in schema.fields:
            columns.append(f"`{field.name}` {field.dataType.simpleString()}")
        
        columns_str = ",\n".join(columns)
        
        create_table_sql = f"""
        CREATE TABLE {table_name} (
        {columns_str}
        )
        USING delta
        LOCATION '{location}'
        """
        audit_df = self.spark.sql(create_table_sql)
        self.audit.audit_log(tableName=table_name, operation="create", audit_df=audit_df, base_file_path="")        


    def _create_table_by_scd_type(self, df, target_table, scd_type, writer_config):
        if scd_type == 1:
            new_table_df = self.spark.createDataFrame([], df.schema).drop(*self.control_columns_list)
        elif scd_type == 2:
            new_table_df = self.spark.createDataFrame([], df.schema)
        else:
            raise Exception("Missing scd_type in function _create_table_by_scd_type")
        
        # Handle Managed and External tables
        if "external_location" in writer_config and writer_config["external_location"] != None and writer_config["external_location"] != "":
            self._create_external_table_from_df(new_table_df, target_table, writer_config["external_location"])
        else:
            #managed table
            new_table_df.writeTo(target_table).create()


    def get_base_file_path_list_from_table(self,target_table) -> list:
        latest_modification_time_rows = f"""
        SELECT 
        substring_index(file_path, '/', length(file_path) - length(replace(file_path, '/', ''))) AS cleaned_file_path,
        MAX(file_modification_time) AS latest_modification_time 
        FROM 
        {target_table} 
        GROUP BY 
        file_path, cleaned_file_path
        ORDER BY 
            latest_modification_time ASC
        """
        cleaned_file_path_pd = self.spark.sql(latest_modification_time_rows).toPandas()
        cleaned_file_path_list = cleaned_file_path_pd["cleaned_file_path"].tolist()
        seen = set()
        unique_list = [x for x in cleaned_file_path_list if x not in seen and not seen.add(x)]
        return unique_list


    def _upsert_to_delta_batch_fe(self, batch_df, batch_id, keys, source_orderBy_column, target_table, scd_type, target_table_type):
        control_columns = ", ".join([ k for k in self.control_columns_list])
        batch_df = batch_df.orderBy([self.config["source_orderBy_column"]], ascending = [True])
        join_statement = " AND ".join([f"target.{k} = source.{k}" for k in keys])
        

        batch_df.createOrReplaceTempView("SourceData")
        # get the list base file path to process in order
        base_file_path_list = self.get_base_file_path_list_from_table("SourceData")

        if scd_type == 1:
            for base_file_path in base_file_path_list:
                #TODO debug - to delete
                #print("base_file_path:", base_file_path)
                
                update_query = f"""
                    INSERT OVERWRITE TABLE {target_table} 
                        SELECT * EXCEPT({control_columns}) from SourceData where startswith(file_path, '{base_file_path}')
                """
                audit_df = self.spark.sql(update_query)
                
                # TODO: debug - to delete
                #audit_df.show()

                self.audit.audit_log(tableName=target_table, operation="overwrite", audit_df=audit_df, base_file_path=base_file_path)

        elif scd_type == 2:
            data_columns = list(set(self._get_data_columns_as_list(batch_df.columns)) - set(keys))
            join_brute_force_statement = " OR ".join([f"target.{k} is distinct from source.{k}" for k in data_columns])
            all_columns_list = batch_df.columns
            all_columns_statement = ", ".join([k for k in all_columns_list])
            nc_columns_statement = ", ".join([f"NC.{k}" for k in keys])
            pk_columns_statement = ", ".join([k for k in keys])
            nc_join_statement = " AND ".join([f"NC.{k} = C.{k}" for k in keys])
            
            for base_file_path in base_file_path_list:
                now = time.time()
                all_columns_values_statement = ", ".join([f"source.{k}" for k in all_columns_list])
                all_columns_values_statement = all_columns_values_statement.replace("source.is_current","1")
                all_columns_values_statement = all_columns_values_statement.replace("source.start_time",f"cast({now} as timestamp)")
                all_columns_values_statement = all_columns_values_statement.replace("source.end_time","NULL")
                
                # Handle tables without data_columns 
                brute_force_statement = ""
                if len(join_brute_force_statement) > 0:
                    brute_force_statement = f""" 
                        WHEN MATCHED AND target.is_current = 1 AND ({join_brute_force_statement}) THEN
                        UPDATE SET target.is_current = 0, target.end_time = cast({now} as timestamp)
                    """        
                
                current_batch_df = self.spark.sql(f"""
                                        SELECT * 
                                            FROM SourceData 
                                            WHERE 
                                                startswith(file_path, '{base_file_path}') 
                                    """)
                #TODO: Debug
                #print("base_file_path:", base_file_path, current_batch_df.count())
                #current_batch_df.select("file_path").distinct().show(truncate=False)
                
                
                current_batch_df.createOrReplaceTempView("CurrentSourceData")

                update_query = f"""
                        MERGE INTO {target_table} AS target
                        USING CurrentSourceData AS source
                        ON {join_statement} AND target.is_current = 1
                        
                        {brute_force_statement}
                        
                        WHEN NOT MATCHED THEN
                        INSERT ({all_columns_statement})
                        VALUES ({all_columns_values_statement})

                        WHEN NOT MATCHED BY SOURCE AND target.is_current = 1 THEN
                        UPDATE SET target.is_current = 0, target.end_time = cast({now} as timestamp),  delete_time = cast({now} as timestamp)
                    """
                #print(update_query)
                audit_df = self.spark.sql(update_query)
                self.audit.audit_log(tableName=target_table, operation="update", audit_df=audit_df, base_file_path=base_file_path)

                insert_updates = f"""
                    INSERT INTO {target_table} ({all_columns_statement})
                        SELECT {all_columns_values_statement}
                        FROM CurrentSourceData source
                        JOIN (
                        SELECT {nc_columns_statement}
                        FROM (SELECT {pk_columns_statement} FROM {target_table} WHERE is_current = 0 and end_time = cast({now} as timestamp)) NC 
                        LEFT ANTI JOIN (SELECT {pk_columns_statement} FROM {target_table} WHERE is_current = 1) C ON {nc_join_statement}
                        ) target ON {join_statement}
                """
                
                #print(insert_updates)
                audit_df = self.spark.sql(insert_updates)
                self.audit.audit_log(tableName=target_table, operation="insert", audit_df=audit_df, base_file_path=base_file_path)
                
        else:
            raise Exception("Missing scd_type in function _upsert_to_delta_fe")

        if batch_id % 101 == 0:
            # this is called inline optimization and it happens here because the streaming is continuous
            # whitout row-level concurrency enabled, merge and optmize could conflict
            # https://docs.databricks.com/en/optimizations/isolation-level.html#enable-row-level-concurrency
            self.spark.sql(f"optimize {target_table}")
            print(f"Optimizing {target_table} based on batch_id")


    def _upsert_to_delta_batch_ie(self, batch_df, keys, source_orderBy_column, target_table_1, target_table_2, target_table_type_1, target_table_type_2):
        control_columns = ", ".join([ k for k in self.control_columns_list])
        batch_df = batch_df.orderBy([self.config["source_orderBy_column"]], ascending = [True])
        join_statement = " AND ".join([f"target.{k} = source.{k}" for k in keys])
        

        batch_df.createOrReplaceTempView("SourceData")
        # get the list base file path to process in order
        base_file_path_list = self.get_base_file_path_list_from_table("SourceData")

        # TODO: debug
        # base_file_path_list = ['abfss://landing@sacscdnausedlhdev.dfs.core.windows.net/sap_dwc/V_MSEG/2024/08/14/V_MSEG_2024-08-14T14:48:27.3301829Z']

        for base_file_path in base_file_path_list:
            current_batch_df = self.spark.sql(f"""
                                        SELECT * 
                                            FROM SourceData 
                                            WHERE 
                                                startswith(file_path, '{base_file_path}') 
                                    """)
            current_batch_df.createOrReplaceTempView("CurrentSourceData")
        
            #TODO debug - to delete
            print("base_file_path:", base_file_path)
            
            # type 1 table
            update_query_1 = f"""
                MERGE INTO {target_table_1} AS target
                USING (SELECT * EXCEPT({control_columns}) FROM CurrentSourceData) as source
                ON {join_statement}

                WHEN MATCHED THEN
                UPDATE SET *
                
                WHEN NOT MATCHED THEN
                INSERT *             
            """
            audit_df = self.spark.sql(update_query_1)
            
            # # TODO: debug - to delete
            # #audit_df.show()

            self.audit.audit_log(tableName=target_table_1, operation="merge", audit_df=audit_df, base_file_path=base_file_path)

            # type 2 table
            data_columns = list(set(self._get_data_columns_as_list(batch_df.columns)) - set(keys))
            join_brute_force_statement = " OR ".join([f"target.{k} is distinct from source.{k}" for k in data_columns])
            all_columns_list = batch_df.columns
            all_columns_statement = ", ".join([k for k in all_columns_list])
            nc_columns_statement = ", ".join([f"NC.{k}" for k in keys])
            pk_columns_statement = ", ".join([k for k in keys])
            nc_join_statement = " AND ".join([f"NC.{k} = C.{k}" for k in keys])
            
            now = time.time()
            all_columns_values_statement = ", ".join([f"source.{k}" for k in all_columns_list])
            all_columns_values_statement = all_columns_values_statement.replace("source.is_current","1")
            all_columns_values_statement = all_columns_values_statement.replace("source.start_time",f"cast({now} as timestamp)")
            all_columns_values_statement = all_columns_values_statement.replace("source.end_time","NULL")
            
            # Handle tables without data_columns 
            brute_force_statement = ""
            if len(join_brute_force_statement) > 0:
                brute_force_statement = f""" 
                    WHEN MATCHED AND target.is_current = 1 AND ({join_brute_force_statement}) THEN
                    UPDATE SET target.is_current = 0, target.end_time = cast({now} as timestamp)
                """        

            update_query_2 = f"""
                    MERGE INTO {target_table_2} AS target
                    USING CurrentSourceData AS source
                    ON {join_statement} AND target.is_current = 1
                    
                    {brute_force_statement}
                    
                    WHEN NOT MATCHED THEN
                    INSERT ({all_columns_statement})
                    VALUES ({all_columns_values_statement})
                """
            #print(update_query_2)

            audit_df = self.spark.sql(update_query_2)
            self.audit.audit_log(tableName=target_table_2, operation="update", audit_df=audit_df, base_file_path=base_file_path)

            insert_updates_2 = f"""
                INSERT INTO {target_table_2} ({all_columns_statement})
                    SELECT {all_columns_values_statement}
                    FROM CurrentSourceData source
                    JOIN (
                    SELECT {nc_columns_statement}
                    FROM (SELECT {pk_columns_statement} FROM {target_table_2} WHERE is_current = 0 and end_time = cast({now} as timestamp)) NC 
                    LEFT ANTI JOIN (SELECT {pk_columns_statement} FROM {target_table_2} WHERE is_current = 1) C ON {nc_join_statement}
                    ) target ON {join_statement}
            """
            
            #print(insert_updates_2)
            audit_df = self.spark.sql(insert_updates_2)
            self.audit.audit_log(tableName=target_table_2, operation="insert", audit_df=audit_df, base_file_path=base_file_path)

            #delete
            if "delete_source_filepath" in self.config:
                date_from_base_file_path = "/".join(base_file_path.split("/")[-4:-1])
                delete_source_filepath = self.config["delete_source_filepath"] + "/" + date_from_base_file_path
                try:
                    delete_df = read_parquet(spark=self.spark, path=delete_source_filepath, reader_config={"recursiveFileLookup": "True"})
                    delete_df.createOrReplaceTempView("DeleteSourceData")
                    delete_update_query_1 = f"""
                        MERGE INTO {target_table_1} AS target
                        USING (SELECT * EXCEPT({control_columns}) FROM DeleteSourceData) AS source
                        ON {join_statement}

                        WHEN NOT MATCHED BY SOURCE THEN
                        DELETE
                    """
                    audit_df = self.spark.sql(delete_update_query_1)
                    self.audit.audit_log(tableName=target_table_1, operation="delete", audit_df=audit_df, base_file_path=base_file_path)

                    delete_update_query_2 = f"""
                        MERGE INTO {target_table_2} AS target
                        USING DeleteSourceData AS source
                        ON {join_statement} AND target.is_current = 1

                        WHEN NOT MATCHED BY SOURCE AND target.is_current = 1 THEN
                        UPDATE SET target.is_current = 0, target.end_time = cast({now} as timestamp),  delete_time = cast({now} as timestamp)
                    """
                    audit_df = self.spark.sql(delete_update_query_2)
                    self.audit.audit_log(tableName=target_table_2, operation="delete", audit_df=audit_df, base_file_path=base_file_path)
                    print(f"- The delete file {delete_source_filepath} was applied to tables {target_table_1} and {target_table_2}.")
                except Exception as e:
                    print(f"- There is no delete data in location {delete_source_filepath}.")

        # this is called inline optimization and it happens here because the streaming is continuous
        # whitout row-level concurrency enabled, merge and optmize could conflict
        # https://docs.databricks.com/en/optimizations/isolation-level.html#enable-row-level-concurrency
        self.spark.sql(f"optimize {target_table_1}")
        print(f"Optimizing {target_table_1}")
        self.spark.sql(f"optimize {target_table_2}")
        print(f"Optimizing {target_table_2}")


    def _get_data_columns_as_list(self, df_column_list):
        return list(set(df_column_list) - set(self.control_columns_list))
    

    def _upsert_to_delta_streaming_fe(self, batch_df, batch_id, keys, source_orderBy_column, target_table, scd_type):
        over_partition_columns = ", ".join([ k for k in keys])
        control_columns = ", ".join([ k for k in self.control_columns_list])
        batch_df = batch_df.orderBy([self.config["source_orderBy_column"]], ascending = [True])
        join_statement = " AND ".join([f"target.{k} = source.{k}" for k in keys])

        if scd_type == 1:
            batch_df.createOrReplaceTempView("updates")

            update_query = f"""
                SELECT * EXCEPT({control_columns}, rn) from (
                    SELECT *,
                        ROW_NUMBER() OVER(PARTITION BY {over_partition_columns} ORDER BY {source_orderBy_column} DESC) as rn
                FROM updates
                ) where is_current = 1 and rn = 1
            """
            audit_df = batch_df.sparkSession.sql(update_query).writeTo(target_table).createOrReplace()
            self.audit.audit_log(tableName=target_table, operation="upsert", audit_df=audit_df, base_file_path="")

        elif scd_type == 2:
            data_columns = self._get_data_columns_as_list(batch_df.columns)
            join_brute_force_statement = " AND ".join([f"target.{k} = source.{k}" for k in data_columns])

            left_join_nulls_pkey_list = " AND ".join([f"source.{k} is null" for k in keys])

            
            batch_df.createOrReplaceTempView('SourceData')
            update_query = f"""
                WITH 
                UpdatedRows AS (
                    SELECT target.* EXCEPT(end_time, is_current, delete_time),
                    current_timestamp() AS end_time, 
                    0 AS is_current, 
                    null AS delete_time  
                FROM SourceData as source
                    JOIN {target_table} as target
                    ON {join_statement}
                    AND target.is_current = 1
                ),
                
                DeletedRows AS (
                    SELECT target.* EXCEPT(end_time, is_current, delete_time), 
                        current_timestamp() AS end_time, 
                        0 AS is_current, 
                        current_timestamp() AS delete_time 
                    FROM {target_table} AS target
                    LEFT JOIN SourceData as source
                        ON {join_statement}
                        WHERE 
                            {left_join_nulls_pkey_list}
                            AND
                            target.is_current = 1
                ),

                SourceDataUpdated AS (
                    SELECT * except(is_current, end_time, delete_time),
                        ROW_NUMBER() OVER (PARTITION BY {over_partition_columns} ORDER BY {source_orderBy_column} DESC) AS rn,
                        CASE
                            WHEN rn != 1 THEN current_timestamp() 
                            ELSE null
                        END AS end_time,
                        CASE 
                            WHEN rn != 1 THEN 0 
                            ELSE 1 
                        END AS is_current,
                        null AS delete_time
                FROM SourceData
                )
                
                MERGE INTO {target_table} AS target
                USING
                
                ( SELECT * EXCEPT(rn) FROM SourceDataUpdated
                
                UNION ALL
                    SELECT * FROM UpdatedRows

                UNION ALL
                    SELECT * FROM DeletedRows
                
                ) AS source
                    ON {join_brute_force_statement}
                    AND target.end_time is null 
                    AND target.is_current = 1
    
                WHEN MATCHED 
                THEN
                    UPDATE SET
                        target.is_current = source.is_current,
                        target.end_time = source.end_time,
                        target.delete_time = source.delete_time
                WHEN NOT MATCHED 
                THEN
                    INSERT *
            """
            
            audit_df = batch_df.sparkSession.sql(update_query)
            self.audit.audit_log(tableName=target_table, operation="upsert", audit_df=audit_df, base_file_path="")

        else:
            raise Exception("Missing scd_type in function _upsert_to_delta")

        if batch_id % 101 == 0:
            # this is called inline optimization and it happens here because the streaming is continuous
            # whitout row-level concurrency enabled, merge and optmize could conflict
            # https://docs.databricks.com/en/optimizations/isolation-level.html#enable-row-level-concurrency
            audit_df = self.spark.sql(f"optimize {target_table}")
            self.audit.audit_log(tableName=target_table, operation="merge", audit_df=audit_df)
            print(f"Optimizing {target_table} based on batch_id")

    
    def _upsert_to_delta_streaming_ie(self, batch_df, batch_id, keys, source_orderBy_column, target_table, scd_type):
        over_partition_columns = ", ".join([ k for k in keys])
        control_columns = ", ".join([ k for k in self.control_columns_list])
        batch_df = batch_df.orderBy([self.config["source_orderBy_column"]], ascending = [True])
        join_statement = " AND ".join([f"target.{k} = source.{k}" for k in keys])

        if scd_type == 1:
            data_columns = self._get_data_columns_as_list(batch_df.columns)
            join_brute_force_statement = " AND ".join([f"target.{k} = source.{k}" for k in data_columns])

            left_join_nulls_pkey_list = " AND ".join([f"source.{k} is null" for k in keys])

            
            batch_df.createOrReplaceTempView('SourceData')
            update_query = f"""
                MERGE INTO {target_table} AS target
                USING
                
                ( SELECT * EXCEPT({control_columns}) FROM SourceData
                ) AS source
                    ON {join_statement}
                WHEN MATCHED 
                THEN
                    UPDATE SET *

                WHEN NOT MATCHED 
                THEN
                    INSERT *
            """
            
            audit_df = batch_df.sparkSession.sql(update_query)
            self.audit.audit_log(tableName=target_table, operation="upsert", audit_df=audit_df, base_file_path="")

        elif scd_type == 2:
            data_columns = self._get_data_columns_as_list(batch_df.columns)
            join_brute_force_statement = " AND ".join([f"target.{k} = source.{k}" for k in data_columns])

            left_join_nulls_pkey_list = " AND ".join([f"source.{k} is null" for k in keys])

            
            batch_df.createOrReplaceTempView('SourceData')
            update_query = f"""
                WITH 
                UpdatedRows AS (
                    SELECT target.* EXCEPT(end_time, is_current, delete_time),
                    current_timestamp() AS end_time, 
                    0 AS is_current, 
                    null AS delete_time  
                FROM SourceData as source
                    JOIN {target_table} as target
                    ON {join_statement}
                    AND target.is_current = 1
                ),
                
                DeletedRows AS (
                    SELECT target.* EXCEPT(end_time, is_current, delete_time), 
                        current_timestamp() AS end_time, 
                        0 AS is_current, 
                        current_timestamp() AS delete_time 
                    FROM {target_table} AS target
                    LEFT JOIN SourceData as source
                        ON {join_statement}
                        WHERE 
                            {left_join_nulls_pkey_list}
                            AND
                            target.is_current = 1
                ),

                SourceDataUpdated AS (
                    SELECT * except(is_current, end_time, delete_time),
                        ROW_NUMBER() OVER (PARTITION BY {over_partition_columns} ORDER BY {source_orderBy_column} DESC) AS rn,
                        CASE
                            WHEN rn != 1 THEN current_timestamp() 
                            ELSE null
                        END AS end_time,
                        CASE 
                            WHEN rn != 1 THEN 0 
                            ELSE 1 
                        END AS is_current,
                        null AS delete_time
                FROM SourceData
                )
                
                MERGE INTO {target_table} AS target
                USING
                
                ( SELECT * EXCEPT(rn) FROM SourceDataUpdated
                
                UNION ALL
                    SELECT * FROM UpdatedRows

                UNION ALL
                    SELECT * FROM DeletedRows
                
                ) AS source
                    ON {join_brute_force_statement}
                    AND target.end_time is null 
                    AND target.is_current = 1
    
                WHEN MATCHED 
                THEN
                    UPDATE SET
                        target.is_current = source.is_current,
                        target.end_time = source.end_time,
                        target.delete_time = source.delete_time
                WHEN NOT MATCHED 
                THEN
                    INSERT *
            """
            
            audit_df = batch_df.sparkSession.sql(update_query)
            self.audit.audit_log(tableName=target_table, operation="upsert", audit_df=audit_df, base_file_path="")

        else:
            raise Exception("Missing scd_type in function _upsert_to_delta")

        if batch_id % 101 == 0:
            # this is called inline optimization and it happens here because the streaming is continuous
            # whitout row-level concurrency enabled, merge and optmize could conflict
            # https://docs.databricks.com/en/optimizations/isolation-level.html#enable-row-level-concurrency
            audit_df = self.spark.sql(f"optimize {target_table}")
            self.audit.audit_log(tableName=target_table, operation="merge", audit_df=audit_df, base_file_path="")
            print(f"Optimizing {target_table} based on batch_id")


    def _upsert_to_delta_streaming_ie_bronze(self, batch_df, batch_id, keys, source_orderBy_column, target_table, scd_type):
        batch_df = batch_df.orderBy([self.config["source_orderBy_column"]], ascending = [True])
        join_statement = " AND ".join([f"target.{k} = source.{k}" for k in keys])
        
        batch_df.createOrReplaceTempView('SourceData')
        merge_query = f"""
            MERGE INTO {target_table} AS target
            USING
            ( 
                SELECT *  FROM SourceData
            ) AS source
                ON {join_statement}
            WHEN MATCHED 
            THEN
                UPDATE SET *
            WHEN NOT MATCHED 
            THEN
                INSERT *
        """
        audit_df = batch_df.sparkSession.sql(merge_query)
        self.audit.audit_log(tableName=target_table, operation="upsert", audit_df=audit_df, base_file_path="")

        if batch_id % 101 == 0:
            # this is called inline optimization and it happens here because the streaming is continuous
            # whitout row-level concurrency enabled, merge and optmize could conflict
            # https://docs.databricks.com/en/optimizations/isolation-level.html#enable-row-level-concurrency
            audit_df = self.spark.sql(f"optimize {target_table}")
            self.audit.audit_log(tableName=target_table, operation="optimize", audit_df=audit_df, base_file_path="")

            print(f"Optimizing {target_table} based on batch_id")


    def streaming_merge_writer(self, df, writer_config, table_type):
        catalog =  writer_config["catalog"]
        schema =  writer_config["schema"]
        table =  writer_config["table"]
        source_table_name = self.config["table_name"]
        target_table = f"{catalog}.{schema}.{table}"
        keys = self.config["keys"]
        source_orderBy_column = self.config["source_orderBy_column"]
        table_medallion_layer = writer_config["table_medallion_layer"] if "table_medallion_layer" in writer_config else ""

        
        if "scd_type" in writer_config:
            scd_type = int(writer_config["scd_type"])
        else:
            scd_type = 1
        
        upsert_data_udf = None
        
        if self.config["source_extraction_type"].lower() == "fe":

            if not self._table_exists(target_table):
                self._create_table_by_scd_type(df, target_table, scd_type, writer_config)
        
                upsert_data_udf = lambda batch_df, batch_id: self._first_insert_to_delta(batch_df=batch_df, batch_id=batch_id, keys=keys, source_orderBy_column=source_orderBy_column, target_table=target_table, scd_type=scd_type)

                print(f"""Streaming writer for source table {source_table_name}: Creating table {target_table}""")    
                # TODO: Enable withWatermark later
                #input_stream_with_watermark = df.withWatermark(source_orderBy_column, "10 minutes")
            else:
                print(f"""Streaming writer for source table {source_table_name}: Merge data to table {target_table}""")    
                upsert_data_udf = lambda batch_df, batch_id: self._upsert_to_delta_streaming_fe(batch_df=batch_df, batch_id=batch_id, keys=keys, source_orderBy_column=source_orderBy_column, target_table=target_table, scd_type=scd_type)
        
        elif self.config["source_extraction_type"].lower() == "ie" and table_medallion_layer.lower() == "bronze":
            
            # scd_type must always be 2 for bronze tables.
            scd_type = 2
            if not self._table_exists(target_table):
                self._create_table_by_scd_type(df, target_table, scd_type, writer_config)
            
            upsert_data_udf = lambda batch_df, batch_id: self._upsert_to_delta_streaming_ie_bronze(batch_df=batch_df, batch_id=batch_id, keys=keys, source_orderBy_column=source_orderBy_column, target_table=target_table, scd_type=scd_type)
        
        elif self.config["source_extraction_type"].lower() == "ie":
            if not self._table_exists(target_table):
                self._create_table_by_scd_type(df, target_table, scd_type, writer_config)
                
                print(f"""Streaming writer for source table {source_table_name}: Creating table {target_table} (Incremental Extract)""")    
                upsert_data_udf = lambda batch_df, batch_id: self._first_insert_to_delta(batch_df=batch_df, batch_id=batch_id, keys=keys, source_orderBy_column=source_orderBy_column, target_table=target_table, scd_type=scd_type)
            else:
                print(f"""Streaming writer for source table {source_table_name}: Merge data to table {target_table} (Incremental Extract)""")    

                upsert_data_udf = lambda batch_df, batch_id: self._upsert_to_delta_streaming_ie(batch_df=batch_df, batch_id=batch_id, keys=keys, source_orderBy_column=source_orderBy_column, target_table=target_table, scd_type=scd_type)

        else:
            raise Exception("source_extraction_type not implemented yet")

        query = (
            df.writeStream
            .format(table_type)
            .trigger(availableNow=True)
            .foreachBatch(upsert_data_udf)
            .option("checkpointLocation", writer_config["checkpointLocation"])
            .start()
        )
        query.awaitTermination()
    

    def batch_merge_write(self, df, writer_config, table_type):
        catalog =  writer_config["catalog"]
        schema =  writer_config["schema"]
        table =  writer_config["table"]
        source_table_name = self.config["table_name"]
        target_table = f"{catalog}.{schema}.{table}"
        target_table_type = writer_config["data_type"]
        keys = self.config["keys"]
        source_orderBy_column = self.config["source_orderBy_column"]
        
        if "scd_type" in writer_config:
            scd_type = int(writer_config["scd_type"])
        else:
            scd_type = 1
        
        if not self._table_exists(target_table):
            self._create_table_by_scd_type(df, target_table, scd_type, writer_config)
            print(f"""Batch writer for source table {source_table_name}: Creating table {target_table}""")    
        else:
            print(f"""Batch writer for source table {source_table_name}: Merge data to table {target_table}""")    
        
        if self.config["source_extraction_type"].lower() == "fe":
            self._upsert_to_delta_batch_fe(batch_df=df, batch_id=0, keys=keys, source_orderBy_column=source_orderBy_column, target_table=target_table, scd_type=scd_type, target_table_type=target_table_type)
        # elif self.config["source_extraction_type"].lower() == "ie":
        #     self._upsert_to_delta_batch_ie(batch_df=df, batch_id=0, keys=keys, source_orderBy_column=source_orderBy_column, target_table=target_table, scd_type=scd_type, target_table_type=target_table_type)
        else:
            raise Exception("Non supported 'source_extraction_type' in config. It must be 'FE' or 'IE'.")

    def batch_merge_write_ie(self, df, writer_config_list):
        source_orderBy_column = self.config["source_orderBy_column"]
        target_table_1 = ""
        target_table_2 = ""
        target_table_type_1 = ""
        target_table_type_2 = ""
        keys = self.config["keys"]
        source_table_name = self.config["table_name"]

        for writer_config in writer_config_list:
            if "scd_type" in writer_config:
                scd_type = int(writer_config["scd_type"])
            else:
                raise Exception("Missing scd_type in 'writes' section.")

            catalog =  writer_config["catalog"]
            schema =  writer_config["schema"]
            table =  writer_config["table"]
            target_table = f"{catalog}.{schema}.{table}"
            target_table_type = writer_config["data_type"]
            
            if not self._table_exists(target_table):
                self._create_table_by_scd_type(df, target_table, scd_type, writer_config)
                print(f"""Batch writer for source table {source_table_name}: Creating table {target_table}""")    
            else:
                print(f"""Batch writer for source table {source_table_name}: Merge data to table {target_table}""")
        
            if scd_type == 1:
                target_table_1 = target_table
                target_table_type_1 = target_table_type
            elif scd_type == 2:
                target_table_2 = target_table
                target_table_type_2 = target_table_type    
        
        self._upsert_to_delta_batch_ie(batch_df=df, keys=keys, source_orderBy_column=source_orderBy_column, target_table_1=target_table_1, target_table_2=target_table_2, target_table_type_1=target_table_type_1, target_table_type_2=target_table_type_2)
        
    
    # TODO: Review
    def streaming_write_file_overwrite(self, df, writer_config, table_type):
        catalog =  writer_config["catalog"]
        schema =  writer_config["schema"]
        table =  writer_config["table"]
        source_table_name = self.config["table_name"]
        target_table = f"{catalog}.{schema}.{table}"
        
        if "scd_type" in writer_config:
            scd_type = int(writer_config["scd_type"])
        else:
            scd_type = 1
        
        if scd_type == 1:
            df = df.drop(*self.control_columns_list)
        
        #upsert_data_udf = lambda batch_df, batch_id: self._overwrite_delta(batch_df=batch_df, batch_id=batch_id, keys=keys, source_orderBy_column=source_orderBy_column, target_table=target_table, scd_type=scd_type)
        
        if not self._table_exists(target_table):
            self._create_table_by_scd_type(df, target_table, scd_type, writer_config)
    
            print(f"""Streaming writer for source table {source_table_name}: Creating table {target_table}""")    
        
        else:
            print(f"""Streaming writer for source table {source_table_name}: Overwriting table {target_table}""")    

        query = (
            df.write
            .format(table_type)
            #.trigger(availableNow=True)
            #.outputMode("overwrite")
            #.foreachBatch(upsert_data_udf)
            .option("checkpointLocation", writer_config["checkpointLocation"])
            #.start()
            .writeTo(target_table)
            .createOrReplace()
        )
        query.awaitTermination()


    def write(self, df):
        """
        Write a dataframe to a target location
        :param df:
        :return:
        """
        c = self.config.copy()

        print(c)
        streaming = False
        if "streaming" in c:
            if c["streaming"].lower() == "true":
                streaming = True
                print("Creating Streaming writer...")

        if not ("writes" in c):
            raise ValueError("writes must be specified in the config")
        if not isinstance(c["writes"], list):
            raise ValueError("writes must be a list")
        
        # Medallion compatibility
        if "medallion_step" not in self.config:
            self.config["medallion_step"] = ""

        if (self.config["source_extraction_type"].lower() == "fe") or (self.config["medallion_step"] == "bronze"):
            for wc in c["writes"]:
                if "target_table_type" not in wc:
                    table_type = "delta"
                else:
                    table_type = wc["target_table_type"]
                print(f"- Writing to table type: {table_type}")

                if not 'mode' in wc:
                    write_mode = "append"
                else:
                    write_mode = wc["mode"]

                if not write_mode in ["overwrite", "append", "merge"]:
                    raise ValueError("mode must be one of 'overwrite', 'append', or 'merge'")

                if (streaming and write_mode=="append" and 
                    table_type=="delta" and 
                    self.config["data_product_name"].lower()=="sap_cdc"):

                    streaming_write_table_append(self.spark,df,c["writes"])
            
                if streaming and self.config["source_workload_type"].lower()=="non_cdc":
                    if write_mode == "merge":
                        self.streaming_merge_writer(df, wc, table_type)
                    elif write_mode == "overwrite":
                        raise Exception("MODE yet to develop")
                        # self.streaming_write_file_overwrite(df, wc, table_type)
                    elif write_mode == "append":
                        self.streaming_append_writer(df, wc, table_type)
            
                elif not streaming and self.config["source_workload_type"].lower()=="non_cdc":
                    if write_mode == "merge":
                        self.batch_merge_write(df, wc, table_type)
                    #elif write_mode == "overwrite":
                    #    overwrite_write(df, wc, table_type)
                    # elif write_mode == "append":
                    #    self.append_write(df, wc, table_type)
        elif self.config["source_extraction_type"].lower() == "ie":
            if not streaming and self.config["source_workload_type"].lower()=="non_cdc":
                if self._writer_modes_check( self.config["writes"],"merge"):
                    self.batch_merge_write_ie(df, c["writes"])
            else:
                raise Exception("Non supported working modes. Check 'source_workload_type' when 'source_extraction_type' == IE.")

    def _writer_modes_check(self, writer_config_list, selected_mode):
        for item in writer_config_list:
            if item.get("mode").lower() != selected_mode.lower():
                return False
        return True

    def simple_append_write_to_delta(self, df, target_table, external_location=None):
    
        if not self._table_exists(target_table):
            self._create_table_by_scd_type(df, target_table, 2, {"external_location": external_location})
            print(f"""Creating table {target_table}""")    
        else:
            print(f"""Appending to table {target_table}""")
        
        df.write.format("delta").mode("append").saveAsTable(target_table)
    
    
    def streaming_append_writer(self, df, writer_config, table_type):
        catalog =  writer_config["catalog"]
        schema =  writer_config["schema"]
        table =  writer_config["table"]
        source_table_name = self.config["table_name"]
        target_table = f"{catalog}.{schema}.{table}"
        table_medallion_layer = writer_config["table_medallion_layer"] if "table_medallion_layer" in writer_config else ""
       
        scd_type = 2
        
        if not self._table_exists(target_table):
            self._create_table_by_scd_type(df, target_table, scd_type, writer_config)
            print(f"""Streaming writer for source table {source_table_name}: Creating {table_medallion_layer} table {target_table}""")    
        else:
            print(f"""Streaming writer for source table {source_table_name}: Appending to {table_medallion_layer} table {target_table}""")    

        query = (
            df.writeStream
            .format(table_type)
            .format("delta")
            .outputMode("append")
            .trigger(availableNow=True)
            .option("checkpointLocation", writer_config["checkpointLocation"])
            .toTable(target_table) 
        )
        query.awaitTermination()

        if self.config["audit_write"].lower() == "true" and "bronze_row_creation_time" in self.config:
            row_count = read_uc_table(table_name=target_table, row_creation_time=self.config["bronze_row_creation_time"], spark=self.spark).count()
            audit_df = self.spark.createDataFrame([{"num_affected_rows":row_count,"num_inserted_rows":row_count}])
            self.audit.audit_log(tableName=target_table, operation="append", audit_df=audit_df, base_file_path="")

