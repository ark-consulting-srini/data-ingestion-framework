from pyspark.sql import functions as F



def _table_exists(spark, table_name):
    return spark.catalog.tableExists(table_name)

def _batch_write(df, config, table_type, mode):
    if not isinstance(config, dict):
        raise ValueError("config must be a dictionary")
    if not isinstance(table_type, str):
        raise ValueError("table_type must be a string")
    target_mode = "table"
    if 'catalog' in config:
        target_table = f"{config['catalog']}.{config['schema']}.{config['table']}"
        target_mode = "table"
    elif 'database' in config:
        target_table = f"{config['database']}.{config['table']}"
        target_mode = "table"
    else:
        target_table = config['target_filepath']
        target_mode = "path"

    if not (table_type in ["delta", "parquet", "csv"]):
        raise ValueError("table_type must be one of 'delta', 'parquet', or 'csv'")

    if not ("target_write_options" in config):
        config["target_write_options"] = {}

    print(f"Writing to table: {target_table}, mode: {mode}")

    if target_mode == "table":
        (df
        .write
        .format(table_type)
        .mode(mode)
         #.options(**config['target_write_options'])
        .saveAsTable(target_table)
         )

    else:
        (df
        .write
        .format(table_type)
        .mode(mode)
        #.options(**config['target_write_options'])
        .save(target_table)
        )


def overwrite_write(df, config, table_type):
    _batch_write(df, config, table_type, "overwrite")


def append_write(df, config, table_type):
    _batch_write(df, config, table_type, "append")


def merge_write(spark, df, config, table_type, verbose=False, target_table=None):
    
    if not isinstance(config, dict):
        raise ValueError("config must be a dictionary")
    if not isinstance(table_type, str):
        raise ValueError("table_type must be a string")
    if not "keys" in config:
        raise ValueError("keys must be specified in the config for merge")

    # spark = SparkSession.builder.getOrCreate()
    # spark = df.sparkSession
    
    if "timestamp_col" in config:
        df = df.withColumn(config["timestamp_col"], F.current_timestamp())

    # TODO: Check for keys
    keys = config["keys"]

    if not isinstance(keys, list):
        raise ValueError("Primary Keys must be a list")
    if len(keys) == 0:
        raise ValueError("Primary Keys must be specified")

    for k in keys:
        if not isinstance(k, str):
            raise ValueError("Primary Keys must be strings")
        # if not k.lower() in [col.lower() for col in df.columns]:
        #     raise ValueError(f"Key {k} not found in DataFrame")

    join_statement = " AND ".join([f"target.{k} = source.{k}" for k in keys])

    if 'except_column_list' in config:
        except_column_list = config["except_column_list"]
        if not isinstance(except_column_list, list):
            raise ValueError("Except Column List must be a list")
        if len(except_column_list) != 0:
            df = df.drop(*except_column_list)

    if target_table is None:
        if 'catalog' in config:
            target_table = f"{config['catalog']}.{config['schema']}.{config['table']}"
        elif 'target_database' in config:
            target_table = f"{config['target_database']}.{config['table']}"
        else:
            raise ValueError("table must be specified in the config")

    if "scd_type" in config:
        scd_type = int(config["scd_type"])
    else:
        scd_type = 1

    if not (scd_type in [1, 2]):
        raise ValueError("SCD Type must be 1 or 2")

    if scd_type == 2:
        df = df.withColumn("is_current", F.lit(1))
        df = df.withColumn("start_time", F.current_timestamp())
        df = df.withColumn("end_time", F.lit(None).cast("timestamp"))

    if not _table_exists(df.sparkSession, target_table):
        print(f"Table {target_table} does not exist, creating table...")
        overwrite_write(df, config, table_type)
        return None
    else:
        print("Table found")
        df.createOrReplaceTempView("updates")

    queries = []
    if scd_type == 1:
        merge_query = f"""
                    MERGE INTO {target_table} AS target
                    USING updates AS source
                    ON {join_statement}
                    WHEN MATCHED THEN
                        UPDATE SET *
                    WHEN NOT MATCHED THEN
                        INSERT *
                    """
        spark.sql(merge_query)
    elif scd_type == 2:
        over_patition_columns = ", ".join([ k for k in keys])
        update_query = f"""
        WITH PreprocessedSource AS (
            SELECT *,
                    ROW_NUMBER() OVER(PARTITION BY {over_patition_columns} ORDER BY file_modification_time DESC) as rn
            FROM updates
        )
        MERGE INTO {target_table} AS target
                    USING (SELECT * FROM PreprocessedSource WHERE rn = 1) AS source
                    ON {join_statement}
                    WHEN MATCHED THEN
                        UPDATE SET is_current = 0, 
                        end_time = current_timestamp()
        """

        queries.append(update_query)
        
        #debug TODO: To delete
        #print("SCD type 2 update queries:", queries)
        #df.writeTo("sandbox.dbx_demo.FSACTION_HISTORY_2").createOrReplace()
        
        df.sparkSession.sql(update_query)
        # TODO: Handle more than just tables
        df.write.format("delta").mode("append").saveAsTable(target_table)
