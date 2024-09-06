from pyspark.sql import functions as F
from datetime import datetime

def create_streaming_reader(spark, config):
    
    row_creation_time = datetime.now()
    
    if 'source_table_type' not in config:
        source_type = 'delta'
    else:
        source_type = config['source_table_type']
    print(f"source type: {source_type}")
    if source_type.lower() == 'cloudfiles':
        source_type = 'cloudFiles'
        is_autoloader = True
    else:
        is_autoloader = False

    valid_source_types = ['parquet', 'csv', 'json', 'orc', 'delta', 'cloudFiles']
    if not source_type in valid_source_types:
        raise ValueError(f"source_table_type must be one of {valid_source_types}")
     
    if not config['source_data_type'] in valid_source_types:
        raise ValueError(f"source_data_type must be one of {valid_source_types}")

    if 'source_reader_options' in config:
        reader_config = config['source_reader_options']
    if 'source_catalog' in config:
        type = 'uc'
    elif 'source_database' in config:
        type = 'hms'
    else:
        type = 'file'

    if type == 'uc':
        full_source_name = f"{config['source_catalog']}.{config['source_schema']}.{config['source_table']}"
    elif type == 'hms':
        full_source_name = f"{config['source_database']}.{config['source_table']}"
    else:
        if not 'source_filepath' in config:
            raise ValueError("source_filepath must be in config")
        full_source_name = config['source_filepath']
  
    source_schema = (spark
                     .read
                     .format(config['source_data_type'])
                     .option("recursiveFileLookup","true")
                     .load(full_source_name)
                     .limit(0)
                     .schema
                     )
 
    df = ((spark
                .readStream
                .format(source_type)
                .schema(source_schema)
                .options(**reader_config)
                .load(full_source_name)
                ).withColumn("row_creation_time", F.lit(row_creation_time))
                .withColumn("file_modification_time", F.expr("_metadata.file_modification_time"))
                .withColumn("file_path", F.expr("_metadata.file_path"))
                .withColumn("start_time", F.lit(None).cast("timestamp"))
                .withColumn("end_time",F.lit(None).cast("timestamp"))
                .withColumn("is_current", F.lit(None).cast("integer"))
                .withColumn("delete_time",F.lit(None).cast("timestamp"))
                )
    #replace "/" with "_" in the column names if exists
    stream_df=df.select([F.col(x).alias(x.replace('/', '_')) for x in df.columns])

    return stream_df, row_creation_time
