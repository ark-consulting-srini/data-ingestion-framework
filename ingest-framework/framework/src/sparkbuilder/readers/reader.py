
from sparkbuilder.readers.batch_readers import read_parquet, read_csv, read_json, read_orc, read_hms_table, read_uc_table, read_delta_path
from sparkbuilder.readers.streaming_readers import create_streaming_reader

class Reader(object):

    def __init__(self, spark, config):
        """

        :param config:
        """
        if not isinstance(config, dict):
            raise ValueError("config must be a dictionary")
        self.config = config
        self.spark = spark

    def read(self):
        """

        :return:
        """
       
        c = self.config
        if "source_table_type" not in c:
            table_type = "delta"
        else:
            table_type = c["source_table_type"]
        if table_type == "delta":
            if 'source_catalog' in c:
                self.type = "uc"
            elif 'source_database' in c:
                self.type = "hms"
            else:
                pass
        if "source_reader_options" in c:
            reader_config = c["source_reader_options"]
        else:
            reader_config = None

        streaming = False
        if "streaming" in c:
            if c["streaming"].lower() == "true":
                streaming = True
        if not streaming:
            print("Creating batch reader...")
            if table_type == "parquet":
                df = read_parquet(self.spark, c["source_filepath"], reader_config)
            elif table_type == "csv":
                df = read_csv(self.spark, c["source_filepath"], reader_config)
            elif table_type == "json":
                df = read_json(self.spark, c["source_filepath"], reader_config)
            elif table_type == "orc":
                df = read_orc(self.spark, c["source_filepath"], reader_config)
            elif table_type == "hms":
                full_table_name = f'{c["source_database"]}.{c["source_table"]}'
                df = read_hms_table(self.spark, full_table_name, reader_config)
            elif table_type == "uc":
                full_table_name = f'{c["source_catalog"]}.{c["source_schema"]}.{c["source_table"]}'
                df = read_uc_table(self.spark, full_table_name, reader_config)
            elif table_type == "delta":
                df = read_delta_path(self.spark, c["source_filepath"], reader_config)

   
        else:
            print("Creating streaming reader...")
            df , row_creation_time = create_streaming_reader(self.spark, c)
        
        return df, row_creation_time
