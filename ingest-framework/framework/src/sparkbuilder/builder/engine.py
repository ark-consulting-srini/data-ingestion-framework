from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# TODO: to remove when build the wheel 
#import sys
#sys.path.append("../..")

from sparkbuilder.transformations.sql_transformation import run_sql_transformation
from sparkbuilder.transformations.python_transformation import run_py_transformation
from sparkbuilder.transformations.common_transformations import run_where_clause, run_select_clause, run_drop_columns, run_rename_columns, run_normalize_cols, rename_and_cast_columns, column_names_to_lower, pyspark_function
from sparkbuilder.transformations.brute_force_comparison import run_brute_force_comparison
from sparkbuilder.readers.reader import Reader, read_uc_table
from sparkbuilder.writers.writer import Writer
from sparkbuilder.dq.dq import DataQuality


class PipelineBuilder(object):

    def __init__(self, spark, config, verbose=False, fncs=[]):
        
        if not isinstance(config, dict):
            raise ValueError("config must be a dictionary")
        self.config = config
        self.spark = spark
        self.transformations = []
        c = self.config
        if "transformations" in c:
            self.transformations = c["transformations"]
            if not isinstance(self.transformations, list):
                raise ValueError("transformations must be a list")
        else:
            self.transformations = []
        self.transfomation_queue = self.transformations.copy()
        self.verbose = verbose
        self.step_counter = 0
        self.transformation_mapping = {
            "sql": run_sql_transformation,
            "py": run_py_transformation,
            "where": run_where_clause,
            "select": run_select_clause,
            "drop": run_drop_columns,
            "rename": run_rename_columns,
            "normalize_cols": run_normalize_cols,
            "brute_force_subtract": run_brute_force_comparison,
            "rename_and_cast_columns": rename_and_cast_columns
        }
        self.fncs = fncs
    
    
    def datatype_conversion(self,df):
     
        # helps to convert string data type to the data types in the config table column: cast_column 
        if len(self.config["cast_column"]) > 0:
           
            for column_name, data_type in self.config["cast_column"][0].items():
                # change column data type 
                try:
                    df = df.withColumn(column_name, F.col(column_name).cast(data_type))
                except Exception as e:
                    print(f"Exception: Datatype conversion Error in sparkbuilder.builder.engine.py => datatype_conversion(). Make sure String value conforms to the datatype: {column_name}.{data_type}")
                    print(str(e))
                    raise    
            return df    
        else:     
            # no data type conversions   
            return df


    def add_transformation(self, transformation):
        if not isinstance(transformation, dict):
            raise ValueError("transformation must be a dictionary")

        self.transfomation_queue.append(transformation)


    def add_transformations(self, transformations):
        if not isinstance(transformations, list):
            raise ValueError("transformations must be a list")

        self.transfomation_queue += transformations


    def __len__(self):
        return len(self.transfomation_queue)


    def __iter__(self):
        return self.transfomation_queue.__iter__()


    def read(self):
        
        r = Reader(self.spark, self.config)
        df, row_creation_time = r.read()
        return df, row_creation_time


    def write(self, df):
        w = Writer(self.spark, self.config)
        w.write(df)


    def step(self, df):
        if len(self.transfomation_queue) == 0:
            return df
        else:
            t = self.transfomation_queue.pop(0)
            if 'id' in t:
                step_id = t['id']
            else:
                step_id = f"step_{self.step_counter}"
            if not isinstance(t, dict):
                raise ValueError("Each transformation must be a dictionary in JSON format")
            if 'py' in t.keys():
                if 'args' in t.keys():
                    args = t['args']
                    df = run_py_transformation(df, t['py'], args=args, fncs=self.fncs)
                else:
                    df = run_py_transformation(df, t['py'], fncs=self.fncs)
            elif 'brute_force_subtract' in t.keys():
                df = run_brute_force_comparison(df, t['brute_force_subtract']["table"], t['brute_force_subtract']["pkeys"])
            elif 'normalize_cols' in t.keys():
                df = run_normalize_cols(df)
            elif "rename_and_cast_columns" in t.keys():
                df = rename_and_cast_columns(df=df, transform_config=t['rename_and_cast_columns'])
            elif "column_names_to_lower" in t.keys():
                df = column_names_to_lower(df=df)
            elif "pyspark_function" in t.keys():
                df = pyspark_function(df=df, transform_config=t['pyspark_function'] )
            else:
                df = self.transformation_mapping[t['type']](df, t, self.config)
        self.step_counter += 1
        return df, step_id


    def run(self, df):
        step_ids = []
        while len(self.transfomation_queue) > 0:
            df, step_id = self.step(df)
            step_ids.append(step_id)
        return df, step_ids

    def _medallion_config(self, full_config, table_medallion_layer):
        if not hasattr(self, 'full_config'):
            self.full_config = full_config.copy()

        wc = []

        for w in full_config["writes"]:
            if "table_medallion_layer" in w:
                if w["table_medallion_layer"].lower() == table_medallion_layer:
                    wc.append(w.copy())
            else:
                raise Exception("table_medallion_layer property must be present in every writer configuration when running a medallion")
        
        if len(wc) == 0:
            raise Exception(f"The table_medallion_layer {table_medallion_layer} was not found in writers configurations. Possible values: bronze, silver")
            
        return wc


    def _run_bronze(self):
        df , row_creation_time = self.read()
        
        # bronze_row_creation_time helps to select non processed data from bronze table
        self.bronze_row_creation_time = row_creation_time
        self.config["bronze_row_creation_time"] = self.bronze_row_creation_time

        #TODO: debug
        # import pandas as pd
        # date_string = '2024-08-14T10:06:25.138+00:00'
        # parsed_datetime = pd.to_datetime(date_string)
        # self.bronze_row_creation_time = parsed_datetime.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
        # self.config["bronze_row_creation_time"] = self.bronze_row_creation_time
        # print("- row_creation_time:", row_creation_time, type(row_creation_time))
        
        wc_list = self._medallion_config(self.config, "bronze")
        self.config["writes"] = wc_list.copy()
        self.config["medallion_step"] = "bronze"
        
        self.write(df)
    
    def _run_silver(self):
        # Get bronze table name
        bronze_wc = self._medallion_config(self.config, "bronze")[0]
        catalog =  bronze_wc["catalog"]
        schema =  bronze_wc["schema"]
        table =  bronze_wc["table"]
        bronze_table_name = f"{catalog}.{schema}.{table}"
        
        #read bronze table
        df = read_uc_table(spark=self.spark, table_name=bronze_table_name, row_creation_time=self.bronze_row_creation_time)

        # get silver table config
        wc_list = self._medallion_config(self.full_config, "silver")
        self.config["medallion_step"] = "silver"
        
        # TODO: Simplify the pipeline config table
        # Setting the pipeline for batch writing with merge
        self.config["writes"] = wc_list.copy()
        self.config["streaming"] = "false"

        #Perform Transformations
        df, _ = self.run(df)

        #Check Data Quality (dq rules) on lastest records
        # TODO: DQ needs a for loop as well.
        if self.config["run_dq_rules"].lower() == "true":
            print("Data Quality checks (DQ) are enabled. Running DQ rules on latest records.")
            dq = DataQuality(self.spark, self.full_config)
            dq.run_dq_rules(df)

        # Enable delete
        if "source_extraction_type" in self.full_config and self.full_config["source_extraction_type"].lower() == "ie":
            for wc in wc_list:
                if "delete_source_filepath" in wc:
                    self.config["delete_source_filepath"] = wc["delete_source_filepath"]
                    break
        
        # Write data to Silver Layer
        self.write(df)


    def run_medallion(self):
        self._run_bronze()
        self._run_silver()
