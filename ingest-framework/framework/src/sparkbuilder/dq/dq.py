# TODO: Document the DEPENDENCY of cuallee python module
# needs pip install cuallee==0.13.1 or the library installed in the cluster
from cuallee import Check, CheckLevel, Control # WARN:0, ERR: 1

import re
from pyspark.sql import functions as F
from pyspark.sql import types as T
from functools import reduce

from sparkbuilder.writers.writer import Writer


class DataQuality(object):

    def __init__(self, spark, config):
        self.config = config
        self.spark = spark
        self.dq_config_table = None
        self.table_name = self.config["table_name"]
        self.data_product_name = self.config["data_product_name"]
        self.check = Check(level=CheckLevel.WARNING, name=self.table_name)
        self.control = Control
        self.dq_rules_list = None
        self.writer = Writer(config=self.config, spark=self.spark)
        self.job_id = self.config["job_id"]
 

        if "dq_config" in self.config and self.config["dq_config"]["dq_config_table"] != "":
            if not self._table_exists(self.config["dq_config"]["dq_config_table"]):
                raise Exception(f"""The DQ config table ({self.config["dq_config_table"]}) does not exist""")
            else:
                self.dq_config_table = self.config["dq_config"]["dq_config_table"]
        else:
            raise Exception(f"""To enable Dq rules, you must setup the "dq_config_table" property in the config table. """)

        # load dq rules
        self.dq_rules_list = None
        self.dq_rules_list = self._get_dq_rules_from_db()


    def _table_exists(self, table_name):
        return self.spark.catalog.tableExists(table_name)
    

    def _dq_dynamic_metod_call(self, function_input, df):
        try:
            return eval(function_input) #Returns a Pyspark Dataframe
        except Exception as e:
            raise Exception(f"Error when executing DQ rule for table: {self.table_name}\n Please check the DQ rule: {function_input}\nException Output: {e}")

    
    def _dq_custom_sql_statement(self, function_sql_input):
        # DQ custom_sql has to return zero rows to PASS in the test
        df = self.spark.sql(function_sql_input).limit(1)
        if df.count() == 0:
            output_status = True
        else:
            output_status = False
        return output_status, df

    

    def _add_support_columns(self,df, base_file_path):
        return (
            df
            .withColumn("table_name", F.lit(self.table_name))
            .withColumn("data_product_name", F.lit(self.data_product_name))
            .withColumn("base_file_path", F.lit(base_file_path))
            .withColumn("job_id", F.lit(self.job_id))
            .drop("id")
        )


    def _validate_dq_rule(self, string):
        cleaned = string.lstrip().lstrip('(')
        first_word = cleaned.split()[1].lower()
        # in control only completeness is a valid checks
        return first_word in ['custom_sql', 'checks', 'control']
    

    def _get_dq_rules_from_db(self):
        # create rules for check module
        rules_df = (
            self.spark.read.table(self.dq_config_table).where(
                (F.lower(F.col("is_enabled")) == F.lit("true")) &
                (F.col("data_product_name") == F.lit(self.data_product_name)) &
                (F.col("table_name") == F.lit(self.config["table_name"]))
            )
        )
        
        check_rules_list = (rules_df
                    .where(F.lower(F.col("dq_module_check")).isin("check"))
                    .groupBy("table_name", "dq_module_check")
                    .agg(F.concat_ws("", 
                        F.lit("self."),
                        F.col("dq_module_check"), 
                        F.collect_list(F.concat_ws("",  
                            F.lit("."), 
                            F.col("check"), 
                            F.lit("("), 
                            F.col("check_column"), 
                            F.lit(")")
                        )
                    )
                    ,F.lit(".validate(df)")
                        ).alias("check_string")
                    ).select("check_string").collect())


        control_rules_list = (rules_df
                    .where(F.lower(F.col("dq_module_check")).isin("control"))
                    .withColumn("check_string", F.concat_ws("", 
                        F.lit("self."),
                        F.col("dq_module_check"), 
                        F.lit("."), 
                        F.col("check"), 
                        F.lit("(df)")
                        )
                    ).select("check_string").collect())
        
        return check_rules_list + control_rules_list
    
    
    def run_dq_rules(self, df):

        results_df_list = []
        df.createOrReplaceTempView("dqData")

        base_file_path_list = self.writer.get_base_file_path_list_from_table("dqData")

        for base_file_path in base_file_path_list:
            current_dq_df = self.spark.sql(f"""
                                            SELECT * 
                                                FROM dqData 
                                                WHERE 
                                                    startswith(file_path, '{base_file_path}') 
                                        """)
            print("- base_file_path:", base_file_path)
            print("- Number of rows for Data Quality check:", current_dq_df.count())
            for rule in self.dq_rules_list:
                results_df_list.append(self._dq_dynamic_metod_call(rule[0], current_dq_df))
            if len(results_df_list) > 0:    
                results_df = self._add_support_columns(self._unionAll(*results_df_list), base_file_path)

                external_location = None
                if "external_location" in self.config["dq_config"]:
                    external_location = self.config["dq_config"]["external_location"]
                self.writer.simple_append_write_to_delta(results_df, self.config["dq_config"]["dq_log_table"], external_location)
            else:
                print(f"- There is no Data Quality rule for the combination: data_product_name:{self.data_product_name},  table_name:{self.table_name}")

    
    def _unionAll(self, *dfs):
        return reduce(lambda df1, df2: df1.union(df2), dfs)

        