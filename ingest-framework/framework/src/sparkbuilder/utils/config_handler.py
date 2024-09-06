import json
import os


class ConfigHandler(object):

    def __init__(self, config_path=None, config=None, default_config_cwd=".", spark=None):
        if config_path is None and config is None:
            raise ValueError("config_path or config must be provided")
        if config is None and config_path is not None:
            if not isinstance(config_path, str):
                raise ValueError("config_path must be a string")
        elif config is not None and config_path is None:
            if not isinstance(config, dict):
                raise ValueError("config must be a dictionary")
        self.config_path = config_path
        self.default_config_cwd = default_config_cwd
        if config is None:
            self.config = self._load_config(config_path)
        else:
            self.config = config
        self.spark = spark
        self._validate_config()
        self.config = self._read_table_configs(self.config)

    def _read_table_configs(self, config):
        for k in config.keys():
            v = config[k]
            if isinstance(v, dict):
                if "table" in v.keys():
                    if not "col" in v.keys():
                        raise ValueError("col must be specified for table config")
                    if not "key" in v.keys():
                        raise ValueError("key must be specified for table config")
                    if not "key_col" in v.keys():
                        raise ValueError("key_col must be specified for table config")
                    table = v["table"]
                    col = v["col"]
                    key = v["key"]
                    key_column = v["key_col"]
                    if not ',' in key_column:
                        config_val = self.spark.table(table).filter(col(key_column) == key).select(col).collect()[0][0]
                    else:
                        key_columns = key_column.split(',')
                        key_values = key.split(',')
                        if len(key_columns) != len(key_values):
                            raise ValueError("key and key_col must have the same number of elements")
                        filter_clause = " and ".join([f"{k} = '{v}'" for k, v in zip(key_columns, key_values)])
                        config_val = self.spark.table(table).filter(filter_clause).select(col).collect()[0][0]
                    config[k] = config_val
            elif isinstance(v, dict):
                config[k] = self._read_table_configs(config[k])
        return config

    def _load_config(self, config_path):
        if not os.path.exists(config_path):
            full_config_path = os.path.join(os.path.abspath(self.default_config_cwd), config_path)
            if not os.path.exists(full_config_path):
                raise ValueError(f"config_path {full_config_path} does not exist")
        else:
            full_config_path = config_path
        with open(full_config_path, 'r') as f:
            return json.load(f)

    def _validate_config(self):
        valid_keys = [
            "data_product_name",
            "source_filepath",
            "source_data_type",
            "source_database",
            "source_catalog",
            "source_schema",
            "source_table",
            "source_reader_options",
            "source_data_quality_expectations_json",
            "target_database",
            "target_catalog",
            "target_schema",
            "target_table",
            "target_data_type",
            "target_write",
            "target_write_options",
            "target_partition_columns",
            "transformations"
        ]
        # for key in self.config:
        #     if key not in valid_keys:
        #         raise ValueError(f"Invalid key {key} in config")

        if 'source_catalog' in self.config and 'source_database' in self.config:
            raise ValueError("source_catalog and source_database cannot be specified together")

        if 'target_catalog' in self.config and 'target_database' in self.config:
            raise ValueError("target_catalog and target_database cannot be specified together")


    def get_config(self):
        return self.config


    def get_source_table_name(self):
        c = self.config
        if 'source_catalog' in c:
            return f"{c['source_catalog']}.{c['source_schema']}.{c['source_table']}"
        elif 'source_database' in c:
            return f"{c['source_database']}.{c['source_table']}"
        else:
            return None

    def get_target_table_name(self):
        c = self.config
        if 'target_catalog' in c:
            return f"{c['target_catalog']}.{c['target_schema']}.{c['target_table']}"
        elif 'target_database' in c:
            return f"{c['target_database']}.{c['target_table']}"
        else:
            return None

    def get_taget_table_type(self):
        c = self.config
        if "target_table_type" not in c:
            return "delta"
        else:
            return c["target_table_type"]


def test():
    config = {
        "data_product_name": "sap_mara",
        "source_data_type": "cloudFiles",
        "source_filepath": {"table": "sandbox.soffer_tmp.control_raw",
                            "col": "landing_file_path_name",
                            "key": "sap_vbfa",
                            "key_col": "table_name"
                            },
        "source_reader_options": {
            "header": "true",
            "inferSchema": "true",
            "cloudFiles.format": "csv",
            "pathGlobFilter": "*.csv",
            "cloudFiles.schemaLocation": "/dbfs/mnt/landing/sap_mara/",
            "delimiter": ",",
        },
        "target_catalog": "dev",
        "target_schema": "raw",
        "target_table": "sap_mara",
        "target_data_type": "delta",
        "target_write": {
            "mode": "merge",
            "keys": [
                "material_number"
            ],
            "timestamp_col": "change_date",
            "scd_type": 1,
        },
        "transformations": [
            {
                "py": "add_timestamp_cols",
            },
            {
                "py": "col_rename",
            },
            {
                "py": "rename_cols",
                "args": {
                    "table_name": "sap_mara",
                    "col_mapping_config_table": "dev.raw.col_mapping_config"
                }
            }
        ]
    }
    ch = ConfigHandler(config=config)
    print(ch.get_config())