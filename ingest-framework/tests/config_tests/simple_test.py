from sparkforge import ConfigHandler
# TODO: to refactor 

config = {
    "data_product_name": "<data_product_name>",
    "source_data_type": "cloudFiles",
    "source_filepath": {"table": "<table>",
                        "col": "landing_file_path_name",
                        "key": "<key>",
                        "key_col": "table_name"
                        },
    "source_reader_options": {
        "header": "true",
        "inferSchema": "true",
        "cloudFiles.format": "json",
        "pathGlobFilter": {"table": "<table>",
                           "col": "landing_file_path_name",
                           "key": "<key>",
                           "key_col": "landing_file_name_format"
                           },
        "cloudFiles.schemaLocation": "/dbfs/mnt/landing/",
        "delimiter": ",",
    },
    "target_catalog": "dev",
    "target_schema": "raw",
    "target_table": "<target_table>",
    "target_data_type": "delta",
    "target_write": {
        "mode": "merge",
        "keys": [
            "<key>"
        ],
        "timestamp_col": "change_date",
        "scd_type": 1,
    },
    "transformations": [
        {
            "py": "rename_cols",
            "args": {
                "table_name": "<table>",
                "col_mapping_config_table": "dev.raw.col_mapping_config"
            }
        },
        {
            "py": "add_timestamp_cols",
        },
        {
            "py": "normalize_cols",
        },
    ]
}

def main():
    c = ConfigHandler(config)
    print(c)

if __name__ == "__main__":
    main()