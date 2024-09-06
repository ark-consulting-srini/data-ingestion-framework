from pyspark.sql import DataFrame


class MergeDataFrame(object):

    def __init__(self, df, keys):
        if not isinstance(df, DataFrame):
            raise ValueError("df must be a DataFrame")
        if not isinstance(keys, list):
            raise ValueError("keys must be a list")
        self.df = df
        self.keys = keys

    def is_valid_primary_key(self):
        # Check for nulls
        for col in self.keys:
            if self.df.filter(self.df[col].isNull()).count() > 0:
                return False
        if self.df.select(*self.keys).distinct().count() == self.df.count():
            return True

# CDC transaction object
class CDCTransactionDataFrame(MergeDataFrame):

    def __init__(self, df, id_cols, operation_col, timestamp_col=None, timestamp_format=None, metadata_cols=None):
        super().__init__(df, id_cols)
        if not isinstance(df, DataFrame):
            raise ValueError("df must be a DataFrame")
        if not isinstance(id_cols, list):
            raise ValueError("id_cols must be a list")
        if not isinstance(operation_col, str):
            raise ValueError("operation_col must be a string")
        if timestamp_col and not isinstance(timestamp_col, str):
            raise ValueError("timestamp_col must be a string")
        if timestamp_format and not isinstance(timestamp_format, str):
            raise ValueError("timestamp_format must be a string")
        if metadata_cols and not isinstance(metadata_cols, list):
            raise ValueError("metadata_cols must be a list")
        self.df = df
        self.id_cols = id_cols
        self.operation_col = operation_col
        self.timestamp_col = timestamp_col
        self.timestamp_format = timestamp_format
        self.non_val_cols = id_cols
        if operation_col:
            self.non_val_cols.append(operation_col)
        if timestamp_col:
            self.non_val_cols.append(timestamp_col)
        if metadata_cols:
            self.non_val_cols += metadata_cols
        self.val_cols = [x for x in df.columns if x not in id_cols + [operation_col, timestamp_col]]

    def get_inserts(self, insert_val="insert"):
        return self.df.filter(self.operation_col == insert_val)

    def get_updates(self, update_val="update"):
        return self.df.filter(self.operation_col == update_val)

    def get_deletes(self, delete_val="delete"):
        return self.df.filter(self.operation_col == delete_val)

    def get(self):
        return self.df


def handle_out_of_order_deletes(updates_df, pkeys, target_table, in_memory=True):
    # Get only the values of updates_df that are also in target_table
    # This is to prevent out-of-order deletes
    if in_memory:
        target_df = spark.table(target_table)
        # remove values from target df
        target_df = target_df.join(updates_df, on=pkeys, how='left_anti')
        target_df.write.mode('overwrite').saveAsTable(target_table)
    else:
        raise NotImplementedError("Only in-memory target table is supported at this time")






