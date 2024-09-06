from pyspark.sql import functions as F

def run_brute_force_comparison(df1, table, pkeys):
    if df1.sparkSession.catalog.tableExists(table) is True:
        df2 = df1.sparkSession.table(table)
        non_pkeys = [col for col in df1.columns if col not in pkeys]
        for col in df1.columns:
            if col in pkeys:
                continue
            df2 = df2.withColumnRenamed(col, f"_{col}")
        
        joined_df = df1.join(df2, on=pkeys, how="outer")
        
        for col in non_pkeys:
            joined_df = joined_df.withColumn(f"{col}_diff", F.when(F.col(col) == F.col(f"_{col}"), 0).otherwise(1))
        
        joined_df = joined_df.withColumn("is_diff", F.expr("coalesce(" + ", ".join([f"{col}_diff" for col in non_pkeys]) + ")"))
        res_df = (joined_df
                  .filter(F.col("is_diff") == 1)
                  .drop("is_diff")
                  .drop(*[f"{col}_diff" for col in non_pkeys])
                  )
        return res_df
    else:
        return df1
    
    


