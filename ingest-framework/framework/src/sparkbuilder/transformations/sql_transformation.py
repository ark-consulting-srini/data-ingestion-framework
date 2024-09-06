import os


def run_sql_transformation(df,
                           sql,
                           config,
                           transformation_cwd=".",
                           verbose=False):
    """
    :param df:
    :param sql:
    :return:
    """
    if not isinstance(sql, str):
        raise ValueError("sql must be a string")
    if len(sql) == 0:
        raise ValueError("sql string cannot be empty")
    if not (' ' in sql and ('.txt' or '.sql') in sql):
        if not os.path.exists(sql):
            sql = os.path.join(os.path.abspath(transformation_cwd), sql)
            if not os.path.exists(sql):
                raise FileNotFoundError(f"File {sql} not found")
        with open(sql, 'r') as f:
            sql = f.read()
    keys = ["source_catalog", "source_schema", "source_table", "target_catalog", "target_schema", "target_table"]
    df.createOrReplaceTempView("DF_TEMP_VIEW")
    sql = sql.replace("{{table_view}}", "DF_TEMP_VIEW")
    for k in keys:
        sql = sql.replace("{{"+k+"}}", config[k])
    if verbose is True:
        print(sql)
    return df.sql(sql)