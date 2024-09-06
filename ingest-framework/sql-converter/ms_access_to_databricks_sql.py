import sqlparse
from sqlparse.sql import Identifier, IdentifierList
from sqlparse.tokens import Keyword, DML
import re


def to_databricks_sql(sql_statement, replace_dict):
    """
    Convert a Microsoft Access SQL statement to Databricks SQL and 
    replace table names.
    """
    sql = replace_tables(sql_statement, replace_dict)
    sql = convert_sql_access_to_databricks(sql)
    return sql


def replace_tables(sql_statement, replace_dict):
    """
    Replace source tables with replace tables in the given SQL statement.

    Args:
    sql_statement (str): The original SQL statement.
    replace_dict (list): A list of dictionaries containing source and replace tables.

    Returns:
    str: The modified SQL statement with replaced table names.
    """
    modified_sql = sql_statement

    for replacement in replace_dict:
        source_table = replacement["source_table"]
        replace_table = replacement["replace_table"]

        # Use regex to find and replace the table names
        pattern = r'\b' + re.escape(source_table) + r'\b'
        modified_sql = re.sub(pattern, replace_table, modified_sql, flags=re.IGNORECASE)

    return modified_sql


def escape_identifiers(sql_statement):
    """
    Escapes identifiers (table and column names) containing spaces or hyphens.
    """
    parsed = sqlparse.parse(sql_statement)
    statement = parsed[0]

    for token in statement.tokens:
        if isinstance(token, IdentifierList):
            for identifier in token.get_identifiers():
                if ' ' in identifier.get_real_name() or '-' in identifier.get_real_name():
                    identifier.value = f'`{identifier.get_real_name()}`'
        elif isinstance(token, Identifier):
            if ' ' in token.get_real_name() or '-' in token.get_real_name():
                token.value = f'`{token.get_real_name()}`'
        elif token.ttype is Keyword and token.value.upper() == 'INTO':
            # Special handling for INTO <TABLE> clause
            idx = statement.token_index(token)
            next_token = statement.token_next(idx)
            if isinstance(next_token, Identifier):
                table_name = next_token.get_real_name().strip('[]')
                statement.tokens[idx+1].value = f'CREATE OR REPLACE TEMPORARY VIEW `{table_name}` AS'
                statement.tokens.remove(token)

    return str(statement)

def convert_sql_access_to_databricks(sql_statement):
    """
    Converts Microsoft Access SQL to Databricks SQL, including function conversions and adding 
    CREATE OR REPLACE TEMPORARY VIEW when the INTO <TABLE> clause is found.
    """
    if not isinstance(sql_statement, str):
        return sql_statement
    
    # Replace double quotes with single quotes
    sql_statement = sql_statement.replace('"', "'")
    
    # Access: IIf(condition, truePart, falsePart) -> Databricks: CASE WHEN condition THEN truePart ELSE falsePart END
    sql_statement = re.sub(r'IIf\(([^,]+),([^,]+),([^)]+)\)', r'CASE WHEN \1 THEN \2 ELSE \3 END', sql_statement)
    
    # Access: Nz(field, value_if_null) -> Databricks: COALESCE(field, value_if_null)
    sql_statement = re.sub(r'Nz\(([^,]+),([^)]+)\)', r'COALESCE(\1, \2)', sql_statement)
    
    # Access: Date() -> Databricks: CURRENT_DATE
    sql_statement = sql_statement.replace("Date()", "CURRENT_DATE")
    
    # Access: Now() -> Databricks: CURRENT_TIMESTAMP
    sql_statement = sql_statement.replace("Now()", "CURRENT_TIMESTAMP")
    
    # Access: Format(field, "format") -> Databricks: DATE_FORMAT(field, 'format')
    sql_statement = re.sub(r'Format\(([^,]+),\s*"([^"]+)"\)', r"DATE_FORMAT(\1, '\2')", sql_statement)
    
    # Access: Right(string, length) -> Databricks: RIGHT(string, length)
    sql_statement = re.sub(r'Right\(([^,]+),([^)]+)\)', r'RIGHT(\1, \2)', sql_statement)
    
    # Remove or replace square brackets [] commonly used in Access for field and table references
    sql_statement = re.sub(r'\[([^\]]+)\]', r'\1', sql_statement)
    sql_statement = re.sub(r'\*', '%', sql_statement)
    
    # Escape table and column names containing spaces or hyphens
    sql_statement = escape_identifiers(sql_statement)
    
    # Check if the INTO <TABLE> statement is present
    match = re.search(r'\bINTO\s+(\[?\w+[\s-]?\w+\]?)', sql_statement, re.IGNORECASE)
    if match:
        table_name = match.group(1).strip('[]')
        # Remove the INTO <TABLE> statement from the original SQL
        sql_statement = re.sub(r'\bINTO\s+\[?\w+[\s-]?\w+\]?', '', sql_statement, flags=re.IGNORECASE)
        # Add "CREATE OR REPLACE TEMPORARY VIEW <TABLE>" at the beginning of the SQL
        sql_statement = f"CREATE OR REPLACE TEMPORARY VIEW `{table_name}` AS\n" + sql_statement.strip()

    return sql_statement