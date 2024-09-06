import os
import logging
import sys

# Configurar variável de ambiente
os.environ['SPARK_CONNECT_LOG_LEVEL'] = 'debug'

# Configuração básica do logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Cria um handler que escreve no sys.stdout
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.INFO)

# Cria um formatter e adiciona ao handler
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)

# Adiciona o handler ao logger
logger.addHandler(handler)

def my_function(row):
    logger.info(f"Processing row: {row}")
    # sua lógica aqui
    return row

if __name__ == "__main__":
    #Databricks Connect specific: To delete
    from databricks.connect import DatabricksSession
    spark = DatabricksSession.builder.serverless().getOrCreate()
    df = spark.createDataFrame([{"id": 1, "value": "a"}, {"id": 2, "value": "b"}])
    df.foreach(my_function)
