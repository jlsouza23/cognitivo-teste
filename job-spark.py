###############################
# Desenvolvido por João Souza #
###############################

import os
import sys
import json
import uuid
import boto3
import logging
import tempfile
from datetime import datetime
from tempfile import NamedTemporaryFile
from pyspark.sql.functions import col, lit, when, isnan
from pyspark.sql import *
from pyspark.sql.types import *

sys.stdout = open(sys.stdout.fileno(), mode='w', encoding='utf8', buffering=1)

SPARK_HOME = '/usr/lib/spark'
#Path for spark source folder
os.environ['SPARK_HOME'] = SPARK_HOME
#Append pyspark to Python Path
sys.path.append(SPARK_HOME + '/python')
sys.path.append(SPARK_HOME + '/python/lib/py4j-0.10.7-src.zip')

print(sys.path)

DEFAULT_LOG_FORMAT = '%(asctime)s - [%(levelname)8s] - %(threadName)24s - %(module)24s - %(funcName)24s - %(message)s'
DEFAULT_LOG_LEVEL = 'INFO'
LOG_LEVEL = os.getenv('LOG_LEVEL', DEFAULT_LOG_LEVEL)
LOG_FORMAT = os.getenv('LOG_FORMAT', DEFAULT_LOG_FORMAT)

# enable logging
logging.basicConfig(format=LOG_FORMAT, level=LOG_LEVEL)
logger = logging.getLogger(__name__)
logger.setLevel(LOG_LEVEL)

BUCKET_NAME = 's3_cognitivo'
TARGET_DATA_PATH = 's3://s3_cognitivo/output/result/'
SOURCE_DATA_PATH = 's3://s3_cognitivo/input/load.csv'
CONFIG_SCHEMA_PATH = 'config/types_mapping.json'
CATALOG_DATABASE_NAME = 'db_cognitivo'
TABLE_NAME = 'tb_result'

spark = SparkSession.builder.master('yarn').appName('teste-cognitivo') \
    .config("spark.sql.parquet.filterPushdown", "true") \
    .config("spark.sql.parquet.mergeSchema", "false") \
    .config('spark.files.maxPartitionBytes', 64 * 1024 * 1024) \
    .config('spark.io.compression.snappy.blockSize', 64 * 1024 * 1024) \
    .config('spark.rpc.message.maxSize', '2000') \
    .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2") \
    .config("spark.speculation", "false") \
    .enableHiveSupport() \
    .getOrCreate()

#Função que faz a tradução dos tipos do json para a tipagem do spark
def getColumnTypeObject(reqType):

    switcher = {
        "string": StringType,
        "timestamp": TimestampType,
        "double": DoubleType,
        "long": LongType,
        "bigint": LongType,
        "float": FloatType,
        "integer": IntegerType
    }
    try:
        func = switcher.get(reqType, lambda: None)
    except:
        logger.info('Invalid column type') 
                
    return func()

#Função que cria o schema com base no json de configuração
def getSchema(schemaDescription):
    logger.info('Create schema from json')
    schema = StructType()
    try:
        for key, value in schemaDescription.items():
            schema.add(key, getColumnTypeObject(value))
    except:
        logger.info('Create schema failed') 

    return schema

#Função que cria um df a partir de um CSV
def create_dataframe_from_csv(source_data_path):
    logger.info('Loading data from {}'.format(source_data_path))

    try:
        df = spark.read.format("csv") \
                    .options(header='true', inferSchema='true')\
                    .load(source_data_path)
    except:
        logger.info('Create Dataframe from csv failed') 

    return df

#Função que cria um df a partir de um CSV
def create_dataframe_from_parquet(source_data_path, schema):
    logger.info('Loading data from {}'.format(source_data_path))

    try:
        df = spark.read.format("parquet") \
                    .schema(schema) \
                    .option("timestampFormat", "yyyy/MM/dd HH:mm:ss") \
                    .load(source_data_path)
    except:
        logger.info('Create Dataframe from parquet failed') 

    return df

#Função que converte o CSV em parquet
def convert_dataframe_from_csv_to_parquet(df):
    logger.info('Converte file from CSV to Parquet')
    try:
        #cria arquivo temporario dentro do hdpf do cluster emr
        tempdir = tempfile.NamedTemporaryFile(delete=False).name
        
        #escreve o dataframe ja no formato parquet no hdfs do cluster emr
        df.write.mode('overwrite').parquet(tempdir)

        #carrega novo dataframe a partir do arquivo parquet criado
        df = spark.read.format("parquet") \
                        .load(tempdir)
    except:
        logger.info('Create Dataframe from csv failed') 

    return df

#Função escreve o df no s3 e cria o catalago da tabela no glue
def persist(statements: DataFrame):
    logger.info('Persisting datasets ...')

    logger.info('Updating catalog for {}'.format(CATALOG_DATABASE_NAME))
    spark.sql('create database if not exists {}'.format(CATALOG_DATABASE_NAME))
    spark.catalog.setCurrentDatabase(CATALOG_DATABASE_NAME)
    logger.info('Updating table {}.{}'.format(CATALOG_DATABASE_NAME, TARGET_DATA_PATH))
    try:
        statements.write \
            .format('parquet') \
            .mode('overwrite') \
            .option('path', TARGET_DATA_PATH) \
            .saveAsTable(TABLE_NAME)
            
        logger.info('Persisting datasets done...')
    except:
        logger.info('Create Dataframe from csv failed') 

def main():
    start = datetime.now()
    s3 = boto3.resource('s3')

    #cria dataframe a partir do arquivo csv
    raw_df = create_dataframe_from_csv(SOURCE_DATA_PATH)

    #transforma o csv em parquet
    raw_df = convert_dataframe_from_csv_to_parquet(raw_df)

    #cria tabela temporaria 
    raw_df.createOrReplaceTempView('temp')

    raw_df.show(10)

    #SQL responsável por tirar as linhas duplicadas
    sql_1  =  """
                     with cte as (
                             SELECT 
                                        id
                                        ,name
                                        ,email
                                        ,phone
                                        ,address
                                        ,age
                                        ,create_date
                                        ,update_date
                                        ,row_number() over(partition by id order by update_date desc) as row_number
                             FROM temp
                    )
                    SELECT *
                    FROM cte
                    WHERE row_number = 1
                """
                        
    logger.info('SQL: {}'.format(sql_1))
    logger.info("Generating table transactionconciliation...")
    
    #Retira do dataframe a coluna "row_number"
    deduplication_df = spark.sql(sql_1).drop('row_number')

    deduplication_df.show(10)

    #Seleciona as colunas que terão o schema alterado
    final_df = deduplication_df.select("age","create_date","update_date")
    final_df.show(10)

    #Persiste no s3 o schema final
    logger.info('Datafreme Final antes de converter o schema')
    persist(final_df)

    #Carrega o Json de map das colunas 
    content_object = s3.Object(BUCKET_NAME, CONFIG_SCHEMA_PATH)
    file_content = content_object.get()['Body'].read().decode('utf-8')
    json_content = json.loads(file_content)

    #Cria o StructType
    schema = getSchema(json_content)
    logger.info('StructType: {}'.format(schema))

    #Le o schema com o nome schema criado
    df_final_new_schema = create_dataframe_from_parquet(TARGET_DATA_PATH, schema)

    df_final_new_schema.show(10)
    logger.info(df_final_new_schema.printSchema())

    spark.stop()
    end = datetime.now()
    logger.info('Total execution time: {}'.format((end - start).total_seconds()))

if __name__ == '__main__':
    main()
