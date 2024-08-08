import os
import re
import shutil
import boto3
import pyspark
from pyspark.sql import SparkSession
import psycopg2
from psycopg2 import OperationalError

conf = pyspark.SparkConf()
conf.setMaster('local[1]')
conf.setAppName('aula-08-exercicio')
conf.set('spark.driver.host', 'awari-jupyterlab')
conf.set('spark.driver.port', '20020')
conf.set('spark.hadoop.fs.s3a.endpoint', 'http://awari-nginx:9000')
conf.set('spark.hadoop.fs.s3a.endpoint.region', 'sa-east-1')
conf.set('spark.hadoop.fs.s3a.access.key', 'zsODxYnFgKiHNuK6')
conf.set('spark.hadoop.fs.s3a.secret.key', 'mVxRW7G801ekpWcQr4Ek72TgOuOs4O6l')
conf.set('spark.hadoop.fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem')
conf.set('spark.hadoop.fs.s3a.connection.ssl.enabled', 'false')
conf.set('spark.hadoop.com.amazonaws.services.s3.enableV2', 'true')
conf.set('spark.hadoop.fs.s3a.committer.staging.conflict-mode', 'replace')
conf.set('spark.hadoop.fs.s3a.fast.upload', 'true')
conf.set('spark.hadoop.fs.s3a.path.style.access', 'true')
conf.set('spark.hadoop.fs.s3a.committer.name', 'directory')
conf.set('spark.hadoop.fs.s3a.committer.staging.tmp.path', '/tmp/staging')

sc = pyspark.SparkContext(conf=conf)
spark = SparkSession(sc)

local_path = 'arquivos/municipios-estados/csv/'
bucket_name = 'aula-08'

s3 = boto3.client(
    's3',
    endpoint_url='http://awari-nginx:9000',
    aws_access_key_id='zsODxYnFgKiHNuK6',
    aws_secret_access_key='mVxRW7G801ekpWcQr4Ek72TgOuOs4O6l',
    region_name='sa-east-1'
)

def get_file_base_name(file_path):
    return re.sub(r'\.[^.]*$', '', os.path.basename(file_path))

# CSV to JSON

print('Starting CSV to JSON loading')

minio_path = 'municipios-estados/json'

def save_as_json(df, local_path, s3_client, bucket, prefix):
    temp_local_dir = local_path + '_temp'
    df.coalesce(1).write.mode('overwrite').json(temp_local_dir)
    temp_file = [os.path.join(temp_local_dir, f) for f in os.listdir(
        temp_local_dir) if f.startswith('part-') and f.endswith('.json')][0]
    final_local_file = local_path + '.json'
    shutil.move(temp_file, final_local_file)
    shutil.rmtree(temp_local_dir)
    s3_key = os.path.join(prefix, os.path.basename(final_local_file))
    s3_client.upload_file(final_local_file, bucket, s3_key)

csv_files = [os.path.join(local_path, f)
             for f in os.listdir(local_path) if f.endswith('.csv')]
print(csv_files)

for file in csv_files:
    df = spark.read.csv(file, header=True, inferSchema=True)
    base_name = get_file_base_name(file)
    print(f'Starting {base_name}')
    json_local_path = os.path.join(local_path, base_name)
    save_as_json(df, json_local_path, s3, bucket_name, minio_path)
    print(f'{base_name} loaded')

# CSV to PARQUET
print('Starting CSV to PARQUET loading')

minio_path = 'municipios-estados/parquet'

def save_as_parquet(df, s3_client, bucket, prefix, file_base_name):
    temp_local_dir = '/tmp/' + file_base_name + '_temp'
    df.coalesce(1).write.mode('overwrite').parquet(temp_local_dir)
    temp_file = [os.path.join(temp_local_dir, f) for f in os.listdir(
        temp_local_dir) if f.startswith('part-') and f.endswith('.parquet')][0]
    s3_key = os.path.join(prefix, file_base_name + '.parquet')
    s3_client.upload_file(temp_file, bucket, s3_key)
    shutil.rmtree(temp_local_dir)  

for file in csv_files:
    df = spark.read.csv(file, header=True, inferSchema=True)
    base_name = get_file_base_name(file)
    print(f'Starting {base_name}')
    save_as_parquet(df, s3, bucket_name, minio_path, base_name)
    print(f'{base_name} loaded')

# JSON to CSV
print('Starting JSON to CSV loading')

minio_path = 'municipios-estados/csv'

def save_as_csv(df, s3_client, bucket, prefix, file_base_name):
    temp_local_dir = '/tmp/' + file_base_name + '_temp'
    df.coalesce(1).write.mode('overwrite').csv(temp_local_dir)
    temp_file = [os.path.join(temp_local_dir, f) for f in os.listdir(
        temp_local_dir) if f.startswith('part-') and f.endswith('.csv')][0]
    s3_key = os.path.join(prefix, file_base_name + '.csv')
    s3_client.upload_file(temp_file, bucket, s3_key)
    shutil.rmtree(temp_local_dir) 

json_files = [os.path.join(local_path, f)
              for f in os.listdir(local_path) if f.endswith('.json')]
print(json_files)

for file in json_files:
    df = spark.read.json(file)
    base_name = get_file_base_name(file)
    print(f'Starting {base_name}')
    save_as_csv(df, s3, bucket_name, minio_path, base_name)
    print(f'{base_name} loaded')

# CSV to POSTGRES
print('Starting postgres loading')

conn_params = {
    "dbname": "postgres",
    "user": "postgres",
    "password": "postgres",
    "host": "awari-postgres",
    "port": "5432"
}

def save_as_postgres(df, conn_params, table_name):
    try:
        pandas_df = df.toPandas()

        conn = psycopg2.connect(**conn_params)
        cursor = conn.cursor()

        columns = pandas_df.columns
        create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} (" + ", ".join(
            [f"{col} TEXT" for col in columns]) + ")"
        cursor.execute(create_table_query)

        for row in pandas_df.itertuples(index=False, name=None):
            insert_query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({', '.join(['%s']*len(row))})"
            cursor.execute(insert_query, row)

        conn.commit()
        cursor.close()
        conn.close()

    except OperationalError as e:
        print(f"Error connecting to PostgreSQL: {e}")

for file in csv_files:
    df = spark.read.csv(file, header=True, inferSchema=True)
    base_name = get_file_base_name(file)
    print(f'Starting {base_name}')
    table_name = base_name
    save_as_postgres(df, conn_params, table_name)
    print(f'{base_name} loaded')

spark.stop()
