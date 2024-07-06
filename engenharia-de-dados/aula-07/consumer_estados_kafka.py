import os
import pandas as pd
import boto3
import botocore
from kafka import KafkaConsumer
from json import loads
from io import StringIO

# função para registrar dados no minio
def save_key_to_s3(data_frame, key, client):
    csv_buffer = StringIO()
    data_frame.to_csv(csv_buffer, index=False)
    client.put_object(Body=csv_buffer.getvalue(), Bucket='aula-07', Key=key)
    response = client.get_object(Bucket='aula-07', Key=key)
    return response

client = boto3.client('s3', 
        endpoint_url='http://awari-minio-nginx:9000',
        aws_access_key_id='zsODxYnFgKiHNuK6',
        aws_secret_access_key='mVxRW7G801ekpWcQr4Ek72TgOuOs4O6l',
        aws_session_token=None,
        config=boto3.session.Config(signature_version='s3v4'),
        verify=False,
        region_name='sa-east-1'
)

# Kafka consumer
consumer = KafkaConsumer(
        'aula07-estados-e-municipios', 
        bootstrap_servers=['awari-kafka:9093'],  
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='pipeline-python',  
        value_deserializer=lambda x: loads(x.decode('utf-8'))
)

def process_and_upload_data(consumer, client, base_path="/home/awari/app/aula-07/exercicios/municipios-estados/streaming/estados"):
    for message in consumer:
        data = message.value
        uf = data['uf']

        # Criar pasta de estado
        state_path = os.path.join(base_path, uf)
        if not os.path.exists(state_path):
            os.makedirs(state_path)
        
        # Adcionar cidades no dataframe correspondente
        city_df = pd.DataFrame([data])
        city_file = os.path.join(state_path, 'cidades.csv')

        if os.path.exists(city_file):
            city_df.to_csv(city_file, mode='a', header=False, index=False)
        else:
            city_df.to_csv(city_file, index=False)
        
        print(f"Processed and saved data for state: {uf}")

        local_path = os.path.join(base_path, uf, 'cidades.csv')
        s3_path = f'estados/{uf}/cidades.csv'
        try:
            client.upload_file(local_path, 'aula-07', s3_path)
            print(f"Uploaded {local_path} to s3://aula-07/{s3_path}")
        except Exception as e:
            print(f"Error uploading {local_path}: {e}")

    print("Processing and upload completed")

process_and_upload_data(consumer, client)
