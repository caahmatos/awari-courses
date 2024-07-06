import os
import pandas as pd
from kafka import KafkaProducer
from json import dumps

# tratamento de dados
def read_and_merge_data():
    est = pd.read_csv("/home/awari/app/aula-07/exercicios/municipios-estados/csv/estados.csv")
    df = (
        pd.read_csv("/home/awari/app/aula-07/exercicios/municipios-estados/csv/municipios.csv")
        .merge(est, on='codigo_uf', how='left', suffixes=("_municipio", "_estado"))
        .sort_values(['codigo_uf', 'codigo_ibge'])
    )
    return est, df

# criar pastas para cada estado
def create_state_folders_and_files(est, df, base_path="/home/awari/app/aula-07/exercicios/municipios-estados/streaming/estados"):
    if not os.path.exists(base_path):
        os.makedirs(base_path)
        print(f"Folder '{base_path}' created")
    else:
        print(f"Folder '{base_path}' already exists")

    for uf in est['uf']:
        path = os.path.join(base_path, uf)
        if not os.path.exists(path):
            os.mkdir(path)
        (
            df.query(f'uf == "{uf}"')
            .to_csv(os.path.join(path, 'cidades.csv'), index=False)
        )
    print("State folders and files created")

# Kakfa producer
producer = KafkaProducer(
        bootstrap_servers=['awari-kafka:9093'],  
        value_serializer=lambda x: dumps(x).encode('utf-8')
)

# Transformar dados em json e enviar para kafka
def send_data_to_kafka(df, producer):
    for _, row in df.iterrows():
        data = row.to_dict()
        print(f"Sending data: {data}")  # Debugging
        producer.send('aula07-estados-e-municipios', value=data)
    producer.flush()
    producer.close()
    print("Data sent to Kafka")

est, df = read_and_merge_data()
create_state_folders_and_files(est, df)
send_data_to_kafka(df, producer)

