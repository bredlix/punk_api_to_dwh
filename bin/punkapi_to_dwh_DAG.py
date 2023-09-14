import os
import json
import requests
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator


def api_to_storage(**kwargs):
    #etl_date = kwargs.get('ds')

    base_url = "https://api.punkapi.com/v2/"
    per_page = 80
    file_path = os.path.join("/", "../object_storage", "beers.jsonl")

    page = 1
    with open(file_path, "w", encoding="utf-8") as file:
        while True:
            response = requests.get(base_url + f"beers?page={page}&per_page={per_page}")
            beers = response.json()

            if len(beers) == 0:
                break

            for beer in beers:
                json_data = json.dumps(beer, ensure_ascii=False)
                file.write(json_data + "\n")

            page += 1


def storage_to_raw_data(**kwargs):
    #etl_date = kwargs.get('ds')

    conn = {
        'host': 'host.docker.internal',
        'port': 54320,
        'database': 'BEER',
        'schema': 'raw_data',
        'user': 'hotdog',
        'password': 'ketchup',
        'options': '-c search_path=raw_data'
    }

    file_path = os.path.join('/', '../object_storage', 'beers.jsonl')

    df = pd.read_json(file_path, lines=True, orient='records')

    data = []
    with open(file_path, "r", encoding="utf-8") as file:
        for line in file:
            data.append(json.loads(line))

    df = pd.DataFrame(data)

    nested_fields = ['ingredients', 'method', 'volume', 'boil_volume']

    for field in nested_fields:
        df[field] = df[field].apply(json.dumps)

    engine = create_engine(
        f"postgresql://{conn['user']}:{conn['password']}@{conn['host']}:{conn['port']}/{conn['database']}",
        connect_args={'options': '-c search_path=' + conn['schema']}
    )

    df.to_sql('beers', engine, if_exists='replace', index=False, schema=conn['schema'])


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 7, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG('punkapi_to_dwh_DAG', default_args=default_args, schedule_interval='0 10 * * *') as dag:
    api_to_storage_task = PythonOperator(
        task_id='api_to_storage_task',
        python_callable=api_to_storage
    )

    storage_to_pg_task = PythonOperator(
        task_id='storage_to_pg_task',
        python_callable=storage_to_raw_data
    )

    api_to_storage_task >> storage_to_pg_task