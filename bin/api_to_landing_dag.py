from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import json
import requests


def api_to_storage(**kwargs):
    base_url = "https://api.punkapi.com/v2/"
    per_page = 80
    file_path = os.path.join("/", "../object_storage", "beers.jsonl")

    page = 1
    while True:
        response = requests.get(base_url + f"beers?page={page}&per_page={per_page}")
        beers = response.json()

        if len(beers) == 0:
            break

        with open(file_path, "a", encoding="utf-8") as file:
            for beer in beers:
                json_data = json.dumps(beer, ensure_ascii=False)
                file.write(json_data + "\n")

        page += 1


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 6, 30),
    'retries': 2,
    'retry_delay': timedelta(seconds=15),
}

with DAG('api_to_storage_dag', default_args=default_args, schedule_interval='@daily') as dag:
    process_api = PythonOperator(
        task_id='process_api',
        python_callable=api_to_storage
    )

    process_api