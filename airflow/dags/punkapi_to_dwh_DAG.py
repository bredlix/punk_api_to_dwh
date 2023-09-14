import os
import json
import requests
import pandas as pd
import logging
from sqlalchemy import create_engine
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.utils.task_group import TaskGroup
from airflow.operators.dummy import DummyOperator


def api_to_storage(**kwargs):

    base_url = "https://api.punkapi.com/v2/"
    per_page = 80
    file_path = os.path.join("/", "object_storage", "beers.jsonl")

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

    file_path = os.path.join('/', 'object_storage', 'beers.jsonl')

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

    logging.info(file_path)


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 6, 29),
    'retries': 2,
    'retry_delay': timedelta(seconds=15),
}

with DAG('punkapi_to_dwh_DAG', default_args=default_args, schedule_interval='0 10 * * *') as dag:
    api_to_storage_task = PythonOperator(
        task_id='api_to_landing_storage',
        python_callable=api_to_storage
    )

    storage_to_pg_task = PythonOperator(
        task_id='landing_to_raw_data',
        python_callable=storage_to_raw_data
    )

    with TaskGroup('raw_to_core') as raw_to_core:
        populate_dim_beer_task = PostgresOperator(
            task_id='poplate_dim_beer',
            postgres_conn_id='DWH',
            sql="repo/populate_dim_beer.sql"
        )
        populate_dim_ingredients_task = PostgresOperator(
            task_id='poplate_dim_ingredients',
            postgres_conn_id='DWH',
            sql="repo/populate_dim_ingredients.sql"
        )
        populate_link_ingredients_task = PostgresOperator(
            task_id='poplate_link_ingredients',
            postgres_conn_id='DWH',
            sql="repo/populate_link_ingredients.sql"
        )
        populate_link_method_task = PostgresOperator(
            task_id='populate_link_method',
            postgres_conn_id='DWH',
            sql="repo/populate_link_method.sql"
        )
        raw_to_core_start_task = DummyOperator(task_id='raw_to_core_start')
        raw_to_core_end_task = DummyOperator(task_id='raw_to_core_end')

    with TaskGroup('core_to_data_mart') as core_to_data_mart:
        v_avg_hops_temp_task = PostgresOperator(
            task_id='v_avg_hops_temp',
            postgres_conn_id='DWH',
            sql="repo/create_view_avg_hops_temp.sql"
        )
        v_avg_top_hops_temp_task = PostgresOperator(
            task_id='v_avg_top_hops_temp',
            postgres_conn_id='DWH',
            sql="repo/create_view_avg_top_hops_temp.sql"
        )



    api_to_storage_task >> storage_to_pg_task >> raw_to_core_start_task

    populate_dim_ingredients_task << raw_to_core_start_task
    populate_dim_beer_task << raw_to_core_start_task

    populate_link_ingredients_task << populate_dim_ingredients_task
    populate_link_ingredients_task << populate_dim_beer_task

    populate_link_method_task << populate_dim_beer_task

    raw_to_core_end_task << populate_link_method_task
    raw_to_core_end_task << populate_link_ingredients_task

    raw_to_core_end_task >> v_avg_hops_temp_task
    raw_to_core_end_task >> v_avg_top_hops_temp_task

