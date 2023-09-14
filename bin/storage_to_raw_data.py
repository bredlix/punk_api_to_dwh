import pandas as pd
import os
import json
from sqlalchemy import create_engine


def storage_to_pg(**kwargs):

    conn = {
        'host': 'localhost',
        'port': 54320,
        'database': 'BEER',
        'schema': 'raw_data',
        'user': 'hotdog',
        'password': 'ketchup',
        'options': '-c search_path=raw_data'
    }

    file_path = os.path.join('../object_storage', 'beers.jsonl')

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


if __name__ == '__main__':
    storage_to_pg()