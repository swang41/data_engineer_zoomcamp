import os
from time import time
import argparse 
import pandas as pd
from sqlalchemy import create_engine


def main(params):
    username = params.username
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url
    csv_name = 'output.csv.gzip' 

    os.system(f"wget {url} -O {csv_name}")
    # some silly stuff with pandas
    engine = create_engine(f'postgresql://{username}:{password}@{host}:{port}/{db}')
    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=10000, compression='gzip')
    t_start = time()
    for i, df_chunk in enumerate(df_iter):
        df_chunk.tpep_pickup_datetime = pd.to_datetime(df_chunk.tpep_pickup_datetime)
        df_chunk.tpep_dropoff_datetime = pd.to_datetime(df_chunk.tpep_dropoff_datetime)
        df_chunk.to_sql(name=table_name, con=engine, if_exists='append')

        if i != 0 and i % 5 == 0:
            print(f'Inserted {i} chunks, took {time() - t_start} seconds')

    print(f"job finished successfully for file: {csv_name}")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Ingest CSV data to Postgres")

    parser.add_argument('--username')
    parser.add_argument('--password')
    parser.add_argument('--host')
    parser.add_argument('--port')
    parser.add_argument('--db')
    parser.add_argument('--table_name')
    parser.add_argument('--url')

    args = parser.parse_args()

    main(args)