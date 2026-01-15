#!/usr/bin/env python
# coding: utf-8
import pandas as pd
from sqlalchemy import create_engine

dtype = {
    "VendorID": "Int64",
    "passenger_count": "Int64",
    "trip_distance": "float64",
    "RatecodeID": "Int64",
    "store_and_fwd_flag": "string",
    "PULocationID": "Int64",
    "DOLocationID": "Int64",
    "payment_type": "Int64",
    "fare_amount": "float64",
    "extra": "float64",
    "mta_tax": "float64",
    "tip_amount": "float64",
    "tolls_amount": "float64",
    "improvement_surcharge": "float64",
    "total_amount": "float64",
    "congestion_surcharge": "float64"
}

parse_dates = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime"
]




def run():
    ## Parameters
    year = 2021
    month = 1
    pg_user = "root"
    pg_pass = "root"
    pg_host = "localhost"
    pg_db = "ny_taxi"
    pg_port = 5432
    chunk_size = 100000
    target_table = "yellow_taxi_data"
    url_prefix = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_"


    # Create the Database Engine
    engine = create_engine(f'postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}')

    # Create the Table
    df = pd.read_csv(
    url_prefix + f'{year}-{month:02d}.csv.gz',
    dtype=dtype,
    parse_dates=parse_dates,
    )

    df.head(0).to_sql(
        name=target_table, 
        con=engine, 
        if_exists="replace"
        )
    
    # Ingest Data in Chunks
    df_iter = pd.read_csv(
        url_prefix + f'{year}-{month:02d}.csv.gz',
        dtype=dtype,
        parse_dates=parse_dates,
        iterator=True,
        chunksize=chunk_size
    )
    
    i=0
    # Iterate over each chunk and insert into the database
    for df_chunk in df_iter:
        i+=len(df_chunk)
        print(i)
        df_chunk.to_sql(name=target_table, con=engine, if_exists="append")

if __name__ == "__main__":
    run()