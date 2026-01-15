#!/usr/bin/env python
# coding: utf-8
import pandas as pd
from sqlalchemy import create_engine
import click

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



## Parameters
@click.command()
@click.option("--year", type=int, default=2021, show_default=True, help="Data year")
@click.option("--month", type=int, default=1, show_default=True, help="Data month (1–12)")
@click.option("--pg_user", default="root", show_default=True, help="Postgres user")
@click.option("--pg_pass", default="root", show_default=True, help="Postgres password")
@click.option("--pg_host", default="localhost", show_default=True, help="Postgres host")
@click.option("--pg_db", default="ny_taxi", show_default=True, help="Postgres database")
@click.option("--pg_port", type=int, default=5432, show_default=True, help="Postgres port")
@click.option("--chunk_size", type=int, default=100_000, show_default=True, help="Chunk size for ingestion")
@click.option(
        "--target_table",
        default="yellow_taxi_data",
        show_default=True,
        help="Target table name",
    )

def run(year,month,pg_user,pg_pass,pg_host,pg_db,pg_port,chunk_size,target_table):

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