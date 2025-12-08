import pandas as pd
from decouple import config
from sqlalchemy import create_engine

# Replace username, password, host, port, dbname with your values, in this case imported from .env file
POSTGRES_URI = (f"postgresql://{config("DB_USERNAME")}:{config("DB_PASS")}@localhost:{config("DB_PORT")}/{config("DB_NAME")}")

# Create SQLAlchemy engine
engine = create_engine(POSTGRES_URI)


def process_month_postgres(df, table_name):
    """
    Process each month's TLC data and insert into PostgreSQL table.

    Parameters
    ----------
    df : pandas.DataFrame
        The monthly TLC trip data
    table_name : str
        Target table in PostgreSQL
    """
    # Optional: transform / select columns
    columns_to_keep = [
        'tpep_pickup_datetime',
        'tpep_dropoff_datetime',
        'passenger_count',
        'trip_distance',
        'fare_amount',
        'payment_type',
        'PULocationID',  # Pickup zone
        'DOLocationID',  # Dropoff zone
    ]

    # Keep only existing columns (some datasets differ)
    df = df[[col for col in columns_to_keep if col in df.columns]]

    # Insert into PostgreSQL (append)
    df.to_sql(
        table_name,
        engine,
        if_exists="append",
        index=False,
        method="multi",
        chunksize=10000
    )