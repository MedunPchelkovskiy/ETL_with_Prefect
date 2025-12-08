from prefect import task, get_run_logger
import requests
import pandas as pd
import pyarrow.parquet as pq
import io
import json
from typing import Optional
import psycopg2
from psycopg2.extras import Json

# ---------------------------------------
# Helpers
# ---------------------------------------

def build_filenames(taxi_type: str, year: int, month: int):
    ym = f"{year}-{month:02d}"
    parquet_name = f"{taxi_type}_tripdata_{ym}.parquet"
    csv_name = f"{taxi_type}_tripdata_{ym}.csv"
    return parquet_name, csv_name

def download_file(url: str) -> Optional[bytes]:
    r = requests.get(url, timeout=30)
    return r.content if r.status_code == 200 else None

def table_name_for(taxi_type: str) -> str:
    return f"raw_{taxi_type}_taxi_trips"


# ---------------------------------------
# Prefect Tasks
# ---------------------------------------

@task(retries=2, retry_delay_seconds=5)
def download_taxi_file(taxi_type: str, year: int, month: int) -> Optional[pd.DataFrame]:
    """
    Downloads green/yellow file.
    Tries parquet first; if missing, tries CSV.
    Returns Pandas DataFrame or None.
    """
    logger = get_run_logger()
    base = "https://d37ci6vzurychx.cloudfront.net/trip-data"

    parquet_name, csv_name = build_filenames(taxi_type, year, month)

    # Try PARQUET first
    parquet_url = f"{base}/{parquet_name}"
    logger.info(f"Trying parquet: {parquet_url}")

    data = download_file(parquet_url)
    if data:
        try:
            table = pq.read_table(io.BytesIO(data))
            df = table.to_pandas()
            logger.info(f"Loaded {len(df)} rows from PARQUET")
            return df
        except Exception as e:
            logger.error(f"Parquet read failed: {e}")

    # Try CSV fallback
    csv_url = f"{base}/{csv_name}"
    logger.info(f"Trying CSV fallback: {csv_url}")

    data = download_file(csv_url)
    if data:
        try:
            df = pd.read_csv(io.BytesIO(data))
            logger.info(f"Loaded {len(df)} rows from CSV")
            return df
        except Exception as e:
            logger.error(f"CSV read failed: {e}")

    logger.warning(f"No data found for {taxi_type} {year}-{month:02d}")
    return None


@task
def to_json_records(df: pd.DataFrame) -> list[dict]:
    """Convert DF to list of dicts."""
    return df.to_dict(orient="records")


@task
def insert_raw_into_postgres(records: list[dict], taxi_type: str, year: int, month: int):
    """
    Stores one record PER FILE into raw table:
        - id (serial)
        - taxi_type
        - year
        - month
        - raw_data (JSONB)
        - row_count
    """

    logger = get_run_logger()
    table_name = table_name_for(taxi_type)

    conn = psycopg2.connect(
        host="localhost",
        dbname="yourdb",
        user="youruser",
        password="yourpass"
    )
    cur = conn.cursor()

    create_sql = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        id SERIAL PRIMARY KEY,
        taxi_type TEXT,
        year INT,
        month INT,
        row_count INT,
        raw_data JSONB
    );
    """
    cur.execute(create_sql)

    insert_sql = f"""
    INSERT INTO {table_name} (taxi_type, year, month, row_count, raw_data)
    VALUES (%s, %s, %s, %s, %s)
    """

    cur.execute(insert_sql, (
        taxi_type,
        year,
        month,
        len(records),
        Json(records)
    ))
    conn.commit()
    cur.close()
    conn.close()

    logger.info(f"Inserted RAW batch for {taxi_type} {year}-{month:02d}: {len(records)} rows")


# ---------------------------------------
# Schema AUDIT task (Bronze)
# ---------------------------------------

@task
def audit_schema(df: pd.DataFrame, taxi_type: str, year: int, month: int):
    """
    Logs schema differences for observability only.
    Does NOT modify DB tables.
    """

    logger = get_run_logger()

    cols = df.columns.tolist()
    dtypes = {col: str(df[col].dtype) for col in cols}

    logger.info(f"SCHEMA AUDIT — {taxi_type} {year}-{month:02d}")
    logger.info(f"Columns: {cols}")
    logger.info(f"Dtypes: {dtypes}")

    return dtypes