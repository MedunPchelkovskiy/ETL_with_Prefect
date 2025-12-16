# from prefect import task, get_run_logger
# import requests
# import pandas as pd
#
#
# from typing import Optional
#
#
# # ---------------------------------------
# # Helpers function for raw tlc layer flow.
# # ---------------------------------------
#
# def build_filenames(taxi_type: str, year: int, month: int):
#     ym = f"{year}-{month:02d}"
#     parquet_name = f"{taxi_type}_tripdata_{ym}.parquet"
#     csv_name = f"{taxi_type}_tripdata_{ym}.csv"
#     return parquet_name, csv_name
#
# def download_file(url: str) -> Optional[bytes]:
#     r = requests.get(url, timeout=30)
#     return r.content if r.status_code == 200 else None
#
# def table_name_for(taxi_type: str) -> str:
#     return f"raw_{taxi_type}_taxi_trips"