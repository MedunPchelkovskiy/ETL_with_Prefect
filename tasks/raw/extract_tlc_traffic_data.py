from io import BytesIO

import pandas as pd
import requests

from helpers.load_raw_data_in_postgres import process_month_postgres


def download_tlc_trip_data(services: list, start_year: int, end_year: int, month: int, filetypes: list):
    db_tables = ["green_taxi_trips", "yellow_taxi_trips"]
    """
    Download NYC TLC trip data and store locally in postgres DB

    Parameters
    ----------
    services : "yellow", "green"
    start_year    : int (e.g., 2021)
    end_year    : int (e.g., 2021)
    month   : int (1–12)
    filetypes: "parquet" or "csv"

    Returns
    -------
    pandas.DataFrame
    """

    for service in services:
        if service not in {"yellow", "green"}:
            raise ValueError("service must be 'yellow', or 'green'")

        for year in range(start_year, end_year + 1):
            for month in range(month, 13):
                for filetype in filetypes:
                    if filetype not in {"parquet", "csv"}:
                        raise ValueError("filetype must be 'parquet' or 'csv'")
                    filename = f"{service}_tripdata_{year}-{month:02d}.{filetype}"
                    url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{filename}"

                    print(f"Fetching: {url}")

                    response = requests.get(url, stream=True)
                    if response.status_code != 200:
                        continue
                    if filetype == "parquet":
                        data = pd.read_parquet(BytesIO(response.content), engine="pyarrow")
                    if filetype == "csv":
                        data = pd.read_csv(BytesIO(response.content), engine="pyarrow")

                    if service == "green":
                        process_month_postgres(data, db_tables[0])
                    if service == "yellow":
                        process_month_postgres(data, db_tables[1])
                    break
