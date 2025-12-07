import requests
import pandas as pd
from io import BytesIO

from helpers.load_raw_data_in_postgres import process_month_postgres


def download_tlc_trip_data(service: str, year: int, month: int, filetype: str = "parquet"):
    """
    Download NYC TLC trip data directly into memory (no storage).

    Parameters
    ----------
    service : "yellow", "green", "fhv"
    year    : int (e.g., 2021)
    month   : int (1–12)
    filetype: "parquet" or "csv"

    Returns
    -------
    pandas.DataFrame
    """

    service = service.lower()
    if service not in {"yellow", "green", "fhv"}:
        raise ValueError("service must be 'yellow', 'green', or 'fhv'")

    if filetype not in {"parquet", "csv"}:
        raise ValueError("filetype must be 'parquet' or 'csv'")

    filename = f"{service}_tripdata_{year}-{month:02d}.{filetype}"
    url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{filename}"

    print(f"Fetching: {url}")

    response = requests.get(url, stream=True)
    if response.status_code != 200:
        raise FileNotFoundError(f"Dataset not available at: {url}")

    data = pd.read_parquet(BytesIO(response.content), engine="pyarrow")

    # if filetype == "parquet":
    #     data_to_load = pd.read_parquet(data)
    # else:
    #     data_to_load = pd.read_csv(data)

    process_month_postgres(data)