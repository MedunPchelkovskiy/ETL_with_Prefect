from prefect import flow, task

from tasks.raw.extract_from_meteostat_com import extract_meteostat_data
from tasks.raw.extract_tlc_traffic_data import download_tlc_trip_data


# @task(name="My Extract Task",
#       description="An extract task for the weather flow.",
#       task_run_name="openweather extraction")
# def extract_weather_data(city_name, country_code, start="01-01-2001", end="02-01-2001",
#                          api_key=config("OPEN_WEATHER_API_KEY")):
#     """ Tested, but not used because a paid plan is required to retrieve historical weather data."""
#     weather_data = extract_historical_data_for_new_york(city_name, country_code, start, end, api_key)
#     return weather_data
#
#
# @task(name="My Second Extract Task",
#       description="An extract task for the weather flow.",
#       task_run_name="weatherapicom extraction ")
# def extract_from_weatherapi_com(city_name, start="2014-01-01", end="2014-02-01", api_key=config("WEATHER_API_KEY_COM")):
#     """ Tested, but not used because a paid plan is required to retrieve historical weather data."""
#     historical_data = extract_historical_data(city_name, start, end, api_key)
#     return historical_data


@task(name="My Third Extract Task",
      description="An extract task for the weather flow.",
      task_run_name="meteostat extraction")
def extract_from_meteostat(api_key, lat, lon, start, end):
    extract_meteostat_data(api_key, lat, lon, start, end)
    return None



@task(name="Traffic data from S3",
      task_run_name="S3 traffic extraction"
      )
def extract_tlc_traffic_data_from_s3(services: list, start_year: int, end_year: int, month: int, file_types: list):
    raw_trip_data = download_tlc_trip_data(services, start_year, end_year, month, file_types)
    return raw_trip_data


@flow(flow_run_name="weather_flow_runs")
def weather_flow(city_name, country_code):
    services = ["green", "yellow"]
    file_types = ["parquet", "csv"]
    # open_weather_data = extract_weather_data(city_name, country_code)
    # weather_api_data = extract_from_weatherapi_com(city_name)
    # meteostat_api_data = extract_from_meteostat(api_key=config("METEOSTAT_API_KEY"), lat=40.7127, lon=-74.0059, start="2014-01-01", end="2014-12-31")
    # print(json.dumps(meteostat_api_data, indent=4))
    tlc_raw_trip_data = extract_tlc_traffic_data_from_s3(services, 2014, 2024, 1, file_types)
    # print(tlc_raw_trip_data.head(13))
    # print(tlc_raw_trip_data.columns.tolist())



# Example usage
if __name__ == "__main__":
    weather_flow("New Work", "US")




























from prefect import flow
from tasks_raw import (
    download_taxi_file,
    to_json_records,
    insert_raw_into_postgres,
    audit_schema
)

@flow
def raw_ingestion_flow():

    for taxi_type in ["yellow", "green"]:
        for year in range(2014, 2025):
            for month in range(1, 13):

                df = download_taxi_file(taxi_type, year, month)
                if df is None:
                    continue

                audit_schema(df, taxi_type, year, month)

                records = to_json_records(df)
                insert_raw_into_postgres(records, taxi_type, year, month)


if name == "main":
    raw_ingestion_flow()
