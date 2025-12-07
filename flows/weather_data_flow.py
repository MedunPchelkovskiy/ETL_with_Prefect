import json

from decouple import config
from prefect import flow, task

from extract.extract_from_meteostat_com import extract_meteostat_data
from extract.extract_from_open_weather import extract_historical_data_for_new_york
from extract.extract_from_weatherapi_com import extract_historical_data
from extract.extract_tlc_traffic_data_S3_bucket import download_tlc_trip_data


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
    meteostat_data = extract_meteostat_data(api_key, lat, lon, start, end)
    return meteostat_data



@task(name="Traffic data from S3",
      task_run_name="S3 traffic extraction"
      )
def extract_tlc_traffic_data_from_S3(service: str, year: int, month: int, filetype: str):
    raw_trip_data = download_tlc_trip_data(service, year, month, filetype)
    return raw_trip_data


@flow(flow_run_name="weather_flow_runs")
def weather_flow(city_name, country_code):
    # open_weather_data = extract_weather_data(city_name, country_code)
    # weather_api_data = extract_from_weatherapi_com(city_name)
    # meteostat_api_data = extract_from_meteostat(api_key=config("METEOSTAT_API_KEY"), lat=40.7127, lon=-74.0059, start="2014-01-01", end="2014-12-31")
    # print(json.dumps(meteostat_api_data, indent=4))
    tlc_raw_trip_data = extract_tlc_traffic_data_from_S3("green", 2014, 1, "parquet")
    # print(tlc_raw_trip_data.head(13))
    # print(tlc_raw_trip_data.columns.tolist())



# Example usage
if __name__ == "__main__":
    weather_flow("New Work", "US")
