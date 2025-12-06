from decouple import config
from prefect import flow, task

from extract.extract_from_open_weather import extract_historical_data_for_new_york


@task(name="My Extract Task",
      description="An extract task for the weather flow.",
      task_run_name="data extraction")
def extract_weather_data(city_name,country_code, start, end, api_key):
    weather_data = extract_historical_data_for_new_york(city_name,country_code, start, end, api_key)
    return weather_data



@flow(flow_run_name="weather_flow_runs")
def weather_flow(city_name, country_code, start, end, api_key):
    # Run extraction in parallel for all cities using .map()
    data = extract_weather_data(city_name, country_code, start, end, api_key)





# Example usage
if __name__ == "__main__":

    weather_flow("New Work", "US", "01-01-2001", "02-01-2001", config("OPEN_WEATHER_API_KEY"))
