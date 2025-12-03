from decouple import config
from prefect import flow, task

from extract.extract_from_open_weather import extract_historical_data_for_new_york


@task
def extract_weather_data(cities_name:str,country_code:str, start:str, end:str, api_key:str):
    weather_data = extract_historical_data_for_new_york(cities_name,country_code, start, end, api_key)
    return weather_data



@flow
def weather_flow_parallel(cities_names, country_code, start, end, api_key):
    # Run extraction in parallel for all cities using .map()
    results = extract_weather_data.map(
        city_name=cities_names,
        country_code=[country_code]*len(cities),
        start=[start]*len(cities),
        end=[end]*len(cities),
        api_key=[api_key]*len(cities)
    )
    return results


# Example usage
if __name__ == "__main__":
    cities = ["New York", "Boston", "Chicago"]
    weather_data = weather_flow_parallel(cities, "US", "01-01-2001", "02-01-2001", config("API_KEY")
