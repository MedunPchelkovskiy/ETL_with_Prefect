import requests



def extract_historical_data_for_new_york(city_name:str,country_code:str, start:str, end:str, api_key:str) -> json:
    url = (f"https://history.openweathermap.org/data/2.5/history/city?q={city_name},"
           f"{country_code}&type=hour&start={start}&end={end}&appid={api_key}")

    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        print(f"API request failed: {e}")
        return None