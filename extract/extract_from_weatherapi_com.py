import requests


def extract_historical_data(city_name, start, end, api_key):
    url = (f"https://api.weatherapi.com/v1/history.json?key={api_key}&q={city_name}&dt={start}&end_dt={end}")

    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        print(f"API request failed: {e}")
        return None