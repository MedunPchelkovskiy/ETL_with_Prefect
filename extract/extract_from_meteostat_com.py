import requests


def extract_meteostat_data(api_key, lat, lon, start, end):
    url = "https://meteostat.p.rapidapi.com/point/daily"

    querystring = {"lat": lat, "lon": lon, "start": start, "end": end}

    headers = {
        "x-rapidapi-key": f"{api_key}",
        "x-rapidapi-host": "meteostat.p.rapidapi.com"
    }

    response = requests.get(url, headers=headers, params=querystring)

    return response.json()
