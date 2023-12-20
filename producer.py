import openmeteo_requests
import json
import requests_cache
import pandas as pd
from retry_requests import retry

from confluent_kafka import Producer
from config import config

config['client.id'] = 'weather-producer'
producer = Producer(config)
topic_name = 'weater_to_email_topic'


def fetch_weather_data():
    # Setup the Open-Meteo API client with cache and retry on error
    cache_session = requests_cache.CachedSession('.cache', expire_after=3600)
    retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
    openmeteo = openmeteo_requests.Client(session=retry_session)

    # Make sure all required weather variables are listed here
    # The order of variables in hourly or daily is important to assign them correctly below
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": 44.804,
        "longitude": 20.4651,
        "current": ["temperature_2m", "rain", "surface_pressure", "wind_speed_10m", "wind_direction_10m"],
        "hourly": ["temperature_2m", "rain", "surface_pressure", "wind_speed_10m"],
        "wind_speed_unit": "kn",
        "timezone": "Europe/Berlin",
        "forecast_days": 1
    }
    responses = openmeteo.weather_api(url, params=params)

    # Process first location. Add a for-loop for multiple locations or weather models
    response = responses[0]
    print(f"Coordinates {response.Latitude()}°E {response.Longitude()}°N")
    print(f"Elevation {response.Elevation()} m asl")
    print(f"Timezone {response.Timezone()} {response.TimezoneAbbreviation()}")
    print(f"Timezone difference to GMT+0 {response.UtcOffsetSeconds()} s")

    # Current values. The order of variables needs to be the same as requested.
    current = response.Current()
    current_temperature_2m = current.Variables(0).Value()
    current_rain = current.Variables(1).Value()
    current_surface_pressure = current.Variables(2).Value()
    current_wind_speed_10m = current.Variables(3).Value()
    current_wind_direction_10m = current.Variables(4).Value()

    print(f"Current time {current.Time()}")
    print(f"Current temperature_2m {current_temperature_2m}")
    print(f"Current rain {current_rain}")
    print(f"Current surface_pressure {current_surface_pressure}")
    print(f"Current wind_speed_10m {current_wind_speed_10m}")
    print(f"Current wind_direction_10m {current_wind_direction_10m}")

    # Process hourly data. The order of variables needs to be the same as requested.
    hourly = response.Hourly()
    hourly_temperature_2m = hourly.Variables(0).ValuesAsNumpy()
    hourly_rain = hourly.Variables(1).ValuesAsNumpy()
    hourly_surface_pressure = hourly.Variables(2).ValuesAsNumpy()
    hourly_wind_speed_10m = hourly.Variables(3).ValuesAsNumpy()

    hourly_data = {"date": pd.date_range(
        start=pd.to_datetime(hourly.Time(), unit="s"),
        end=pd.to_datetime(hourly.TimeEnd(), unit="s"),
        freq=pd.Timedelta(seconds=hourly.Interval()),
        inclusive="left"
    ), "temperature_2m": hourly_temperature_2m, "rain": hourly_rain, "surface_pressure": hourly_surface_pressure,
        "wind_speed_10m": hourly_wind_speed_10m}

    hourly_dataframe = pd.DataFrame(data=hourly_data)
    print(hourly_dataframe)
    return hourly_data


data_to_send = fetch_weather_data()
print(data_to_send)
json_data = json.dumps(data_to_send)


def delivery_report(err, msg):
    if err is not None:
        print('Error: {}'.format(err))
    else:
        print('Message sent on topic {} [{}]'.format(msg.topic(), msg.partition()))


producer.produce(topic_name, value=json_data, callback=delivery_report)

# Čekanje da se sve poruke potvrde pre zatvaranja producenta
producer.flush()
