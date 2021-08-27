from zipfile import ZipFile
from loguru import logger
import pandas as pd
from pathlib import Path
from geopy.geocoders import  Here
from concurrent.futures import ThreadPoolExecutor
import numpy as np
from geopy.extra.rate_limiter import RateLimiter
import math
import requests
import json
from datetime import date, datetime
import time
from celery import shared_task
import matplotlib.pyplot as plt
from get_central_coord import get_coordinates_of_central

API_KEY = 'dYrajXLId5Rnp_WKZHaonsR-SMy2Z0xOlPRM4uELmfY'

key = 'af194677845bda5d5065ebc3a0093a12'

logger.add("debug_info.txt", format="{time} {level} {message}", level="DEBUG")

def filter(df):
    """Function that filters data by name, longitude and latitude"""
    correct_df = df[pd.to_numeric(df["Latitude"], errors='coerce').notnull() &
                  pd.to_numeric(df["Longitude"], errors='coerce').notnull() &
                  ~pd.to_numeric(df["Name"], errors='coerce').notnull()]

    filtered_df = correct_df[(abs(correct_df.Latitude.astype(float)) <= 90) &
                        (correct_df.Longitude.astype(float) >= -180) &
                        (correct_df.Longitude.astype(float) <= 180)]
    return filtered_df


def unzip():
    "Function that extracts all csv files to main directory"
    with ZipFile('hotels.zip', 'r') as zip_obj:
        zip_obj.extractall()



def primary_data_proc():
    """Function that return dataframe with cities which have the most number of hotels"""
    unzip()
    all_df_list = (filter(pd.read_csv(path, encoding='utf-8').dropna()) for path in Path.cwd().glob("*.csv"))
    result_frame = pd.concat(all_df_list, ignore_index=True)
    countries = result_frame[['City', "Country"]].value_counts()[:].sort_values(ascending=False)

    top_hotels = {}
    for pair in countries.to_dict().items():
        if pair[0][1] not in top_hotels:
            top_hotels[pair[0][1]] = pair[0][0]
    return result_frame[result_frame.City.isin(top_hotels.values())]



def define_address():
    """Function that adds addresses into dataframe"""
    df = primary_data_proc()
    geocoder = Here(apikey =API_KEY, user_agent = "weather.script.py", timeout=3)
    geocode = RateLimiter(geocoder.reverse, min_delay_seconds=0.1)

    with ThreadPoolExecutor(max_workers=10) as pool:
        responses = pool.map(geocode, zip(df['Latitude'].tolist(), df["Longitude"].tolist()))
    df["Adress"] = np.array(responses)
    return df


def calculate_central_area(result_df):
    """Function that returns dictionary with city and it's center"""
    return {city:get_coordinates_of_central(dataframe) for city, dataframe in result_df.groupby(["City"])}


def fetch(url):
    return requests.get(url)




@logger.catch
def forecast_weather(result_dataframe):
    cities_and_coordinate = calculate_central_area(result_dataframe)
    forecast_weather_list =[]
    cities = list(np.repeat([city for city in cities_and_coordinate],8))
    coordinates = [coordinate for coordinate in cities_and_coordinate.values()]
    urls = [f"https://api.openweathermap.org/data/2.5/onecall?lat={coordinate[0]}&lon={coordinate[1]}&exclude={'hourly'}&appid={key}" for coordinate in coordinates]
    with ThreadPoolExecutor(max_workers=len(urls)) as pool:
        responses = pool.map(fetch, urls)
    for response in responses:
        weather_per_week = eval(response.text)['daily']
        for day in weather_per_week:
            forecast_weather_list.append({
                "City":cities.pop(0),
                "min_temp":day["temp"]["min"],
                "max_temp": day["temp"]["max"],
                "day":datetime.utcfromtimestamp(day["dt"]).strftime('%Y-%m-%d')})
    return pd.DataFrame(forecast_weather_list)



def prev_5_days():
    current_date = int(datetime.timestamp(datetime.today()))
    return [current_date - index * 86400 for index in range(1,6)]

def prev_weather(result_dataframe):
    urls = []
    day_list = prev_5_days()
    cities_and_coordinates = calculate_central_area(result_dataframe)
    cities = list(np.repeat([city for city in cities_and_coordinates],5))
    coordinates = [coordinate for coordinate in cities_and_coordinates.values()]
    weather_list = []
    for coordinate in coordinates:
        for timestamp in day_list:
            urls.append(f"https://api.openweathermap.org/data/2.5/onecall/timemachine?lat={coordinate[0]}&lon={coordinate[1]}&dt={timestamp}&appid={key}")
    with ThreadPoolExecutor(max_workers=len(urls)) as pool:
        responses = pool.map(fetch, urls)
    for response in responses:
        weather_per_day = eval(response.text)
        daily_temperature = [element['temp'] for element in weather_per_day['hourly']]
        weather_list.append({"City":cities.pop(0),
                            "min_temp":min(daily_temperature),
                            "max_temp":max(daily_temperature),
                            "day":datetime.utcfromtimestamp(weather_per_day['hourly'][0]['dt']).strftime('%Y-%m-%d')})
    return pd.DataFrame(weather_list)


#
# def create_plots(df):
#     for city, dataframe in df.groupby(df["City"]):
#         df = dataframe.pivot_table(["max_temp", "min_temp"],"day")
#         print(city, df)
#         df.plot(kind = 'bar')
#         plt.ylabel("Temperature in Celsius")
#         plt.title(f"{city}")
#         plt.show()
# df = pd.read_csv("results.csv")


def create_csv(df):
    for city, dataframe in df.groupby(df["City"]):
        dataframe.to_csv(f"{city}.csv")


@logger.catch
def weather_script():
    top_hotels_df = define_address()
    weather_df = pd.concat([prev_weather(top_hotels_df), prev_weather(top_hotels_df)])















