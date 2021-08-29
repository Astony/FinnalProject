import requests
from typing import List
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import math
from pandas import pd
from loguru import logger

WEATHER_API_KEY = "744f5ed08d92e8cf016db6d4d47560c3"

def get_urls(mode: str, coordinates: List) -> List:
    """Function that return list of urls for request to API depending on mode"""
    if mode == "forecast":
        return [
            f"https://api.openweathermap.org/data/2.5/onecall?lat={coordinate[0]}&lon={coordinate[1]}&exclude={'hourly'}&appid={WEATHER_API_KEY}"
            for coordinate in coordinates
        ]
    elif mode == "historical":
        urls = []
        current_date = int(datetime.timestamp(datetime.today()))
        day_list = [current_date - index * 86400 for index in range(1, 6)]
        for coordinate in coordinates:
            for timestamp in day_list:
                urls.append(
                    f"https://api.openweathermap.org/data/2.5/onecall/timemachine?lat={coordinate[0]}&lon={coordinate[1]}&dt={timestamp}&appid={WEATHER_API_KEY}"
                )
        return urls

def fetch(url: str) -> requests:
    return requests.get(url)


def forecast_weather(
    cities: List, countries: List, coordinates: List, workers: int
) -> pd.DataFrame:
    """Function that returns dataframe with information about temperature in city's central area per current day and in next 7 days"""
    forecast_weather_list = []
    urls = get_urls("forecast", coordinates)
    city_index = 0
    with ThreadPoolExecutor(max_workers=workers) as pool:
        responses = pool.map(fetch, urls)
    for response in responses:
        weather_per_week = eval(response.text)["daily"]
        for day in weather_per_week:
            forecast_weather_list.append(
                {
                    "City": cities[math.floor(city_index / 8)],
                    "Country": countries[math.floor(city_index / 8)],
                    "min_temp": round((day["temp"]["min"] - 273), 3),
                    "max_temp": round((day["temp"]["max"] - 273), 3),
                    "day": datetime.utcfromtimestamp(day["dt"]).strftime("%Y-%m-%d"),
                }
            )
            city_index += 1
    logger.info("Predict weather in 7 next days")
    return pd.DataFrame(forecast_weather_list)


def prev_weather(
    cities: List, countries: List, coordinates: List, workers: int
) -> pd.DataFrame:
    """Function that returns dataframe with information about temperature in city's central area in previous 5 days"""

    prev_weather_list = []
    urls = get_urls("historical", coordinates)
    city_index = 0
    with ThreadPoolExecutor(max_workers=workers) as pool:
        responses = pool.map(fetch, urls)
    for response in responses:
        weather_per_day = eval(response.text)
        daily_temperature = [element["temp"] for element in weather_per_day["hourly"]]
        prev_weather_list.append(
            {
                "City": cities[math.floor(city_index / 5)],
                "Country": countries[math.floor(city_index / 5)],
                "min_temp": round((min(daily_temperature) - 273), 3),
                "max_temp": round((max(daily_temperature) - 273), 3),
                "day": datetime.utcfromtimestamp(
                    weather_per_day["hourly"][0]["dt"]
                ).strftime("%Y-%m-%d"),
            }
        )
        city_index += 1
    logger.info("Get information about weather on previous week")
    return pd.DataFrame(prev_weather_list)