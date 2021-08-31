import math
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from typing import Callable, List

import pandas as pd
import requests
from loguru import logger
from ratelimit import RateLimitException, limits, sleep_and_retry
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

WEATHER_API_KEY = "744f5ed08d92e8cf016db6d4d47560c3"


def setup_get_method(rpm: int) -> Callable:
    """Setup limit of requests per minute, backoff and timeout methods for requests.get method

    :param rpm: number of requests per minute
    :type rpm: int
    :return: get method with additional functionality
    :rtype: Callable
    """

    @sleep_and_retry
    @limits(calls=rpm, period=60)
    def get_session(url: str) -> requests.models.Response:
        """

        :param url: URL for get information about weather using OpenWeatherMap API
        :type url: str
        :return: API response with weather information
        :rtype: requests.models.Response
        """
        retry = Retry(
            total=3,
            status_forcelist=[429, 500, 502, 503, 504],
            method_whitelist=["HEAD", "GET", "OPTIONS"],
        )
        retry_adapter = HTTPAdapter(max_retries=retry)
        session = requests.Session()
        session.mount("https://", retry_adapter)
        session.mount("http://", retry_adapter)
        return session.get(url, timeout=3)

    return get_session


def get_urls(mode: str, coordinates: List) -> List:
    """Function that return list of urls for API requests depending on mode and coordinates

    :param mode: Determine what type of weather information will be received from the output list of URLs
    :type mode: str
    :param coordinates: Pair of latitide,longitude of area where you want to know weather
    :type coordinates: Tuple
    :return: List of urls for requests to API
    :rtype: List
    """
    if mode == "forecast":
        return [
            f"https://api.openweathermap.org/data/2.5/onecall?lat={coordinate[0]}&lon={coordinate[1]}&exclude={'hourly'}&appid={WEATHER_API_KEY}"
            for coordinate in coordinates
        ]
    elif mode == "historical":
        urls = []
        current_date = int(datetime.timestamp(datetime.today()))
        seconds_in_day = 86400
        day_list = [current_date - index * seconds_in_day for index in range(1, 6)]
        for coordinate in coordinates:
            for timestamp in day_list:
                urls.append(
                    f"https://api.openweathermap.org/data/2.5/onecall/timemachine"
                    f"?lat={coordinate[0]}&lon={coordinate[1]}&dt={timestamp}&appid={WEATHER_API_KEY}"
                )

        return urls


def forecast_weather(
    cities: List,
    countries: List,
    coordinates: List,
    workers: int,
    rpm: int,
    setup_get_method: Callable = setup_get_method,
) -> pd.DataFrame:
    """Function that returns dataframe with information about temperature in city's central area per current day and in next 5 days

    :param cities: List with cities
    :type cities: List
    :param countries:List with countries
    :type countries: List
    :param coordinates:List with coordinates
    :type coordinates: List
    :param workers: number of threads for parallel processing
    :type workers: int
    :param rpm: number of requests per minute
    :type rpm: int
    :param setup_get_method: func that return get method with additional functionality
    :type setup_get_method: Callable
    :return: DataFrame with weather info in specific city in current day and next 5 days
    :rtype: pd.DataFrame
    """
    forecast_weather_list = []
    urls = (
        f"https://api.openweathermap.org/data/2.5/onecall?"
        f"lat={coordinate[0]}&lon={coordinate[1]}&exclude={'hourly'}&appid={WEATHER_API_KEY}"
        for coordinate in coordinates
    )
    city_index = 0
    get_method = setup_get_method(rpm)
    with ThreadPoolExecutor(max_workers=workers) as pool:
        responses = pool.map(get_method, urls)
    for response in responses:
        weather_per_week = eval(response.text)["daily"]
        for day in weather_per_week[:-2]:
            forecast_weather_list.append(
                {
                    "City": cities[math.floor(city_index / 6)],
                    "Country": countries[math.floor(city_index / 6)],
                    "min_temp": round((day["temp"]["min"] - 273), 3),
                    "max_temp": round((day["temp"]["max"] - 273), 3),
                    "day": datetime.utcfromtimestamp(day["dt"]).strftime("%Y-%m-%d"),
                }
            )
            city_index += 1
    logger.info("Predict weather in 5 next days")
    return pd.DataFrame(forecast_weather_list)


def prev_weather(
    cities: List,
    countries: List,
    coordinates: List,
    workers: int,
    rpm: int,
    setup_get_method: Callable = setup_get_method,
) -> pd.DataFrame:
    """Function that returns dataframe with information about temperature in city's central area in previous 5 days

    :param cities: List with cities
    :type cities: List
    :param countries:List with countries
    :type countries: List
    :param coordinates:List with coordinates
    :type coordinates: List
    :param workers: number of threads for parallel processing
    :type workers: int
    :param rpm: number of requests per minute
    :type rpm: int
    :param setup_get_method: func that return get method with additional functionality
    :type setup_get_method: Callable
    :return: DataFrame with weather info in specific city in current day and next 5 days
    :rtype: pd.DataFrame
    """

    prev_weather_list = []
    get_method = setup_get_method(rpm)
    city_index = 0
    current_date = int(datetime.timestamp(datetime.today()))
    seconds_in_day = 86400
    day_list = [current_date - index * seconds_in_day for index in range(1, 6)]
    urls = (
        f"https://api.openweathermap.org/data/2.5/onecall/timemachine"
        f"?lat={coordinate[0]}&lon={coordinate[1]}&dt={timestamp}&appid={WEATHER_API_KEY}"
        for coordinate in coordinates
        for timestamp in day_list
    )
    with ThreadPoolExecutor(max_workers=workers) as pool:
        responses = pool.map(get_method, urls)
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
