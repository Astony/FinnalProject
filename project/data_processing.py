import math
from concurrent.futures import ThreadPoolExecutor
from typing import List, Tuple

import pandas as pd
from geopy.extra.rate_limiter import RateLimiter
from geopy.geocoders import Here
from loguru import logger

GEOLOCATION_API_KEY = "dYrajXLId5Rnp_WKZHaonsR-SMy2Z0xOlPRM4uELmfY"


def geocoder_setup(geo_req_per_min: int) -> Here:
    """
    Setup limit of requests per minute for geocoder

    :param geo_req_per_min: number of requests per minute
    :type geo_req_per_min: int
    :return: geocoder obj for parsing geolocation info
    :rtype: Here
    """
    delay = 60 / geo_req_per_min
    """Function that return geocoder object with required setups"""
    geocoder = Here(
        apikey=GEOLOCATION_API_KEY, user_agent="weather.script.py", timeout=3
    )
    geocoder = RateLimiter(
        geocoder.reverse,
        min_delay_seconds=delay,
        max_retries=2,
        error_wait_seconds=5.0,
        swallow_exceptions=True,
        return_value_on_exception=None,
    )
    return geocoder


def define_address(top_cities_df: pd.DataFrame, workers: int, geocoder) -> pd.DataFrame:
    """Define and add addresses into top_hotels_in_city dataframe

    :param top_cities_df: dataframe with cities which contains of the most number of hotels in country
    :type top_cities_df: pd.DataFrame
    :param workers: number of threads for parallel processing
    :type workers: int
    :param geocoder: obj for parsing geolocation info
    :type geocoder: Here
    :return: input dataframe with information about addresses of hotels
    :rtype: pd.Dataframe
    """

    logger.info("Start to add addresses")
    with ThreadPoolExecutor(max_workers=workers) as pool:
        responses = pool.map(
            geocoder,
            zip(
                top_cities_df["Latitude"].tolist(), top_cities_df["Longitude"].tolist()
            ),
        )
    top_cities_df["Address"] = list(responses)
    logger.info("Added addresses for each hotel")
    return top_cities_df


def calc_central_coord(dataframe: pd.DataFrame) -> Tuple:
    """Calculate central coordinates

    :param dataframe: dataframe that contains of latitude and longitude columns
    :type dataframe: pd.Dataframe
    :return: tuple with central coordinates
    :rtype: Tuple
    """
    x, y, z = 0.0, 0.0, 0.0

    for i, coord in dataframe.iterrows():
        latitude = math.radians(float(coord.Latitude))
        longitude = math.radians(float(coord.Longitude))

        x += math.cos(latitude) * math.cos(longitude)
        y += math.cos(latitude) * math.sin(longitude)
        z += math.sin(latitude)

    total = len(dataframe)

    x = x / total
    y = y / total
    z = z / total

    central_longitude = math.atan2(y, x)
    central_square_root = math.sqrt(x * x + y * y)
    central_latitude = math.atan2(z, central_square_root)
    return math.degrees(central_latitude), math.degrees(central_longitude)


def get_cities_countries_central_coord(dataframe: pd.DataFrame) -> Tuple:
    """Get lists with all cities, countries and that's central coordinates

    :param dataframe: dataframe with info about city, country and coordinates
    :type dataframe: pd.Dataframe
    :return: lists with all cities, countries and that's central coordinates
    :rtype:  List
    """
    logger.info("Calculate central area")
    cities, countries, coordinates = [], [], []
    for city_country, dataframe in dataframe.groupby(["City", "Country"]):
        city, country = city_country
        cities.append(city)
        countries.append(country)
        coordinates.append(calc_central_coord(dataframe))
    return cities, countries, coordinates
