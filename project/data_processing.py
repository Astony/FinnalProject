import math
from concurrent.futures import ThreadPoolExecutor
from typing import Dict

import pandas as pd
from geopy.extra.rate_limiter import RateLimiter
from geopy.geocoders import Here
from loguru import logger

GEOLOCATION_API_KEY = "dYrajXLId5Rnp_WKZHaonsR-SMy2Z0xOlPRM4uELmfY"


def geocoder_setup(limit=True) -> Here:
    """Function that return geocoder object with required setups"""
    geocoder = Here(
        apikey=GEOLOCATION_API_KEY, user_agent="weather.script.py", timeout=3
    )
    if limit:
        geocoder = RateLimiter(geocoder.reverse, min_delay_seconds=0.1, max_retries=2, error_wait_seconds=5.0,
                               swallow_exceptions=True, return_value_on_exception=None)
    return geocoder


def define_address(
    top_cities_df: pd.DataFrame, geocoder: Here, workers: int
) -> pd.DataFrame:
    """Function that adds addresses into top_hotels_in_city dataframe"""
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


def get_coordinates_of_central(dataframe):
    """Function that calculates teh center area with hotels"""
    x = 0.0
    y = 0.0
    z = 0.0

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


def get_centr(dataframe: pd.DataFrame) -> Dict:
    """Function that returns dictionary with (city, country) and it's center"""
    logger.info("Calculate central area")
    return {
        city_country: get_coordinates_of_central(dataframe)
        for city_country, dataframe in dataframe.groupby(["City", "Country"])
    }
