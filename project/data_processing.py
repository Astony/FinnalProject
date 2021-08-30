import math
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, Tuple, List

import pandas as pd
from geopy.extra.rate_limiter import RateLimiter
from geopy.geocoders import Here
from loguru import logger

GEOLOCATION_API_KEY = "dYrajXLId5Rnp_WKZHaonsR-SMy2Z0xOlPRM4uELmfY"


def geocoder_setup(geo_req_per_min) -> Here:
    delay = 60 / geo_req_per_min
    """Function that return geocoder object with required setups"""
    geocoder = Here(
        apikey=GEOLOCATION_API_KEY, user_agent="weather.script.py", timeout=3
    )
    geocoder = RateLimiter(geocoder.reverse, min_delay_seconds=delay, max_retries=2, error_wait_seconds=5.0,
                            swallow_exceptions=True, return_value_on_exception=None)
    return geocoder




def define_address(
    top_cities_df: pd.DataFrame, workers: int, geocoder
) -> pd.DataFrame:
    """Define and add addresses into top_hotels_in_city dataframe"""

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
    """Calculate central coordinates"""
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


def get_cities_countries_central_coord(dataframe: pd.DataFrame) -> List:
    """Get dictionary with (city, country) and it's center"""
    logger.info("Calculate central area")
    city_country_centr =  {
        city_country: calc_central_coord(dataframe)
        for city_country, dataframe in dataframe.groupby(["City", "Country"])
    }
    return [[city_country[0] for city_country in city_country_centr],
            [city_country[1] for city_country in city_country_centr],
    [coordinate for coordinate in city_country_centr.values()],
    ]