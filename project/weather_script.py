import argparse

import pandas as pd

from loguru import logger

from data_processing import city_center_coord, define_address, geocoder_setup
from post_processing_functions import save_main_info
from preparation_of_data_functions import primary_data_proc, unzip
from weather_api_functions import forecast_weather, prev_weather

logger.add("debug_info.txt", format="{time} {level} {message}", level="DEBUG")


def weather_script(
    init_data_path: str,
    output_path: str,
    workers: int,
    weatherAPI_rpm: int,
    geoAPI_rpm: int,
) -> None:
    """Main function that reads data from csv, processes it,
      defines addresses, defines weather in specific area, create plots,
      save info about weather in cities with the most number of hotels in country

    :param init_data_path: Path to folder with hotels.zip
    :type init_data_path: str
    :param output_path: Path to folder where you should save results
    :type output_path: str
    :param workers: number of threads for parallel processing
    :type workers: int
    :param weatherAPI_rpm: number of requests per minute for weather API
    :type weatherAPI_rpm: int
    :param geoAPI_rpm: number of requests per minute for geolocation API
    :type geoAPI_rpm: int
    :return: None
    """
    unzip(init_data_path, output_path)
    top_hotels_dataframe_without_addresses = primary_data_proc(output_path)
    geocoder = geocoder_setup(geoAPI_rpm)
    top_hotels_df_with_addresses = define_address(
        top_hotels_dataframe_without_addresses,
        workers,
        geocoder,
    )
    cities, countries, coordinates = city_center_coord(top_hotels_df_with_addresses)
    weather_df = pd.concat(
        [
            prev_weather(cities, countries, coordinates, workers, weatherAPI_rpm),
            forecast_weather(cities, countries, coordinates, workers, weatherAPI_rpm),
        ]
    )

    logger.info("Start to save results")
    save_main_info(output_path, weather_df, top_hotels_df_with_addresses)
    logger.info("Finish")


parser = argparse.ArgumentParser()
parser.add_argument("inp", type=str, help="path to directory with file hotels.zip")
parser.add_argument(
    "out", type=str, help="path to the directory where the results will be located"
)
parser.add_argument(
    "workers", type=int, help="number of threads for parallel data processing"
)
parser.add_argument(
    "weatherAPI_rpm", type=int, help="Number of requests per minute for weather API"
)
parser.add_argument(
    "geoAPI_rpm", type=int, help="Number of requests per minute for geolocation API"
)
args = parser.parse_args()

weather_script(args.inp, args.out, args.workers, args.weatherAPI_rpm, args.geoAPI_rpm)
