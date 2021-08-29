import argparse
import pandas as pd
from loguru import logger

from data_processing import calc_centr, define_address, geocoder_setup
from post_processing_functions import save_main_info
from preparation_of_data_functions import primary_data_proc, unzip
from weather_api_functions import forecast_weather, prev_weather

logger.add("debug_info.txt", format="{time} {level} {message}", level="DEBUG")


def weather_script(init_data_path: str, output_path: str, workers: int) -> None:
    """Main script"""
    unzip(init_data_path, output_path)
    hotels_dataframe_without_addresses = primary_data_proc(output_path)
    geocoder = geocoder_setup()
    top_hotels_df_with_addresses = define_address(
        hotels_dataframe_without_addresses, geocoder, workers
    )

    cities_countries_central_coord = calc_centr(top_hotels_df_with_addresses)
    cities = [city_country[0] for city_country in cities_countries_central_coord]
    coordinates = [coordinate for coordinate in cities_countries_central_coord.values()]
    countries = [city_country[1] for city_country in cities_countries_central_coord]

    weather_df = pd.concat(
        [
            prev_weather(cities, countries, coordinates, workers),
            forecast_weather(cities, countries, coordinates, workers),
        ]
    )

    logger.info("Start to save results")
    save_main_info(output_path, weather_df, top_hotels_df_with_addresses)


parser = argparse.ArgumentParser()
parser.add_argument("inp", type=str, help="path to directory with file hotels.zip")
parser.add_argument(
    "out", type=str, help="path to the directory where the results will be located"
)
parser.add_argument(
    "workers", type=int, help="number of threads for parallel data processing"
)
args = parser.parse_args()

weather_script(args.inp, args.out, args.workers)
