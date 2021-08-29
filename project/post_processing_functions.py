import matplotlib.pyplot as plt
from loguru import logger
import pandas as pd
from pathlib import Path
import math


def create_plots(
    city: str, country: str, output_path: str, dataframe: pd.DataFrame
) -> None:
    """Function that create bar plots with mon and max temperature per day in city"""
    df_for_save = dataframe.pivot_table(["max_temp", "min_temp"], "day")
    df_for_save.plot(kind="bar")
    plt.ylabel("Temperature in Celsius")
    plt.title(f"{city} {country}")
    plt.savefig(f"{output_path}/output_folder/{country}/{city}/{city}.png")


def find_max_temp(city: str, dataframe: pd.DataFrame) -> str:
    """Function that finds max temp in city"""
    return f"The max temp in {city} per {dataframe[dataframe.max_temp == dataframe.max_temp.max()].day.max()} is {dataframe.max_temp.max()}"


def max_deviation_of_max_temp(city: str, dataframe: pd.DataFrame) -> str:
    """Function that finds max deviation of max temp in city"""
    std_score = (
        dataframe.max_temp - dataframe.max_temp.mean()
    ) / dataframe.max_temp.std()
    return f"The max deviation of max temp is in {city} for period is {round(std_score.abs().max(),3)}"


def find_min_temp(city: str, dataframe: pd.DataFrame) -> str:
    """Function that finds min temp in city"""
    return f"The min temp in {city} per {dataframe[dataframe.max_temp == dataframe.max_temp.min()].day.max()} is {dataframe.max_temp.min()}"


def max_deviation_of_temp(city: str, dataframe: pd.DataFrame) -> str:
    """Function that finds difference between max and min temperature in city"""
    std_score = dataframe.max_temp - dataframe.min_temp
    return f"The max deviation of temp is in {city} for period is {round(std_score.abs().max(),3)}"


def write_in_file(
    city: str, country: str, output_path: str, dataframe: pd.DataFrame
) -> None:
    """Function that write files with weather statistics for city"""
    with open(
        f"{output_path}/output_folder/{country}/{city}/{city}_weather_statistics.txt",
        "w",
    ) as file:
        file.write(
            f"{find_max_temp(city, dataframe)}\n"
            f"{max_deviation_of_max_temp(city, dataframe)}\n"
            f"{find_min_temp(city, dataframe)}\n"
            f"{max_deviation_of_temp(city, dataframe)}"
        )


def create_dir(city: str, country: str, output_path: str) -> None:
    """Function that creates folders for country and it's city"""
    Path(f"{output_path}/output_folder/{country}").mkdir()
    Path(f"{output_path}/output_folder/{country}/{city}").mkdir()


def save_csv(
    city: str, country: str, output_path: str, dataframe: pd.DataFrame
) -> None:
    """Save information about hotels in specific city into csv file"""
    size_of_csv = 99
    number_of_files = math.ceil(len(dataframe) / size_of_csv)
    for idx in range(number_of_files):
        df_chunk = dataframe[size_of_csv * idx : size_of_csv * (idx + 1)]
        df_chunk.to_csv(
            f"{output_path}/output_folder/{country}/{city}/{city}_{idx + 1}.csv",
            index=False,
        )


def save_main_info(
    output_path: str,
    weather_df: pd.DataFrame,
    top_hotels_df_with_addresses: pd.DataFrame,
) -> None:
    """Save plots, statistics and hotels info for specific city"""
    for city_country, df in weather_df.groupby(["City", "Country"]):
        create_dir(city_country[0], city_country[1], output_path)
        logger.info(f"Create dir {output_path}/{city_country[0]}/{city_country[1]}")
        write_in_file(city_country[0], city_country[1], output_path, df)
        logger.info(f"Write txt")
        create_plots(city_country[0], city_country[1], output_path, df)
        logger.info(f"Create plot")

    for city_country, df in top_hotels_df_with_addresses.groupby(["City", "Country"]):
        logger.info(f"Save csv")
        save_csv(city_country[0], city_country[1], output_path, df)