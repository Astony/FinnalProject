import math
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
from loguru import logger


def create_plots(
    city: str, country: str, output_path: str, dataframe: pd.DataFrame
) -> None:
    """Function that create bar plots with min and max temperature per day in city
    :param city: Specific city
    :type city: str
    :param country: Specific country
    :type country: str
    :param output_path: Path to folder where you should save results
    :type output_path: str
    :param dataframe: Dataframe with information about weather in city
    :type dataframe: pd.DataFrame
    :return: None
    :rtype: None
    """
    df_for_save.plot(kind="bar", color={"max_temp": "orange", "min_temp": "blue"})
    df_for_save.plot(kind="bar")
    plt.ylabel("Temperature in Celsius")
    plt.title(f"{city} {country}")
    plt.savefig(f"{output_path}/output_folder/{country}/{city}/{city}.png")


def find_max_temp(city: str, dataframe: pd.DataFrame) -> str:
    """Function that finds max temp in city per specific period
    :param city: Specific city
    :type city: str
    :param dataframe: Dataframe with information about weather in city
    :type dataframe: pd.DataFrame
    :return: String with info about max temp in city per specific period
    :rtype: str
    """
    return f"The max temp in {city} per {dataframe[dataframe.max_temp == dataframe.max_temp.max()].day.item()} is {dataframe.max_temp.max()}"


def max_deviation_of_max_temp(city: str, dataframe: pd.DataFrame) -> str:
    """Function that finds max deviation of max temp in city per specific period
    :param city: Specific city
    :type city: str
    :param dataframe: Dataframe with information about weather in city
    :type dataframe: pd.DataFrame
    :return: String with info about max deviation of max temp in city per specific period
    :rtype: str
    """
    std_score = dataframe.max_temp.max() - dataframe.max_temp.min()
    return (
        f"The max deviation of max temp is in {city} for period is {round(std_score,3)}"
    )


def find_min_temp(city: str, dataframe: pd.DataFrame) -> str:
    """Function that finds min temp in city per specific period
    :param city: Specific city
    :type city: str
    :param dataframe: Dataframe with information about weather in city
    :type dataframe: pd.DataFrame
    :return: String with info about min temp in city per specific period
    :rtype: str
    """
    return f"The min temp in {city} per {dataframe[dataframe.min_temp == dataframe.min_temp.min()].day.item()} is {dataframe.min_temp.min()}"


def max_min_temp_difference(city: str, dataframe: pd.DataFrame) -> str:
    """Function that finds difference between max and min temperature in city per specific day
    :param city: Specific city
    :type city: str
    :param dataframe: Dataframe with information about weather in city
    :type dataframe: pd.DataFrame
    :return: String with info about difference between max and min temperature in city per specific day
    :rtype: str
    """

    max_difference = (dataframe["max_temp"] - dataframe["min_temp"]).max()
    row_with_max_difference = dataframe[
        (dataframe["max_temp"] - dataframe["min_temp"]) == max_difference
    ]
    return f"The max difference between max and min temperature in {city} per {row_with_max_difference['day'].item()} is {max_difference.item()}"


def write_weather_stat_in_file(
    city: str, country: str, output_path: str, dataframe: pd.DataFrame
) -> None:
    """Function that write files with weather statistics for city
    :param city: Specific city
    :type city: str
    :param country: Specific country
    :type country: str
    :param output_path: Path to folder where you should save results
    :type output_path: str
    :param dataframe: Dataframe with information about weather in city
    :type dataframe: pd.DataFrame
    :return: None
    :rtype: None
    """
    with open(
        f"{output_path}/output_folder/{country}/{city}/{city}_weather_statistics.txt",
        "w",
    ) as file:
        file.write(
            f"{find_max_temp(city, dataframe)}\n"
            f"{max_deviation_of_max_temp(city, dataframe)}\n"
            f"{find_min_temp(city, dataframe)}\n"
            f"{max_min_temp_difference(city, dataframe)}"
        )


def create_dir(city: str, country: str, output_path: str) -> None:
    """Function that creates folders for country and it's city
    :param city: Specific city
    :type city: str
    :param country: Specific country
    :type country: str
    :param output_path: Path to folder where you should save results
    :type output_path: str
    :return: None
    :rtype: None
    """
    Path(f"{output_path}/output_folder/{country}").mkdir()
    Path(f"{output_path}/output_folder/{country}/{city}").mkdir()


def save_csv(
    city: str, country: str, output_path: str, dataframe: pd.DataFrame
) -> None:
    """Save information about hotels in specific city into csv files no more than 100 rows into one csv
    :param city: Specific city
    :type city: str
    :param country: Specific country
    :type country: str
    :param output_path: Path to folder where you should save results
    :type output_path: str
    :param dataframe: Dataframe with information about weather in city
    :type dataframe: pd.DataFrame
    :return: None
    :rtype: None
    """
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
    """Save plots, statistics and hotels info for specific city
    :param output_path: Path to folder where you should save results
    :type output_path: str
    :param weather_df: Dataframe with information about weather in city
    :type weather_df: pd.DataFrame
    :param top_hotels_df_with_addresses: Dataframe with cities which contains of the most number of hotels in country
    :type top_hotels_df_with_addresses: pd.DataFrame
    :return: None
    :rtype: None
    """
    for city_country, df in weather_df.groupby(["City", "Country"]):
        create_dir(city_country[0], city_country[1], output_path)
        logger.info(f"Create dir {output_path}/{city_country[0]}/{city_country[1]}")
        write_weather_stat_in_file(city_country[0], city_country[1], output_path, df)
        logger.info(f"Write txt")
        create_plots(city_country[0], city_country[1], output_path, df)
        logger.info(f"Create plot")

    for city_country, df in top_hotels_df_with_addresses.groupby(["City", "Country"]):
        logger.info(f"Save csv")
        save_csv(city_country[0], city_country[1], output_path, df)
