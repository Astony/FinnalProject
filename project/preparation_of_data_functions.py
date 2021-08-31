from pathlib import Path
from zipfile import ZipFile

import pandas as pd
from loguru import logger


def unzip(init_data_path: str, output_path: str) -> None:
    """Create output folder and extracts all csv files there

    :param init_data_path: Path to folder with hotels.zip
    :type init_data_path: str
    :param output_path: Path to folder where you should save results
    :type output_path: str
    :return: None
    """
    Path(f"{output_path}/output_folder").mkdir()
    with ZipFile(f"{init_data_path}/hotels.zip", "r") as zip_obj:
        zip_obj.extractall(Path(f"{output_path}/output_folder"))
    logger.info("Extract all csv into current dir")


def filter_df_from_invalid_rows(invalid_dataframe: pd.DataFrame) -> pd.DataFrame:
    """Filter data by name, longitude and latitude

    :param invalid_dataframe: Dataframe with invalid latitude, longitude, hotels name
    :type invalid_dataframe: pd.Dataframe
    :return: filtered dataframe woth correct latitude, longitude and hotels name
    :rtype: pd.DataFrame
    """
    df_with_correct_rows = invalid_dataframe[
        pd.to_numeric(invalid_dataframe["Latitude"], errors="coerce").notnull()
        & pd.to_numeric(invalid_dataframe["Longitude"], errors="coerce").notnull()
        & pd.to_numeric(invalid_dataframe["Name"], errors="coerce").isnull()
    ]

    filtered_df_by_lat_lon = df_with_correct_rows[
        (abs(df_with_correct_rows.Latitude.astype(float)) < 90)
        & (df_with_correct_rows.Longitude.astype(float) > -180)
        & (df_with_correct_rows.Longitude.astype(float) < 180)
    ]
    logger.info("Filtered all csv from invalid data")
    return filtered_df_by_lat_lon.dropna()


def primary_data_proc(output_path: str) -> pd.DataFrame:
    """Return dataframe with cities which have the most number of hotels

    :param output_path: Path to folder where you should save results
    :type output_path: str
    :return: Dataframe with cities which contains of the most number of hotels in country
    :rtype: pd.DataFrame
    """
    all_df_list = (
        filter_df_from_invalid_rows(pd.read_csv(path, encoding="utf-8"))
        for path in Path(f"{output_path}/output_folder").glob("*.csv")
    )
    result_frame = pd.concat(all_df_list, ignore_index=True)
    sorted_df_by_num_of_hotels = (
        result_frame[["City", "Country"]].value_counts()[:].sort_values(ascending=False)
    )

    top_hotels_country_and_city = {}
    for pair in sorted_df_by_num_of_hotels.to_dict().items():
        city, country = pair[0]
        if (
            country not in top_hotels_country_and_city
            and city not in top_hotels_country_and_city.values()
        ):
            top_hotels_country_and_city[country] = city

    logger.info("Choose cities with the most number of hotels for every country")
    bool_list = [
        pair in zip(top_hotels_country_and_city, top_hotels_country_and_city.values())
        for pair in zip(
            result_frame["Country"].to_list(), result_frame["City"].to_list()
        )
    ]
    return result_frame[bool_list]
