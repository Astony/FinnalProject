from zipfile import ZipFile
from loguru import logger
import pandas as pd
from pathlib import Path
from geopy.geocoders import  Here
from concurrent.futures import ThreadPoolExecutor
import numpy as np
from geopy.extra.rate_limiter import RateLimiter





API_KEY = 'dYrajXLId5Rnp_WKZHaonsR-SMy2Z0xOlPRM4uELmfY'


def filter(df):
    """Function that filters data by name, longitude and latitude"""
    correct_df = df[pd.to_numeric(df["Latitude"], errors='coerce').notnull() &
                  pd.to_numeric(df["Longitude"], errors='coerce').notnull() &
                  ~pd.to_numeric(df["Name"], errors='coerce').notnull()]
    filtered_df = correct_df[(abs(correct_df.Latitude.astype(float)) <= 90) &
                        (correct_df.Longitude.astype(float) >= -180) &
                        (correct_df.Longitude.astype(float) <= 180)]
    return filtered_df


def unzip():
    with ZipFile('hotels.zip', 'r') as zip_obj:
        zip_obj.extractall()


@logger.catch
def primary_data_proc():
    unzip()
    all_df_list = (filter(pd.read_csv(path, encoding='utf-8').dropna()) for path in Path.cwd().glob("*.csv"))
    result_frame = pd.concat(all_df_list, ignore_index=True)
    countries = result_frame[['City', "Country"]].value_counts()[:].sort_values(ascending=False)
    top_hotels = {}
    for pair in countries.to_dict().items():
        if pair[0][1] not in top_hotels:
            top_hotels[pair[0][1]] = pair[0][0]
    countries_with_most_num_of_hotels = result_frame[result_frame.City.isin(top_hotels.values())]

    return countries_with_most_num_of_hotels



def define_address():

    """Function that adds addresses into dataframe"""
    df = primary_data_proc()
    geocoder = Here(apikey =API_KEY, user_agent = "weather.script.py", timeout=3)
    geocode = RateLimiter(geocoder.reverse, min_delay_seconds=0.5)
    with ThreadPoolExecutor(max_workers=10) as pool:
        responses = pool.map(geocode, zip(df['Latitude'].tolist(), df["Longitude"].tolist()))
    df["Adress"] = np.array(responses)
    return df





