from zipfile import ZipFile
from loguru import logger
import pandas as pd
from pathlib import Path
from geopy.geocoders import Here
from concurrent.futures import ThreadPoolExecutor
import numpy as np
from geopy.extra.rate_limiter import RateLimiter
import math
import requests
from datetime import datetime
import time
from celery import shared_task
import matplotlib.pyplot as plt
# from project.get_central_coord import get_coordinates_of_central
from get_central_coord import get_coordinates_of_central
import json

API_KEY = 'dYrajXLId5Rnp_WKZHaonsR-SMy2Z0xOlPRM4uELmfY'

key = '744f5ed08d92e8cf016db6d4d47560c3'

logger.add("debug_info.txt", format="{time} {level} {message}", level="DEBUG")


def filter_df_from_invalid_rows(df):
    """Function that filters data by name, longitude and latitude"""
    df_with_correct_rows = df[pd.to_numeric(df["Latitude"], errors='coerce').notnull() &
                  pd.to_numeric(df["Longitude"], errors='coerce').notnull() &
                  ~pd.to_numeric(df["Name"], errors='coerce').notnull()]

    filtered_df_by_lat_lon = df_with_correct_rows[(abs(df_with_correct_rows.Latitude.astype(float)) <= 90) &
                        (df_with_correct_rows.Longitude.astype(float) >= -180) &
                        (df_with_correct_rows.Longitude.astype(float) <= 180)]
    logger.info("Filtered all csv from invalid data")
    return filtered_df_by_lat_lon.dropna()


def unzip():
    "Function that extracts all csv files to main directory"
    with ZipFile('hotels.zip', 'r') as zip_obj:
        zip_obj.extractall()
    logger.info("Extract all csv into current dir")


def primary_data_proc():
    """Function that return dataframe with cities which have the most number of hotels"""
    all_df_list = (filter_df_from_invalid_rows(pd.read_csv(path, encoding='utf-8')) for path in Path.cwd().glob("*.csv"))
    result_frame = pd.concat(all_df_list, ignore_index=True)
    sorted_df_by_num_of_hotels = result_frame[['City', "Country"]].value_counts()[:].sort_values(ascending=False)
    top_hotels_country_and_city = {}
    for pair in sorted_df_by_num_of_hotels.to_dict().items():
        country = pair[0][1]
        city = pair[0][0]
        if country not in top_hotels_country_and_city:
            top_hotels_country_and_city[country] = city
    logger.info("Choose cities with the most number of hotels for every country")
    return result_frame[result_frame.City.isin(top_hotels_country_and_city.values())]


def define_address(top_cities_df):
    """Function that adds addresses into dataframe"""
    geocoder = Here(apikey =API_KEY, user_agent = "weather.script.py", timeout=3)
    geocode = RateLimiter(geocoder.reverse, min_delay_seconds=0.1)
    with ThreadPoolExecutor(max_workers=10) as pool:
        responses = pool.map(geocode, zip(top_cities_df['Latitude'].tolist(), top_cities_df["Longitude"].tolist()))
    top_cities_df["Adress"] = np.array(responses)
    logger.info("Add addresses for each hotel")
    return top_cities_df


def calculate_central_area(result_df):
    """Function that returns dictionary with city and it's center"""
    logger.info("Calculate central area")
    return {city:get_coordinates_of_central(dataframe) for city, dataframe in result_df.groupby(["City"])}




def prev_5_days():
    current_date = int(datetime.timestamp(datetime.today()))
    return [current_date - index * 86400 for index in range(1,6)]


def get_urls(mode, coordinates):
    coordinates = [coordinate for coordinate in coordinates]
    if mode=="forecast":
        return [f"https://api.openweathermap.org/data/2.5/onecall?lat={coordinate[0]}&lon={coordinate[1]}&exclude={'hourly'}&appid={key}" for coordinate in coordinates]
    elif mode=="historical":
        urls = []
        day_list = prev_5_days()
        for coordinate in coordinates:
            for timestamp in day_list:
                urls.append(
                    f"https://api.openweathermap.org/data/2.5/onecall/timemachine?lat={coordinate[0]}&lon={coordinate[1]}&dt={timestamp}&appid={key}")
        return urls


def fetch(url):
    return requests.get(url)

@logger.catch
def forecast_weather(cities, coordinates):
    forecast_weather_list =[]
    urls = get_urls("forecast", coordinates)
    city_index = 0
    with ThreadPoolExecutor(max_workers=len(urls)) as pool:
        responses = pool.map(fetch, urls)
    for response in responses:
        weather_per_week = eval(response.text)['daily']
        for day in weather_per_week:
            forecast_weather_list.append({
                "City":cities[math.floor(city_index/8)],
                "min_temp":round((day["temp"]["min"]-273), 3),
                "max_temp": round((day["temp"]["max"]-273), 3),
                "day":datetime.utcfromtimestamp(day["dt"]).strftime('%Y-%m-%d')})
            city_index += 1
    logger.info("Predict weather in 7 next days")
    return pd.DataFrame(forecast_weather_list)






def prev_weather(cities, coordinates):
    prev_weather_list = []
    urls = get_urls("historical", coordinates)
    city_index = 0
    with ThreadPoolExecutor(max_workers=len(urls)) as pool:
        responses = pool.map(fetch, urls)
    for response in responses:
        weather_per_day = eval(response.text)
        daily_temperature = [element['temp'] for element in weather_per_day['hourly']]
        prev_weather_list.append({"City":cities[math.floor(city_index/5)],
                            "min_temp":round((min(daily_temperature)- 273),3),
                            "max_temp":round((max(daily_temperature)-273),3),
                            "day":datetime.utcfromtimestamp(weather_per_day['hourly'][0]['dt']).strftime('%Y-%m-%d')})
        city_index += 1
    logger.info("Get information about weather on previous week")
    return pd.DataFrame(prev_weather_list)


#
def create_plots(city, df):
    df = df.pivot_table(["max_temp", "min_temp"],"day")
    df.plot(kind = 'bar')
    plt.ylabel("Temperature in Celsius")
    plt.title(f"{city}")
    plt.savefig(Path.cwd() / city / f"{city}.png")


def find_max_temp(city, df):
    return(f"The max temp in {city} per {df[df.max_temp == df.max_temp.max()].day.max()} is {df.max_temp.max()}")


def max_deviation_of_max_temp(city, df):
  std_score = (df.max_temp - df.max_temp.mean()) / df.max_temp.std()
  return(f"The max deviation of max temp is in {city} for period is {round(std_score.abs().max(),3)}")


def find_min_temp(city, df):
    return(f"The min temp in {city} per {df[df.max_temp == df.max_temp.min()].day.max()} is {df.max_temp.min()}")


def max_deviation_of_temp(city, df):
  std_score = (df.max_temp - df.min_temp)
  return(f"The max deviation of temp is in {city} for period is {round(std_score.abs().max(),3)}")


def write_in_file(city, df):
    with open(Path.cwd() / city /f"{city}.txt", "w") as file:
        file.write(f"{find_max_temp(city, df)}\n"
                   f"{max_deviation_of_max_temp(city, df)}\n"
                   f"{find_min_temp(city, df)}\n"
                   f"{max_deviation_of_temp(city, df)}")


def create_dir(city):
    current_path = Path.cwd()
    Path(current_path / city).mkdir()


def save_csv(city, df):
    size_of_csv = 99
    number_of_files = math.ceil(len(df)/size_of_csv)
    for idx in range(number_of_files):
        df_chunk = df[size_of_csv * idx:size_of_csv * (idx + 1)]
        df_chunk.to_csv(Path.cwd() / f"{city}" / f'{city}_{idx + 1}.csv', index=False)


# @logger.catch
# def weather_script():
#     top_hotels_df = define_address()
#     cities_and_central_coord = calculate_central_area(top_hotels_df)
#     cities = [city for city in cities_and_coordinates]
#     coordinates = [coordinate for coordinate in cities_and_coordinates.values()]
#     weather_df = pd.concat([prev_weather(top_hotels_df), forecast_weather(top_hotels_df)])
#     # weather_df = pd.read_csv("weather.csv")
#     # weather_df.to_csv("weather.csv")
#     logger.info("Start to save results")
#     for city, df in weather_df.groupby("City"):
#         create_dir(city)
#         write_in_file(city, df)
#         create_plots(city, df)
#     for city, df in top_hotels_df.groupby("City"):
#         save_csv(city, df)

# with open("cities.json", "r") as file:
#     cities_and_coordinates = json.load(file)
# result_df = pd.read_csv("results.csv")
# cities =[city for city in cities_and_coordinates]
# coordinates = [coordinate for coordinate in cities_and_coordinates.values()]
#
# print(prev_weather(cities, coordinates))
# url = f"https://api.openweathermap.org/data/2.5/onecall/timemachine?lat={52.36300739181349}&lon={4.887544782213236}&dt={1629967939}&appid={key}"
# response = requests.get(url)
# print(response.text)
# testing_df = pd.DataFrame({"Name":["21345", "Spb", "America", "", "Sincity"],
#                    "Longitude":[100, 100, 10000, 100,100],
#                    "Latitude":[0, 0, 0, 100000, 100]})
print(primary_data_proc())


















