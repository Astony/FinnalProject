from zipfile import ZipFile
from loguru import logger
import pandas as pd
from pathlib import Path
from geopy.geocoders import Here
from concurrent.futures import ThreadPoolExecutor
from geopy.extra.rate_limiter import RateLimiter
import math
import requests
from datetime import datetime
import matplotlib.pyplot as plt
# from project.get_central_coord import get_coordinates_of_central
from get_central_coord import get_coordinates_of_central


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


def unzip(init_data_path, output_path):
    "Function that extracts all csv files to main directory"
    Path(f"{output_path}/output_folder").mkdir()
    with ZipFile(f'{init_data_path}/hotels.zip', 'r') as zip_obj:
        zip_obj.extractall(Path(f"{output_path}/output_folder"))
    logger.info("Extract all csv into current dir")


def primary_data_proc(output_path):
    """Function that return dataframe with cities which have the most number of hotels"""
    all_df_list = (filter_df_from_invalid_rows(pd.read_csv(path, encoding='utf-8')) for path in Path(f"{output_path}/output_folder").glob("*.csv"))
    result_frame = pd.concat(all_df_list, ignore_index=True)
    sorted_df_by_num_of_hotels = result_frame[['City', "Country"]].value_counts()[:].sort_values(ascending=False)
    top_hotels_country_and_city = {}
    for pair in sorted_df_by_num_of_hotels.to_dict().items():
        country = pair[0][1]
        city = pair[0][0]
        if country not in top_hotels_country_and_city and city not in top_hotels_country_and_city.values():
            top_hotels_country_and_city[country] = city
    logger.info("Choose cities with the most number of hotels for every country")
    bool_list = [pair in  zip(top_hotels_country_and_city, top_hotels_country_and_city.values()) for pair in zip(result_frame["Country"].to_list(),result_frame["City"].to_list())]
    return result_frame[bool_list]


def geocoder_setup(limit=True):
    geocoder = Here(apikey=API_KEY, user_agent="weather.script.py", timeout=3)
    if limit:
        geocoder =  RateLimiter(geocoder.reverse, min_delay_seconds=0.1)
    return geocoder


def define_address(top_cities_df, geocoder, workers):
    """Function that adds addresses into dataframe"""
    logger.info("Start to add addresses")
    with ThreadPoolExecutor(max_workers=workers) as pool:
        responses = pool.map(geocoder, zip(top_cities_df['Latitude'].tolist(), top_cities_df["Longitude"].tolist()))
    top_cities_df["Address"] = list(responses)
    logger.info("Added addresses for each hotel")
    return top_cities_df


def calculate_central_area(result_df):
    """Function that returns dictionary with city and it's center"""
    logger.info("Calculate central area")
    return {city_country:get_coordinates_of_central(dataframe) for city_country, dataframe in result_df.groupby(["City","Country"])}


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
def forecast_weather(cities, countries, coordinates):
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
                "Country":countries[math.floor(city_index/8)],
                "min_temp":round((day["temp"]["min"]-273), 3),
                "max_temp": round((day["temp"]["max"]-273), 3),
                "day":datetime.utcfromtimestamp(day["dt"]).strftime('%Y-%m-%d')})
            city_index += 1
    logger.info("Predict weather in 7 next days")
    return pd.DataFrame(forecast_weather_list)


def prev_weather(cities, countries, coordinates):
    prev_weather_list = []
    urls = get_urls("historical", coordinates)
    city_index = 0
    with ThreadPoolExecutor(max_workers=len(urls)) as pool:
        responses = pool.map(fetch, urls)
    for response in responses:
        weather_per_day = eval(response.text)
        daily_temperature = [element['temp'] for element in weather_per_day['hourly']]
        prev_weather_list.append({"City":cities[math.floor(city_index/5)],
                                  "Country":countries[math.floor(city_index/5)],
                            "min_temp":round((min(daily_temperature)- 273),3),
                            "max_temp":round((max(daily_temperature)-273),3),
                            "day":datetime.utcfromtimestamp(weather_per_day['hourly'][0]['dt']).strftime('%Y-%m-%d')})
        city_index += 1
    logger.info("Get information about weather on previous week")
    return pd.DataFrame(prev_weather_list)



def create_plots(city, country, output_path, df):
    df = df.pivot_table(["max_temp", "min_temp"],"day")
    df.plot(kind = 'bar')
    plt.ylabel("Temperature in Celsius")
    plt.title(f"{city}")
    plt.savefig(f"{output_path}/output_folder/{country}/{city}/{city}.png")


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


def write_in_file(city, country, output_path, df):
    with open(f"{output_path}/output_folder/{country}/{city}/{city}.txt", "w") as file:
        file.write(f"{find_max_temp(city, df)}\n"
                   f"{max_deviation_of_max_temp(city, df)}\n"
                   f"{find_min_temp(city, df)}\n"
                   f"{max_deviation_of_temp(city, df)}")


def create_dir(city, country, output_path):
    Path(f"{output_path}/output_folder/{country}").mkdir()
    Path(f"{output_path}/output_folder/{country}/{city}").mkdir()


def save_csv(city, country, output_path, df):
    size_of_csv = 99
    number_of_files = math.ceil(len(df)/size_of_csv)
    for idx in range(number_of_files):
        df_chunk = df[size_of_csv * idx:size_of_csv * (idx + 1)]
        df_chunk.to_csv(f"{output_path}/output_folder/{country}/{city}/{city}_{idx + 1}.csv", index=False)


def save_main_info(output_path, weather_df, top_hotels_df_with_addresses):
    for city_country, df in weather_df.groupby(["City","Country"]):
        create_dir(city_country[0],city_country[1],output_path)
        logger.info(f"Create dir {output_path}/{city_country[0]}/{city_country[1]}")
        write_in_file(city_country[0],city_country[1],output_path,df)
        logger.info(f"Write txt")
        create_plots(city_country[0],city_country[1],output_path,df)
        logger.info(f"Create plot")

    for city_country, df in top_hotels_df_with_addresses.groupby(["City","Country"]):
        logger.info(f"Save csv")
        save_csv(city_country[0],city_country[1],output_path, df)


@logger.catch
def weather_script(init_data_path, output_path, workers):
    unzip(init_data_path, output_path)
    hotels_dataframe_without_addresses = primary_data_proc(output_path)
    geocoder = geocoder_setup()
    top_hotels_df_with_addresses = define_address(hotels_dataframe_without_addresses, geocoder, workers)

    cities_countries_central_coord = calculate_central_area(top_hotels_df_with_addresses)
    cities = [city_country[0] for city_country in cities_countries_central_coord]
    coordinates = [coordinate for coordinate in cities_countries_central_coord.values()]
    countries = [city_country[1] for city_country in cities_countries_central_coord]

    weather_df = pd.concat([prev_weather(cities, countries, coordinates),
                            forecast_weather(cities,countries,coordinates)])

    logger.info("Start to save results")
    save_main_info(output_path, weather_df, top_hotels_df_with_addresses)

weather_script("input", "output", 10)










