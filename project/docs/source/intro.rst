Introduction
============
``weather_script`` is a console utility that allows you to get detailed information about the weather in the area of city with the largest number of hotels.

Functionality
****************
Full functionality of this utility at running:

1) Unpacks csv files with information about cities, countries, hotels and their coordinates into the out / output_folder folder, where "out" is the directory that you choose to save the results.

2) Filters all csv files by hotels names, coordinates, empty values.

3) Filters files by cities with the largest number of hotels in the country and add an address for each hotel in multi-threaded mode.

4) Calculate the geographic center of a city area equidistant from the each hotel in city.

5) Gets weather data for the calculated center
    ->For the last 5 days

    ->For the current day

    ->For the next five days

6) Plots graphs of the dependence of the maximum and minimum temperature for each day from period that described above.

7) Get the next statistics for each center:
    ->Day of observation with the maximum temperature for the period

    ->Maximum change of maximum temperature for the period

    ->Day of observation with the minimum temperature for the period

    ->Day with the maximum difference between the maximum and minimum temperatures.

8) Saves results into out/output_folder/country/city where "out" is the directory that you choose to save the results.

Parameters
***********

1) init_data_path: str

The path to the directory with the hotels.zip file where placed csv files with information about hotels are contained.

2)  output_path: str

    Path to folder where you should save results

3)  workers: int

    Number of threads for parallel processing

4)   weatherAPI_rpm: int

     Limit of requests per minute for weather API (recommended 60)

5)   geoAPI_rpm: int

     Limit of requests per minute for geolocation API (recommended 600)


Running
***********
To run this utility, enter in the console:

\path_to_directory_with_utility python weather_script.py {init_data_path} {output_path} {workers} {weatherAPI_rpm} {geoAPI_rpm}

Example:

F:\Users\utility_dir python weather_script.py input output 100 60 600

Test hotels.zip file is placed in project\input
