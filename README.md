# Introduction #

__weather_script__ is a console utility that allows you to get detailed information about the weather in the area of city with the largest number of hotels.
_______________________________________________________________________________________________________________________________________________________________________
## Functionality ##
Full functionality of this utility at running:

+ Unpacks csv files with information about cities, countries, hotels and their coordinates into the out / output_folder folder, where "out" is the directory that you choose to save the results.


+ Filters all csv files by hotels names, coordinates, empty values.


+ Filters files by cities with the largest number of hotels in the country and add an address for each hotel in multi-threaded mode.


+ Calculates the geographic center of a city area equidistant from the each hotel in city.


+ Gets weather data for the calculated center

    ->For the last 5 days

    ->For the current day

    ->For the next five days


+ Plots graphs of the dependence of the maximum and minimum temperature for each day from period that described above.


+ Gets the next statistics for each center:
  
    ->Day of observation with the maximum temperature for the period

    ->Maximum change of maximum temperature for the period

    ->Day of observation with the minimum temperature for the period

    ->Day with the maximum difference between the maximum and minimum temperatures.


+ Saves results into out/output_folder/country/city where "out" is the directory that you choose to save the results.

____________________________________________________________________________________________________________________________________
## Parameters ##

1) init_data_path: str

    The path to the directory with the hotels.zip file where placed csv files with information about hotels.


2)  output_path: str

    Path to folder where you want save results


3)  workers: int

    A number of threads for parallel processing


4)   weatherAPI_rpm: int

     A limit of requests per minute for weather API (recommended 60)


5)   geoAPI_rpm: int

     A limit of requests per minute for geolocation API (recommended 600)

_______________________________________________________________________________________________
## Running ##

To run this utility, enter in the console:

``F:\Users\Name\PycharmProjects\FinalProject\project python weather_script.py {init_data_path} {output_path} {workers} {weatherAPI_rpm} {geoAPI_rpm}``

Where values in {} are input parameters. Note that you should input parameters strictly in the order that was described above!


Example:
 
To quickly test the utility, run the following commands:


``pip install -r requirements.txt``
 
 
``cd project``
 
 
 ``mkdir output``
 
 

``F:\Users\Name\PycharmProjects\FinalProject\project python weather_script.py input output 100 60 600``


Test hotels.zip file is placed in project\input

For more information read documentation:

https://astony.github.io/Weather_script_documentation/


