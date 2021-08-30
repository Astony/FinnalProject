from post_processing_functions import create_plots, find_max_temp, max_deviation_of_max_temp,\
    find_min_temp, max_deviation_of_temp
import pandas as pd
from pathlib import Path


def test_create_plots_func(create_dir):
    test_df = pd.DataFrame({"max_temp":[100], "min_temp":[200],"day":["the most lonely"]})
    create_plots("Testcity", "Testcountry","tests", test_df)
    p = Path("tests/output_folder/Testcountry/Testcity/Testcity.png")
    assert p.exists()

def test_find_max_temp_func():
    test_df = pd.DataFrame({"max_temp":[100,200,150],"day":[1,2,3]})
    assert find_max_temp("testcity", test_df) == "The max temp in testcity per 2 is 200"

def test_max_deviation_of_max_temp_func():
    test_df = pd.DataFrame({"max_temp":[100,1]})
    assert max_deviation_of_max_temp("testcity", test_df) == "The max deviation of max temp is in testcity for period is 99"

def test_find_min_temp_func():
    test_df = pd.DataFrame({"min_temp":[100,200,150],"day":[1,2,3]})
    assert find_min_temp("testcity", test_df) == "The min temp in testcity per 1 is 100"

def test_max_deviation_of_temp():
    test_df = pd.DataFrame({"min_temp":[-100,0,0],"max_temp":[100,1,1]})
    assert max_deviation_of_temp("testcity", test_df) == "The max deviation of temp is in testcity for period is 200"




