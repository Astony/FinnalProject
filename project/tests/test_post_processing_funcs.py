from pathlib import Path

import pandas as pd
from post_processing_functions import (
    create_plots,
    find_max_temp,
    find_min_temp,
    max_deviation_of_max_temp,
    max_min_temp_difference,
    save_csv,
)


def test_create_plots_func(create_dir_for_test_create_plot_func_and_delete_it):
    """Test that func create_plots creates png file in correct directory"""
    test_df = pd.DataFrame(
        {"max_temp": [100], "min_temp": [200], "day": ["the most lonely"]}
    )
    create_plots("Testcity", "Testcountry", "project/tests", test_df)
    p = Path("project/tests/output_folder/Testcountry/Testcity/Testcity.png")
    assert p.exists()


def test_find_max_temp_func():
    """Test find_max_temp returns the max temperature per given days"""
    test_df = pd.DataFrame({"max_temp": [100, 200, 150], "day": [1, 2, 3]})
    assert find_max_temp("testcity", test_df) == "The max temp in testcity per 2 is 200"


def test_max_deviation_of_max_temp():
    """Test max_deviation_of_max_temp returns difference between max temperatures per two days"""
    test_df = pd.DataFrame({"max_temp": [100, 1]})
    assert (
        max_deviation_of_max_temp("testcity", test_df)
        == "The max deviation of max temp is in testcity for period is 99"
    )


def test_find_min_temp_func():
    """Test find_min_temp returns the min temperature per given days"""
    test_df = pd.DataFrame({"min_temp": [100, 200, 150], "day": [1, 2, 3]})
    assert find_min_temp("testcity", test_df) == "The min temp in testcity per 1 is 100"


def test_max_min_temp_difference():
    """Test find_min_temp returns the max difference between max and min temperatures per day"""
    test_df = pd.DataFrame(
        {"min_temp": [-100, 0, 0], "max_temp": [100, 1, 1], "day": ["12", "13", "14"]}
    )
    assert (
        max_min_temp_difference("testcity", test_df)
        == "The max difference between max and min temperature in testcity per 12 is 200"
    )


def test_create_csv_function(create_dir_for_test_create_csv_func_and_del_it):
    """Test save_csv will divide big csv file with number of rows > 100 on several
    csv and each of them contains of less than 100 rows"""
    test_df = pd.DataFrame([{"Count": count} for count in range(150)])
    save_csv("Testcity", "Testcountry", "project/tests", test_df)
    p1 = Path("project/tests/output_folder/Testcountry/Testcity/Testcity_1.csv")
    p2 = Path("project/tests/output_folder/Testcountry/Testcity/Testcity_2.csv")
    assert p1.exists()
    assert p2.exists()
