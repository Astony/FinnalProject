from pathlib import Path
from zipfile import ZipFile

import pandas as pd
import pytest


@pytest.fixture
def create_zip_file_and_del_it():
    """Create zip file for test unzip func and then remove all test files and directories"""
    with ZipFile("project/tests/hotels.zip", "w") as zip:
        with zip.open("test.csv", "w") as f:
            pass
    yield
    Path("project/tests/hotels.zip").unlink()
    Path("project/tests/output_folder/test.csv").unlink()
    Path("project/tests/output_folder").rmdir()


@pytest.fixture
def create_csv_for_primary_data_proc_func():
    """Create csv file for test primary_data_proc function with only one correct row
    and then remove all test files and directories"""
    dataframe = pd.DataFrame(
        {
            "City": [
                "Tumen",
                "Togliatty",
                "Tumen",
                "Amsterdam",
                "Amsterdam",
                "Somecity",
            ],
            "Country": ["RU", "RU", "RU", "NL", "NL", "NL"],
            "Hotel": ["a", "a", "a", "a", "a", "a"],
            "Latitude": [1, 1, 1, 1, 1, 1],
            "Longitude": [1, 1, 1, 1, 1, 1],
            "Name": ["a", "a", "a", "a", "a", "a"],
        }
    )
    Path("output_folder").mkdir()
    dataframe.to_csv("output_folder/1.csv")
    yield
    Path("output_folder/1.csv").unlink()
    Path("output_folder").rmdir()


def create_out_testcountry_testcity_folder():
    """Create directories for test post processing functions"""
    Path("project/tests/output_folder").mkdir()
    Path("project/tests/output_folder/Testcountry").mkdir()
    Path("project/tests/output_folder/Testcountry/Testcity").mkdir()


def delete_out_testcountry_testcity_folder():
    """Delete directories after test post processing functions"""
    Path("project/tests/output_folder/Testcountry/Testcity").rmdir()
    Path("project/tests/output_folder/Testcountry").rmdir()
    Path("project/tests/output_folder").rmdir()


@pytest.fixture
def create_dir_for_test_create_plot_func_and_delete_it():
    """Start test for create_plot func and then remove all test files and directories"""
    create_out_testcountry_testcity_folder()
    yield
    Path("project/tests/output_folder/Testcountry/Testcity/Testcity.png").unlink()
    delete_out_testcountry_testcity_folder()


@pytest.fixture
def create_dir_for_test_create_csv_func_and_del_it():
    """Start test for create_csv func and then remove all test files and directories"""
    create_out_testcountry_testcity_folder()
    yield
    Path("project/tests/output_folder/Testcountry/Testcity/Testcity_1.csv").unlink()
    Path("project/tests/output_folder/Testcountry/Testcity/Testcity_2.csv").unlink()
    delete_out_testcountry_testcity_folder()
