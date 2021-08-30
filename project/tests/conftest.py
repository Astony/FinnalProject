import os
from pathlib import Path
from zipfile import ZipFile

import pandas as pd
import pytest


@pytest.fixture
def create_csv():
    dataframe = pd.DataFrame(
        {
            "City": ["Tumen", "Togliatty", "Tumen", "Amsterdam"],
            "Country": ["RU", "RU", "RU", "NL"],
            "Hotel": ["a", "a", "a", "a"],
            "Latitude": [1, 1, 1, 1],
            "Longitude": [1, 1, 1, 1],
            "Name": ["a", "a", "a", "a"],
        }
    )
    Path("output_folder").mkdir()
    dataframe.to_csv("output_folder/1.csv")
    yield
    Path("output_folder/1.csv").unlink()
    Path("output_folder").rmdir()


@pytest.fixture
def delete_dir_and_files():

    yield
    Path("tests/output_folder/test.csv").unlink()
    Path("tests/output_folder").rmdir()

@pytest.fixture
def create_dir():
    Path("tests/output_folder").mkdir()
    Path("tests/output_folder/Testcountry").mkdir()
    Path("tests/output_folder/Testcountry/Testcity").mkdir()
    yield
    os.remove("tests/output_folder/Testcountry/Testcity/Testcity.png")
    Path("tests/output_folder/Testcountry/Testcity").rmdir()
    Path("tests/output_folder/Testcountry").rmdir()
    Path("tests/output_folder").rmdir()

@pytest.fixture
def create_dir_for_csv():
    Path("tests/output_folder").mkdir()
    Path("tests/output_folder/Testcountry").mkdir()
    Path("tests/output_folder/Testcountry/Testcity").mkdir()
    yield
    os.remove("tests/output_folder/Testcountry/Testcity/Testcity_1.csv")
    os.remove("tests/output_folder/Testcountry/Testcity/Testcity_2.csv")
    Path("tests/output_folder/Testcountry/Testcity").rmdir()
    Path("tests/output_folder/Testcountry").rmdir()
    Path("tests/output_folder").rmdir()
