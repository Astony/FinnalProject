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
    os.remove("output_folder/1.csv")
    Path("output_folder").rmdir()


@pytest.fixture
def delete_dir_and_files():

    yield
    os.remove("tests/output_folder/test.csv")
    Path("tests/output_folder").rmdir()

@pytest.fixture
def create_dir():
    Path("tests/output_folder").mkdir()
    yield
    os.remove("tests/output_folder/Testcity.png")
    Path("tests/output_folder").rmdir()
