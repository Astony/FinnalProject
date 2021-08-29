import pytest
import pandas as pd
import os

@pytest.fixture
def create_csv():
    dataframe = pd.DataFrame({'City':['Tumen', "Togliatty","Tumen", "Amsterdam"],
                 'Country':['RU', 'RU', "RU", "NL"],
                 'Hotel':["a","a","a","a"],
                 'Latitude':[1,1,1,1],
                "Longitude":[1, 1, 1, 1],
                "Name":["a","a","a","a"]}
    )
    dataframe.to_csv("1.csv")
    yield
    os.remove("1.csv")


