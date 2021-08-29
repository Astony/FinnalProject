import pandas as pd
from data_processing import define_address, get_centr, get_coordinates_of_central



def mock_geocoder_setup(*args):
    return "Test address"


def test_define_addresses_function(mocker):
    test_df = pd.DataFrame({"Latitude": [100], "Longitude": [0]})
    geocoder = mock_geocoder_setup
    result_df = define_address(test_df, geocoder, 1)
    assert result_df.Address.item() == "Test address"

def test_get_of_central_coordinates():
    test_df = pd.DataFrame({"Latitude": [60, 80], "Longitude": [100,100]})
    assert get_coordinates_of_central(test_df) == (70,100)

def test_get_center_of_city():
    test_df = pd.DataFrame({"Latitude": [60, 80],
                            "Longitude": [100,100],
                            "City":["Fakecity", "Fakecity"],
                            "Country":["fakecountry", "fakecountry"]})
    assert get_centr(test_df) == {("Fakecity", "fakecountry"):(70,100)}
