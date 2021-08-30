import pandas as pd
from data_processing import define_address, get_cities_countries_central_coord, calc_central_coord



def mock_geocoder_setup(*args):
    def mock_geocoder(*args):
        return "Test address"
    return mock_geocoder


def test_define_addresses_function():
    test_df = pd.DataFrame({"Latitude": [100], "Longitude": [0]})
    geocoder = mock_geocoder_setup("somearg")
    result_df = define_address(test_df, 1, geocoder)
    assert result_df.Address.item() == "Test address"
    assert result_df.Latitude.item() == 100

def test_get_of_central_coordinates():
    test_df = pd.DataFrame({"Latitude": [60, 80], "Longitude": [100,100]})
    assert calc_central_coord(test_df) == (70,100)

def test_get_center_of_city():
    test_df = pd.DataFrame({"Latitude": [60, 80],
                            "Longitude": [100,100],
                            "City":["Fakecity", "Fakecity"],
                            "Country":["fakecountry", "fakecountry"]})
    assert get_cities_countries_central_coord(test_df) == [["Fakecity"], ["fakecountry"], [(70,100)]]
