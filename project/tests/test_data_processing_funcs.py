import pandas as pd
from data_processing import calc_central, city_center_coord, define_address


def mock_geocoder(*args):
    """Mock function for geocoder to avoid requests to API"""
    return "Test address"


def test_define_addresses_function():
    """Test that define_addresses_function add address that was got by geocoder"""
    test_df = pd.DataFrame({"Latitude": [100], "Longitude": [0]})
    geocoder = mock_geocoder
    result_df = define_address(test_df, 1, geocoder)
    assert result_df.Address.item() == "Test address"
    assert result_df.Latitude.item() == 100


def test_calc_central_coord():
    """Test that calc_central return the middle between to coordinates"""
    test_df = pd.DataFrame({"Latitude": [60, 80], "Longitude": [100, 100]})
    assert calc_central(test_df) == (70, 100)


def test_get_cities_countries_central_coord():
    """Test city_center_coord return the central coordinates for 2 hotels in one city"""
    test_df = pd.DataFrame(
        {
            "Latitude": [60, 80],
            "Longitude": [100, 100],
            "City": ["Fakecity", "Fakecity"],
            "Country": ["fakecountry", "fakecountry"],
        }
    )
    assert city_center_coord(test_df) == (
        ["Fakecity"],
        ["fakecountry"],
        [(70, 100)],
    )
