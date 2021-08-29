from data_processing import define_address
import pandas as pd

def mock_geocoder_setup(*args):
    return "Test address"

def test_define_addresses_function(mocker):
    test_df = pd.DataFrame({"Latitude":[100], "Longitude":[0]})
    geocoder = mock_geocoder_setup
    result_df = define_address(test_df, geocoder,1)
    assert result_df.Address.item() == "Test address"