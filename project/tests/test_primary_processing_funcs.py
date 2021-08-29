from preparation_of_data_functions import filter_df_from_invalid_rows, primary_data_proc
from mock import patch
import pandas as pd
from pathlib import Path

def test_filter_func():
    testing_df = pd.DataFrame({"Name":["21345", "Spb", "America", "", "Sincity"],
                   "Longitude":[100, 100, 10000, 100,100],
                   "Latitude":[0, 0, 0, 100000, 100]})
    result_df = filter_df_from_invalid_rows(testing_df)
    assert len(result_df) == 1
    assert result_df.Name.item() == "Spb"

def test_primary_data_proc(create_csv):
    test_frame = primary_data_proc("project")
    assert test_frame["City"].to_list() == ['Tumen', "Tumen", "Amsterdam"]



