from pathlib import Path

import pandas as pd
from preparation_of_data_functions import (
    filter_df_from_invalid_rows,
    primary_data_proc,
    unzip,
)


def test_unzip_func(create_zip_file_and_del_it):
    """Test that unzip function extract all files into output dir"""
    unzip("project/tests", "project/tests")
    p = Path("project/tests/output_folder/test.csv")
    assert p.exists()


def test_filter_func():
    """Test that filter function filter all invalid data from df"""
    testing_df = pd.DataFrame(
        {
            "Name": ["21345", "Russia", "America", None, "Sincity"],
            "Longitude": [0, 50, "a", 50, 100],
            "Latitude": [0, 50, 5, 50, 10000],
        }
    )
    result_df = filter_df_from_invalid_rows(testing_df)
    assert len(result_df) == 1
    assert result_df.Name.item() == "Russia"


def test_primary_data_proc(create_csv_for_primary_data_proc_func):
    """Test that primary_data_proc return cities with the most number of hotels"""
    test_frame = primary_data_proc(f"{Path.cwd()}")
    assert test_frame["City"].to_list() == ["Tumen", "Tumen", "Amsterdam", "Amsterdam"]
