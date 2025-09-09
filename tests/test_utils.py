import polars as pl
from datetime import datetime
from src.utils import add_query_params, iso_to_human, handle_date_adjustment, convert_dataframe_to_parquet, preprocess_apod_data

def test_add_query_params():
    url = "https://example.com"
    params = {"search": "test", "page": 2}
    expected = "https://example.com?search=test&page=2"
    assert add_query_params(url, params) == expected

def test_iso_to_human():
    iso_string = "P345DT8H18M1S"
    expected = "345 days, 8 hours, 18 minutes, 1 seconds"
    assert iso_to_human(iso_string) == expected

def test_handle_date_adjustment():
    today_dt = datetime.strptime("2025-09-01T12:34:56Z", "%Y-%m-%dT%H:%M:%SZ")
    today_date = today_dt.date()
    years = 5
    expected = "2020-09-01"
    assert handle_date_adjustment(today_date, years).strftime("%Y-%m-%d") == expected

def test_convert_dateframe_to_parquet():
    test_dataframe = pl.DataFrame({
        'column1': [1, 2, 3],
        'column2': ['a', 'b', 'c']
    })
    test_parquet_buffer = convert_dataframe_to_parquet(test_dataframe)

    loaded_dataframe = pl.read_parquet(test_parquet_buffer)
    assert loaded_dataframe.shape == (3, 2)
    assert loaded_dataframe['column1'].to_list() == [1, 2, 3]
    assert loaded_dataframe['column2'].to_list() == ['a', 'b', 'c']

def test_preprocess_apod_data():
    test_dataframe = pl.DataFrame({
        'title': ['a'],
        'date': ['2025-01-01'] 
    })
    test_parquet_buffer = convert_dataframe_to_parquet(test_dataframe)
    loaded_dataframe = pl.read_parquet(test_parquet_buffer)
    fake_apod_preprocessed_dataframe = preprocess_apod_data(loaded_dataframe)

    expected_columns = [
        "resource", "concept_tags", "title", "date", "url", "hdurl", "media_type",
        "explanation", "concepts", "thumbnail_url", "service_version", "copyright"
    ]

    assert fake_apod_preprocessed_dataframe.shape == (1, 12)
    assert fake_apod_preprocessed_dataframe.columns == expected_columns
