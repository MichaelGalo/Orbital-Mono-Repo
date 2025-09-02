from datetime import datetime
from src.utils import add_query_params, iso_to_human, handle_date_adjustment

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