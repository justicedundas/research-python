import pytest

from geodataimport.utils.countries import COUNTRIES
from geodataimport.yahoo import Ticker

TICKERS = [Ticker("aapl", country="brazil")]


@pytest.fixture(params=TICKERS)
def ticker(request):
    return request.param


def test_country_change(ticker):
    ticker.country = "hong kong"
    assert ticker.country == "hong kong"


def test_bad_country():
    with pytest.raises(ValueError):
        assert Ticker("aapl", country="china")


def test_default_query_param(ticker):
    assert ticker.default_query_params == COUNTRIES[ticker.country]
