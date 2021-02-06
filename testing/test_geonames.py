import pytest

from geodataimport.compat import is_list_like
from geodataimport.geonames import get_cities, get_countries


def test_countries():
    df = get_countries()
    test_cols = [
        "id",
        "name",
        "iso",
        "iso3",
        "isoNumeric",
        "areaSqKm",
        "currencyCode",
        "currencyName",
        "geonameId",
        "neighbours",
        "languages",
    ]
    assert list(df.columns) == test_cols
    assert is_list_like(df.iloc[1]["neighbours"])


def test_cities():
    df = get_cities()
    assert df.empty is False
