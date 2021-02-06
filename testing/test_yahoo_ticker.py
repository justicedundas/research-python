import itertools
import os
from datetime import datetime

import pytest

from geodataimport.yahoo import Ticker

TICKERS = [
    Ticker("aapl ^GSPC btcusd=x brk-b logo.is l&tfh.ns", asynchronous=True),
    Ticker(["aapl", "aaapl"]),
    Ticker("hasgx"),
    Ticker("btcusd=x", formatted=True, validate=True),
]

FINANCIALS = ["cash_flow", "income_statement", "balance_sheet"]

SEPERATE_ENDPOINTS = FINANCIALS + [
    "option_chain",
    "history",
    "all_modules",
    "get_modules",
    "symbols",
]


def props(cls):
    return [
        i
        for i in cls.__dict__.keys()
        if i[:1] != "_" and i[:2] != "p_" and i not in SEPERATE_ENDPOINTS
    ]


@pytest.fixture(params=TICKERS)
def ticker(request):
    return request.param


def test_symbols_change(ticker):
    ticker.symbols = "aapl msft fb"
    assert ticker.symbols == ["aapl", "msft", "fb"]


def test_option_chain(ticker):
    assert ticker.option_chain is not None


def test_bad_multiple_modules_wrong(ticker):
    with pytest.raises(ValueError):
        assert ticker.get_modules(["asetProfile", "summaryProfile"])


def test_multiple_modules(ticker):
    assert ticker.get_modules(["assetProfile", "summaryProfile"]) is not None


def test_multiple_modules_str(ticker):
    assert ticker.get_modules("assetProfile summaryProfile") is not None


def test_news(ticker):
    assert ticker.news() is not None


def test_news_start(ticker):
    assert ticker.news(start="2020-01-01", count=100) is not None


def test_all_modules(ticker):
    assert ticker.all_modules is not None
    data = ticker.all_modules
    assert sorted(list(data.keys())) == sorted(ticker.all_modules)


@pytest.mark.parametrize("module", props(Ticker))
def test_modules(ticker, module):
    assert getattr(ticker, module) is not None


@pytest.mark.parametrize(
    "module, frequency", [el for el in itertools.product(FINANCIALS, ["q", "a"])]
)
def test_financials(ticker, frequency, module):
    assert getattr(ticker, module)(frequency) is not None


def test_bad_financials_arg():
    with pytest.raises(KeyError):
        assert Ticker("aapl").income_statement("r")


def test_get_financial_data(ticker):
    assert (
        ticker.get_financial_data("GrossProfit NetIncome TotalAssets ForwardPeRatio")
        is not None
    )


@pytest.mark.parametrize(
    "period, interval",
    [
        (p, i)
        for p, i in zip(
            ["1d", "1mo", "1y", "5y", "max"], ["1m", "1m", "1d", "1wk", "3mo"]
        )
    ],
)
def test_history(ticker, period, interval):
    assert ticker.history(period, interval) is not None


@pytest.mark.parametrize(
    "start, end",
    [
        (start, end)
        for start, end in zip(
            [datetime(2019, 1, 1), "2019-01-01"], ["2019-12-30", datetime(2019, 12, 30)]
        )
    ],
)
def test_history_start_end(ticker, start, end):
    assert ticker.history(start=start, end=end) is not None


@pytest.mark.parametrize(
    "period, interval", [(p, i) for p, i in zip(["2d", "1mo"], ["1m", "3m"])]
)
def test_history_bad_args(ticker, period, interval):
    with pytest.raises(ValueError):
        assert ticker.history(period, interval)


def test_adj_ohlc(ticker):
    assert ticker.history(period="max", adj_ohlc=True) is not None
