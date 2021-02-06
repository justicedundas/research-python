import datetime as dt
import random
import re
import time
import warnings
from io import BytesIO
from urllib.request import urlopen
from zipfile import ZipFile

import pandas as pd
from pandas import to_datetime
from requests import Session
from requests.adapters import HTTPAdapter
from requests_futures.sessions import FuturesSession
from urllib3.util.retry import Retry

from geodataimport.compat import is_number

DEFAULT_TIMEOUT = 5


class SymbolWarning(UserWarning):
    pass


class RemoteDataError(IOError):
    pass


USER_AGENT_LIST = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.138 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.138 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.138 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.129 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.138 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.138 Safari/537.36",
]

headers = {
    "accept": "*/*",
    "accept-encoding": "gzip, deflate, br",
    "accept-language": "en-US,en;q=0.9",
    "origin": "https://api.worldbank.org/v2",
    "referer": "https://api.worldbank.org/v2",
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-site",
}

yahoo_headers = {
    "accept": "*/*",
    "accept-encoding": "gzip, deflate, br",
    "accept-language": "en-US,en;q=0.9",
    "origin": "https://finance.yahoo.com",
    "referer": "https://finance.yahoo.com",
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-site",
}

wb_errors = {
    "105": {
        "code": "503",
        "key": "Service currently unavailable",
        "description": "The requested service is temporarily unavailable.",
    },
    "110": {
        "code": "404",
        "key": "API Version not found.",
        "description": "The requested API version was not found.",
    },
    "111": {
        "code": "404",
        "key": "Invalid format",
        "description": "The requested response format was not found.",
    },
    "112": {
        "code": "404",
        "key": "Invalid method",
        "description": "The requested method was not found.",
    },
    "115": {
        "code": "404",
        "key": "Missing required parameter",
        "description": "Parameters which are required have not been sent.",
    },
    "120": {
        "code": "404",
        "key": "Invalid value",
        "description": "The provided parameter value is not valid.",
    },
    "140": {
        "code": "400",
        "key": "Endpoint not found.",
        "description": "The requested endpoint was not found.",
    },
    "150": {
        "code": "400",
        "key": "Language not supported",
        "description": "Response requested in an unsupported language.",
    },
    "160": {
        "code": "400",
        "key": " Filtering data-set on an indicator value without indicating a date range is meaningless and is not allowed.",
        "description": "You need to indicate date-range if you want to filter by an indicator value.",
    },
    "199": {
        "code": "500",
        "key": "Unexpected error",
        "description": "An unexpected error was encountered while processing the request.",
    },
}


class TimeoutHTTPAdapter(HTTPAdapter):
    def __init__(self, *args, **kwargs):
        self.timeout = DEFAULT_TIMEOUT
        if "timeout" in kwargs:
            self.timeout = kwargs["timeout"]
            del kwargs["timeout"]
        super(TimeoutHTTPAdapter, self).__init__(*args, **kwargs)

    def send(self, request, **kwargs):
        timeout = kwargs.get("timeout")
        if timeout is None:
            kwargs["timeout"] = self.timeout
        return super(TimeoutHTTPAdapter, self).send(request, **kwargs)


def _init_session(session, **kwargs):
    """Initiate Session

    Parameters
    ----------
    Backoff factor: float -> Default = 0.3
        Time delay when repeating API call
    end : str, int, date, dt, Timestamp
        Desired end date
    """
    if kwargs.get("headers") == "yahoo":
        session_headers = yahoo_headers
    else:
        session_headers = headers
    if session is None:
        if kwargs.get("asynchronous"):
            session = FuturesSession(max_workers=kwargs.get("max_workers", 8))
        else:
            session = Session()
        if kwargs.get("proxies"):
            session.proxies = kwargs.get("proxies")
        retries = Retry(
            total=kwargs.get("retry", 5),
            backoff_factor=kwargs.get("backoff_factor", 0.3),
            status_forcelist=kwargs.get("status_forcelist", [429, 500, 502, 503, 504]),
            method_whitelist=["HEAD", "GET", "OPTIONS", "POST", "TRACE"],
        )
        if kwargs.get("verify"):
            session.verify = kwargs.get("verify")
        session.mount(
            "https://",
            TimeoutHTTPAdapter(
                max_retries=retries, timeout=kwargs.get("timeout", DEFAULT_TIMEOUT)
            ),
        )
        user_agent = kwargs.get("user_agent", random.choice(USER_AGENT_LIST))
        session_headers["User-Agent"] = user_agent
        session.headers.update(**session_headers)
    return session


def _flatten_list(ls):
    return [item for sublist in ls for item in sublist]


def _convert_to_list(levels, comma_split=False):
    """Convert levels into list format"""
    if isinstance(levels, list):
        return levels
    if comma_split:
        return [x.strip() for x in levels.split(",")]
    return re.findall(r"[\w\-.=^&]+", levels)


def collapse(values):
    """Collapse multiple values to a colon-separated list of values"""
    if isinstance(values, str):
        return values
    if values is None:
        return "all"
    if isinstance(values, list):
        return ";".join([collapse(v) for v in values])
    return str(values)


def _convert_to_timestamp(date=None, start=True):
    if date is None:
        date = int((-858880800 * start) + (time.time() * (not start)))
    elif isinstance(date, dt):
        date = int(time.mktime(date.timetuple()))
    else:
        date = int(time.mktime(time.strptime(str(date), "%Y-%m-%d")))
    return date


def search(table, pattern, columns=None):
    """Return the rows of the table for which a column matches the pattern"""
    assert isinstance(table, pd.DataFrame), "'table' must be a Pandas DataFrame"

    if columns is None:
        columns = [col for col in table.columns if table[col].dtype.kind == "O"]

    if not columns:
        raise ValueError(
            "Please specific a non-empty columns arguments, and run the search "
            "on a table that has string columns"
        )

    if isinstance(pattern, str):
        if not pattern.startswith(".*"):
            pattern = ".*(" + pattern + ")"
        pattern = re.compile(pattern, re.IGNORECASE)

    found = pd.Series(0, index=table.index)
    for col in columns:
        found += table[col].apply(lambda x: 1 if pattern.match(x) else 0)

    return table.loc[found > 0]


def _raise_country_error(bad_countries, errors):
    if len(bad_countries) > 0:
        tmp = ", ".join(bad_countries)
        if errors == "raise":
            raise ValueError("Invalid Country Code(s): %s" % tmp)
        if errors == "warn":
            warnings.warn("Non-standard ISO " "country codes: %s" % tmp, UserWarning)


def _sanitize_dates(start, end):
    """
    Return (timestamp_start, timestamp_end) tuple
    if start is None - default is 5 years before the current date
    if end is None - default is today
    Parameters
    ----------
    start : str, int, date, dt, Timestamp
        Desired start date
    end : str, int, date, dt, Timestamp
        Desired end date
    """
    if is_number(start):
        # regard int as year
        start = dt.datetime(start, 1, 1)
    start = to_datetime(start)

    if is_number(end):
        end = dt.datetime(end, 1, 1)
    end = to_datetime(end)

    if start is None:
        # default to 5 years before today
        today = dt.date.today()
        start = today - dt.timedelta(days=365 * 5)
    if end is None:
        # default to today
        end = dt.date.today()
    try:
        start = to_datetime(start)
        end = to_datetime(end)
    except (TypeError, ValueError):
        raise ValueError("Invalid date format.")
    if start > end:
        raise ValueError("start must be an earlier date than end")
    return start, end


def unzip_url_file(url):
    resp = urlopen(url)
    zipfile = ZipFile(BytesIO(resp.read()))
    return zipfile


def _history_dataframe(data, symbol, params, adj_timezone=True):
    df = pd.DataFrame(data[symbol]["indicators"]["quote"][0])
    if data[symbol]["indicators"].get("adjclose"):
        df["adjclose"] = data[symbol]["indicators"]["adjclose"][0]["adjclose"]
    df.index = pd.to_datetime(data[symbol]["timestamp"], unit="s") + pd.Timedelta(
        (data[symbol]["meta"]["gmtoffset"] * adj_timezone), "s"
    )
    if params["interval"][-1] not in ["m", "h"]:
        df.index = df.index.date
    df.dropna(inplace=True)
    if data[symbol].get("events"):
        df = pd.merge(
            df,
            _events_to_dataframe(data, symbol, params, adj_timezone),
            how="left",
            left_index=True,
            right_index=True,
        )
    return df


def _events_to_dataframe(data, symbol, params, adj_timezone):
    dataframes = []
    for event in ["dividends", "splits"]:
        try:
            df = pd.DataFrame(data[symbol]["events"][event].values())
            df.set_index("date", inplace=True)
            df.index = pd.to_datetime(df.index, unit="s") + pd.Timedelta(
                (data[symbol]["meta"]["gmtoffset"] * adj_timezone), "s"
            )
            if params["interval"][-1] not in ["m", "h"]:
                df.index = df.index.date
            if event == "dividends":
                df.rename(columns={"amount": "dividends"}, inplace=True)
            else:
                df["splits"] = df["numerator"] / df["denominator"]
                df = df[["splits"]]
            dataframes.append(df)
        except KeyError:
            pass
    return (
        pd.merge(
            dataframes[0], dataframes[1], how="left", left_index=True, right_index=True
        )
        if len(dataframes) > 1
        else dataframes[0]
    )
