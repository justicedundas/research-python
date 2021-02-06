# from distutils.version import LooseVersion
import json
import warnings
from functools import reduce
from io import StringIO
from urllib.error import HTTPError

import pandas as pd
from pandas.api.types import is_list_like, is_number, is_string_dtype
from pandas.io import common as com
from pandas.testing import assert_frame_equal

string_types = (str,)
binary_type = bytes


def str_to_bytes(s, encoding=None):
    if isinstance(s, bytes):
        return s
    return s.encode(encoding or "ascii")


def bytes_to_str(b, encoding=None):
    return b.decode(encoding or "utf-8")


def lmap(*args, **kwargs):
    return list(map(*args, **kwargs))


def lrange(*args, **kwargs):
    return list(range(*args, **kwargs))


def concat(*args, **kwargs):
    return pd.concat(*args, **kwargs)


def cast_float(col):
    try:
        return [float(x) if x != "" else "" for x in col]
    except (ValueError, TypeError):
        return None


def validateJSON(jsonData):
    try:
        json.loads(jsonData)
    except ValueError:
        return False
    return True
