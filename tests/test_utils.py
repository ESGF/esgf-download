from datetime import datetime

import pytest

from esgpull.utils import format_date, format_date_iso, index2url, parse_date

ESGF_INDEX = "esgf-node.ipsl.upmc.fr"
ESGF_URL = "https://esgf-node.ipsl.upmc.fr/esg-search/search"


def test_parse_date():
    fmt = "%Y-%m-%d"
    expected = datetime(year=2022, month=1, day=1)
    assert parse_date("2022-01-01", fmt=fmt) == expected
    assert parse_date(datetime(year=2022, month=1, day=1), fmt=fmt) == expected
    with pytest.raises(ValueError):
        parse_date("20220101", fmt=fmt)
    with pytest.raises(ValueError):
        parse_date(20220101, fmt=fmt)


def test_format_date():
    fmt = "%Y-%m-%d"
    expected = "2022-01-01"
    assert format_date("2022-01-01", fmt=fmt) == expected
    assert (
        format_date(datetime(year=2022, month=1, day=1), fmt=fmt) == expected
    )
    with pytest.raises(ValueError):
        format_date("20220101", fmt=fmt)
    with pytest.raises(ValueError):
        format_date(20220101, fmt=fmt)


def test_format_date_iso():
    expected = "2022-01-01T00:00:00Z"
    assert format_date_iso("2022-01-01") == expected
    assert format_date_iso(datetime(year=2022, month=1, day=1)) == expected
    with pytest.raises(ValueError):
        format_date_iso("20220101")
    with pytest.raises(ValueError):
        format_date_iso(20220101)


def test_index2url():
    assert index2url(ESGF_INDEX) == ESGF_URL
    assert index2url(ESGF_URL) == ESGF_URL
