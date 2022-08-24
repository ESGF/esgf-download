import pytest
from datetime import datetime

from esgpull.utils import format_date, index2url
from esgpull.constants import DEFAULT_ESGF_INDEX, DEFAULT_ESGF_URL


def test_format_date():
    expected = "2022-01-01T00:00:00Z"
    assert format_date("2022-01-01") == expected
    assert format_date(datetime(year=2022, month=1, day=1)) == expected
    with pytest.raises(ValueError):
        format_date("20220101")


def test_index2url():
    assert index2url(DEFAULT_ESGF_INDEX) == DEFAULT_ESGF_URL
    assert index2url(DEFAULT_ESGF_URL) == DEFAULT_ESGF_URL
