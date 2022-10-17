from datetime import datetime

import pytest

from esgpull.utils import format_date, index2url

ESGF_INDEX = "esgf-node.ipsl.upmc.fr"
ESGF_URL = "https://esgf-node.ipsl.upmc.fr/esg-search/search"


def test_format_date():
    expected = "2022-01-01T00:00:00Z"
    assert format_date("2022-01-01") == expected
    assert format_date(datetime(year=2022, month=1, day=1)) == expected
    with pytest.raises(ValueError):
        format_date("20220101")
    with pytest.raises(ValueError):
        format_date(20220101)


def test_index2url():
    assert index2url(ESGF_INDEX) == ESGF_URL
    assert index2url(ESGF_URL) == ESGF_URL
