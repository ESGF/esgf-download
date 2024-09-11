import pytest

from esgpull.cli.utils import parse_facets
from esgpull.models import Selection

test_cases = [
    (["ocean", "temperature"], Selection(query=["ocean", "temperature"])),
    (["ocean temperature"], Selection(query='"ocean temperature"')),
    (
        [
            "project:CMIP6",
            "experiment_id:ssp126,ssp245,ssp585",
            "variable_id:hurt,lai,pr,sfcWind,tasmax,tasmin,ts,tos",
            "frequency:day",
            "nominal_resolution:100 km",
        ],
        Selection(
            project="CMIP6",
            experiment_id=["ssp126", "ssp245", "ssp585"],
            variable_id=[
                "hurt",
                "lai",
                "pr",
                "sfcWind",
                "tasmax",
                "tasmin",
                "ts",
                "tos",
            ],
            frequency="day",
            nominal_resolution='"100 km"',
        ),
    ),
]


@pytest.mark.parametrize("facets, expected_query", test_cases)
def test_parse_facets(facets, expected_query):
    result = parse_facets(facets)
    assert result == expected_query
