from datetime import datetime

import pytest

from esgpull.utils import format_date, format_date_iso, parse_date
from esgpull.models.utils import extract_httpserver_url


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


@pytest.mark.parametrize(
    [
        "url",
        "expected",
    ],
    [
        pytest.param(
            [
                "https://esgf.ceda.ac.uk/thredds/fileServer/esg_cmip6/CMIP6/AerChemMIP/CNRM-CERFACS/CNRM-ESM2-1/hist-1950HC/r1i1p1f2/fx/sftlf/gr/v20190621/sftlf_fx_CNRM-ESM2-1_hist-1950HC_r1i1p1f2_gr.nc|application/netcdf|HTTPServer",
                "https://esgf.ceda.ac.uk/thredds/dodsC/esg_cmip6/CMIP6/AerChemMIP/CNRM-CERFACS/CNRM-ESM2-1/hist-1950HC/r1i1p1f2/fx/sftlf/gr/v20190621/sftlf_fx_CNRM-ESM2-1_hist-1950HC_r1i1p1f2_gr.nc.html|application/opendap-html|OPENDAP",
            ],
            "https://esgf.ceda.ac.uk/thredds/fileServer/esg_cmip6/CMIP6/AerChemMIP/CNRM-CERFACS/CNRM-ESM2-1/hist-1950HC/r1i1p1f2/fx/sftlf/gr/v20190621/sftlf_fx_CNRM-ESM2-1_hist-1950HC_r1i1p1f2_gr.nc",
            id="common case",
        ),
        pytest.param(
            "https://esgf.ceda.ac.uk/thredds/fileServer/esg_cmip6/CMIP6/AerChemMIP/CNRM-CERFACS/CNRM-ESM2-1/hist-1950HC/r1i1p1f2/fx/sftlf/gr/v20190621/sftlf_fx_CNRM-ESM2-1_hist-1950HC_r1i1p1f2_gr.nc|application/netcdf|HTTPServer",
            "https://esgf.ceda.ac.uk/thredds/fileServer/esg_cmip6/CMIP6/AerChemMIP/CNRM-CERFACS/CNRM-ESM2-1/hist-1950HC/r1i1p1f2/fx/sftlf/gr/v20190621/sftlf_fx_CNRM-ESM2-1_hist-1950HC_r1i1p1f2_gr.nc",
            id="str correct url case",
        ),
        pytest.param(
            "https://esgf.ceda.ac.uk/thredds/dodsC/esg_cmip6/CMIP6/AerChemMIP/CNRM-CERFACS/CNRM-ESM2-1/hist-1950HC/r1i1p1f2/fx/sftlf/gr/v20190621/sftlf_fx_CNRM-ESM2-1_hist-1950HC_r1i1p1f2_gr.nc.html|application/opendap-html|OPENDAP",
            None,
            id="str wrong url case",
        ),
        pytest.param(
            [
                "https://esgf.ceda.ac.uk/thredds/fileServer/esg_cmip6/CMIP6/AerChemMIP/CNRM-CERFACS/CNRM-ESM2-1/hist-1950HC/r1i1p1f2/fx/sftlf/gr/v20190621/sftlf_fx_CNRM-ESM2-1_hist-1950HC_r1i1p1f2_gr.nc|application/netcdf|HTTPServer"
            ],
            "https://esgf.ceda.ac.uk/thredds/fileServer/esg_cmip6/CMIP6/AerChemMIP/CNRM-CERFACS/CNRM-ESM2-1/hist-1950HC/r1i1p1f2/fx/sftlf/gr/v20190621/sftlf_fx_CNRM-ESM2-1_hist-1950HC_r1i1p1f2_gr.nc",
            id="single item correct url case",
        ),
        pytest.param(
            [
                "https://esgf.ceda.ac.uk/thredds/dodsC/esg_cmip6/CMIP6/AerChemMIP/CNRM-CERFACS/CNRM-ESM2-1/hist-1950HC/r1i1p1f2/fx/sftlf/gr/v20190621/sftlf_fx_CNRM-ESM2-1_hist-1950HC_r1i1p1f2_gr.nc.html|application/opendap-html|OPENDAP",
            ],
            None,
            id="single item wrong url case",
        ),
        pytest.param(
            123,
            None,
            marks=pytest.mark.xfail(raises=TypeError, strict=True),
            id="wrong type",
        ),
    ],
)
def test_extract_httpserver_url(url: str | list[str], expected: str | None):
    result = extract_httpserver_url(url)
    assert result == expected
