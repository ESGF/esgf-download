import pytest

import asyncio

from esgpull.download import Download
from esgpull.types import File


# fmt:off
@pytest.fixture
def smallfile():
    return File(
        file_id="CMIP6.CMIP.IPSL.IPSL-CM6A-LR.historical.r10i1p1f1.Eyr.cLitterLut.gr.v20180803.cLitterLut_Eyr_IPSL-CM6A-LR_historical_r10i1p1f1_gr_1851-2015.nc",
        dataset_id="CMIP6.CMIP.IPSL.IPSL-CM6A-LR.historical.r10i1p1f1.Eyr.cLitterLut.gr.v20180803",
        url="http://vesg.ipsl.upmc.fr/thredds/fileServer/cmip6/CMIP/IPSL/IPSL-CM6A-LR/historical/r10i1p1f1/Eyr/cLitterLut/gr/v20180803/cLitterLut_Eyr_IPSL-CM6A-LR_historical_r10i1p1f1_gr_1851-2015.nc",
        version="v20180803",
        filename="cLitterLut_Eyr_IPSL-CM6A-LR_historical_r10i1p1f1_gr_1851-2015.nc",
        local_path="CMIP6/CMIP/IPSL/IPSL-CM6A-LR/historical/r10i1p1f1/Eyr/cLitterLut/gr/v20180803",
        data_node="vesg.ipsl.upmc.fr",
        checksum="47958756e90cb6afcd20451dcd138b4ced1e1845afdd1ea12c1f962991da2f87",
        checksum_type="SHA256",
        size=6512402,
    )
# fmt:on


# @pytest.mark.slow
def test_download_using_file_or_url(smallfile):
    download = Download(file=smallfile)
    data_file = asyncio.run(download.aget())
    assert len(data_file) == smallfile.size

    download = Download(url=smallfile.url)
    data_url = asyncio.run(download.aget())
    assert data_url == data_file

    with pytest.raises(ValueError):
        Download()


def test_download_url_multiple_version_correct():
    # fmt:off
    url_old = "http://vesg.ipsl.upmc.fr/thredds/fileServer/cmip6/CMIP/IPSL/IPSL-CM6A-LR/1pctCO2/r1i1p1f1/Oyr/bfe/gn/v20180727/bfe_Oyr_IPSL-CM6A-LR_1pctCO2_r1i1p1f1_gn_1850-1999.nc"
    url_new = "http://vesg.ipsl.upmc.fr/thredds/fileServer/cmip6/CMIP/IPSL/IPSL-CM6A-LR/1pctCO2/r1i1p1f1/Oyr/bfe/gn/v20190305/bfe_Oyr_IPSL-CM6A-LR_1pctCO2_r1i1p1f1_gn_1850-1999.nc"
    # fmt:on
    download_old = Download(url=url_old)
    download_new = Download(url=url_new)
    assert download_old.file.filename == download_new.file.filename
    assert download_old.file.version != download_new.file.version


def test_download_not_esgf_url():
    with pytest.raises(ValueError):
        Download(url="https://www.ipsl.fr/")
