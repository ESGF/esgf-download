import asyncio

import pytest

from esgpull.auth import Auth
from esgpull.db.models import File
from esgpull.fs import Filesystem
from esgpull.processor import Task
from esgpull.settings import Settings


# fmt:off
@pytest.fixture
def smallfile():
    return File(
        file_id="CMIP6.CMIP.IPSL.IPSL-CM6A-LR.historical.r10i1p1f1.Eyr.cLitterLut.gr.v20180803.cLitterLut_Eyr_IPSL-CM6A-LR_historical_r10i1p1f1_gr_1851-2015.nc",
        dataset_id="CMIP6.CMIP.IPSL.IPSL-CM6A-LR.historical.r10i1p1f1.Eyr.cLitterLut.gr.v20180803",
        master_id="CMIP6.CMIP.IPSL.IPSL-CM6A-LR.historical.r10i1p1f1.Eyr.cLitterLut.gr.cLitterLut_Eyr_IPSL-CM6A-LR_historical_r10i1p1f1_gr_1851-2015",
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


@pytest.fixture
def settings(tmp_path):
    return Settings.from_path(tmp_path)


@pytest.fixture
def fs(settings):
    return Filesystem.from_settings(settings)


@pytest.fixture
def auth(settings):
    return Auth.from_settings(settings)


@pytest.fixture
def from_file(auth, fs, settings, smallfile):
    return Task(auth, fs, settings, file=smallfile)


@pytest.fixture
def from_url(auth, fs, settings, smallfile):
    return Task(auth, fs, settings, url=smallfile.url)


@pytest.fixture(params=["from_file", "from_url"])
def task(request):
    return request.getfixturevalue(request.param)


async def run_task(task_):
    semaphore = asyncio.Semaphore(1)
    async for chunk in task_.stream(semaphore):
        ...
    return chunk


def test_task(auth, fs, settings, smallfile, task):
    result = asyncio.run(run_task(task))
    assert result.ok
    with fs.path_of(smallfile).open("rb") as f:
        data = f.read()
    assert len(data) == smallfile.size


def test_task_no_file_or_url(auth, fs, settings, smallfile):
    with pytest.raises(ValueError):
        Task(auth, fs, settings)


# def test_task_url_multiple_version_correct():
#     # fmt:off
#     url_old = "http://vesg.ipsl.upmc.fr/thredds/fileServer/cmip6/CMIP/IPSL/IPSL-CM6A-LR/1pctCO2/r1i1p1f1/Oyr/bfe/gn/v20180727/bfe_Oyr_IPSL-CM6A-LR_1pctCO2_r1i1p1f1_gn_1850-1999.nc"
#     url_new = "http://vesg.ipsl.upmc.fr/thredds/fileServer/cmip6/CMIP/IPSL/IPSL-CM6A-LR/1pctCO2/r1i1p1f1/Oyr/bfe/gn/v20190305/bfe_Oyr_IPSL-CM6A-LR_1pctCO2_r1i1p1f1_gn_1850-1999.nc"
#     # fmt:on
#     download_old = Simple(auth, url=url_old)
#     download_new = Simple(auth, url=url_new)
#     assert download_old.file.filename == download_new.file.filename
#     assert download_old.file.version != download_new.file.version


# def test_task_not_esgf_url():
#     with pytest.raises(ValueError):
#         Simple(auth, url="https://www.ipsl.fr/")
