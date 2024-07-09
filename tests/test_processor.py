import asyncio

import httpx
import pytest

from esgpull.fs import FileCheck, Filesystem
from esgpull.models import File
from esgpull.processor import Task
from esgpull.result import Ok


@pytest.fixture
def smallfile():
    dataset_id = (
        "CMIP6.CMIP.IPSL.IPSL-CM6A-LR.historical.r10i1p1f1.Eyr.cLitterLut.gr"
        ".v20180803"
    )
    dataset_master, version = dataset_id.rsplit(".", 1)
    filename = (
        "cLitterLut_Eyr_IPSL-CM6A-LR_historical_r10i1p1f1_gr_1851-2015.nc"
    )
    file_id = ".".join([dataset_id, filename])
    master_id = ".".join([dataset_master, filename])
    data_node = "vesg.ipsl.upmc.fr"
    host = f"https://{data_node}/thredds/fileServer"
    url = "/".join(
        [
            host,
            "cmip6",
            *dataset_id.split(".")[1:],
            filename,
        ]
    )
    checksum = (
        "47958756e90cb6afcd20451dcd138b4ced1e1845afdd1ea12c1f962991da2f87"
    )
    file = File(
        file_id=file_id,
        dataset_id=dataset_id,
        master_id=master_id,
        url=url,
        version=version,
        filename=filename,
        local_path=dataset_id.replace(".", "/"),
        data_node=data_node,
        checksum=checksum,
        checksum_type="SHA256",
        size=6512402,
    )
    file.compute_sha()
    return file


@pytest.fixture
def fs(config):
    return Filesystem.from_config(config, install=True)


@pytest.fixture
def task(config, fs, smallfile):
    return Task(config, fs, file=smallfile)


async def run_task(task_):
    semaphore = asyncio.Semaphore(1)
    async with httpx.AsyncClient() as client:
        async for result in task_.stream(semaphore, client):
            ...
    return result


@pytest.mark.xfail(
    raises=(httpx.ConnectTimeout, httpx.ReadTimeout),
    reason="this is dependent on the IPSL data node's health (unstable)",
)
def test_task(fs, smallfile, task):
    result = asyncio.run(run_task(task))
    if not result.ok:
        raise result.err
    assert fs.finalize(smallfile) == Ok(FileCheck.Ok)
    with fs[smallfile].drs.open("rb") as f:
        data = f.read()
    assert len(data) == smallfile.size


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
