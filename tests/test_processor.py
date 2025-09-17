import asyncio
import json

import httpx
import pytest

from esgpull.fs import FileCheck, Filesystem
from esgpull.models import File
from esgpull.processor import Task
from esgpull.result import Ok

FILE_JSON: dict = json.loads("""
{
  "id": "CMIP6.AerChemMIP.CNRM-CERFACS.CNRM-ESM2-1.hist-1950HC.r1i1p1f2.fx.sftlf.gr.v20190621.sftlf_fx_CNRM-ESM2-1_hist-1950HC_r1i1p1f2_gr.nc|esgf.ceda.ac.uk",
  "version": "1",
  "activity_drs": [
    "AerChemMIP"
  ],
  "activity_id": [
    "AerChemMIP"
  ],
  "cf_standard_name": [
    "land_area_fraction"
  ],
  "checksum": [
    "cfdc9d9113d8ab81dd51f5b133267e2d3d4db830ee0a399b444cbb1d540f512e"
  ],
  "checksum_type": [
    "SHA256"
  ],
  "citation_url": [
    "http://cera-www.dkrz.de/WDCC/meta/CMIP6/CMIP6.AerChemMIP.CNRM-CERFACS.CNRM-ESM2-1.hist-1950HC.r1i1p1f2.fx.sftlf.gr.v20190621.json"
  ],
  "data_node": "esgf.ceda.ac.uk",
  "data_specs_version": [
    "01.00.21"
  ],
  "dataset_id": "CMIP6.AerChemMIP.CNRM-CERFACS.CNRM-ESM2-1.hist-1950HC.r1i1p1f2.fx.sftlf.gr.v20190621|esgf.ceda.ac.uk",
  "dataset_id_template_": [
    "%(mip_era)s.%(activity_drs)s.%(institution_id)s.%(source_id)s.%(experiment_id)s.%(member_id)s.%(table_id)s.%(variable_id)s.%(grid_label)s"
  ],
  "directory_format_template_": [
    "%(root)s/%(mip_era)s/%(activity_drs)s/%(institution_id)s/%(source_id)s/%(experiment_id)s/%(member_id)s/%(table_id)s/%(variable_id)s/%(grid_label)s/%(version)s"
  ],
  "experiment_id": [
    "hist-1950HC"
  ],
  "experiment_title": [
    "historical forcing, but with1950s halocarbon concentrations; initialized in 1950"
  ],
  "frequency": [
    "fx"
  ],
  "further_info_url": [
    "https://furtherinfo.es-doc.org/CMIP6.CNRM-CERFACS.CNRM-ESM2-1.hist-1950HC.none.r1i1p1f2"
  ],
  "grid": [
    "data regridded to a T127 gaussian grid (128x256 latlon) from a native atmosphere T127l reduced gaussian grid"
  ],
  "grid_label": [
    "gr"
  ],
  "index_node": "esgf.ceda.ac.uk",
  "instance_id": "CMIP6.AerChemMIP.CNRM-CERFACS.CNRM-ESM2-1.hist-1950HC.r1i1p1f2.fx.sftlf.gr.v20190621.sftlf_fx_CNRM-ESM2-1_hist-1950HC_r1i1p1f2_gr.nc",
  "institution_id": [
    "CNRM-CERFACS"
  ],
  "latest": true,
  "master_id": "CMIP6.AerChemMIP.CNRM-CERFACS.CNRM-ESM2-1.hist-1950HC.r1i1p1f2.fx.sftlf.gr.sftlf_fx_CNRM-ESM2-1_hist-1950HC_r1i1p1f2_gr.nc",
  "member_id": [
    "r1i1p1f2"
  ],
  "metadata_format": "THREDDS",
  "mip_era": [
    "CMIP6"
  ],
  "model_cohort": [
    "Registered"
  ],
  "nominal_resolution": [
    "250 km"
  ],
  "pid": [
    "hdl:21.14100/37f7d29f-8d1f-354f-8554-f71adda35d22"
  ],
  "product": [
    "model-output"
  ],
  "project": [
    "CMIP6"
  ],
  "realm": [
    "atmos"
  ],
  "replica": true,
  "size": 53605,
  "source_id": [
    "CNRM-ESM2-1"
  ],
  "source_type": [
    "AOGCM",
    "BGC",
    "AER",
    "CHEM"
  ],
  "sub_experiment_id": [
    "none"
  ],
  "table_id": [
    "fx"
  ],
  "timestamp": "2020-03-10T01:53:55Z",
  "title": "sftlf_fx_CNRM-ESM2-1_hist-1950HC_r1i1p1f2_gr.nc",
  "tracking_id": [
    "hdl:21.14100/70b80649-e719-48df-9e3b-a7b7299c1c3c"
  ],
  "type": "File",
  "url": [
    "https://esgf.ceda.ac.uk/thredds/fileServer/esg_cmip6/CMIP6/AerChemMIP/CNRM-CERFACS/CNRM-ESM2-1/hist-1950HC/r1i1p1f2/fx/sftlf/gr/v20190621/sftlf_fx_CNRM-ESM2-1_hist-1950HC_r1i1p1f2_gr.nc|application/netcdf|HTTPServer",
    "https://esgf.ceda.ac.uk/thredds/dodsC/esg_cmip6/CMIP6/AerChemMIP/CNRM-CERFACS/CNRM-ESM2-1/hist-1950HC/r1i1p1f2/fx/sftlf/gr/v20190621/sftlf_fx_CNRM-ESM2-1_hist-1950HC_r1i1p1f2_gr.nc.html|application/opendap-html|OPENDAP"
  ],
  "variable": [
    "sftlf"
  ],
  "variable_id": [
    "sftlf"
  ],
  "variable_long_name": [
    "Land Area Fraction"
  ],
  "variable_units": [
    "%"
  ],
  "variant_label": [
    "r1i1p1f2"
  ],
  "retracted": false,
  "_timestamp": "2020-03-11T20:36:07.909Z",
  "score": 7.750845,
  "_version_": 1803414002300092400
}
""")


@pytest.fixture
def smallfile():
    return File.serialize(FILE_JSON)


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


# @pytest.mark.xfail(
#     raises=(httpx.ConnectTimeout, httpx.ReadTimeout),
#     reason="this is dependent on the IPSL data node's health (unstable)",
# )
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
