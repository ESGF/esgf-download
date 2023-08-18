import pytest

from esgpull.config import Config
from esgpull.constants import CONFIG_FILENAME
from esgpull.install_config import InstallConfig
from esgpull.models import File, FileStatus


@pytest.fixture
def root(tmp_path):
    idx = InstallConfig.add(tmp_path / "esgpull")
    InstallConfig.choose(idx=idx)
    return InstallConfig.installs[idx].path


@pytest.fixture
def config_path(root):
    return root / CONFIG_FILENAME


@pytest.fixture
def config(root):
    cfg = Config.load(root)
    cfg.paths.db.mkdir(parents=True)
    return cfg


@pytest.fixture
def file():
    f = File(
        file_id="file",
        dataset_id="dataset",
        master_id="master",
        url="file",
        version="v0",
        filename="file.nc",
        local_path="project/folder",
        data_node="data_node",
        checksum="0",
        checksum_type="0",
        size=0,
        status=FileStatus.Queued,
    )
    f.compute_sha()
    return f
