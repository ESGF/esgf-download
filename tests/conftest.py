import pytest

from esgpull.config import RootSolver
from esgpull.db.models import File, FileStatus


@pytest.fixture
def root(tmp_path):
    RootSolver.root = tmp_path / "esgpull"
    RootSolver.mkdir()
    return RootSolver.root


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
    f.id = 1
    return f
