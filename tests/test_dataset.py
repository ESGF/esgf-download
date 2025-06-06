import pytest

from esgpull.database import Database
from esgpull.models import Dataset, DatasetRecord, File


@pytest.fixture
def db(config):
    return Database.from_config(config)


def test_dataset_record_serialize():
    """Test DatasetRecord can parse ESGF API response."""
    source = {
        "instance_id": "CMIP6.CMIP.NCAR.CESM2.historical.r1i1p1f1.Amon.tas.gn.v20190308|esgf-data.ucar.edu",
        "data_node": "esgf-data.ucar.edu",
        "size": 123456789,
        "number_of_files": 10,
    }

    record = DatasetRecord.serialize(source)

    assert (
        record.dataset_id
        == "CMIP6.CMIP.NCAR.CESM2.historical.r1i1p1f1.Amon.tas.gn.v20190308"
    )
    assert (
        record.master_id
        == "CMIP6.CMIP.NCAR.CESM2.historical.r1i1p1f1.Amon.tas.gn"
    )
    assert record.version == "v20190308"
    assert record.data_node == "esgf-data.ucar.edu"
    assert record.size == 123456789
    assert record.number_of_files == 10


def test_dataset_file_relationship(db):
    """Test the relationship between Dataset and File models."""
    # Create a dataset
    dataset_id = (
        "CMIP6.CMIP.NCAR.CESM2.historical.r1i1p1f1.Amon.tas.gn.v20190308"
    )
    dataset = Dataset(
        dataset_id=dataset_id,
        total_files=2,
    )
    db.session.add(dataset)

    # Create files
    file1 = File(
        file_id=f"{dataset_id}.nc_1",
        dataset_id=dataset_id,
        master_id="master",
        url="http://example.com/file1.nc",
        version="v20190308",
        filename="file1.nc",
        local_path="/data/file1.nc",
        data_node="example.com",
        checksum="checksum1",
        checksum_type="sha256",
        size=1000,
    )
    file1.compute_sha()
    db.session.add(file1)

    db.session.commit()

    # Test relationship
    assert len(dataset.files) == 1
    assert dataset.files[0].file_id == file1.file_id
    assert file1.dataset == dataset
