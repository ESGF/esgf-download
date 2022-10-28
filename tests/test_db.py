from pathlib import Path

import pytest

from esgpull import __version__
from esgpull.db.core import Database
from esgpull.db.models import File, FileStatus, Param
from esgpull.query import Query


@pytest.fixture
def db(tmp_path):
    return Database.from_path(tmp_path)


@pytest.fixture
def file_(tmp_path):
    return File(
        file_id="file",
        dataset_id="dataset",
        master_id="master",
        url="file",
        version="v0",
        filename="file.nc",
        local_path=str(tmp_path / "v0"),  # version is required in path
        data_node="data_node",
        checksum="0",
        checksum_type="0",
        size=0,
        status=FileStatus.Queued,
    )


def test_empty(tmp_path, db):
    assert str(tmp_path) in db.url
    assert db.version == __version__


def test_CRUD(db):
    with db.select(Param) as select:
        assert len(select.scalars) == 0
    param = Param("name", "value")
    db.add(param)
    with db.select(Param) as select:
        results = select.scalars
    assert results == [param]
    param.value = "other"
    db.add(param)  # this is really UPDATE since `param` is already stored
    with db.select(Param) as select:
        results = select.where(Param.name == "name").scalars
    assert len(results) == 1
    assert results[0].value == "other"
    db.delete(param)
    with db.select(Param) as select:
        assert len(select.scalars) == 0


def test_scalar(db):
    params = [Param(f"name{i}", "value{i}") for i in range(2)]
    db.add(*params)
    with db.select(Param) as select:
        assert select.results == [(params[0],), (params[1],)]
        with pytest.raises(AssertionError):
            select.scalar  # `scalar` expects single result
        assert select.where(Param.name == "name0").scalar == params[0]


def test_has(db, file_):
    filepath = Path(file_.local_path, file_.filename)
    assert not db.has(file_)
    assert not db.has(filepath=filepath)
    db.add(file_)
    assert db.has(file_)
    assert db.has(filepath=filepath)
    db.delete(file_)
    assert not db.has(file_)
    assert not db.has(filepath=filepath)
    with pytest.raises(ValueError):
        assert db.has()


def test_search(db, file_):
    # setup
    variable_ids = ["a", "b", "c"]
    files = []
    for variable_id in variable_ids:
        f = file_.clone()
        # required for UniqueConstraint on file_id
        f.file_id += variable_id
        f.raw = {"project": ["test"], "variable_id": [variable_id]}
        files.append(f)
    db.add(*files)

    # test search simple
    query = Query()
    query.project = "test"
    # query variable_ids `b` and `c`
    for variable_id in variable_ids[1:]:
        subquery = query.add()
        subquery.variable_id = variable_id
    assert db.search(query=query) == files[1:]

    # test search no duplicates
    query = Query()
    a = query.add()
    a.variable_id = "a"
    test = query.add()
    test.project = "test"
    results_query = db.search(query=query)
    results_a = db.search(query=a)
    results_test = db.search(query=test)
    assert len(results_query) < len(results_a) + len(results_test)


def test_search_status(db, file_):
    db.add(file_)
    assert db.search(statuses=[FileStatus.Queued]) == [file_]
    assert db.search(statuses=[FileStatus.Done]) == []
    assert db.has(file_)
