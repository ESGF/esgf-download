from pathlib import Path

import pytest

from esgpull import __version__
from esgpull.config import Config
from esgpull.db.core import Database
from esgpull.db.models import FileStatus, Param
from esgpull.query import Query


@pytest.fixture
def config(root):
    cfg = Config.load(root)
    cfg.paths.db.mkdir()
    return cfg


@pytest.fixture
def db(config):
    return Database.from_config(config)


def test_empty(root, db):
    assert str(root) in db.url
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
    db.add(param)  # this is an UPDATE as `param` comes from a SELECT
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


def test_has(db, file):
    filepath = Path(file.local_path, file.filename)
    assert not db.has(file)
    assert not db.has(filepath=filepath)
    db.add(file)
    assert db.has(file)
    assert db.has(filepath=filepath)
    db.delete(file)
    assert not db.has(file)
    assert not db.has(filepath=filepath)
    with pytest.raises(ValueError):
        assert db.has()


def test_search(db, file):
    # setup
    variable_ids = ["a", "b", "c"]
    files = []
    for variable_id in variable_ids:
        f = file.clone()
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


def test_search_status(db, file):
    db.add(file)
    assert db.search(statuses=[FileStatus.Queued]) == [file]
    assert db.search(statuses=[FileStatus.Done]) == []
    assert db.has(file)
