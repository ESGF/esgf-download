import pytest
import sqlalchemy as sa

from esgpull import __version__
from esgpull.database import Database
from esgpull.models import Facet, FileStatus, sql


@pytest.fixture
def db(config):
    return Database.from_config(config)


def test_empty(root, db):
    assert str(root) in db.url
    assert db.version == __version__


def test_CRUD(db):
    stmt = sa.select(Facet)
    facets = db.scalars(stmt)
    assert len(facets) == 0
    facet = Facet(name="name", value="value")
    facet.compute_sha()
    db.add(facet)
    facets = db.scalars(stmt)
    assert facets == [facet]
    facet.value = "other"
    facet.compute_sha()
    db.add(facet)  # this is an UPDATE as `facet` comes from a SELECT
    facets_with_name = db.scalars(stmt.where(Facet.name == "name"))
    assert len(facets_with_name) == 1
    assert facets_with_name[0].value == "other"
    db.delete(facet)
    facets = db.scalars(stmt)
    assert len(facets) == 0


def test_rows(db):
    facets = [Facet(name=f"name{i}", value=f"value{i}") for i in range(2)]
    for facet in facets:
        facet.compute_sha()
    db.add(*facets)
    stmt = sa.select(Facet)
    rows = db.rows(stmt)
    assert rows == [(facets[0],), (facets[1],)]
    assert db.scalars(stmt.where(Facet.name == "name0")) == [facets[0]]


def test_in(db, file):
    assert file not in db
    db.add(file)
    assert file in db
    db.delete(file)
    assert file not in db


def test_sql_status(db, file):
    db.add(file)
    assert db.scalars(sql.file.with_status(FileStatus.Queued)) == [file]
    assert db.scalars(sql.file.with_status(FileStatus.Done)) == []
