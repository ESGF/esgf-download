from pathlib import Path

from click.testing import CliRunner

from esgpull.cli.flush import flush
from esgpull.config import Config
from esgpull.database import Database
from esgpull.models import File, FileStatus, Query, sql


def make_file(i: int, status: FileStatus = FileStatus.Queued) -> File:
    f = File(
        file_id=f"file{i}",
        dataset_id=f"dataset{i}",
        master_id=f"master{i}",
        url=f"url{i}",
        version="v1",
        filename=f"file{i}.nc",
        local_path="proj/folder",
        data_node="node",
        checksum=str(i),
        checksum_type="SHA256",
        size=10,
        status=status,
    )
    f.compute_sha()
    return f


def make_query() -> Query:
    q = Query()
    q.compute_sha()
    return q


def commit(db):
    db.session.commit()


def flush_all_files(config: Config) -> list[str]:
    return [
        f.sha for f in Database.from_config(config).scalars(sql.file.all())
    ]


def test_flush_requires_exclusive_target(config: Config):
    runner = CliRunner()
    assert runner.invoke(flush, []).exit_code != 0
    assert runner.invoke(flush, ["--all", "--orphans"]).exit_code != 0


def test_flush_by_query(config: Config, db):
    query = make_query()
    db.add(query)
    files = [make_file(i) for i in range(3)]
    db.add(*files)
    for file in files:
        db.link(query, file)
    commit(db)

    runner = CliRunner()
    result = runner.invoke(flush, [query.sha, "--yes"])
    assert result.exit_code == 0, result.output

    assert flush_all_files(config) == []
    # query remains, files unlinked
    assert Database.from_config(config).get(Query, query.sha) is not None


def test_flush_orphans(config: Config, db):
    query = make_query()
    db.add(query)
    linked = make_file(0)
    orphan = make_file(1)
    db.add(linked, orphan)
    db.link(query, linked)
    commit(db)

    runner = CliRunner()
    result = runner.invoke(flush, ["--orphans", "--yes"])
    assert result.exit_code == 0, result.output

    assert flush_all_files(config) == [linked.sha]


def test_flush_all(config: Config, db):
    db.add(*[make_file(i) for i in range(3)])
    runner = CliRunner()
    result = runner.invoke(flush, ["--all", "--yes"])
    assert result.exit_code == 0, result.output
    assert flush_all_files(config) == []


def test_flush_done_keeps_disk(config: Config, db, tmp_path: Path):
    file = make_file(0, status=FileStatus.Done)
    db.add(file)
    on_disk = tmp_path / "file0.nc"
    on_disk.write_text("data")
    runner = CliRunner()
    result = runner.invoke(flush, ["--all", "--yes"])
    assert result.exit_code == 0, result.output
    assert flush_all_files(config) == []
    assert on_disk.exists()  # never deletes from disk
