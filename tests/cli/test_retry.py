from click.testing import CliRunner

from esgpull.cli.retry import retry
from esgpull.models import File, FileStatus, sql


def make_file(i: int, status: FileStatus = FileStatus.Error) -> File:
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


def statuses(db) -> dict[FileStatus, list[str]]:
    out: dict[FileStatus, list[str]] = {}
    for f in db.scalars(sql.file.all()):
        out.setdefault(f.status, []).append(f.sha)
    return out


def test_retry_queued_errors(config, db):
    db.add(make_file(0, status=FileStatus.Queued))
    runner = CliRunner()
    result = runner.invoke(retry, ["queued"])
    assert result.exit_code != 0
    assert "esgpull download" in result.output
    assert statuses(db)[FileStatus.Queued]


def test_retry_done_errors(config, db):
    db.add(make_file(0, status=FileStatus.Done))
    runner = CliRunner()
    result = runner.invoke(retry, ["done"])
    assert result.exit_code != 0
    assert "cannot retry done files" in result.output
    assert statuses(db)[FileStatus.Done]


def test_retry_default(config, db):
    db.add(make_file(0, status=FileStatus.Error))
    runner = CliRunner()
    result = runner.invoke(retry, [])
    assert result.exit_code == 0, result.output
    assert statuses(db)[FileStatus.Queued]


def test_retry_error(config, db):
    db.add(make_file(0, status=FileStatus.Error))
    runner = CliRunner()
    result = runner.invoke(retry, ["error"])
    assert result.exit_code == 0, result.output
    assert statuses(db)[FileStatus.Queued]
