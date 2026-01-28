import re
from pathlib import Path

from click.testing import CliRunner

from esgpull import Esgpull
from esgpull.cli.download import download
from esgpull.config import Config
from esgpull.models import File, FileStatus, Query


def test_download_errors_in_progress(root: Path, config: Config):
    # Configure the environment
    config.generate(overwrite=True)
    runner = CliRunner()
    esg = Esgpull(root)

    # Create a query without updating (no datasets will be created)
    query = Query()
    query.compute_sha()
    query.track(query.options.default())

    # Manually create files
    failing_file = File(
        file_id="failing_file",
        dataset_id="test_dataset_id",
        master_id="test_master_id",
        url="https://nonexistent.path/to/file.nc",
        version="1.0",
        filename="failing.nc",
        local_path="test/failing",
        data_node="test_node",
        checksum="12345",
        checksum_type="SHA256",
        size=1000,
        status=FileStatus.Queued,
    )
    failing_file.compute_sha()

    # Add files to query
    query.files.append(failing_file)

    # Add query to database
    esg.db.add(query)

    # Run the download instruction in the CLI
    result_download = runner.invoke(download)

    # Parse the output to extract
    output = result_download.output
    assert "download failed" in output
