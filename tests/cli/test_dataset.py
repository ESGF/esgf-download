"""End-to-end tests for dataset completion calculation."""

import pytest
from click.testing import CliRunner

from esgpull import Esgpull
from esgpull.cli.add import add
from esgpull.cli.self import install
from esgpull.cli.update import update
from esgpull.install_config import InstallConfig
from esgpull.models import File, FileStatus, Query


@pytest.mark.slow
def test_update_displays_dataset_completion(tmp_path):
    """Test that show command displays dataset completion after update.

    Simple test: add query → update → check show displays "X / Y datasets"
    """
    # Setup
    InstallConfig.setup(tmp_path)
    install_path = tmp_path / "esgpull"
    runner = CliRunner()

    result_install = runner.invoke(install, [f"{install_path}"])
    assert result_install.exit_code == 0

    # Add a small, stable query
    result_add = runner.invoke(
        add,
        [
            "project:CMIP6",
            "table_id:fx",
            "variable_id:areacella",
            "experiment_id:1pctCO2",
            "--distrib",
            "false",
            "--track",
        ],
    )
    assert result_add.exit_code == 0

    # Update to get real data
    try:
        result_update = runner.invoke(update, ["--yes"])
        if result_update.exit_code != 0:
            pytest.skip(f"ESGF API unavailable: {result_update.output}")
    except Exception as e:
        pytest.skip(f"ESGF API unavailable: {e}")

    # Test: show should display dataset completion
    esg = Esgpull(install_path)
    esg.graph.load_db()

    query_id = list(esg.graph._shas)[0]
    query = esg.graph.get(query_id)

    tree_str = esg.ui.render(query._rich_tree())

    # Should show "X / Y" datasets, not "? / ?"
    assert "datasets:" in tree_str, "Should show dataset information"
    assert "? / ?" not in tree_str, (
        "Should show actual dataset counts after update"
    )

    # Should match pattern like "datasets: 1 / 1" or "datasets: 0 / 2"
    import re

    dataset_match = re.search(r"datasets:\s*(\d+)\s*/\s*(\d+)", tree_str)
    assert dataset_match, (
        f"Should show dataset completion format in: {tree_str}"
    )

    # Cleanup
    InstallConfig.setup()


def test_dataset_completion_with_orphaned_files(tmp_path):
    """Test dataset completion calculation when files exist without corresponding datasets.

    This tests the edge case where files have dataset_ids but no Dataset records exist,
    which can happen with legacy data or incomplete updates.
    """
    # Setup: Initialize esgpull installation
    InstallConfig.setup(tmp_path)
    install_path = tmp_path / "esgpull"
    runner = CliRunner()

    result_install = runner.invoke(install, [f"{install_path}"])
    assert result_install.exit_code == 0

    esg = Esgpull(install_path)

    # Create a query without updating (no datasets will be created)
    query = Query(
        selection={
            "project": "TEST",
            "variable_id": "tas",
        }
    )
    query.compute_sha()
    esg.graph.add(query)

    # Manually create files with dataset_ids but no corresponding Dataset records
    with esg.db.commit_context():
        # Create orphaned files (files with dataset_ids but no Dataset records)
        orphaned_file1 = File(
            file_id="test.dataset1.file1.nc",
            dataset_id="test.dataset1.v1",
            master_id="test.dataset1.file1",
            url="https://example.com/file1.nc",
            version="v1",
            filename="file1.nc",
            local_path="/test/file1.nc",
            data_node="example.com",
            checksum="abc123",
            checksum_type="sha256",
            size=1000,
            status=FileStatus.Done,
        )
        orphaned_file1.compute_sha()

        orphaned_file2 = File(
            file_id="test.dataset2.file2.nc",
            dataset_id="test.dataset2.v1",
            master_id="test.dataset2.file2",
            url="https://example.com/file2.nc",
            version="v1",
            filename="file2.nc",
            local_path="/test/file2.nc",
            data_node="example.com",
            checksum="def456",
            checksum_type="sha256",
            size=2000,
            status=FileStatus.Queued,
        )
        orphaned_file2.compute_sha()

        # Add files to query
        query.files.extend([orphaned_file1, orphaned_file2])

        esg.db.session.add_all([orphaned_file1, orphaned_file2])

    # Test _rich_tree with orphaned files
    tree = query._rich_tree()
    tree_str = esg.ui.render(tree)

    # Should show "? / ?" because files exist but no proper datasets
    assert "? / ?" in tree_str, (
        "Should show unknown dataset count for orphaned files"
    )
    assert "update for accurate datasets" in tree_str, (
        "Should show update hint"
    )

    # Cleanup
    InstallConfig.setup()
