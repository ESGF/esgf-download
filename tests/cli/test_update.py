from time import perf_counter

from click.testing import CliRunner

from esgpull import Esgpull
from esgpull.cli.add import add
from esgpull.cli.config import config
from esgpull.cli.self import install
from esgpull.cli.update import update
from esgpull.install_config import InstallConfig


def test_fast_update(tmp_path):
    InstallConfig.setup(tmp_path)
    install_path = tmp_path / "esgpull"
    runner = CliRunner()
    result_install = runner.invoke(install, [f"{install_path}"])
    assert result_install.exit_code == 0
    result_config = runner.invoke(config, ["api.page_limit", "10000"])
    assert result_config.exit_code == 0
    result_add = runner.invoke(
        add,
        [
            "table_id:fx",
            "experiment_id:dcpp*",
            "--distrib",
            "false",
            "--track",
        ],
    )
    assert result_add.exit_code == 0
    start = perf_counter()
    result_update = runner.invoke(update, ["--yes"])
    stop = perf_counter()
    assert result_update.exit_code == 0
    assert stop - start < 30  # 30 seconds to fetch ~6k files is plenty enough
    InstallConfig.setup()


def test_update_updates_timestamp(tmp_path):
    InstallConfig.setup(tmp_path)
    install_path = tmp_path / "esgpull"
    runner = CliRunner()
    result_install = runner.invoke(install, [f"{install_path}"])
    assert result_install.exit_code == 0
    result_config = runner.invoke(config, ["api.page_limit", "10000"])
    assert result_config.exit_code == 0

    # Add a small query - using a small dataset to keep test fast
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

    # Initialize Esgpull to get initial timestamp
    esg = Esgpull(install_path)
    esg.graph.load_db()

    # Get the only query in the database
    assert len(esg.graph._shas) == 1, "Expected only one query in database"
    query_id = list(esg.graph._shas)[0]
    query = esg.graph.get(query_id)

    initial_timestamp = query.updated_at

    # Run update with --yes to automatically add files
    result_update = runner.invoke(update, [query_id, "--yes"])
    assert result_update.exit_code == 0

    # Get the query again and check timestamp
    esg = Esgpull(install_path)  # Reinitialize to ensure fresh data
    esg.graph.load_db()
    query = esg.graph.get(query_id)

    # Verify the timestamp was updated
    assert query.updated_at > initial_timestamp, (
        "updated_at timestamp was not updated after adding files"
    )

    # Save the timestamp after files were added
    timestamp_after_update = query.updated_at

    # Run a second update - no new files should be added
    result_update_2 = runner.invoke(update, [query_id, "--yes"])
    assert result_update_2.exit_code == 0

    # Get the query again
    esg = Esgpull(install_path)  # Reinitialize to ensure fresh data
    esg.graph.load_db()
    query = esg.graph.get(query_id)

    # Verify the timestamp was NOT updated when no new files were added
    assert query.updated_at == timestamp_after_update, (
        "updated_at timestamp should not change when no new files are added"
    )
