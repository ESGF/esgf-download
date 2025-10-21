from pathlib import Path

from click.testing import CliRunner

from esgpull import Esgpull
from esgpull.cli.add import add
from esgpull.cli.config import config as config_cmd
from esgpull.cli.remove import remove
from esgpull.cli.update import update
from esgpull.config import Config


def test_update_after_remove(root: Path, config: Config):
    config.generate(overwrite=True)
    runner = CliRunner()
    result_config = runner.invoke(config_cmd, ["api.page_limit", "10000"])
    assert result_config.exit_code == 0
    result_add = runner.invoke(
        add,
        [
            "activity_id:CMIP",
            "experiment_id:1pctCO2",
            "source_id:CNRM-ESM2-1",
            "--distrib",
            "false",
            "--track",
        ],
    )
    assert result_add.exit_code == 0
    result_update = runner.invoke(update, ["--yes"])
    assert result_update.exit_code == 0

    esg = Esgpull(root)  # Reinitialize to ensure fresh data
    esg.graph.load_db()
    query_id = list(esg.graph._shas)[0]
    query = esg.graph.get(query_id)
    nb_files_after_update = len(query.files)
    assert nb_files_after_update > 0

    result_remove = runner.invoke(remove, [query_id, "--yes"])
    assert result_remove.exit_code == 0

    result_add = runner.invoke(
        add,
        [
            "activity_id:CMIP",
            "experiment_id:1pctCO2",
            "source_id:CNRM-ESM2-1",
            "--distrib",
            "false",
            "--track",
        ],
    )
    assert result_add.exit_code == 0
    result_update = runner.invoke(update, ["--yes"])
    assert result_update.exit_code == 0

    esg = Esgpull(root)  # Reinitialize to ensure fresh data
    query = esg.graph.get(query_id)
    assert nb_files_after_update == len(query.files)
