from click.testing import CliRunner

from esgpull import Esgpull
from esgpull.cli.add import add
from esgpull.cli.config import config
from esgpull.cli.remove import remove
from esgpull.cli.self import install
from esgpull.cli.update import update
from esgpull.install_config import InstallConfig


def test_update_after_remove(tmp_path):
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
            "activity_id:CMIP",
            "experiment_id:1pctCO2",
            "frequency:fx",
            "institution_id:CNRM-CERFACS",
            "member_id:r9i1p1f2",
            "source_id:CNRM-ESM2-1",
            "table_id:fx",
            "variable:areacella",
            "variable_id:areacella",
            "--distrib",
            "false",
            "--track",
        ],
    )
    assert result_add.exit_code == 0
    result_update = runner.invoke(update, ["--yes"])
    assert result_update.exit_code == 0

    esg = Esgpull(install_path)  # Reinitialize to ensure fresh data
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
            "frequency:fx",
            "institution_id:CNRM-CERFACS",
            "member_id:r9i1p1f2",
            "source_id:CNRM-ESM2-1",
            "table_id:fx",
            "variable:areacella",
            "variable_id:areacella",
            "--distrib",
            "false",
            "--track",
        ],
    )
    assert result_add.exit_code == 0
    result_update = runner.invoke(update, ["--yes"])
    assert result_update.exit_code == 0

    esg = Esgpull(install_path)  # Reinitialize to ensure fresh data
    query = esg.graph.get(query_id)
    assert nb_files_after_update == len(query.files)

    InstallConfig.setup()
