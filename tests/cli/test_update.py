from time import perf_counter

from click.testing import CliRunner

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
