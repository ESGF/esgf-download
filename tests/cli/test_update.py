from time import perf_counter

import pytest
from click.testing import CliRunner

from esgpull.cli.add import add
from esgpull.cli.config import config
from esgpull.cli.update import update
from esgpull.constants import INSTALLS_PATH_ENV
from esgpull.install_config import _InstallConfig


@pytest.fixture
def runner(tmp_path):
    InstallConfig = _InstallConfig(tmp_path)
    idx = InstallConfig.add(tmp_path / "esgpull")
    InstallConfig.choose(idx=idx)
    InstallConfig.write()
    _runner = CliRunner(env={INSTALLS_PATH_ENV: tmp_path.as_posix()})
    return _runner


def test_fast_update(runner: CliRunner):
    result_update = runner.invoke(config, ["api.page_limit", "10000"])
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
    start = perf_counter()
    result_update = runner.invoke(update, ["--yes"])
    stop = perf_counter()
    assert result_add.exit_code == 0
    assert result_update.exit_code == 0
    assert stop - start < 30  # 30 seconds to fetch ~6k files is plenty enough
