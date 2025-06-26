import shutil
import tempfile
from pathlib import Path

import pytest
from click.testing import CliRunner

from esgpull.cli.plugins import (
    config_plugin,
    create_plugin,
    disable_plugin_cmd,
    enable_plugin_cmd,
    list_plugins,
)


@pytest.fixture
def cli_runner():
    """Create a CLI runner for testing CLI commands"""
    return CliRunner()


@pytest.fixture
def plugin_dir():
    """Create a temporary plugin directory for testing"""
    temp_dir = Path(tempfile.mkdtemp()) / "plugins"
    temp_dir.mkdir(parents=True)
    yield temp_dir
    shutil.rmtree(temp_dir.parent)


@pytest.fixture
def assets_path():
    """Path to the assets directory containing sample plugins."""
    return Path(__file__).parent.parent / "assets"


def test_plugins_list_command(tmp_path, assets_path):
    """Test the 'esgpull plugins ls' command"""
    import shutil

    from esgpull.cli.config import config
    from esgpull.cli.self import install
    from esgpull.install_config import InstallConfig

    InstallConfig.setup(tmp_path)
    install_path = tmp_path / "esgpull"
    runner = CliRunner()

    # Install esgpull
    result_install = runner.invoke(install, [f"{install_path}"])
    assert result_install.exit_code == 0

    # Enable plugin system
    result_config = runner.invoke(config, ["plugins.enabled", "true"])
    assert result_config.exit_code == 0

    # Copy sample plugin to plugins directory
    plugins_dir = install_path / "plugins"
    plugins_dir.mkdir(exist_ok=True)
    shutil.copy(assets_path / "sample_plugin.py", plugins_dir)

    # Test list command with no plugins loaded initially
    result = runner.invoke(list_plugins)
    assert result.exit_code == 0
    assert "sample_plugin" in result.output

    # Test JSON output
    result = runner.invoke(list_plugins, ["--json"])
    assert result.exit_code == 0
    # Should contain JSON output with plugin info
    assert "sample_plugin" in result.output

    # Clean up
    InstallConfig.setup()


def test_plugins_create_command(tmp_path):
    """Test the 'esgpull plugins create' command"""
    from esgpull.cli.config import config
    from esgpull.cli.self import install
    from esgpull.install_config import InstallConfig

    InstallConfig.setup(tmp_path)
    install_path = tmp_path / "esgpull"
    runner = CliRunner()

    # Install esgpull
    result_install = runner.invoke(install, [f"{install_path}"])
    assert result_install.exit_code == 0

    # Enable plugin system
    result_config = runner.invoke(config, ["plugins.enabled", "true"])
    assert result_config.exit_code == 0

    # Test creating a plugin with specific events
    result = runner.invoke(
        create_plugin,
        ["--name", "test_plugin", "file_complete", "file_error"],
    )
    assert result.exit_code == 0

    # Verify plugin file was created
    plugin_file = install_path / "plugins" / "test_plugin.py"
    assert plugin_file.exists()

    # Verify content contains expected handlers
    content = plugin_file.read_text()
    assert "handle_file_complete" in content
    assert "handle_file_error" in content
    assert "@on(Event.file_complete" in content
    assert "@on(Event.file_error" in content
    assert (
        "handle_dataset_complete" not in content
    )  # Should only include specified events

    # Test creating a plugin with no specific events (should include all)
    result = runner.invoke(create_plugin, ["--name", "all_events_plugin"])
    assert result.exit_code == 0

    plugin_file_all = install_path / "plugins" / "all_events_plugin.py"
    assert plugin_file_all.exists()

    content_all = plugin_file_all.read_text()
    assert "handle_file_complete" in content_all
    assert "handle_file_error" in content_all
    assert "handle_dataset_complete" in content_all

    # Test creating a plugin that already exists
    result = runner.invoke(create_plugin, ["--name", "test_plugin"])
    assert result.exit_code == 0
    assert "already exists" in result.output

    # Clean up
    InstallConfig.setup()


def test_plugins_enable_disable_command(tmp_path, assets_path):
    """Test the enable and disable plugin commands"""
    import shutil

    from esgpull.cli.config import config
    from esgpull.cli.self import install
    from esgpull.install_config import InstallConfig

    InstallConfig.setup(tmp_path)
    install_path = tmp_path / "esgpull"
    runner = CliRunner()

    # Install esgpull
    result_install = runner.invoke(install, [f"{install_path}"])
    assert result_install.exit_code == 0

    # Enable plugin system
    result_config = runner.invoke(config, ["plugins.enabled", "true"])
    assert result_config.exit_code == 0

    # Copy sample plugin to plugins directory
    plugins_dir = install_path / "plugins"
    plugins_dir.mkdir(exist_ok=True)
    shutil.copy(assets_path / "sample_plugin.py", plugins_dir)

    # Test enabling a plugin
    result = runner.invoke(enable_plugin_cmd, ["sample_plugin"])
    assert result.exit_code == 0
    assert "enabled" in result.output

    # Test disabling a plugin
    result = runner.invoke(disable_plugin_cmd, ["sample_plugin"])
    assert result.exit_code == 0
    assert "disabled" in result.output

    # Test enabling a non-existent plugin
    result = runner.invoke(enable_plugin_cmd, ["nonexistent_plugin"])
    assert result.exit_code == 0
    assert "not found" in result.output

    # Test disabling a non-existent plugin
    result = runner.invoke(disable_plugin_cmd, ["nonexistent_plugin"])
    assert result.exit_code == 0
    assert "not found" in result.output

    # Clean up
    InstallConfig.setup()


def test_plugins_config_command(tmp_path, assets_path):
    """Test the plugin configuration command"""
    import shutil

    from esgpull.cli.config import config
    from esgpull.cli.self import install
    from esgpull.install_config import InstallConfig

    InstallConfig.setup(tmp_path)
    install_path = tmp_path / "esgpull"
    runner = CliRunner()

    # Install esgpull
    result_install = runner.invoke(install, [f"{install_path}"])
    assert result_install.exit_code == 0

    # Enable plugin system
    result_config = runner.invoke(config, ["plugins.enabled", "true"])
    assert result_config.exit_code == 0

    # Copy sample plugin to plugins directory
    plugins_dir = install_path / "plugins"
    plugins_dir.mkdir(exist_ok=True)
    shutil.copy(assets_path / "sample_plugin.py", plugins_dir)

    # Enable the plugin first so it gets discovered
    result = runner.invoke(enable_plugin_cmd, ["sample_plugin"])
    assert result.exit_code == 0

    # Test viewing all plugin configuration
    result = runner.invoke(config_plugin)
    assert result.exit_code == 0

    # Test setting a plugin config value
    result = runner.invoke(config_plugin, ["sample_plugin.log_level", "DEBUG"])
    assert result.exit_code == 0

    # Test viewing specific plugin config
    result = runner.invoke(config_plugin, ["sample_plugin.log_level"])
    assert result.exit_code == 0
    assert "DEBUG" in result.output

    # Test resetting to default
    result = runner.invoke(
        config_plugin, ["sample_plugin.log_level", "--default"]
    )
    assert result.exit_code == 0

    # Test generate config
    result = runner.invoke(config_plugin, ["--generate"])
    assert result.exit_code == 0
    assert "generated" in result.output

    # Test invalid plugin (should fail)
    result = runner.invoke(config_plugin, ["nonexistent_plugin.option"])
    assert result.exit_code != 0
    assert "not found" in result.output

    # Clean up
    InstallConfig.setup()
