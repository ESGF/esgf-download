import asyncio
import shutil
from datetime import datetime
from pathlib import Path

import pytest

import esgpull.plugin
from esgpull.esgpull import Esgpull
from esgpull.models import File, FileStatus
from esgpull.plugin import (
    Event,
    PluginManager,
)


@pytest.fixture
def plugin_dir(tmp_path: str):
    """Create a temporary plugin directory for testing"""
    temp_dir = Path(tmp_path) / "plugins"
    temp_dir.mkdir(parents=True)
    return temp_dir


@pytest.fixture
def config_path(plugin_dir: Path) -> Path:
    """Create a temporary config file path"""
    return plugin_dir / "plugins.toml"


@pytest.fixture
def plugin_manager(config_path: Path) -> PluginManager:
    """Create a plugin manager with a temporary config file"""
    pm = PluginManager(config_path)
    pm.enabled = True
    esgpull.plugin._plugin_manager = pm
    return pm


@pytest.fixture
def assets_path() -> Path:
    """Path to the assets directory containing sample plugins."""
    return Path(__file__).parent / "assets"


def copy_plugin(plugin_name: str, plugin_dir: Path, assets_path: Path):
    plugin_file = f"{plugin_name}.py"
    shutil.copy(assets_path / plugin_file, plugin_dir / plugin_file)
    return plugin_name


@pytest.fixture
def sample_plugin(plugin_dir: Path, assets_path: Path) -> str:
    return copy_plugin("sample_plugin", plugin_dir, assets_path)


@pytest.fixture
def incompatible_plugin(plugin_dir: Path, assets_path: Path) -> str:
    return copy_plugin("incompatible_plugin", plugin_dir, assets_path)


@pytest.fixture
def priority_test_plugin(plugin_dir: Path, assets_path: Path) -> str:
    return copy_plugin("priority_test_plugin", plugin_dir, assets_path)


@pytest.fixture
def error_plugin(plugin_dir: Path, assets_path: Path) -> str:
    return copy_plugin("error_plugin", plugin_dir, assets_path)


def test_plugin_discovery_and_loading(
    plugin_dir: Path,
    plugin_manager: PluginManager,
    sample_plugin: str,
):
    """Test that plugin discovery finds and loads plugins correctly"""
    plugin_manager.discover_plugins(plugin_dir, load_all=True)
    assert sample_plugin in plugin_manager.plugins

    # Check the plugin attributes
    plugin = plugin_manager.plugins[sample_plugin]
    assert plugin.name == sample_plugin
    assert plugin.min_version == "0.8.0"
    assert plugin.max_version is None

    # Verify config class was found
    assert hasattr(plugin.module, "Config")
    assert plugin.module.Config.log_level == "INFO"
    assert plugin.module.Config.max_retries == 3

    # Check that handlers are registered for the right events
    assert any(
        h.plugin_name == sample_plugin and h.event == Event.file_complete
        for h in plugin_manager._handlers_by_event[Event.file_complete]
    )
    assert any(
        h.plugin_name == sample_plugin and h.event == Event.file_error
        for h in plugin_manager._handlers_by_event[Event.file_error]
    )
    assert any(
        h.plugin_name == sample_plugin and h.event == Event.dataset_complete
        for h in plugin_manager._handlers_by_event[Event.dataset_complete]
    )

    # Verify priorities
    file_complete_handler = next(
        h
        for h in plugin_manager._handlers_by_event[Event.file_complete]
        if h.plugin_name == sample_plugin
    )
    assert file_complete_handler.priority == "normal"

    file_error_handler = next(
        h
        for h in plugin_manager._handlers_by_event[Event.file_error]
        if h.plugin_name == sample_plugin
    )
    assert file_error_handler.priority == "high"

    dataset_complete_handler = next(
        h
        for h in plugin_manager._handlers_by_event[Event.dataset_complete]
        if h.plugin_name == sample_plugin
    )
    assert dataset_complete_handler.priority == "low"


def test_discovery_only_enabled(
    plugin_dir: Path,
    plugin_manager: PluginManager,
    sample_plugin: str,
):
    """Test that plugin discovery finds and loads plugins correctly"""
    plugin_manager.discover_plugins(plugin_dir)
    assert len(plugin_manager.plugins) == 0
    plugin_manager.enable_plugin(sample_plugin)
    plugin_manager.discover_plugins(plugin_dir)
    assert len(plugin_manager.plugins) == 1


def test_plugin_enable_disable(plugin_dir, plugin_manager, sample_plugin):
    """Test enabling and disabling plugins"""
    plugin_manager.discover_plugins(plugin_dir, load_all=True)
    plugin_manager.enable_plugin(sample_plugin)
    assert sample_plugin in plugin_manager.config.enabled
    assert plugin_manager.is_plugin_enabled(sample_plugin) is True

    plugin_manager.disable_plugin(sample_plugin)
    assert sample_plugin in plugin_manager.config.disabled
    assert sample_plugin not in plugin_manager.config.enabled
    assert plugin_manager.is_plugin_enabled(sample_plugin) is False


def test_triggered_plugin(plugin_dir, plugin_manager, sample_plugin):
    plugin_manager.discover_plugins(plugin_dir, load_all=True)
    # Create a test File object
    test_file = File(
        file_id="enable_disable_test",
        dataset_id="test_dataset_id",
        master_id="test_master_id",
        url="http://example.com/test.nc",
        version="1.0",
        filename="enable_disable_test.nc",
        local_path="/path/to/test",
        data_node="test_node",
        checksum="12345",
        checksum_type="md5",
        size=1000,
        status=FileStatus.Done,
    )
    destination = Path()

    plugin_manager.enable_plugin(sample_plugin)
    plugin_manager.plugins[sample_plugin].module.calls["file_complete"] = 0
    plugin_manager.trigger_event(
        Event.file_complete,
        file=test_file,
        destination=destination,
        start_time=datetime.now(),
        end_time=datetime.now(),
    )
    assert (
        plugin_manager.plugins[sample_plugin].module.calls["file_complete"]
        == 1
    )

    plugin_manager.disable_plugin(sample_plugin)
    plugin_manager.plugins[sample_plugin].module.calls["file_complete"] = 0
    plugin_manager.trigger_event(
        Event.file_complete,
        file=test_file,
        destination=destination,
        start_time=datetime.now(),
        end_time=datetime.now(),
    )
    assert (
        plugin_manager.plugins[sample_plugin].module.calls["file_complete"]
        == 0
    )


def test_version_compatibility(
    plugin_dir: Path,
    plugin_manager: PluginManager,
    sample_plugin: str,
    incompatible_plugin: str,
):
    """Test version compatibility checks for plugins"""
    plugin_manager.discover_plugins(plugin_dir, load_all=True)
    assert sample_plugin in plugin_manager.plugins
    assert incompatible_plugin not in plugin_manager.plugins

    # Check that only the compatible plugin's handlers are registered
    assert any(
        h.plugin_name == sample_plugin
        for h in plugin_manager._handlers_by_event[Event.file_complete]
    )
    assert not any(
        h.plugin_name == incompatible_plugin
        for h in plugin_manager._handlers_by_event[Event.file_complete]
    )


def test_priority_order(plugin_dir, plugin_manager, priority_test_plugin):
    """Test that handlers execute in priority order"""
    plugin_manager.enable_plugin(priority_test_plugin)
    plugin_manager.discover_plugins(plugin_dir)
    assert priority_test_plugin in plugin_manager.plugins
    plugin = plugin_manager.plugins[priority_test_plugin]
    plugin.module.execution_order = []

    test_file = File(
        file_id="priority_test_file",
        dataset_id="test_dataset_id",
        master_id="test_master_id",
        url="http://example.com/test.nc",
        version="1.0",
        filename="priority_test.nc",
        local_path="/path/to/test",
        data_node="test_node",
        checksum="12345",
        checksum_type="md5",
        size=1000,
        status=FileStatus.Done,
    )
    destination = Path()
    plugin_manager.trigger_event(
        Event.file_complete,
        file=test_file,
        destination=destination,
        start_time=datetime.now(),
        end_time=datetime.now(),
    )
    assert plugin.module.execution_order == ["high", "normal", "low"]


def test_plugin_config_set(plugin_dir, plugin_manager, sample_plugin):
    """Test plugin set config value"""
    plugin_manager.enable_plugin(sample_plugin)
    plugin_manager.discover_plugins(plugin_dir)
    assert sample_plugin in plugin_manager.plugins

    plugin = plugin_manager.plugins[sample_plugin]
    assert plugin.module.Config.log_level == "INFO"
    assert plugin.module.Config.notification_method == "console"
    assert plugin.module.Config.max_retries == 3

    assert sample_plugin in plugin_manager.config.plugins
    plugin_config = plugin_manager.config.plugins[sample_plugin]
    assert plugin_config["log_level"] == "INFO"
    assert plugin_config["notification_method"] == "console"
    assert plugin_config["max_retries"] == 3

    # Modify a config value
    plugin_manager.set_plugin_config(sample_plugin, "log_level", "DEBUG")
    assert plugin_config["log_level"] == "DEBUG"
    assert plugin.module.Config.log_level == "DEBUG"
    assert plugin.module.Config.notification_method == "console"
    assert plugin.module.Config.max_retries == 3


def test_plugin_config_load(plugin_dir, plugin_manager, sample_plugin):
    """Test plugin load config after settings values"""
    plugin_manager.enable_plugin(sample_plugin)
    plugin_manager.discover_plugins(plugin_dir)
    plugin_manager.set_plugin_config(sample_plugin, "log_level", "DEBUG")
    plugin_manager.set_plugin_config(sample_plugin, "max_retries", "150")
    plugin_manager.write_config(generate_full_config=True)

    # Create new plugin manager with same config file
    new_pm = PluginManager(plugin_manager.config_path)
    new_pm.enabled = True
    new_pm.load_config()
    new_pm.discover_plugins(plugin_dir)
    plugin_config = new_pm.config.plugins[sample_plugin]
    assert plugin_config["log_level"] == "DEBUG"
    assert plugin_config["notification_method"] == "console"
    assert plugin_config["max_retries"] == 150
    plugin = new_pm.plugins[sample_plugin]
    assert plugin.module.Config.log_level == "DEBUG"
    assert plugin.module.Config.notification_method == "console"
    assert plugin.module.Config.max_retries == 150


def test_error_isolation(
    plugin_dir, plugin_manager, sample_plugin, error_plugin
):
    """Test that errors in one plugin don't affect others"""
    plugin_manager.enable_plugin(sample_plugin)
    plugin_manager.enable_plugin(error_plugin)
    plugin_manager.discover_plugins(plugin_dir)
    assert sample_plugin in plugin_manager.plugins
    assert error_plugin in plugin_manager.plugins
    error_plugin_calls = plugin_manager.plugins[error_plugin].module.calls
    sample_plugin_calls = plugin_manager.plugins[sample_plugin].module.calls
    error_plugin_calls["file_complete"] = 0
    sample_plugin_calls["file_complete"] = 0

    # Create a test File object
    test_file = File(
        file_id="error_isolation_test",
        dataset_id="test_dataset_id",
        master_id="test_master_id",
        url="http://example.com/test.nc",
        version="1.0",
        filename="error_isolation_test.nc",
        local_path="/path/to/test",
        data_node="test_node",
        checksum="12345",
        checksum_type="md5",
        size=1000,
        status=FileStatus.Done,
    )
    destination = Path()

    # All handlers are correctly called even if one raises
    plugin_manager.trigger_event(
        Event.file_complete,
        file=test_file,
        destination=destination,
        start_time=datetime.now(),
        end_time=datetime.now(),
    )
    assert error_plugin_calls["file_complete"] == 1
    assert sample_plugin_calls["file_complete"] == 1

    with pytest.raises(ValueError):
        # Trigger the file_complete event
        # The error_plugin has high priority and will run first and raise an exception
        plugin_manager.trigger_event(
            Event.file_complete,
            file=test_file,
            destination=destination,
            start_time=datetime.now(),
            end_time=datetime.now(),
            reraise=True,
        )
    assert error_plugin_calls["file_complete"] == 2
    assert sample_plugin_calls["file_complete"] == 1


def test_download_event_workflow(assets_path, tmp_path):
    """Test plugin integration with the download workflow"""

    # Create an Esgpull instance with install=True to set up the directory
    install_path = tmp_path / "esgpull"
    esg = Esgpull(path=install_path, install=True)

    # Copy the sample plugin to the plugins directory
    esg.config.paths.plugins.mkdir(parents=True, exist_ok=True)
    plugin_name = "sample_plugin"
    plugin_file = f"{plugin_name}.py"
    shutil.copy(
        assets_path / plugin_file, esg.config.paths.plugins / plugin_file
    )

    # Create an Esgpull instance
    esg = Esgpull(install_path)
    esg.plugin_manager.enabled = True
    esg.plugin_manager.enable_plugin(plugin_name)
    esg.plugin_manager.discover_plugins(esg.config.paths.plugins)
    assert plugin_name in esg.plugin_manager.plugins
    module = esg.plugin_manager.plugins[plugin_name].module

    # Create a File object for our test file
    test_file = File(
        file_id="test_file",
        dataset_id="esgpull",
        master_id="license",
        url="https://raw.githubusercontent.com/ESGF/esgf-download/refs/heads/main/LICENSE",
        version="1",
        filename="license",
        local_path="test/path",
        data_node="github",
        checksum="954a863c65831c7c639140ee6208cb2d8d974175e7fc41d4edf9eb4161c6ba07",
        checksum_type="SHA256",
        size=1540,
        status=FileStatus.Queued,
    )
    test_file.compute_sha()
    module.calls["file_complete"] = 0
    module.calls["file_error"] = 0
    coro = esg.download([test_file], show_progress=False, use_db=False)
    downloaded, errors = asyncio.run(coro)
    assert len(downloaded) == 1
    assert len(errors) == 0
    assert module.calls["file_complete"] == 1
    assert module.calls["file_error"] == 0

    # Create a File object with a nonexistent URL
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
    module.calls["file_complete"] = 0
    module.calls["file_error"] = 0
    coro = esg.download([failing_file], show_progress=False, use_db=False)
    downloaded, errors = asyncio.run(coro)
    assert len(downloaded) == 0
    assert len(errors) == 1
    assert module.calls["file_complete"] == 0
    assert module.calls["file_error"] == 1
