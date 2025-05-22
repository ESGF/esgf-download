import asyncio
import shutil
import tempfile
from pathlib import Path

import pytest

from esgpull.esgpull import Esgpull
from esgpull.models import File, FileStatus, Query
from esgpull.plugin import (
    Event,
    PluginManager,
    get_plugin_manager,
    set_plugin_manager,
)


@pytest.fixture
def plugin_dir():
    """Create a temporary plugin directory for testing"""
    temp_dir = Path(tempfile.mkdtemp()) / "plugins"
    temp_dir.mkdir(parents=True)
    yield temp_dir
    shutil.rmtree(temp_dir.parent)


@pytest.fixture
def config_path(tmp_path) -> Path:
    """Create a temporary config file path"""
    return tmp_path / "plugins.toml"


@pytest.fixture
def plugin_manager(config_path) -> PluginManager:
    """Create a plugin manager with a temporary config file"""
    try:
        return get_plugin_manager()
    except ValueError:
        pm = PluginManager(config_path)
        pm.enabled = True
        set_plugin_manager(pm)
        return pm


@pytest.fixture
def assets_path() -> Path:
    """Path to the assets directory containing sample plugins."""
    return Path(__file__).parent / "assets"


def test_plugin_enable_disable(plugin_dir, plugin_manager, assets_path):
    """Test enabling and disabling plugins"""
    # Copy the sample plugin to the test plugins directory
    plugin_name = "sample_plugin"
    plugin_file = f"{plugin_name}.py"
    shutil.copy(assets_path / plugin_file, plugin_dir / plugin_file)

    # Enable the plugin via the plugin manager
    plugin_manager.enable_plugin(plugin_name)

    # Verify it's in the enabled list
    assert plugin_name in plugin_manager.config.enabled
    assert plugin_manager.is_plugin_enabled(plugin_name) is True

    # Run discovery process
    plugin_manager.discover_plugins(plugin_dir)

    # Verify the plugin was loaded
    assert plugin_name in plugin_manager.plugins

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

    # Reset the call counter
    plugin_manager.plugins[plugin_name].module.calls["file_downloaded"] = 0

    # Trigger the event
    plugin_manager.trigger_event(Event.file_downloaded, file=test_file)

    # Verify the handler executed
    assert (
        plugin_manager.plugins[plugin_name].module.calls["file_downloaded"]
        == 1
    )

    # Now disable the plugin
    plugin_manager.disable_plugin(plugin_name)

    # Verify it's in the disabled list and not in enabled list
    assert plugin_name in plugin_manager.config.disabled
    assert plugin_name not in plugin_manager.config.enabled
    assert plugin_manager.is_plugin_enabled(plugin_name) is False

    # Reset the call counter
    plugin_manager.plugins[plugin_name].module.calls["file_downloaded"] = 0

    # Trigger the event again
    plugin_manager.trigger_event(Event.file_downloaded, file=test_file)

    # Verify the handler was not called this time
    assert (
        plugin_manager.plugins[plugin_name].module.calls["file_downloaded"]
        == 0
    )


def test_plugin_discovery_and_loading(plugin_dir, plugin_manager, assets_path):
    """Test that plugin discovery finds and loads plugins correctly"""
    # Copy the sample plugin to the test plugins directory
    plugin_name = "sample_plugin"
    plugin_file = f"{plugin_name}.py"
    shutil.copy(assets_path / plugin_file, plugin_dir / plugin_file)

    # Enable the plugin
    plugin_manager.enable_plugin(plugin_name)

    # Run discovery process
    plugin_manager.discover_plugins(plugin_dir)

    # Verify the plugin was loaded
    assert plugin_name in plugin_manager.plugins

    # Check the plugin attributes
    plugin = plugin_manager.plugins[plugin_name]
    assert plugin.name == plugin_name
    assert plugin.min_version == "0.8.0"
    assert plugin.max_version is None

    # Verify config class was found
    assert hasattr(plugin.module, "Config")
    assert plugin.module.Config.log_level == "INFO"
    assert plugin.module.Config.max_retries == 3

    # Check that handlers are registered for the right events
    assert any(
        h.plugin_name == plugin_name and h.event == Event.file_downloaded
        for h in plugin_manager._handlers_by_event[Event.file_downloaded]
    )
    assert any(
        h.plugin_name == plugin_name and h.event == Event.download_failure
        for h in plugin_manager._handlers_by_event[Event.download_failure]
    )
    assert any(
        h.plugin_name == plugin_name and h.event == Event.query_updated
        for h in plugin_manager._handlers_by_event[Event.query_updated]
    )

    # Verify priorities
    file_dl_handler = next(
        h
        for h in plugin_manager._handlers_by_event[Event.file_downloaded]
        if h.plugin_name == plugin_name
    )
    assert file_dl_handler.priority == "normal"

    dl_fail_handler = next(
        h
        for h in plugin_manager._handlers_by_event[Event.download_failure]
        if h.plugin_name == plugin_name
    )
    assert dl_fail_handler.priority == "high"

    query_handler = next(
        h
        for h in plugin_manager._handlers_by_event[Event.query_updated]
        if h.plugin_name == plugin_name
    )
    assert query_handler.priority == "low"


def test_version_compatibility(plugin_dir, plugin_manager, assets_path):
    """Test version compatibility checks for plugins"""
    # Copy both plugins to the test directory
    plugin_name = "sample_plugin"
    incompatible_name = "incompatible_plugin"

    shutil.copy(
        assets_path / f"{plugin_name}.py", plugin_dir / f"{plugin_name}.py"
    )
    shutil.copy(
        assets_path / f"{incompatible_name}.py",
        plugin_dir / f"{incompatible_name}.py",
    )

    # Enable both plugins
    plugin_manager.enable_plugin(plugin_name)
    plugin_manager.enable_plugin(incompatible_name)

    # Run discovery process
    plugin_manager.discover_plugins(plugin_dir)

    # Verify only the compatible plugin was loaded
    assert plugin_name in plugin_manager.plugins
    assert incompatible_name not in plugin_manager.plugins

    # Check that only the compatible plugin's handlers are registered
    assert any(
        h.plugin_name == plugin_name
        for h in plugin_manager._handlers_by_event[Event.file_downloaded]
    )
    assert not any(
        h.plugin_name == incompatible_name
        for h in plugin_manager._handlers_by_event[Event.file_downloaded]
    )


def test_event_execution(plugin_dir, plugin_manager, assets_path):
    """Test event execution and handler invocation"""
    # Copy the sample plugin to the test plugins directory
    plugin_name = "sample_plugin"
    plugin_file = f"{plugin_name}.py"
    shutil.copy(assets_path / plugin_file, plugin_dir / plugin_file)

    # Enable the plugin
    plugin_manager.enable_plugin(plugin_name)

    # Run discovery process
    plugin_manager.discover_plugins(plugin_dir)

    # Verify the plugin was loaded
    assert plugin_name in plugin_manager.plugins

    # Create a test File object
    test_file = File(
        file_id="test_file_id",
        dataset_id="test_dataset_id",
        master_id="test_master_id",
        url="http://example.com/test.nc",
        version="1.0",
        filename="test_file.nc",
        local_path="/path/to/test",
        data_node="test_node",
        checksum="12345",
        checksum_type="md5",
        size=1000,
        status=FileStatus.Done,
    )

    # Reset the call counter
    plugin_manager.plugins[plugin_name].module.calls["file_downloaded"] = 0

    # Trigger the file_downloaded event
    plugin_manager.trigger_event(Event.file_downloaded, file=test_file)

    # Verify the handler was called
    assert (
        plugin_manager.plugins[plugin_name].module.calls["file_downloaded"]
        == 1
    )

    # Test download_failure event
    test_exception = ValueError("Test download error")
    plugin_manager.plugins[plugin_name].module.calls["download_failure"] = 0

    # Trigger download_failure event
    plugin_manager.trigger_event(
        Event.download_failure,
        file=test_file,
        exception=test_exception,
    )

    # Verify the failure handler was called
    assert (
        plugin_manager.plugins[plugin_name].module.calls["download_failure"]
        == 1
    )

    # Test query_updated event
    test_query = Query(selection={"project": "CMIP6"})
    plugin_manager.plugins[plugin_name].module.calls["query_updated"] = 0

    # Trigger query_updated event
    plugin_manager.trigger_event(Event.query_updated, query=test_query)

    # Verify the query handler was called
    assert (
        plugin_manager.plugins[plugin_name].module.calls["query_updated"] == 1
    )


def test_priority_order(plugin_dir, plugin_manager, assets_path):
    """Test that handlers execute in priority order"""
    # Copy the priority test plugin to the test plugins directory
    plugin_name = "priority_test_plugin"
    plugin_file = f"{plugin_name}.py"
    shutil.copy(assets_path / plugin_file, plugin_dir / plugin_file)

    # Enable the plugin
    plugin_manager.enable_plugin(plugin_name)

    # Run discovery process
    plugin_manager.discover_plugins(plugin_dir)

    # Verify the plugin was loaded
    assert plugin_name in plugin_manager.plugins

    # Clear execution order tracking list
    plugin_manager.plugins[plugin_name].module.execution_order = []

    # Create a test File object
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

    # Trigger the file_downloaded event
    plugin_manager.trigger_event(Event.file_downloaded, file=test_file)

    # Get the execution order from the plugin module
    execution_order = plugin_manager.plugins[
        plugin_name
    ].module.execution_order

    # Verify handlers executed in correct priority order (high → normal → low)
    assert execution_order == ["high", "normal", "low"]


def test_plugin_configuration(plugin_dir, plugin_manager, assets_path):
    """Test plugin configuration management"""
    # Copy the sample plugin to the test plugins directory
    plugin_name = "sample_plugin"
    plugin_file = f"{plugin_name}.py"
    shutil.copy(assets_path / plugin_file, plugin_dir / plugin_file)

    # Enable the plugin
    plugin_manager.enable_plugin(plugin_name)

    # Run discovery process
    plugin_manager.discover_plugins(plugin_dir)

    # Verify the plugin was loaded
    assert plugin_name in plugin_manager.plugins

    # Verify default config was applied
    plugin = plugin_manager.plugins[plugin_name]
    assert plugin.module.Config.log_level == "INFO"
    assert plugin.module.Config.notification_method == "console"
    assert plugin.module.Config.max_retries == 3

    # Check config in plugin_manager.config.plugins
    assert plugin_name in plugin_manager.config.plugins
    assert plugin_manager.config.plugins[plugin_name]["log_level"] == "INFO"
    assert (
        plugin_manager.config.plugins[plugin_name]["notification_method"]
        == "console"
    )
    assert plugin_manager.config.plugins[plugin_name]["max_retries"] == 3

    # Modify a config value
    plugin_manager.set_plugin_config(plugin_name, "log_level", "DEBUG")

    # Verify change was applied to Config class
    assert plugin.module.Config.log_level == "DEBUG"

    # Verify change was applied to config
    assert plugin_manager.config.plugins[plugin_name]["log_level"] == "DEBUG"

    # Verify other config values remain unchanged
    assert plugin.module.Config.notification_method == "console"
    assert plugin.module.Config.max_retries == 3

    # Write config to file
    plugin_manager.write_config(generate_full_config=True)

    # Create new plugin manager with same config file
    new_pm = PluginManager(plugin_manager.config_path)
    new_pm.enabled = True

    # Load config
    new_pm.load_config()

    # Verify config values are preserved
    assert plugin_name in new_pm.config.plugins
    assert new_pm.config.plugins[plugin_name]["log_level"] == "DEBUG"
    assert (
        new_pm.config.plugins[plugin_name]["notification_method"] == "console"
    )
    assert new_pm.config.plugins[plugin_name]["max_retries"] == 3


def test_error_isolation(plugin_dir, plugin_manager, assets_path):
    """Test that errors in one plugin don't affect others"""
    # Copy both plugins to the test plugins directory
    plugin_name = "sample_plugin"
    error_plugin_name = "error_plugin"

    shutil.copy(
        assets_path / f"{plugin_name}.py", plugin_dir / f"{plugin_name}.py"
    )
    shutil.copy(
        assets_path / f"{error_plugin_name}.py",
        plugin_dir / f"{error_plugin_name}.py",
    )

    # Enable both plugins
    plugin_manager.enable_plugin(plugin_name)
    plugin_manager.enable_plugin(error_plugin_name)

    # Run discovery process
    plugin_manager.discover_plugins(plugin_dir)

    # Verify both plugins were loaded
    assert plugin_name in plugin_manager.plugins
    assert error_plugin_name in plugin_manager.plugins

    # Reset call counters
    plugin_manager.plugins[plugin_name].module.calls["file_downloaded"] = 0
    plugin_manager.plugins[error_plugin_name].module.calls[
        "file_downloaded"
    ] = 0

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

    # Capture log messages
    with pytest.raises(Exception):
        try:
            # Trigger the file_downloaded event
            # The error_plugin has high priority and will run first and raise an exception
            plugin_manager.trigger_event(
                Event.file_downloaded, file=test_file, reraise=True
            )
        except Exception as e:
            # Verify both handlers were called despite the error
            assert (
                plugin_manager.plugins[error_plugin_name].module.calls[
                    "file_downloaded"
                ]
                == 1
            )
            assert (
                plugin_manager.plugins[plugin_name].module.calls[
                    "file_downloaded"
                ]
                == 1
            )
            raise e

    # No error with normal usage although an error actually occurs
    plugin_manager.trigger_event(
        Event.file_downloaded, file=test_file, reraise=False
    )


def test_download_event_workflow(assets_path, tmp_path):
    """Test plugin integration with the download workflow"""

    # Create an Esgpull instance with install=True to set up the directory
    install_path = tmp_path / "esgpull"
    esg = Esgpull(path=install_path, install=True)

    # Copy the sample plugin to the plugins directory
    plugin_name = "sample_plugin"
    plugin_file = f"{plugin_name}.py"
    plugins_dir = install_path / "plugins"
    plugins_dir.mkdir(exist_ok=True)
    shutil.copy(assets_path / plugin_file, plugins_dir / plugin_file)

    # Create an Esgpull instance
    esg = Esgpull(install_path)
    esg.plugin_manager.enabled = True
    esg.plugin_manager.enable_plugin(plugin_name)
    esg.plugin_manager.discover_plugins(plugins_dir)
    assert esg.plugin_manager.enabled is True
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
    module.calls["file_downloaded"] = 0
    module.calls["download_failure"] = 0
    coro = esg.download([test_file], show_progress=False, use_db=False)
    downloaded, errors = asyncio.run(coro)
    assert len(downloaded) == 1
    assert len(errors) == 0
    assert module.calls["file_downloaded"] == 1
    assert module.calls["download_failure"] == 0

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
    module.calls["file_downloaded"] = 0
    module.calls["download_failure"] = 0
    coro = esg.download([failing_file], show_progress=False, use_db=False)
    downloaded, errors = asyncio.run(coro)
    assert len(downloaded) == 0
    assert len(errors) == 1
    assert module.calls["file_downloaded"] == 0
    assert module.calls["download_failure"] == 1
