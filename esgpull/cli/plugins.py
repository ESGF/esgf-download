from collections import OrderedDict
from textwrap import dedent

import click
from click.exceptions import Abort, BadOptionUsage

from esgpull import Esgpull
from esgpull.cli.decorators import args, opts
from esgpull.cli.utils import extract_subdict, init_esgpull, totable
from esgpull.models import Dataset, File, FileStatus
from esgpull.plugin import Event, emit
from esgpull.tui import Verbosity
from esgpull.version import __version__


@click.group()
def plugins():
    """Manage plugins."""
    pass


def check_enabled(esg: Esgpull):
    if not esg.plugin_manager.enabled:
        esg.ui.print("Plugin system is [red]disabled[/]")
        esg.ui.print("To enable it, run:")
        esg.ui.print("    esgpull config plugins.enabled true")
        raise Abort


@plugins.command("ls")
@click.option(
    "--json", "json_output", is_flag=True, help="Output in JSON format"
)
@opts.verbosity
def list_plugins(verbosity: Verbosity, json_output: bool = False):
    """List all available plugins and their status."""
    esg = init_esgpull(verbosity=verbosity)

    with esg.ui.logging("plugins", onraise=Abort):
        check_enabled(esg)
        esg.plugin_manager.discover_plugins(
            esg.config.paths.plugins, load_all=True
        )
        if json_output:
            # Format for JSON output
            result = {}
            for name, p in esg.plugin_manager.plugins.items():
                # Get handlers by event type
                handlers = {}
                for h in p.handlers:
                    event_type = h.event.value
                    if event_type not in handlers:
                        handlers[event_type] = []
                    handlers[event_type].append(
                        {
                            "function": h.func.__name__,
                            "priority": h.priority,
                        }
                    )

                result[name] = {
                    "enabled": esg.plugin_manager.is_plugin_enabled(name),
                    "handlers": handlers,
                    "min_version": p.min_version,
                    "max_version": p.max_version,
                }

            esg.ui.print(result, json=True)
        else:
            # Format for human-readable output
            if not esg.plugin_manager.plugins:
                esg.ui.print("No plugins found.")
                return

            # Prepare table data
            table_data = []
            for name, p in esg.plugin_manager.plugins.items():
                enabled = esg.plugin_manager.is_plugin_enabled(name)
                status_circle = "🟢" if enabled else "🔴"
                plugin_name = f"{status_circle} {name}"

                # Collect event handlers with their details
                handler_rows = []
                events_by_type = {}
                for h in p.handlers:
                    event_type = h.event.value
                    if event_type not in events_by_type:
                        events_by_type[event_type] = []
                    events_by_type[event_type].append(h.func.__name__)

                # Create handler rows with event type and function names
                for event_type, handler_names in events_by_type.items():
                    for handler_name in handler_names:
                        handler_rows.append(
                            {"event": event_type, "function": handler_name}
                        )

                # First row with plugin info and first handler
                if handler_rows:
                    first_handler = handler_rows[0]
                    first_row = OrderedDict(
                        [
                            ("plugin", plugin_name),
                            ("event", first_handler["event"]),
                            ("function", first_handler["function"]),
                        ]
                    )
                    table_data.append(first_row)

                    # Additional rows for remaining handlers
                    for handler in handler_rows[1:]:
                        additional_row = OrderedDict(
                            [
                                ("plugin", ""),
                                ("event", handler["event"]),
                                ("function", handler["function"]),
                            ]
                        )
                        table_data.append(additional_row)
                else:
                    # Plugin with no handlers
                    row = OrderedDict(
                        [
                            ("plugin", plugin_name),
                            ("event", ""),
                            ("function", ""),
                        ]
                    )
                    table_data.append(row)

            # Create and print table
            table = totable(table_data)
            esg.ui.print(table)


@plugins.command("enable")
@click.argument("plugin_name")
@opts.verbosity
def enable_plugin_cmd(plugin_name: str, verbosity: Verbosity):
    """Enable a plugin."""
    esg = init_esgpull(verbosity=verbosity)
    plugin_path = esg.config.paths.plugins / f"{plugin_name}.py"
    if not plugin_path.exists():
        esg.ui.print(f"Plugin file '{plugin_name}.py' not found.")
        return

    result = esg.plugin_manager.enable_plugin(plugin_name)
    if result:
        esg.ui.print(f"Plugin '{plugin_name}' enabled.")
    else:
        esg.ui.print(f"Failed to enable plugin '{plugin_name}'.")


@plugins.command("disable")
@click.argument("plugin_name")
@opts.verbosity
def disable_plugin_cmd(plugin_name: str, verbosity: Verbosity):
    """Disable a plugin."""
    esg = init_esgpull(verbosity=verbosity)

    # Check if the plugin file exists (even if not loaded)
    plugin_path = esg.config.paths.plugins / f"{plugin_name}.py"
    if not plugin_path.exists():
        esg.ui.print(f"Plugin file '{plugin_name}.py' not found.")
        return

    result = esg.plugin_manager.disable_plugin(plugin_name)
    if result:
        esg.ui.print(f"Plugin '{plugin_name}' disabled.")
    else:
        esg.ui.print(f"Failed to disable plugin '{plugin_name}'.")


@plugins.command("config")
@args.key
@args.value
@opts.default
@opts.generate
@opts.verbosity
def config_plugin(
    verbosity: Verbosity,
    key: str | None,
    value: str | None,
    default: bool,
    generate: bool,
    # config_set: str | None = None,
    # config_unset: str | None = None,
):
    """View or modify plugin configuration."""
    esg = init_esgpull(verbosity=verbosity)

    with esg.ui.logging("plugins", onraise=Abort):
        check_enabled(esg)
        esg.plugin_manager.discover_plugins(
            esg.config.paths.plugins, load_all=True
        )
        if key is not None:
            plugin_name = key.split(".", 1)[0]
            plugin_path = esg.config.paths.plugins / f"{plugin_name}.py"
            if not plugin_path.exists():
                esg.ui.print(f"Plugin file '{plugin_name}.py' not found.")
                raise Abort
        if key is not None and value is not None:
            """update config"""
            if default:
                raise BadOptionUsage(
                    "default",
                    dedent(
                        f"""
                        --default/-d is invalid with a value.
                        Instead use:

                        $ esgpull plugins config {key} -d
                        """
                    ),
                )
            # Split key into plugin_name and remaining_path
            plugin_name, *rest = key.split(".", 1)
            remaining_path = rest[0] if rest else None

            # Set the configuration value
            if remaining_path:
                old_value = esg.plugin_manager.set_plugin_config(
                    plugin_name, remaining_path, value
                )
                info = extract_subdict(esg.plugin_manager.config.plugins, key)
                esg.ui.print(info, toml=True)
                esg.ui.print(f"Previous value: {old_value}")
            else:
                esg.ui.print(f"Cannot set plugin name directly: {plugin_name}")
                raise Abort
        elif key is not None:
            if default:
                # Split key into plugin_name and remaining_path
                plugin_name, *rest = key.split(".", 1)
                remaining_path = rest[0] if rest else None

                if remaining_path:
                    esg.plugin_manager.unset_plugin_config(
                        plugin_name, remaining_path
                    )
                    msg = f":+1: Config reset to default for {key}"
                    esg.ui.print(msg)
                else:
                    esg.ui.print(
                        f"Cannot reset plugin directly: {plugin_name}"
                    )
                    raise Abort
            else:
                config = esg.plugin_manager.config.plugins
                config = extract_subdict(config, key)
                esg.ui.print(config, toml=True)
        elif generate:
            # Write the config file with all current settings
            esg.plugin_manager.write_config(generate_full_config=True)
            msg = f":+1: Plugin config generated at {esg.plugin_manager.config_path}"
            esg.ui.print(msg)

        else:
            esg.ui.rule(str(esg.plugin_manager.config_path))
            esg.ui.print(esg.plugin_manager.config.plugins, toml=True)


@plugins.command("test")
@click.argument(
    "event_type",
    type=click.Choice([e.value for e in Event]),
)
@opts.verbosity
def test_plugin(
    event_type: str,
    verbosity: Verbosity,
):
    """Test plugin events."""
    esg = init_esgpull(verbosity=verbosity)
    with esg.ui.logging("plugins", onraise=Abort):
        check_enabled(esg)
        event = Event(event_type)
        # Create sample test data
        file1 = File(
            file_id="file1",
            dataset_id="dataset",
            master_id="master",
            url="file",
            version="v0",
            filename="file.nc",
            local_path="project/folder",
            data_node="data_node",
            checksum="0",
            checksum_type="0",
            size=2**42,
            status=FileStatus.Queued,
        )
        file2 = file1.clone()
        file2.file_id = "file2"
        file2.status = FileStatus.Done
        error = ValueError("Placeholder example error")
        dataset = Dataset(dataset_id="dataset", total_files=2)
        dataset.files = [file1, file2]
        emit(
            event,
            file=file1,
            dataset=dataset,
            error=error,
        )


@plugins.command("create")
@click.argument(
    "events",
    nargs=-1,
    type=click.Choice([e.value for e in Event]),
)
@click.option("-n", "--name", required=True, type=str)
@opts.verbosity
def create_plugin(
    name: str, verbosity: Verbosity, events: list[str] | None = None
):
    """Create a new plugin template."""
    esg = init_esgpull(verbosity=verbosity)

    # Create plugin file path
    plugin_path = esg.config.paths.plugins / f"{name}.py"
    if plugin_path.exists():
        esg.ui.print(f"Plugin file already exists: {plugin_path}")
        return

    # If no events specified, include them all
    if not events:
        events = [e.value for e in Event]

    # Generate template
    template = f"""# {name} plugin for ESGPull
# 
# This plugin was auto-generated. Edit as needed.
from datetime import datetime
from pathlib import Path
from logging import Logger

from esgpull.models import File, Query
from esgpull.plugin import Event, on

# Specify version compatibility (optional)
MIN_ESGPULL_VERSION = "{__version__}"
MAX_ESGPULL_VERSION = None

# Configuration class for the plugin (optional)
class Config:
    \"""Configuration for {name} plugin\"""
    # Add your configuration options here
    timeout = 30  # Example config option
    log_level = "INFO"  # Example config option

"""

    # Add handlers for requested events
    handlers = {
        "file_complete": """
# File complete event handler
@on(Event.file_complete, priority="normal")
def handle_file_complete(
    file: File, 
    destination: Path, 
    start_time: datetime,
    end_time: datetime,
    logger: Logger
):
    \"""Handle file complete event\"""
    logger.info(f"File downloaded: {file.filename}")
    # Add your custom logic here
""",
        "file_error": """
# Download error event handler
@on(Event.file_error, priority="normal")
def handle_file_error(file: File, exception: Exception, logger: Logger):
    \"""Handle file error event\"""
    logger.error(f"Download failed for {file.filename}: {exception}")
    # Add your custom logic here
""",
        "dataset_complete": """
# Dataset complete event handler
@on(Event.dataset_complete, priority="normal")
def handle_dataset_complete(dataset: Dataset, logger: Logger):
    \"""Handle dataset complete event\"""
    logger.error(f"Dataset downloaded: {dataset.dataset_id}")
    # Add your custom logic here
""",
    }

    for event in events:
        template += handlers[event]

    # Write the plugin file
    with open(plugin_path, "w") as f:
        f.write(template)

    esg.ui.print(f"Plugin template created at: {plugin_path}")
    esg.ui.print("Edit the file to implement your custom plugin logic.")
