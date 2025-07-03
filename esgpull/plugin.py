import importlib.util
import inspect
import logging
import sys
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Literal

import tomlkit
from packaging import version

import esgpull.models
from esgpull.config import cast_value
from esgpull.tui import logger
from esgpull.version import __version__


# Define event types
class Event(str, Enum):
    file_complete = "file_complete"
    file_error = "file_error"
    dataset_complete = "dataset_complete"


@dataclass
class HandlerSpec:
    event: Event
    func: Callable
    source: str
    parameters: list[inspect.Parameter]


EventSpecs: dict[Event, HandlerSpec] = {}


def spec(event: Event):
    def decorator(func: Callable):
        global EventSpecs
        source = inspect.getsource(func)
        sig = inspect.signature(func)
        parameters = list(sig.parameters.values())
        EventSpecs[event] = HandlerSpec(
            event=event,
            func=func,
            source=source,
            parameters=parameters,
        )

    return decorator


@spec(Event.file_complete)
def my_file_complete(
    file: esgpull.models.File,
    destination: Path,
    start_time: datetime,
    end_time: datetime,
    logger: logging.Logger,
):
    """Spec for Event.file_complete handler."""


@spec(Event.file_error)
def my_file_error(
    file: esgpull.models.File,
    exception: Exception,
    logger: logging.Logger,
):
    """Spec for Event.file_error handler."""


@spec(Event.dataset_complete)
def my_dataset_complete(
    dataset: esgpull.models.Dataset,
    logger: logging.Logger,
):
    """Spec for Event.dataset_complete handler."""


@dataclass
class EventHandler:
    """Represents a registered event handler function"""

    func: Callable
    event: Event
    plugin_name: str
    priority: Literal["low", "normal", "high"] = "normal"

    def validate_signature(self):
        """Validate handler signature based on parameter names"""
        sig = inspect.signature(self.func)
        spec = EventSpecs[self.event]
        checked: set[str] = set()
        for param in spec.parameters:
            if param.name not in sig.parameters:
                raise ValueError(
                    f"Handler for {self.event} must have '{param.name}' parameter"
                )
            checked.add(param.name)
        for name, param in sig.parameters.items():
            if name in checked:
                continue
            if param.default == inspect._empty:
                raise ValueError(
                    f"Handler cannot have extra parameters without default values: {param.name}"
                )


@dataclass
class Plugin:
    """Represents a loaded plugin"""

    name: str
    module: Any
    handlers: list[EventHandler] = field(default_factory=list)

    @property
    def config_class(self) -> type | None:
        """Get the plugin's Config class if it exists"""
        return getattr(self.module, "Config", None)

    @property
    def min_version(self) -> str | None:
        """Get the plugin's minimum compatible version"""
        return getattr(self.module, "MIN_ESGPULL_VERSION", None)

    @property
    def max_version(self) -> str | None:
        """Get the plugin's maximum compatible version"""
        return getattr(self.module, "MAX_ESGPULL_VERSION", None)

    def is_compatible(self) -> bool:
        """Check if the plugin is compatible with the current app version"""
        current = version.parse(__version__)

        if self.min_version and current < version.parse(self.min_version):
            return False

        if self.max_version and current > version.parse(self.max_version):
            return False

        return True


@dataclass
class PluginConfig:
    enabled: set[str] = field(default_factory=set)
    disabled: set[str] = field(default_factory=set)
    plugins: dict[str, dict[str, Any]] = field(default_factory=dict)
    _raw: dict[str, dict[str, Any]] = field(default_factory=dict)


class PluginManager:
    """Manages plugin discovery, loading and execution"""

    config_path: Path
    enabled: bool
    plugins: dict[str, Plugin]
    config: PluginConfig

    def __init__(self, config_path: Path):
        self.plugins = {}
        self._handlers_by_event: dict[Event, list[EventHandler]] = {
            event_type: [] for event_type in Event
        }
        self.config_path = config_path
        self.config = PluginConfig()
        self.load_config()
        self._lock = threading.RLock()
        self.enabled = False

    def discover_plugins(
        self,
        plugins_dir: Path,
        load_all: bool = False,
    ) -> None:
        """Discover and load plugins from the plugins directory"""
        # Load each Python file as a plugin
        python_files = list(plugins_dir.glob("*.py"))

        for plugin_path in python_files:
            plugin_name = plugin_path.stem

            # For normal operation (not load_all), only load enabled plugins
            if not load_all and not self.is_plugin_enabled(plugin_name):
                logger.debug(f"Skipping disabled plugin: {plugin_name}")
                continue

            self._load_plugin(plugin_name, plugin_path)

    def load_config(self) -> None:
        """Load plugin configuration from dedicated plugins.toml file"""
        if not self.config_path or not self.config_path.exists():
            return

        try:
            with open(self.config_path, "r") as f:
                raw = tomlkit.parse(f.read())
                self.config.enabled = set(raw.get("enabled", []))
                self.config.disabled = set(raw.get("disabled", []))
                self.config.plugins = raw.get("plugins", {})
                # Store the raw plugin configuration to preserve what's on disk
                self.config._raw = dict(raw.get("plugins", {}))
        except Exception as e:
            logger.error(f"Failed to load plugin config: {e}")

    def write_config(self, generate_full_config: bool = False) -> None:
        """Save plugin configuration to dedicated plugins.toml file

        Args:
            generate_full_config: If True, write the entire config.
                                  If False, only write explicitly changed values.
        """
        if not self.config_path:
            return

        try:
            doc = tomlkit.document()
            doc["enabled"] = list(self.config.enabled)
            doc["disabled"] = list(self.config.disabled)

            # For plugins section, handle differently based on generate_full_config flag
            if generate_full_config:
                doc["plugins"] = self.config.plugins
            else:
                doc["plugins"] = self.config._raw

            with open(self.config_path, "w") as f:
                f.write(tomlkit.dumps(doc))
        except Exception as e:
            logger.error(f"Failed to save plugin config: {e}")

    def _load_plugin(self, name: str, path: Path) -> None:
        """Load a plugin module from the given path"""
        if name in self.plugins:
            logger.debug(f"Plugin {name} already loaded")
            return
        try:
            logger.debug(f"Loading plugin: {name} from {path}")

            # Load the module using importlib
            spec = importlib.util.spec_from_file_location(
                f"esgpull.plugins.{name}", path
            )
            if spec is None or spec.loader is None:
                raise ValueError()

            module = importlib.util.module_from_spec(spec)
            sys.modules[spec.name] = module

            # Create plugin instance
            plugin = Plugin(name=name, module=module)
            self.plugins[name] = plugin

            # Execute the module
            spec.loader.exec_module(module)
            logger.debug(f"Successfully executed module code for {name}")

            # Check compatibility after module execution
            if not plugin.is_compatible():
                raise ValueError(
                    f"Plugin {name} is not compatible with current app version"
                )

            # Validate handler signatures after version compatibility check
            for handler in plugin.handlers:
                handler.validate_signature()

            # Apply configuration
            if plugin.config_class is not None:
                if name not in self.config.plugins:
                    self.config.plugins[name] = {}
                for key, value in vars(plugin.config_class).items():
                    if key.startswith("__"):
                        continue
                    if key in self.config.plugins[name]:
                        continue
                    self.config.plugins[name][key] = value
            self._configure_plugin(plugin)

            # Register plugin
            self.plugins[name] = plugin
            logger.info(f"Successfully loaded plugin: {name}")

        except Exception as e:
            if name in self.plugins:
                plugin = self.plugins.pop(name)
                for handler in plugin.handlers:
                    for handlers in self._handlers_by_event.values():
                        if handler in handlers:
                            handlers.remove(handler)
            logger.error(f"Failed to load plugin {name}: {e}")
            logger.exception(e)

    def is_plugin_enabled(self, plugin_name: str) -> bool:
        """Check if a plugin is enabled in the configuration"""
        # Check if explicitly disabled
        if plugin_name in self.config.disabled:
            return False

        else:
            return plugin_name in self.config.enabled

    def _configure_plugin(self, plugin: Plugin) -> None:
        """Configure a plugin based on plugins.toml configuration"""
        if not plugin.config_class:
            return

        # Get plugin-specific configuration
        plugin_section = f"{plugin.name}"
        if plugin_section in self.config.plugins:
            plugin_config = self.config.plugins[plugin_section]

            # Apply configuration to the plugin's Config class
            for key, value in plugin_config.items():
                if hasattr(plugin.config_class, key):
                    setattr(plugin.config_class, key, value)
                else:
                    logger.error(
                        f"Skipping wrong option for {plugin.name}: {key}"
                    )

    def register_handler(
        self,
        event: Event,
        func: Callable,
        plugin_name: str,
        priority: Literal["low", "normal", "high"] = "normal",
    ) -> None:
        """Register an event handler"""
        handler = EventHandler(
            func=func,
            event=event,
            plugin_name=plugin_name,
            priority=priority,
        )

        # Add handler to plugin
        if plugin_name in self.plugins:
            self.plugins[plugin_name].handlers.append(handler)

        # Add to event registry
        self._handlers_by_event[event].append(handler)

        # Sort handlers by priority
        self._sort_handlers(event)

    def _sort_handlers(self, event: Event) -> None:
        """Sort handlers by priority (high, normal, low)"""

        def handler_key(h: EventHandler) -> int:
            match h.priority:
                case "high":
                    return 0
                case "normal":
                    return 1
                case "low":
                    return 2
                case _:
                    return 1

        self._handlers_by_event[event].sort(key=handler_key)

    def trigger_event(
        self, event_type: Event, reraise: bool = False, **kwargs
    ) -> list[Any]:
        """Trigger an event, executing all registered handlers synchronously"""
        # Skip if plugins are globally disabled
        if not self.enabled:
            return []

        with self._lock:
            handlers = self._handlers_by_event[event_type]
            results = []

            handler_kwargs = {}
            spec = EventSpecs[event_type]
            for param in spec.parameters:
                if param.name in kwargs:
                    handler_kwargs[param.name] = kwargs[param.name]

            for handler in handlers:
                plugin = self.plugins.get(handler.plugin_name)
                if not plugin or not self.is_plugin_enabled(
                    handler.plugin_name
                ):
                    continue

                # Execute handler with timing
                start_time = time.perf_counter()
                try:
                    plugin_logger = logging.getLogger(
                        f"esgpull.plugins.{plugin.name}"
                    )
                    result = handler.func(
                        logger=plugin_logger, **handler_kwargs
                    )
                    end_time = time.perf_counter()
                    execution_time = (
                        end_time - start_time
                    ) * 1000  # Convert to ms

                    # Always log trace info (will only show at INFO level)
                    logger.info(
                        f"[TRACE] Plugin {plugin.name}.{handler.func.__name__} executed ({execution_time:.1f}ms)"
                    )

                    results.append(result)

                except Exception as e:
                    end_time = time.perf_counter()
                    execution_time = (end_time - start_time) * 1000

                    logger.error(
                        f"Plugin {plugin.name} failed on {event_type}: {e}"
                    )
                    logger.exception(e)

                    # Always log trace info for failed execution too
                    logger.info(
                        f"[TRACE] Plugin {plugin.name}.{handler.func.__name__} failed ({execution_time:.1f}ms)"
                    )

                    if reraise:
                        raise

            return results

    # Configuration management methods
    def enable_plugin(self, plugin_name: str) -> bool:
        """Enable a plugin by name"""
        if plugin_name in self.config.disabled:
            self.config.disabled.remove(plugin_name)
        self.config.enabled.add(plugin_name)
        self.write_config()
        return True

    def disable_plugin(self, plugin_name: str) -> bool:
        """Disable a plugin by name"""
        self.config.disabled.add(plugin_name)
        if plugin_name in self.config.enabled:
            self.config.enabled.remove(plugin_name)
        self.write_config()
        return True

    def set_plugin_config(
        self,
        plugin_name: str,
        key: str,
        value: Any,
    ) -> Any:
        """Set a configuration value for a plugin"""
        # Ensure plugin section exists
        if plugin_name not in self.plugins:
            raise ValueError(f"Plugin {plugin_name} not found")
        plugin = self.plugins[plugin_name]
        if plugin.config_class is None:
            raise ValueError(f"Plugin {plugin_name} has no Config class")

        # Make sure plugin exists in both plugins and _raw dicts
        if plugin_name not in self.config.plugins:
            self.config.plugins[plugin_name] = {}
        if plugin_name not in self.config._raw:
            self.config._raw[plugin_name] = {}

        # Update the value in both places
        if key in self.config.plugins[plugin_name]:
            old_value = self.config.plugins[plugin_name][key]
            new_value = cast_value(old_value, value, key)
            self.config.plugins[plugin_name][key] = new_value
            # Also update in _raw to keep in sync
            self.config._raw[plugin_name][key] = new_value
        else:
            raise KeyError(key, self.config.plugins[plugin_name])

        self.write_config()
        setattr(plugin.config_class, key, value)
        return old_value

    def unset_plugin_config(
        self,
        plugin_name: str,
        key: str,
    ) -> None:
        """Unset a configuration value for a plugin"""
        # Ensure plugin section exists
        if plugin_name not in self.plugins:
            raise ValueError(f"Plugin {plugin_name} not found")
        plugin = self.plugins[plugin_name]
        if plugin.config_class is None:
            raise ValueError(f"Plugin {plugin_name} has no Config class")

        # Make sure the plugin exists in both configs
        if plugin_name not in self.config.plugins:
            self.config.plugins[plugin_name] = {}
        if plugin_name not in self.config._raw:
            self.config._raw[plugin_name] = {}

        # Remove the key from both configs
        if key in self.config.plugins[plugin_name]:
            self.config.plugins[plugin_name].pop(key)
        else:
            raise KeyError(key, self.config.plugins[plugin_name])

        # Also remove from _raw if it exists
        if (
            plugin_name in self.config._raw
            and key in self.config._raw[plugin_name]
        ):
            self.config._raw[plugin_name].pop(key)

        self.write_config()

    def get_plugin_config(self, plugin_name: str) -> dict:
        """Get configuration for a plugin"""
        if plugin_name in self.config.plugins:
            return self.config.plugins[plugin_name]
        return {}


# Lazy-loaded singleton
_plugin_manager: PluginManager | None = None


def set_plugin_manager(pm: PluginManager) -> None:
    """Create the plugin manager singleton"""
    global _plugin_manager
    if _plugin_manager is None:
        _plugin_manager = pm
    else:
        raise ValueError("PluginManager is already initialized")


def get_plugin_manager() -> PluginManager:
    """Get the plugin manager singleton"""
    if _plugin_manager is None:
        raise ValueError("PluginManager was never initialized")
    return _plugin_manager


def on(
    event: Event,
    *,
    priority: Literal["low", "normal", "high"] = "normal",
) -> Callable:
    """Decorator to register a function as an event handler."""

    def decorator(handler_func):
        # Get plugin name from module
        module_parts = handler_func.__module__.split(".")
        plugin_name = module_parts[-1]

        get_plugin_manager().register_handler(
            event=event,
            func=handler_func,
            plugin_name=plugin_name,
            priority=priority,
        )
        return handler_func

    return decorator


def emit(event_type, **kwargs):
    """
    Emit an event to be handled by plugins.

    This function runs handlers in the current thread and returns their results.
    """
    return get_plugin_manager().trigger_event(event_type, **kwargs)
