# Plugins

`esgpull` includes a plugin system that allows you to extend functionality by running custom code when specific events occur during data operations.

## Available events

- **file_complete**: A file download completes successfully
- **file_error**: A file download fails
- **dataset_complete**: All files from a dataset are downloaded

Each event handler receives only the parameters relevant to that specific event.

## Enable plugins

Plugins are disabled by default. Enable them using the main config command:

```shell
$ esgpull config plugins.enabled true
```

```shell
[plugins]
enabled = true

Previous value: False
```


## Plugin management

### List available plugins

```shell
$ esgpull plugins ls
```

```shell
        plugin        │      event       │        function         
══════════════════════╪══════════════════╪═════════════════════════
 🟢 notification     │  file_complete │       notify_download   
                      │ file_error │        notify_error   
 🔴 checksum_verify  │  file_complete │      verify_checksum    
 🔴 archive_backup   │  file_complete │      backup_file  
                    │  dataset_complete │        backup_dataset
```

This detailed information can also be shown with JSON format with `--json`.

### Create a new plugin

Create a plugin template with all available events:

```shell
$ esgpull plugins create -n notification
```

```shell
Plugin template created at: /path/to/esgpull/plugins/notification.py
Edit the file to implement your custom plugin logic.
```

Create a plugin for specific events only:

```shell
$ esgpull plugins create -n notification file_complete file_error
```

```shell
Plugin template created at: /path/to/esgpull/plugins/notification.py
Edit the file to implement your custom plugin logic.
```

### Enable and disable plugins

```shell
$ esgpull plugins enable notification
```

```shell
Plugin 'notification' enabled.
```

```shell
$ esgpull plugins disable notification
```

```shell
Plugin 'notification' disabled.
```


### Test plugins

Test plugins by triggering one event with sample data:

```shell
$ esgpull plugins test file_complete
```

```shell
[2025-06-19 17:33:09]  INFO      esgpull.plugins.notification
✅ Downloaded: tas_Amon_CESM2_historical_r1i1p1f1_gn_185001-201412.nc
   Size: 524288000 bytes

[2025-06-19 17:33:09]  INFO      esgpull.plugins.checksum_verify
✓ Checksum verified for tas_Amon_CESM2_historical_r1i1p1f1_gn_185001-201412.nc
```

This is the primary debugging tool for plugin development. Use it to verify handlers work correctly before running actual downloads or updates.

## Plugin example

Here's a simple notification plugin that sends a message when files are downloaded:

```python title="plugins/notification.py"
import pathlib
import logging
from esgpull.plugin import Event, on
import esgpull.models

@on(Event.file_complete, priority="normal")
def notify_download(file: esgpull.models.File, destination: pathlib.Path, logger: logging.Logger):
    """Send notification when a file is downloaded."""
    print(f"✅ Downloaded: {file.filename}")
    print(f"   Size: {file.size} bytes")
    logger.info(f"Notified download: {file.filename}")

@on(Event.file_error, priority="normal") 
def notify_error(file: esgpull.models.File, exception: Exception, logger: logging.Logger):
    """Send notification when a download fails."""
    print(f"❌ Failed: {file.filename}")
    print(f"   Error: {exception}")
    logger.error(f"Notified error: {file.filename}")
```

## Plugin configuration

Plugins are configured via an optional `Config` class in the plugin code. The `Config` class attributes define parameters and must include default values.

Plugin configuration is stored separately from esgpull's main config file. This file contains a manifest of enabled/disabled plugins and their custom configurations (which override the `Config` class defaults). Use the `esgpull plugins config` subcommand to manage these settings.

Let's extend our notification plugin with configuration:

```python title="plugins/notification.py"
import pathlib
import logging
from esgpull.plugin import Event, on
import esgpull.models

class Config:
    enabled = True
    email_address = "user@example.com"
    include_size = True
    error_alerts = True

@on(Event.file_complete, priority="normal")
def notify_download(file: esgpull.models.File, destination: pathlib.Path, logger: logging.Logger):
    """Send notification when a file is downloaded."""
    if Config.enabled:
        print(f"✅ Downloaded: {file.filename}")
        if Config.include_size:
            print(f"   Size: {file.size} bytes")
        logger.info(f"Notified download to {Config.email_address}: {file.filename}")

@on(Event.file_error, priority="normal") 
def notify_error(file: esgpull.models.File, exception: Exception, logger: logging.Logger):
    """Send notification when a download fails."""
    if Config.enabled and Config.error_alerts:
        print(f"❌ Failed: {file.filename}")
        print(f"   Error: {exception}")
        logger.error(f"Notified error to {Config.email_address}: {file.filename}")
```

View all plugin configurations:
```shell
$ esgpull plugins config
```

```shell
─ /path/to/esgpull/plugins.toml ─
[notification]
enabled = true
email_address = "admin@myproject.org"
include_size = true
error_alerts = true

[checksum_verify]
enabled = false
algorithm = "sha256"

[archive_backup]
enabled = false
archive_path = "/backup/esgf"
```

View specific plugin configuration:
```shell
$ esgpull plugins config notification
```

```shell
[notification]
enabled = true
email_address = "admin@myproject.org"
include_size = true
error_alerts = true
```

View a specific configuration value:
```shell
$ esgpull plugins config notification.email_address
```

```shell
[notification]
email_address = "admin@myproject.org"
```

Set a configuration value:
```shell
$ esgpull plugins config notification.email_address alerts@newdomain.com
```

```shell
[notification]
email_address = "alerts@newdomain.com"

Previous value: admin@myproject.org
```

Reset to default value:
```shell
$ esgpull plugins config notification.email_address --default
```

```shell
👍 Config reset to default for notification.email_address
```

### Config class details

The `Config` class supports these data types:

- **Strings** (`str`): Text values like `"INFO"` or `"user@example.com"`
- **Integers** (`int`): Whole numbers like `3` or `100`  
- **Floats** (`float`): Decimal numbers like `0.5` or `10.75`
- **Booleans** (`bool`): `True` or `False` values

```python
class Config:
    """Example showing all supported data types"""
    # String configuration
    log_level = "INFO"
    email_address = "user@example.com"
    
    # Integer configuration  
    max_retries = 3
    timeout_seconds = 30
    
    # Float configuration
    threshold = 0.75
    delay_factor = 1.5
    
    # Boolean configuration
    notifications_enabled = True
    debug_mode = False
```

**Important limitations:**
- Lists and dictionaries can be used in the `Config` class, but cannot be configured through the CLI `esgpull plugins config` command
- For list/dictionary attributes, you must edit the `plugins.toml` file directly
- Only simple scalar values (strings, integers, floats, booleans) can be managed via CLI commands
- Configuration values are automatically type-checked when modified through CLI

### Configuration management

Plugin configurations are stored separately from the main `esgpull` configuration in a `plugins.toml` file. Only plugins with a `Config` class can be configured.

The configuration system follows these principles:

- **Defaults in code**: Class attributes define default values
- **Overrides in file**: Only explicitly changed values are saved to `plugins.toml`
- **Type safety**: Values are automatically converted and validated based on the default type
