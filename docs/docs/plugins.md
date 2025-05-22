# Plugins

`esgpull` includes a plugin system that allows you to extend functionality by running custom code when specific events occur during data operations. Plugins can respond to file downloads, query updates, and download failures.

## Enable plugins

Plugins are disabled by default. Enable them using the configuration command:

```shell
$ esgpull config plugins.enabled true
```

## Basic workflow example

```shell
$ esgpull config plugins.enabled true
$ esgpull plugins ls
$ esgpull plugins create notification_plugin
$ esgpull plugins enable notification_plugin
$ esgpull plugins ls
```

## Plugin management

### List available plugins

```shell
$ esgpull plugins ls
```

View detailed information in JSON format:

```shell
$ esgpull plugins ls --json
```

### Create a new plugin

Create a plugin template with all available events:

```shell
$ esgpull plugins create my_plugin
```

Create a plugin for specific events only:

```shell
$ esgpull plugins create notification_plugin --events file_downloaded
```

### Enable and disable plugins

```shell
$ esgpull plugins enable my_plugin
$ esgpull plugins disable my_plugin
```

### View event signatures

See available events and their handler signatures:

```shell
$ esgpull plugins signatures
```

## Plugin example

Here's a simple notification plugin that sends a message when files are downloaded:

```python title="plugins/notification_plugin.py"
from esgpull.plugin import Event, on

@on(Event.file_downloaded, priority="normal")
def notify_download(file, logger):
    """Send notification when a file is downloaded."""
    print(f"✅ Downloaded: {file.filename}")
    print(f"   Size: {file.size} bytes")
    logger.info(f"Notified download: {file.filename}")

@on(Event.download_failure, priority="normal") 
def notify_failure(file, exception, logger):
    """Send notification when a download fails."""
    print(f"❌ Failed: {file.filename}")
    print(f"   Error: {exception}")
    logger.error(f"Notified failure: {file.filename}")
```

## Plugin configuration

Plugins can define their own configuration options by including a `Config` class in the plugin code:

```python title="plugins/configurable_plugin.py"
from esgpull.plugin import Event, on

class Config:
    notification_enabled = True
    max_retries = 3
    email_address = "user@example.com"

@on(Event.file_downloaded, priority="normal")
def notify_download(file, logger):
    if Config.notification_enabled:
        print(f"Sending notification to {Config.email_address}")
        print(f"✅ Downloaded: {file.filename}")
        logger.info(f"Sent notification for {file.filename}")
```

View and modify plugin settings:

```shell
# View all plugin configurations
$ esgpull plugins config

# View specific plugin configuration
$ esgpull plugins config configurable_plugin

# Set a configuration value
$ esgpull plugins config configurable_plugin.email_address user@newdomain.com

# Reset to default value
$ esgpull plugins config configurable_plugin.email_address --default
```

Plugin configurations are stored separately from the main `esgpull` configuration in a `plugins.toml` file. Only plugins with a `Config` class can be configured.

## Available events

- **file_downloaded**: Triggered when a file download completes successfully
- **download_failure**: Triggered when a file download fails
- **query_updated**: Triggered when query data is refreshed from ESGF

Each event provides relevant data to the handler function. Use `esgpull plugins signatures` to see the exact parameters for each event type.