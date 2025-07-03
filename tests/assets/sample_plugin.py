# Sample plugin for testing
# This plugin defines handlers for all supported event types

from esgpull.plugin import Event, on

# Version compatibility
MIN_ESGPULL_VERSION = "0.8.0"
MAX_ESGPULL_VERSION = None


# Configuration class
class Config:
    """Configuration for sample_plugin"""

    log_level = "INFO"
    notification_method = "console"
    max_retries = 3


# Track calls for testing
calls = {"file_complete": 0, "file_error": 0, "dataset_complete": 0}


@on(Event.file_complete, priority="normal")
def handle_file_complete(file, destination, start_time, end_time, logger):
    """Handle file complete event"""
    logger.info(f"File downloaded: {file.filename}")
    calls["file_complete"] += 1


@on(Event.file_error, priority="high")
def handle_file_error(file, exception, logger):
    """Handle file error event"""
    logger.error(f"Download failed for {file.filename}: {exception}")
    calls["file_error"] += 1


@on(Event.dataset_complete, priority="low")
def handle_dataset_complete(dataset, logger):
    """Handle dataset_complete updated event"""
    logger.info(f"Dataset downloaded: {dataset.dataset_id}")
    calls["dataset_complete"] += 1
