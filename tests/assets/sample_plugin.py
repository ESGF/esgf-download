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
calls = {"file_complete": 0, "download_failure": 0, "query_updated": 0}


@on(Event.file_complete, priority="normal")
def handle_file_complete(file, logger):
    """Handle file complete event"""
    logger.info(f"File downloaded: {file.filename}")
    calls["file_complete"] += 1


@on(Event.download_failure, priority="high")
def handle_download_failure(file, exception, logger):
    """Handle download failure event"""
    logger.error(f"Download failed for {file.filename}: {exception}")
    calls["download_failure"] += 1


@on(Event.query_updated, priority="low")
def handle_query_updated(query, logger):
    """Handle query updated event"""
    logger.info(f"Query updated: {query.name if query.name else 'unnamed'}")
    calls["query_updated"] += 1

