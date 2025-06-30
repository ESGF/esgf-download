# Plugin with incompatible version requirements for testing

from esgpull.plugin import Event, on

# Version compatibility - requiring a future version
MIN_ESGPULL_VERSION = (
    "999.0.0"  # Intentionally using a version that doesn't exist
)
MAX_ESGPULL_VERSION = None


# Configuration class
class Config:
    """Configuration for incompatible_plugin"""

    log_level = "INFO"


# Track calls for testing
calls = {"file_complete": 0}


@on(Event.file_complete)
def handle_file_complete(file, destination, start_time, end_time, logger):
    """Handle file complete event"""
    logger.info(f"This shouldn't run: {file.filename}")
    calls["file_complete"] += 1
