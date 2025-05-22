# Plugin that raises an exception for testing error isolation

from esgpull.plugin import Event, on

# Track calls for testing
calls = {
    "file_downloaded": 0
}

@on(Event.file_downloaded, priority="high")  # High priority to run before other plugins
def handle_file_downloaded(file, logger):
    """Handler that raises an exception"""
    logger.info(f"About to raise an exception for: {file.filename}")
    calls["file_downloaded"] += 1
    # Deliberately raise an exception
    raise ValueError(f"Deliberate test exception in error_plugin for {file.filename}")