# Plugin for testing execution priority order

from esgpull.plugin import Event, on

# Track execution order for testing
execution_order = []

@on(Event.file_downloaded, priority="high")
def handle_file_downloaded_high(file, logger):
    """High priority handler for file_downloaded event"""
    logger.info(f"High priority handler: {file.filename}")
    execution_order.append("high")

@on(Event.file_downloaded, priority="normal")
def handle_file_downloaded_normal(file, logger):
    """Normal priority handler for file_downloaded event"""
    logger.info(f"Normal priority handler: {file.filename}")
    execution_order.append("normal")

@on(Event.file_downloaded, priority="low")
def handle_file_downloaded_low(file, logger):
    """Low priority handler for file_downloaded event"""
    logger.info(f"Low priority handler: {file.filename}")
    execution_order.append("low")