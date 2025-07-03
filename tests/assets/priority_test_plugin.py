# Plugin for testing execution priority order

from esgpull.plugin import Event, on

# Track execution order for testing
execution_order = []


@on(Event.file_complete, priority="high")
def handle_file_complete_high(file, destination, start_time, end_time, logger):
    """High priority handler for file_complete event"""
    logger.info(f"High priority handler: {file.filename}")
    execution_order.append("high")


@on(Event.file_complete, priority="normal")
def handle_file_complete_normal(
    file, destination, start_time, end_time, logger
):
    """Normal priority handler for file_complete event"""
    logger.info(f"Normal priority handler: {file.filename}")
    execution_order.append("normal")


@on(Event.file_complete, priority="low")
def handle_file_complete_low(file, destination, start_time, end_time, logger):
    """Low priority handler for file_complete event"""
    logger.info(f"Low priority handler: {file.filename}")
    execution_order.append("low")
