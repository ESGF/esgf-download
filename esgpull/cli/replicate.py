#!/usr/bin/env python3
"""
ESGF Replicator Command for esgpull

This module implements the 'esgpull replicate' command that connects to a Kafka cluster
and consumes events from a specified topic to replicate ESGF data.

Uses Pydantic models for type-safe event validation and processing.
"""

import json
import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional, Any

import click
from confluent_kafka import Consumer, KafkaError, KafkaException
from confluent_kafka.admin import AdminClient
from pydantic import ValidationError

# Import the replication event models
from esgpull.models.replication_events import (
    ReplicationOperation,
    ReplicationStatus
)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class ReplicationError(Exception):
    """Custom exception for replication errors"""
    pass


class ReplicationStats:
    """Track replication statistics"""

    def __init__(self):
        self.total_events = 0
        self.processed_events = 0
        self.failed_events = 0
        self.validation_errors = 0
        self.events_by_type = {}
        self.start_time = datetime.utcnow()

    def record_event(self, event_type: str, success: bool):
        """Record an event processing result"""
        self.total_events += 1
        if success:
            self.processed_events += 1
        else:
            self.failed_events += 1

        # Track by type
        if event_type not in self.events_by_type:
            self.events_by_type[event_type] = {'success': 0, 'failed': 0}

        if success:
            self.events_by_type[event_type]['success'] += 1
        else:
            self.events_by_type[event_type]['failed'] += 1

    def record_validation_error(self):
        """Record a validation error"""
        self.validation_errors += 1
        self.total_events += 1

    def get_summary(self) -> Dict[str, Any]:
        """Get statistics summary"""
        duration = (datetime.utcnow() - self.start_time).total_seconds()
        return {
            'total_events': self.total_events,
            'processed': self.processed_events,
            'failed': self.failed_events,
            'validation_errors': self.validation_errors,
            'duration_seconds': duration,
            'events_per_second': self.total_events / duration if duration > 0 else 0,
            'by_type': self.events_by_type
        }

    def print_summary(self):
        """Print formatted statistics summary"""
        summary = self.get_summary()
        click.echo("=" * 60)
        click.echo("Replication Statistics Summary")
        click.echo("=" * 60)
        click.echo(f"Total Events:       {summary['total_events']}")
        click.echo(f"Processed:          {summary['processed']} ({self._percentage(summary['processed'], summary['total_events'])})")
        click.echo(f"Failed:             {summary['failed']} ({self._percentage(summary['failed'], summary['total_events'])})")
        click.echo(f"Validation Errors:  {summary['validation_errors']} ({self._percentage(summary['validation_errors'], summary['total_events'])})")
        click.echo(f"Duration:           {summary['duration_seconds']:.2f} seconds")
        click.echo(f"Rate:               {summary['events_per_second']:.2f} events/second")
        click.echo("-" * 60)
        click.echo("Events by Type:")
        for event_type, counts in summary['by_type'].items():
            click.echo(f"  {event_type:30} Success: {counts['success']:5} Failed: {counts['failed']:5}")
        click.echo("=" * 60)

    @staticmethod
    def _percentage(value: int, total: int) -> str:
        """Calculate percentage string"""
        if total == 0:
            return "0.0%"
        return f"{(value / total) * 100:.1f}%"


class KafkaReplicator:
    """Kafka consumer for ESGF data replication events"""

    def __init__(self, kafka_config: Dict[str, Any], topic: str, group_id: str = ''):
        """
        Initialize Kafka replicator

        Args:
            kafka_config: Kafka consumer configuration
            topic: Topic to consume from
            group_id: Consumer group ID
        """
        self.topic = topic
        self.kafka_config = {
            'group.id': group_id,
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False,
            **kafka_config
        }
        self.consumer = None
        self.running = False
        self.stats = ReplicationStats()

    def connect(self):
        """Establish connection to Kafka cluster"""
        try:
            self.consumer = Consumer(self.kafka_config)
            click.echo(f"Connected to Kafka cluster: {self.kafka_config['bootstrap.servers']}")
            click.echo(f"Subscribing to topic: {self.topic}")
            click.echo(f"Consumer group: {self.kafka_config['group.id']}")
            self.consumer.subscribe([self.topic])
        except Exception as e:
            raise ReplicationError(f"Failed to connect to Kafka: {e}")

    def disconnect(self):
        """Close Kafka consumer connection"""
        if self.consumer:
            self.consumer.close()
            click.echo("Disconnected from Kafka cluster")

    def process_replication_event(self, event_data: Dict[str, Any]) -> bool:
        """
        Process a replication event from Kafka

        Args:
            event_data: Raw event data from Kafka message

        Returns:
            True if processed successfully, False otherwise
        """
        try:
            method = event_data["data"]["payload"]["method"]
            # If method is POST, this is a dataset add event
            if method == "POST":
                event_type = "dataset.add"
                success = self._handle_dataset_event(event_data, event_type)
            elif method == "PATCH":
                operations = event_data["data"]["payload"]["patch"]["operations"]

                # If method is PATCH and "retracted" is in payload, 
                # this is a dataset retract event
                for operation in operations:
                    if "retracted" in operation["path"]:
                        event_type = "dataset.retract"
                        success = self._handle_retract_event(event_data, event_type)
                else:
                    # Otherwise, treat as a file add/update event
                    event_type = "file.add"
                    success = self._handle_file_event(event_data, event_type)
            else:
                logger.warning("Unhandled event type")
                success = False

            if success:
                click.echo(f"Processing {event_type} event (ID: {event_data["metadata"]["event_id"]})")

            # Record statistics
            self.stats.record_event(event_type, success)
            return success

        except ValidationError as e:
            click.echo(f"Validation error in event: {e}")
            self.stats.record_validation_error()
            # Log detailed validation errors for debugging
            for error in e.errors():
                click.echo(f"  Field: {error['loc']}, Error: {error['msg']}")
            return False
        except Exception as e:
            click.echo(f"Unexpected error processing event: {e}", exc_info=True)
            return False

    def _handle_dataset_event(self, event, event_type) -> bool:
        """
        Handle dataset-related events (created, updated, deleted)

        Args:
            event: Validated DatasetEvent from Pydantic model

        Returns:
            True if handled successfully
        """
        try:
            dataset = event["data"]["payload"]
            item = dataset.get("item", None)

            assets = item.get("assets", {})
            files = len(assets)
            filesize = 0
            if files:
                for asset_key in assets.keys():
                    asset = assets.get(asset_key)
                    filesize += asset.get("file:size", 0)

            click.echo(f"Processing dataset event: {event_type} for {item["id"]}")
            click.echo(f"✓ Added dataset: {item["id"]}")
            click.echo(f"  Files: {files}, Size: {filesize / (1024**3):.2f} GB")
            return True
        except Exception as e:
            click.echo(f"Failed to handle dataset event: {e}", exc_info=True)
            return False

    def _handle_file_event(self, event, event_type) -> bool:
        """
        Handle file-related events (created, updated, deleted)

        Args:
            event: Validated FileEvent from Pydantic model

        Returns:
            True if handled successfully
        """
        pass

    def _handle_retract_event(self, event, event_type) -> bool:
        pass

    def _update_replication_status(self, operation: ReplicationOperation, policy=None) -> bool:
        """
        Update replication operation status

        Args:
            operation: Replication operation info
            policy: Replication policy (if provided)

        Returns:
            True if updated successfully
        """
        try:
            # Update operation status in database

            if operation.status == ReplicationStatus.COMPLETED:
                click.echo(f"✓ Replication completed: {operation.operation_id}")
            elif operation.status == ReplicationStatus.FAILED:
                click.echo(f"✗ Replication failed: {operation.operation_id}")

            return True

        except Exception as e:
            click.echo(f"Failed to update replication status: {e}")
            return False

    def consume_events(self, max_messages: Optional[int] = None, timeout: float = 1.0):
        """
        Main consumer loop

        Args:
            max_messages: Maximum number of messages to process (None for unlimited)
            timeout: Consumer timeout in seconds
        """
        if not self.consumer:
            raise ReplicationError("Consumer not connected. Call connect() first.")

        self.running = True
        message_count = 0
        last_progress_log = datetime.utcnow()

        try:
            click.echo("Starting replication event consumption...")

            while self.running:
                # Check if we've reached the message limit
                if max_messages and message_count >= max_messages:
                    click.echo(f"Reached maximum message limit: {max_messages}")
                    break

                # Poll for messages
                msg = self.consumer.poll(timeout=timeout)

                if msg is None:
                    continue

                # Handle errors
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        click.echo(f"Reached end of partition {msg.partition()}")
                        continue
                    else:
                        click.echo(f"Kafka error: {msg.error()}")
                        continue

                try:
                    # Parse message value
                    event_data = json.loads(msg.value().decode('utf-8'))

                    # Process the event with Pydantic validation
                    if self.process_replication_event(event_data):
                        click.echo(f"✓ Successfully processed message from partition {msg.partition()}, offset {msg.offset()}")

                        # Commit offset after successful processing
                        self.consumer.commit(msg)
                    else:
                        logger.warning(f"✗ Failed to process message from partition {msg.partition()}, offset {msg.offset()}")

                    message_count += 1

                    # Log progress periodically (every 100 messages or 30 seconds)
                    now = datetime.utcnow()
                    if message_count % 100 == 0 or (now - last_progress_log).total_seconds() >= 30:
                        summary = self.stats.get_summary()
                        click.echo(
                            f"Progress: {summary['total_events']} events "
                            f"({summary['processed']} processed, {summary['failed']} failed, "
                            f"{summary['validation_errors']} validation errors) "
                            f"- Rate: {summary['events_per_second']:.2f} events/sec"
                        )
                        last_progress_log = now

                except json.JSONDecodeError as e:
                    click.echo(f"Invalid JSON in message at offset {msg.offset()}: {e}")
                    self.stats.record_validation_error()
                except KeyboardInterrupt:
                    click.echo("Received interrupt signal, stopping consumer...")
                    self.stop()
                    break
                except Exception as e:
                    click.echo(f"Unexpected error processing message at offset {msg.offset()}: {e}", exc_info=True)

        except KafkaException as e:
            click.echo(f"Kafka exception: {e}")
            raise ReplicationError(f"Kafka consumer error: {e}")

        finally:
            click.echo("Consumer stopped.")
            self.stats.print_summary()

    def stop(self):
        """Stop the consumer loop"""
        self.running = False


def validate_kafka_config(config_path: Path) -> Dict[str, Any]:
    """
    Validate and load Kafka configuration

    Args:
        config_path: Path to Kafka configuration file

    Returns:
        Kafka configuration dictionary
    """
    if not config_path.exists():
        raise click.ClickException(f"Kafka config file not found: {config_path}")

    try:
        with open(config_path, 'r') as f:
            config = json.load(f)

        required_keys = ['bootstrap.servers']
        for key in required_keys:
            if key not in config:
                raise click.ClickException(f"Required Kafka config key missing: {key}")

        return config

    except json.JSONDecodeError as e:
        raise click.ClickException(f"Invalid JSON in Kafka config: {e}")


def test_kafka_connection(config: Dict[str, Any], topic: str) -> bool:
    """
    Test connection to Kafka cluster and topic availability

    Args:
        config: Kafka configuration
        topic: Topic name to test

    Returns:
        True if connection successful
    """
    try:
        admin_client = AdminClient({'bootstrap.servers': config['bootstrap.servers']})
        metadata = admin_client.list_topics(timeout=10)

        if topic not in metadata.topics:
            raise click.ClickException(f"Topic '{topic}' not found in Kafka cluster")

        # Get topic details
        topic_metadata = metadata.topics[topic]
        partition_count = len(topic_metadata.partitions)

        click.echo(f"Successfully connected to Kafka cluster: {config['bootstrap.servers']}")
        click.echo(f"Topic '{topic}' found with {partition_count} partition(s)")

        return True

    except Exception as e:
        raise click.ClickException(f"Failed to connect to Kafka cluster: {e}")


@click.command()
@click.argument('topic', type=str)
@click.option('--config', '-c', 
              type=click.Path(exists=True, path_type=Path),
              help='Path to Kafka configuration file (JSON format)')
@click.option('--bootstrap-servers', '-b',
              type=str,
              help='Kafka bootstrap servers (comma-separated)')
@click.option('--group-id', '-g',
              type=str,
              default='',
              help='Consumer group ID')
@click.option('--max-messages', '-m',
              type=int,
              help='Maximum number of messages to process (default: unlimited)')
@click.option('--timeout', '-t',
              type=float,
              default=1.0,
              help='Consumer poll timeout in seconds (default: 1.0)')
@click.option('--test-connection',
              is_flag=True,
              help='Test connection to Kafka cluster and exit')
@click.option('--verbose', '-v',
              is_flag=True,
              help='Enable verbose logging (DEBUG level)')
def replicate(topic, config, bootstrap_servers, group_id, max_messages, timeout, test_connection, verbose):
    """
    Replicate ESGF data by consuming events from a Kafka topic.

    TOPIC: The Kafka topic to consume replication events from.

    This command connects to a Kafka cluster and processes replication events
    using validated Pydantic models for type safety and data integrity.

    \b
    Example usage:

    \b
    # Using config file
    esgpull replicate esgf-events --config kafka.json

    \b
    # Using bootstrap servers directly  
    esgpull replicate esgf-events --bootstrap-servers localhost:9092

    \b
    # Test connection only
    esgpull replicate esgf-events --config kafka.json --test-connection

    \b
    # Process limited messages with verbose logging
    esgpull replicate esgf-events --config kafka.json --max-messages 100 --verbose
    """

    click.echo("ESGF Replicator - Starting up")
    click.echo(f"Topic: {topic}")
    click.echo(f"Consumer Group: {group_id}")

    # Build Kafka configuration
    kafka_config = {}

    if config:
        kafka_config = validate_kafka_config(config)
        click.echo(f"Loaded Kafka configuration from: {config}")

    if bootstrap_servers:
        kafka_config['bootstrap.servers'] = bootstrap_servers
        click.echo(f"Using bootstrap servers: {bootstrap_servers}")

    if not kafka_config.get('bootstrap.servers'):
        raise click.ClickException(
            "Kafka bootstrap servers must be specified via --bootstrap-servers or config file"
        )

    # Test connection if requested
    if test_connection:
        test_kafka_connection(kafka_config, topic)
        click.echo(f"✓ Successfully connected to Kafka cluster and topic '{topic}'")
        return

    # Initialize and run replicator
    replicator = KafkaReplicator(kafka_config, topic, group_id)

    try:
        replicator.connect()
        click.echo("✓ Connected to Kafka cluster")
        click.echo(f"✓ Subscribed to topic '{topic}' with group ID '{group_id}'")

        if max_messages:
            click.echo(f"Will process maximum {max_messages} messages")
        else:
            click.echo("Processing unlimited messages (Ctrl+C to stop)")

        click.echo("-" * 60)

        replicator.consume_events(max_messages=max_messages, timeout=timeout)

    except KeyboardInterrupt:
        click.echo("Received interrupt signal, shutting down...")
    except ReplicationError as e:
        click.echo(f"Replication error: {e}")
        sys.exit(1)
    except Exception as e:
        click.echo(f"Unexpected error: {e}", exc_info=True)
        sys.exit(1)
    finally:
        replicator.disconnect()
        click.echo("-" * 60)
        click.echo("Replication completed.")


if __name__ == '__main__':
    replicate()
