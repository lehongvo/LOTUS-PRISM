from azure.eventhub import EventHubProducerClient, EventData
from azure.eventhub.aio import EventHubConsumerClient
import asyncio
import json
import logging
from typing import List, Dict, Any, Callable

logger = logging.getLogger(__name__)

class EventHub:
    def __init__(self, connection_str: str, eventhub_name: str):
        """
        Initialize Azure Event Hub connection
        
        Args:
            connection_str: Event Hub connection string
            eventhub_name: Event Hub name
        """
        self.connection_str = connection_str
        self.eventhub_name = eventhub_name
        self.producer = None
        self.consumer = None

    def _create_producer(self):
        """Create Event Hub producer client"""
        try:
            self.producer = EventHubProducerClient.from_connection_string(
                conn_str=self.connection_str,
                eventhub_name=self.eventhub_name
            )
            logger.info("Successfully created Event Hub producer")
        except Exception as e:
            logger.error(f"Failed to create Event Hub producer: {str(e)}")
            raise

    def _create_consumer(self):
        """Create Event Hub consumer client"""
        try:
            self.consumer = EventHubConsumerClient.from_connection_string(
                conn_str=self.connection_str,
                consumer_group="$Default",
                eventhub_name=self.eventhub_name
            )
            logger.info("Successfully created Event Hub consumer")
        except Exception as e:
            logger.error(f"Failed to create Event Hub consumer: {str(e)}")
            raise

    def send_event(self, event_data: Dict[str, Any]) -> bool:
        """
        Send event to Event Hub
        
        Args:
            event_data: Event data to send
            
        Returns:
            bool: True if send successful, False otherwise
        """
        try:
            if not self.producer:
                self._create_producer()

            event = EventData(json.dumps(event_data))
            with self.producer:
                self.producer.send_event(event)
            logger.info(f"Successfully sent event: {event_data}")
            return True
        except Exception as e:
            logger.error(f"Failed to send event: {str(e)}")
            return False

    def send_batch(self, events: List[Dict[str, Any]]) -> bool:
        """
        Send batch of events to Event Hub
        
        Args:
            events: List of events to send
            
        Returns:
            bool: True if send successful, False otherwise
        """
        try:
            if not self.producer:
                self._create_producer()

            event_batch = [EventData(json.dumps(event)) for event in events]
            with self.producer:
                self.producer.send_batch(event_batch)
            logger.info(f"Successfully sent {len(events)} events")
            return True
        except Exception as e:
            logger.error(f"Failed to send batch: {str(e)}")
            return False

    async def receive_events(self, callback: Callable[[Dict[str, Any]], None]):
        """
        Receive events from Event Hub
        
        Args:
            callback: Function to process received events
        """
        try:
            if not self.consumer:
                self._create_consumer()

            async def on_event(partition_context, event):
                try:
                    event_data = json.loads(event.body_as_str())
                    callback(event_data)
                    await partition_context.update_checkpoint(event)
                except Exception as e:
                    logger.error(f"Error processing event: {str(e)}")

            async with self.consumer:
                await self.consumer.receive(
                    on_event=on_event,
                    track_last_enqueued_event_properties=True
                )
        except Exception as e:
            logger.error(f"Failed to receive events: {str(e)}")
            raise

    def close(self):
        """Close Event Hub connections"""
        try:
            if self.producer:
                self.producer.close()
            if self.consumer:
                self.consumer.close()
            logger.info("Successfully closed Event Hub connections")
        except Exception as e:
            logger.error(f"Failed to close Event Hub connections: {str(e)}")
            raise 