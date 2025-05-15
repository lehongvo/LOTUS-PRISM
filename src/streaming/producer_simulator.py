"""
Price event producer simulator module for LOTUS-PRISM.
The send_price_event() function simulates sending price events to Event Hub.
"""

import json
import random
import time
from azure.eventhub import EventHubProducerClient, EventData


def send_price_event(connection_str, eventhub_name):
    """
    Simulate a producer sending price events to Event Hub.
    - Generate random price events
    - Send events to Event Hub
    """
    producer = EventHubProducerClient.from_connection_string(connection_str, eventhub_name=eventhub_name)
    
    while True:
        event = {
            'product_id': random.randint(1, 100),
            'price': random.uniform(10, 1000)
        }
        event_data = EventData(json.dumps(event))
        producer.send(event_data)
        print(f"Event sent: {event}")
        time.sleep(1)
