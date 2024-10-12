import pandas as pd
from confluent_kafka import Producer
from concurrent.futures import ThreadPoolExecutor
import time
import os

topic = "Challenge6test"  # Kafka topic to send messages to

# Kafka configuration
# 47.254.134.189
kafka_config = {
    'bootstrap.servers': 'kafka-1:19092',  # Update with your Kafka broker address
}

# Initialize the Kafka producer
producer = Producer(kafka_config)

def delivery_report(err, msg):
    """ Callback called once Kafka acknowledges the message """
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        #print(f"Message delivered to {msg.topic()} [{msg.partition()}]")
        pass

producer.produce(topic, key='test1', value='message', callback=delivery_report)  # Send message to Kafka
producer.poll(0)  # Trigger the delivery report callback
producer.flush()  # Ensure all messages are sent before exiting