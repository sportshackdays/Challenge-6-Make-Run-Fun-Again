from confluent_kafka import Consumer, KafkaError, KafkaException
import sys

import pandas as pd
from concurrent.futures import ThreadPoolExecutor
import time
import os

conf = {'bootstrap.servers': 'kafka-1:19092',
        'group.id': 'challenge6',
        'auto.offset.reset': 'earliest'}

consumer = Consumer(conf)
topics = ["Challenge6test"]

running = True

def basic_consume_loop(consumer, topics):
    try:
        consumer.subscribe(topics)

        while running:
            msg = consumer.poll(timeout=1.0)
            if msg is None: 
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                                     (msg.topic(), msg.partition(), msg.offset()))
                elif msg.error():
                    raise KafkaException(msg.error())
            else:
                pass

            # print(msg.value().decode('utf-8'))
            print(f'key: {msg.key().decode("utf-8")}, value: {msg.value().decode("utf-8")}')
    finally:
        # Close down consumer to commit final offsets.
        consumer.close()

def shutdown():
    running = False

basic_consume_loop(consumer, topics)