from confluent_kafka import Consumer, KafkaError, KafkaException
import sys
from geopy.distance import geodesic
import json

conf = {'bootstrap.servers': 'kafka-1:19092',
        'group.id': 'challenge6',
        'auto.offset.reset': 'earliest'}

consumer = Consumer(conf)
topics = ["Challenge6RunnerData"]

def shutdown():
    global running  # Declare running as global
    running = False

running = True
runners_at_3km = 0  # Global variable to track number of runners who reached 3km

def basic_consume_loop(consumer, topics):
    global runners_at_3km  # Access global variable

    try:
        consumer.subscribe(topics)

        dynamic_dict = {}

        while running:
            msg = consumer.poll(timeout=1.0)
            if msg is None: 
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                                     (msg.topic(), msg.partition(), msg.offset()))
                else:
                    raise KafkaException(msg.error())
            else:
                pass

            key = msg.key().decode("utf-8")
            value = json.loads(msg.value().decode("utf-8"))

            # Check if the runner already reached 3km
            if key in dynamic_dict and dynamic_dict[key].get("reached_3km", False):
                # Skip updating if the runner has already reached 3km
                continue

            # If it's a new runner, initialize the runner data
            if key not in dynamic_dict:
                dynamic_dict[key] = {
                    'start_time': value.get("timestamp"),
                    'lat': value.get("latitude"),
                    'lon': value.get("longitude"),
                    'distance': 0,
                    'reached_3km': False
                }
            else:
                # Calculate the distance traveled since the last update
                dynamic_dict[key]["distance"] += geodesic(
                    (dynamic_dict[key]["lat"], dynamic_dict[key]["lon"]),
                    (value.get("latitude"), value.get("longitude"))
                ).meters

                # Update the runner's latest coordinates
                dynamic_dict[key]["lat"] = value.get("latitude")
                dynamic_dict[key]["lon"] = value.get("longitude")

                # Check if the runner has reached 3km
                if dynamic_dict[key]["distance"] > 3000:
                    dynamic_dict[key]["end_time"] = value.get("timestamp") - dynamic_dict[key]["start_time"]
                    dynamic_dict[key]["reached_3km"] = True  # Mark runner as reached 3km
                    runners_at_3km += 1
                    print(f"Runner {key} has reached 3km!")

                    # Check if 4 runners have reached 3km - hardcoded for simplicity (data for 4 runners was provided)
                    if runners_at_3km == 4:
                        print("First 4 runners have reached 3km!")
                        print(dynamic_dict)
                        shutdown()  # Stop the consumer loop

    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()



basic_consume_loop(consumer, topics)
