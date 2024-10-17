from confluent_kafka import Consumer, KafkaError, KafkaException, Producer
import sys
from geopy.distance import geodesic
import json

# Kafka consumer and producer configurations
conf_consumer = {
    'bootstrap.servers': 'kafka-1:19092',
    'group.id': 'challenge6',
    'auto.offset.reset': 'earliest'
}

conf_producer = {
    'bootstrap.servers': 'kafka-1:19092'  # Kafka broker address
}

# Initialize Kafka consumer and producer
consumer = Consumer(conf_consumer)
producer = Producer(conf_producer)

topics = ["Challenge6RunnerData"]
output_topic = "Challenge6RunnerDistanceComparisonAdd"  # The new topic to write the distance

running = True

# Hardcoded runners to compare
rabbit = "301725"
runner = "301612"

# Define the target cumulative distance
TARGET_DISTANCE = 18000  # in meters

# Dictionary to store per-runner data
runner_data = {
    rabbit: {
        'last_processed_timestamp_bin': None,
        'cumulative_distance': 0.0,
        'last_point': None
    },
    runner: {
        'last_processed_timestamp_bin': None,
        'cumulative_distance': 0.0,
        'last_point': None
    }
}

def shutdown():
    global running
    running = False

# Dictionary to hold data points for each timestamp bin
timestamp_data = {}

def delivery_report(err, msg):
    """Callback function for producer to check if message is delivered successfully."""
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

def basic_consume_loop(consumer, topics):
    global running  # Declare running as global to modify it inside the function

    try:
        consumer.subscribe(topics)
    
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
    
            # Process the data
            runner_id = key  # Assuming the key is the runner ID
            data_point = value  # Assuming value is a dict with 'latitude', 'longitude', 'timestamp'
    
            # Validate data_point contains required fields
            if not all(k in data_point for k in ('latitude', 'longitude', 'timestamp')):
                continue  # Skip if data is incomplete
    
            # Get the runner's data
            if runner_id not in [rabbit, runner]:
                continue  # Ignore other runners if necessary
    
            # Get current runner data
            runner_info = runner_data[runner_id]
            timestamp = float(data_point['timestamp'])
            timestamp_bin = int(timestamp)  # Convert to int seconds

            # Check if this timestamp_bin has already been processed for this runner
            if runner_info['last_processed_timestamp_bin'] == timestamp_bin:
                continue

            # Update cumulative distance
            if runner_info['last_point'] is not None:
                last_point = runner_info['last_point']
                # Calculate distance from last_point to current data_point
                coord1 = (last_point['latitude'], last_point['longitude'])
                coord2 = (data_point['latitude'], data_point['longitude'])
                distance = geodesic(coord1, coord2).meters
                runner_info['cumulative_distance'] += distance
            else:
                # No previous point, cumulative_distance remains as is
                pass

            # Update runner_info
            runner_info['last_processed_timestamp_bin'] = timestamp_bin
            runner_info['last_point'] = data_point  # Update last_point to current data_point

            # Store data for this timestamp_bin
            if timestamp_bin not in timestamp_data:
                timestamp_data[timestamp_bin] = {}
            timestamp_data[timestamp_bin][runner_id] = {
                'data_point': data_point,
                'cumulative_distance': runner_info['cumulative_distance']
            }

            # Check if both runners have data for this timestamp_bin
            if rabbit in timestamp_data[timestamp_bin] and runner in timestamp_data[timestamp_bin]:
                data_rabbit = timestamp_data[timestamp_bin][rabbit]
                data_runner = timestamp_data[timestamp_bin][runner]

                # Determine the leader to change the sign of distance_between_runners later 
                if data_rabbit['cumulative_distance'] >= data_runner['cumulative_distance']:
                    leader = 1
                else:
                    leader = -1

                # Calculate distance between runners
                coord_rabbit = (data_rabbit['data_point']['latitude'], data_rabbit['data_point']['longitude'])
                coord_runner = (data_runner['data_point']['latitude'], data_runner['data_point']['longitude'])
                distance_between_runners = geodesic(coord_rabbit, coord_runner).meters
                
                # change the sign of distance_between_runners accordingly to who is leading
                distance_between_runners_sign = distance_between_runners*leader

                # Prepare output message
                output_message = {
                    'timestamp': timestamp_bin,
                    'rabbit': rabbit,
                    'distance_between_runners': distance_between_runners_sign,
                    'runner_data': {
                        'cumulative_distance': data_runner['cumulative_distance'],
                        'speed': data_runner['data_point']['speed']*3.6,
                        'latitude': data_runner['data_point']['latitude'],
                        'longitude': data_runner['data_point']['longitude'],
                        'altitude': data_runner['data_point']['altitude']
                    },
                    'rabbit_data': {
                        'cumulative_distance': data_rabbit['cumulative_distance'],
                        'speed': data_rabbit['data_point']['speed']*3.6,
                        'latitude': data_rabbit['data_point']['latitude'],
                        'longitude': data_rabbit['data_point']['longitude'],
                        'altitude': data_rabbit['data_point']['altitude']
                    }
                }

                # Send to output topic
                producer.produce(output_topic, key=runner, value=json.dumps(output_message).encode('utf-8'), callback=delivery_report)
                producer.flush()

                # Clean up old data from timestamp_data
                del timestamp_data[timestamp_bin]

                # *** Added Stop Condition ***
                # Check if both runners have reached or exceeded the target distance
                if (runner_data[rabbit]['cumulative_distance'] >= TARGET_DISTANCE and
                    runner_data[runner]['cumulative_distance'] >= TARGET_DISTANCE):
                    print(f"Both runners have reached the cumulative distance of {TARGET_DISTANCE} meters.")
                    shutdown()

    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()

# Start the consumer loop
basic_consume_loop(consumer, topics)
