import pandas as pd
from confluent_kafka import Producer
from concurrent.futures import ThreadPoolExecutor
import time
import os

folder_path = os.getcwd() + "/runs"

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
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

def read_and_send_messages(csv_file, topic):
    """ Read CSV and send messages to Kafka """
    file_path = os.path.join(folder_path, csv_file)
    df = pd.read_csv(file_path)  # Read CSV file
    for _, row in df.iterrows():
        message = row.to_json()  # Convert row to JSON string
        print(f"Sending message: {message}, Key: {csv_file[5:11]}")
        producer.produce(topic, key=csv_file[5:11], value=message, callback=delivery_report)  # Send message to Kafka
        producer.poll(0)  # Trigger the delivery report callback
        time.sleep(0.1)  # Optional delay to control message flow

    producer.flush()  # Ensure all messages are sent before exiting

def send_messages_with_time_difference(csv_files, topic):
    """ Send messages from multiple CSVs in parallel with a start delay between each CSV file """
    with ThreadPoolExecutor() as executor:
        futures = []
        futures.append(executor.submit(read_and_send_messages(csv_files[0], topic)))
        time.sleep(4)
        futures.append(executor.submit(read_and_send_messages(csv_files[1], topic)))
        time.sleep(6)
        futures.append(executor.submit(read_and_send_messages(csv_files[2], topic)))
        time.sleep(5)
        futures.append(executor.submit(read_and_send_messages(csv_files[3], topic)))

        # Wait for all threads to complete
        for future in futures:
            future.result()

if __name__ == "__main__":
    # List of CSV files
    csv_files = ["dump_301977_2024_10_06T07_45_00_000000Zcleaned.csv", 
                 "dump_301612_2024_10_06T07_45_00_000000Zcleaned.csv", 
                 "dump_301725_2024_10_06T07_45_00_000000Zcleaned.csv",
                 "dump_300857_2024_10_06T07_45_00_000000Zcleaned.csv"]  # Update with actual file paths
    kafka_topic = "Challenge6RunnerData"  # Kafka topic to send the messages to

    # Send messages with a time difference between starting each CSV file
    send_messages_with_time_difference(csv_files, kafka_topic)