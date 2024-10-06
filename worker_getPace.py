from confluent_kafka import Consumer, Producer
import json

conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'python-consumer',
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(conf)

topic = 'RunEvent'
consumer.subscribe([topic])

# Kafka producer configuration
producer_conf = {'bootstrap.servers': 'localhost:9092'}
producer = Producer(producer_conf)


# transform m/s to min/km
def speed_to_Pace(speed_mps):
    return 60 / (speed_mps * 3.6)

def consume_Message(consumer):
    try:
        while True:
            message=consumer.poll(1.0)
            if message is None:
                continue
            if message.error():
                print(f'Error: {message.error()}')
                continue
            else:
                data = message.value().decode('utf-8')
                pdf_Run = json.loads(data)
                
                # transform speed in m/s to min/km
                pdf_Run['Pace[min/km]'] = speed_to_Pace(float(pdf_Run['speed']))
                
                # write result to Pace Topic
                output_topic = 'EventPace'
                producer.produce(output_topic, json.dumps(pdf_Run).encode('utf-8'))
                producer.flush()


    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
        producer.close()


if __name__ == '__main__':
    consume_Message(consumer)