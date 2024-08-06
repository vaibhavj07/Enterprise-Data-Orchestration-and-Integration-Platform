import json
import time
import random 
from kafka import KafkaProducer
from config import kafka_config, kafka_secrets
from data_generator import generate_event

class KafkaEventProducer:
    def __init__(self, config, secrets):
        self.producer = KafkaProducer(
            bootstrap_servers=config['bootstrap_servers'],
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            security_protocol=secrets.get('security_protocol', 'PLAINTEXT'),
            ssl_cafile=secrets.get('ssl_cafile'),
            ssl_certfile=secrets.get('ssl_certfile'),
            ssl_keyfile=secrets.get('ssl_keyfile')
        )

    def send_event(self, topic, event):
        self.producer.send(topic, event)
        self.producer.flush()
        print(f"Sent event: {event}")

if __name__ == "__main__":
    producer = KafkaEventProducer(kafka_config, kafka_secrets)
    while True:
        event = generate_event()
        producer.send_event('website_events', event)
        time.sleep(random.uniform(0.1, 1))  # Adjust the rate of event generation as needed
