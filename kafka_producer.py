import os
import json
import time
import logging
import requests
from kafka import KafkaProducer

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Kafka configuration from environment variables
BOOTSTRAP_SERVERS = os.getenv('BOOTSTRAP_SERVERS', 'kafka:9092')
TOPIC_NAME = os.getenv('KAFKA_TOPIC', 'sample_topic')
MESSAGE_COUNT = int(os.getenv('MESSAGE_COUNT', '20'))


# Kafka Producer class
class KafkaProducerSimple:
    def __init__(self):
        self.producer = None

    def fetch_random_user(self):
        """Fetch random user data from the Random User API"""
        try:
            response = requests.get('https://randomuser.me/api/')
            response.raise_for_status()
            user_data = response.json()['results'][0]
            current_time = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime())
            return {
                'name': f"{user_data['name']['first']} {user_data['name']['last']}",
                'email': user_data['email'],
                'location': f"{user_data['location']['city']}, {user_data['location']['country']}",
                'age': user_data['dob']['age'],
                'registered': {
                    'date': user_data['registered']['date'],
                    'age': user_data['registered']['age']
                },
                'phone': user_data['phone'],
                'cell': user_data['cell'],
                'time': current_time
            }
        except requests.RequestException as e:
            logger.error(f"Error fetching random user: {e}")
            return None

    def kafka_producer(self):
        """Produce messages to Kafka"""
        self.producer = KafkaProducer(
            bootstrap_servers=BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )

        logger.info(f"Starting Kafka Producer to produce {MESSAGE_COUNT} messages.")
        for i in range(MESSAGE_COUNT):
            user_data = self.fetch_random_user()
            if not user_data:
                logger.warning(f"Skipping message {i} due to failed user data fetch.")
                continue

            message = {
                'id': i,
                'user': user_data
            }
            self.producer.send(TOPIC_NAME, value=message)
            logger.info(f"Produced message: {message}")

            time.sleep(1)  # Simulate a delay between producing messages

        # Close the Kafka producer connection
        self.producer.close()
        logger.info("Kafka Producer closed.")


# Usage example
if __name__ == "__main__":
    producer = KafkaProducerSimple()
    producer.kafka_producer()
