from kafka import KafkaConsumer
import json
import os
import logging
import time


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')


KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'kafka:9092')
CONSUMER_TOPIC = os.getenv('CONSUMER_TOPIC', 'process_transaction')


def create_kafka_consumer():
    return KafkaConsumer(
        CONSUMER_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )


def main():
    logging.info("Starting Transaction Service...")

    try:
        consumer = create_kafka_consumer()
        logging.info(f"Connected to Kafka broker at {KAFKA_BROKER}, consuming topic '{CONSUMER_TOPIC}'")
    except Exception as e:
        logging.error(f"Failed to connect to Kafka: {e}")
        time.sleep(5)
        return

    for message in consumer:
        try:
            transaction = message.value
            logging.info(f"Processed transaction: {transaction}")
        except Exception as e:
            logging.error(f"Error processing transaction: {e}")


if __name__ == '__main__':
    main()
