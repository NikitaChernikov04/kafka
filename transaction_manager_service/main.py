from kafka import KafkaConsumer, KafkaProducer
import json
import os
import logging
import time


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')


KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'kafka:9092')
CONSUMER_TOPIC = os.getenv('CONSUMER_TOPIC', 'transaction_topic')
PRODUCER_TOPIC = os.getenv('PRODUCER_TOPIC', 'process_transaction')


def create_kafka_consumer():
    return KafkaConsumer(
        CONSUMER_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )


def create_kafka_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

def main():
    logging.info("Starting Transaction Manager Service...")

    try:
        consumer = create_kafka_consumer()
        producer = create_kafka_producer()
    except Exception as e:
        logging.error(f"Failed to connect to Kafka: {e}")
        time.sleep(5)
        return

    for message in consumer:
        try:
            transaction_data = message.value
            producer.send(PRODUCER_TOPIC, transaction_data)
            logging.info(f"Transaction {transaction_data} is being processed")
        except Exception as e:
            logging.error(f"Failed to process transaction: {e}")


if __name__ == '__main__':
    main()
