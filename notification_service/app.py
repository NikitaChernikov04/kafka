from kafka import KafkaConsumer
import json

KAFKA_BROKER = 'kafka:9092'
TOPIC_RESPONSES = 'currency_responses'

consumer = KafkaConsumer(
    TOPIC_RESPONSES,
    bootstrap_servers=KAFKA_BROKER,
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)


def send_notification(data):
    print(f"Notification: Conversion result - {data}")


print("Notification Service Started...")
for message in consumer:
    send_notification(message.value)
