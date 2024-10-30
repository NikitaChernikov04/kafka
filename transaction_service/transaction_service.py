from kafka import KafkaConsumer
import json

consumer = KafkaConsumer('process_transaction', bootstrap_servers='localhost:9092', value_deserializer=lambda x: json.loads(x.decode('utf-8')))

for message in consumer:
    transaction = message.value
    print(f"Processed transaction: {transaction}")
