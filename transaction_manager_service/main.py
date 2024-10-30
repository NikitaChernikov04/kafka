from kafka import KafkaConsumer, KafkaProducer
import json

consumer = KafkaConsumer('transaction_topic', bootstrap_servers='localhost:9092', value_deserializer=lambda x: json.loads(x.decode('utf-8')))
producer = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=lambda v: json.dumps(v).encode('utf-8'))

for message in consumer:
    transaction_data = message.value
    producer.send('process_transaction', transaction_data)
    print(f"Transaction {transaction_data} is being processed")
