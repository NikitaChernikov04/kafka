import redis
from kafka import KafkaConsumer, KafkaProducer
import json

r = redis.StrictRedis(host='localhost', port=6379, db=0)
consumer = KafkaConsumer('conversion_topic', bootstrap_servers='localhost:9092', value_deserializer=lambda x: json.loads(x.decode('utf-8')))
producer = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=lambda v: json.dumps(v).encode('utf-8'))

for message in consumer:
    conversion_request = message.value
    from_currency = conversion_request['from']
    to_currency = conversion_request['to']
    amount = conversion_request['amount']
    rate = float(r.get(f"{from_currency}_{to_currency}") or 1)
    converted_amount = amount * rate
    print(f"Converted amount: {converted_amount}")
