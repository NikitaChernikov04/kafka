from flask import Flask, request, jsonify
import redis
from kafka import KafkaProducer, KafkaConsumer
import json

app = Flask(__name__)


KAFKA_BROKER = 'kafka:9092'
TOPIC_REQUESTS = 'currency_requests'
TOPIC_RESPONSES = 'currency_responses'

producer = KafkaProducer(bootstrap_servers=KAFKA_BROKER, value_serializer=lambda v: json.dumps(v).encode('utf-8'))
consumer = KafkaConsumer(TOPIC_REQUESTS, bootstrap_servers=KAFKA_BROKER, value_deserializer=lambda m: json.loads(m.decode('utf-8')))


REDIS_HOST = 'redis'
REDIS_PORT = 6379
redis_client = redis.StrictRedis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)


@app.route('/update_rate', methods=['POST'])
def update_rate():
    data = request.json
    from_currency = data['from']
    to_currency = data['to']
    rate = data['rate']
    redis_client.set(f"{from_currency}_{to_currency}", rate)
    return jsonify({"message": "Rate updated successfully"}), 200


@app.route('/get_rate', methods=['GET'])
def get_rate():
    from_currency = request.args.get('from')
    to_currency = request.args.get('to')
    rate = redis_client.get(f"{from_currency}_{to_currency}")
    if rate:
        return jsonify({"from": from_currency, "to": to_currency, "rate": rate}), 200
    return jsonify({"message": "Rate not found"}), 404


@app.route('/convert', methods=['POST'])
def convert_currency():
    data = request.json
    producer.send(TOPIC_REQUESTS, data)
    return jsonify({"message": "Request sent to Kafka"}), 200


def process_conversion_requests():
    for message in consumer:
        request_data = message.value
        from_currency = request_data['from']
        to_currency = request_data['to']
        amount = request_data['amount']

        rate = redis_client.get(f"{from_currency}_{to_currency}")
        if rate:
            converted_amount = float(rate) * amount
            response_data = {
                "from": from_currency,
                "to": to_currency,
                "amount": amount,
                "converted_amount": converted_amount
            }
            producer.send(TOPIC_RESPONSES, response_data)
        else:
            print(f"Rate for {from_currency} to {to_currency} not found.")


if __name__ == '__main__':
    from threading import Thread
    Thread(target=process_conversion_requests, daemon=True).start()
    app.run(host='0.0.0.0', port=5000)