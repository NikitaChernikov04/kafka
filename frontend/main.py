from flask import Flask, jsonify, request
from kafka import KafkaProducer
import json

app = Flask(__name__)
producer = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=lambda v: json.dumps(v).encode('utf-8'))

@app.route('/transaction', methods=['POST'])
def create_transaction():
    data = request.json
    producer.send('transaction_topic', data)
    return jsonify({"status": "Transaction initiated"}), 200

@app.route('/convert', methods=['POST'])
def convert_currency():
    data = request.json
    producer.send('conversion_topic', data)
    return jsonify({"status": "Conversion request sent"}), 200


if __name__ == '__main__':
    app.run(port=5000)
