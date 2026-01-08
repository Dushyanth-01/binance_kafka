from kafka import KafkaProducer
import json
import pandas as pd

def to_serializable(record):
    for k, v in record.items():
        if isinstance(v, pd.Timestamp):
            record[k] = v.isoformat()
    return record

def get_producer():
    return KafkaProducer(
        bootstrap_servers="kafka:9092",  # internal docker network
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

def send_to_kafka(topic, data):
    if isinstance(data, pd.DataFrame):
        data = data.to_dict(orient="records")
    if isinstance(data, dict):
        data = [data]

    producer = get_producer()
    for record in data:
        record = to_serializable(record)
        producer.send(topic, record)

    producer.flush()
    print(f" Successfully sent {len(data)} messages to Kafka topic: {topic}")