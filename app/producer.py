from kafka import KafkaProducer
import json
import time

# Parameters
topic = "test"
bootstrap_server = "localhost:9092"  # Updated to Docker Kafka container hostname

producer = KafkaProducer(
    bootstrap_servers=bootstrap_server,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

for i in range(100000):
    producer.send(topic=topic, value=f"Message to Kafka No: {i}")
    producer.flush()
    print(f"Message to Kafka No: {i}")
    time.sleep(1)

producer.close()
