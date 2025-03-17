import time
import json
import random
from confluent_kafka import Producer

KAFKA_BOOTSTRAP_SERVERS = "broker:29092"
KAFKA_TOPIC = "sensor_data_topic"

producer_conf = {
    'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS
}
producer = Producer(producer_conf)

def send_to_kafka(data):
    """Gửi dữ liệu lên Kafka."""
    try:
        message_json = json.dumps(data)
        producer.produce(KAFKA_TOPIC, value=message_json)
        producer.flush()
        print(f"✅ Sent to Kafka: {data}")
    except Exception as e:
        print(f"⚠️ Error sending to Kafka: {e}")

def read_sensor_data():
    """Giả lập dữ liệu cảm biến."""
    temperature = random.uniform(20, 30)
    humidity = random.uniform(40, 60)
    return {
        "sensor_id": "sensor_01",
        "temperature": temperature,
        "humidity": humidity,
        "timestamp": int(time.time())
    }

def main():
    while True:
        data = read_sensor_data()
        print(f"Read sensor data: {data}")
        send_to_kafka(data)
        time.sleep(5)

if __name__ == "__main__":
    main()
