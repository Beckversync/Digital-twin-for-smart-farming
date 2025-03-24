import os
import time
import json
import random
import argparse
from confluent_kafka import Producer, KafkaException
from datetime import datetime

# Cấu hình Kafka cho Farm 2
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:39092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC_FARM2", "sensor_data_farm2")  # Topic riêng cho Farm 2

producer_conf = {'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS}
producer = Producer(producer_conf)

def delivery_callback(err, msg):
    if err:
        print(f"⚠️ Lỗi khi gửi tin: {err}")
    else:
        print(f"✅ Tin gửi thành công tới {msg.topic()} [partition {msg.partition()}] offset {msg.offset()}")

def send_to_kafka(data):
    try:
        message_json = json.dumps(data)
        producer.produce(KAFKA_TOPIC, value=message_json, callback=delivery_callback)
        producer.poll(0)
    except KafkaException as e:
        print(f"⚠️ Kafka Error: {e}")
    except Exception as e:
        print(f"⚠️ Lỗi khi gửi lên Kafka: {e}")

def read_sensor_data(sensor_id, temperature_range=(15, 25), humidity_range=(50, 70)):
    temperature = random.uniform(*temperature_range)
    humidity = random.uniform(*humidity_range)
    return {
        "sensor_id": sensor_id,
        "temperature": temperature,
        "humidity": humidity,
        "timestamp": datetime.utcnow().isoformat()
    }

def main(sensors, interval):
    try:
        while True:
            for sensor in sensors:
                data = read_sensor_data(sensor_id=sensor["sensor_id"],
                                        temperature_range=sensor.get("temperature_range", (15, 25)),
                                        humidity_range=sensor.get("humidity_range", (50, 70)))
                print(f"📡 Đọc dữ liệu cảm biến từ Farm 2: {data}")
                send_to_kafka(data)
            time.sleep(interval)
    except KeyboardInterrupt:
        print("🚀 Đang dừng gateway Farm 2, flush tin nhắn còn lại...")
    finally:
        producer.flush()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Kafka Sensor Data Producer - Farm 2")
    parser.add_argument("--interval", type=int, default=5, help="Thời gian giữa các lần gửi (giây)")
    args = parser.parse_args()

    sensors = [
        {"sensor_id": f"sensor_farm2_{i+1}", "temperature_range": (15 + i, 20 + i), "humidity_range": (50 + i, 70 + i)}
        for i in range(10)
    ]
    main(sensors, args.interval)
