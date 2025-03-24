import os
import time
import json
import random
import argparse
from threading import Thread
from confluent_kafka import Producer, KafkaException
from datetime import datetime

# Cấu hình Kafka cho Farm 2
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:39092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC_FARM2", "sensor_data_farm2")  # Topic riêng cho Farm 2

# Cấu hình producer cho Kafka
producer_conf = {
    'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
    'compression.type': 'snappy',  # Nén dữ liệu để giảm tải
}
producer = Producer(producer_conf)

def delivery_callback(err, msg):
    """Callback để xử lý kết quả gửi tin không đồng bộ."""
    if err:
        print(f"⚠️ Lỗi khi gửi tin: {err}")
    else:
        print(f"✅ Tin gửi thành công tới {msg.topic()} [partition {msg.partition()}] offset {msg.offset()}")

def send_to_kafka(data):
    """Gửi dữ liệu JSON lên Kafka với xử lý lỗi."""
    try:
        message_json = json.dumps(data)
        producer.produce(KAFKA_TOPIC, value=message_json, callback=delivery_callback)
        producer.poll(0)
    except KafkaException as e:
        print(f"⚠️ Kafka Error: {e}")
    except Exception as e:
        print(f"⚠️ Lỗi khi gửi lên Kafka: {e}")


def read_sensor_data(sensor_id, temperature_range=(20, 30), humidity_range=(40, 60)):
    """Giả lập dữ liệu cảm biến."""
    return {
        "sensor_id": sensor_id,
        "temperature": random.uniform(*temperature_range),
        "humidity": random.uniform(*humidity_range),
        "timestamp": datetime.utcnow().isoformat()
    }

def sensor_thread(sensor_id, temperature_range, humidity_range, interval):
    """Luồng riêng cho mỗi cảm biến để gửi dữ liệu liên tục."""
    while True:
        data = read_sensor_data(sensor_id, temperature_range, humidity_range)
        print(f"📡 Đọc dữ liệu cảm biến: {data}")
        send_to_kafka(data)
        time.sleep(interval)

def main(sensors, interval):
    """Tạo luồng riêng cho từng cảm biến để gửi dữ liệu."""
    threads = [Thread(target=sensor_thread, args=(sensor["sensor_id"], sensor["temperature_range"], sensor["humidity_range"], interval)) for sensor in sensors]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Kafka Sensor Data Producer")
    parser.add_argument("--interval", type=int, default=5, help="Thời gian giữa các lần gửi (giây)")
    args = parser.parse_args()

    sensors = [
        {"sensor_id": f"sensor_farm2_{i+1}", "temperature_range": (20 + i, 25 + i), "humidity_range": (40 + i, 60 + i)}
        for i in range(10)
    ]
    main(sensors, args.interval)

