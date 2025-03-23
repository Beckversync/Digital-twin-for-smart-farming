import os
import time
import json
import random
import argparse
from confluent_kafka import Producer, KafkaException
from datetime import datetime

# Lấy cấu hình Kafka từ biến môi trường hoặc dùng giá trị mặc định
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:39092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "sensor_data")  # Đồng nhất với consumer

# Cấu hình producer cho Kafka
producer_conf = {
    'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
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
    temperature = random.uniform(*temperature_range)
    humidity = random.uniform(*humidity_range)
    return {
        "sensor_id": sensor_id,
        "temperature": temperature,
        "humidity": humidity,
        "timestamp": datetime.utcnow().isoformat()
    }

def main(sensors, interval):
    """Gửi dữ liệu liên tục."""
    try:
        while True:
            for sensor in sensors:
                data = read_sensor_data(
                    sensor_id=sensor["sensor_id"],
                    temperature_range=sensor.get("temperature_range", (20, 30)),
                    humidity_range=sensor.get("humidity_range", (40, 60))
                )
                print(f"📡 Đọc dữ liệu cảm biến: {data}")
                send_to_kafka(data)
            time.sleep(interval)
    except KeyboardInterrupt:
        print("🚀 Đang dừng gateway, flush tin nhắn còn lại...")
    finally:
        producer.flush()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Kafka Sensor Data Producer")
    parser.add_argument("--interval", type=int, default=5, help="Thời gian giữa các lần gửi (giây)")
    args = parser.parse_args()

    sensors = [
        {"sensor_id": f"sensor_{i+1}", "temperature_range": (20 + i, 25 + i), "humidity_range": (40 + i, 60 + i)}
        for i in range(10)
    ]
    main(sensors, args.interval)
