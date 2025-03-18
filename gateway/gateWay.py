import os
import time
import json
import random
import argparse
from confluent_kafka import Producer
from datetime import datetime
# Lấy cấu hình Kafka từ biến môi trường hoặc dùng giá trị mặc định
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:39092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "sensor_data_topics")

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
        print(f"✅ Tin được gửi thành công tới {msg.topic()} [partition {msg.partition()}] offset {msg.offset()}")

def send_to_kafka(data):
    """Gửi dữ liệu dạng JSON lên Kafka một cách không đồng bộ, giảm độ trễ."""
    try:
        message_json = json.dumps(data)
        producer.produce(KAFKA_TOPIC, value=message_json, callback=delivery_callback)
        # Xử lý các event không đồng bộ
        producer.poll(0)
    except Exception as e:
        print(f"⚠️ Lỗi khi gửi lên Kafka: {e}")

def read_sensor_data(sensor_id, temperature_range=(20, 30), humidity_range=(40, 60)):
    """Giả lập dữ liệu cảm biến cho sensor xác định với nhiệt độ và độ ẩm ngẫu nhiên."""
    temperature = random.uniform(*temperature_range)
    humidity = random.uniform(*humidity_range)
    return {
        "sensor_id": sensor_id,
        "temperature": temperature,
        "humidity": humidity,
        "timestamp": datetime.now().isoformat()
    }

def main(sensors, interval):
    """Chạy vòng lặp gửi dữ liệu cho nhiều cảm biến với khoảng thời gian tùy chỉnh."""
    try:
        while True:
            for sensor in sensors:
                data = read_sensor_data(
                    sensor_id=sensor["sensor_id"],
                    temperature_range=sensor.get("temperature_range", (20, 30)),
                    humidity_range=sensor.get("humidity_range", (40, 60))
                )
                print(f"Đọc dữ liệu cảm biến: {data}")
                send_to_kafka(data)
            time.sleep(interval)
    except KeyboardInterrupt:
        print("Đang tắt chương trình, flush tin nhắn tồn...")
    finally:
        producer.flush()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Kafka Sensor Data Producer")
    parser.add_argument("--interval", type=int, default=5,
                        help="Khoảng thời gian (giây) giữa các lần gửi dữ liệu")
    args = parser.parse_args()

    # Định nghĩa danh sách các cảm biến với thông số tùy chỉnh
    sensors = [
        {"sensor_id": "sensor_01", "temperature_range": (18, 22), "humidity_range": (35, 50)},
        {"sensor_id": "sensor_02", "temperature_range": (22, 27), "humidity_range": (45, 65)},
        {"sensor_id": "sensor_03", "temperature_range": (30, 35), "humidity_range": (50, 70)},
        {"sensor_id": "sensor_04", "temperature_range": (15, 20), "humidity_range": (30, 50)},
        {"sensor_id": "sensor_05", "temperature_range": (20, 25), "humidity_range": (40, 60)},
        {"sensor_id": "sensor_06", "temperature_range": (25, 30), "humidity_range": (50, 75)},
        {"sensor_id": "sensor_07", "temperature_range": (28, 33), "humidity_range": (55, 80)},
        {"sensor_id": "sensor_08", "temperature_range": (32, 38), "humidity_range": (60, 85)},
        {"sensor_id": "sensor_09", "temperature_range": (35, 40), "humidity_range": (65, 90)},
        {"sensor_id": "sensor_10", "temperature_range": (40, 45), "humidity_range": (70, 95)}
    ]
    main(sensors, args.interval)
