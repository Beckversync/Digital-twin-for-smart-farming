import os
import time
import json
import random
import argparse
import atexit
import logging
from threading import Thread, Event
from confluent_kafka import Producer, KafkaException
from datetime import datetime

# Cấu hình logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Cấu hình Kafka
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:39092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "sensor_data")

# Cấu hình producer
producer_conf = {
    'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
    'compression.type': 'snappy',
}
producer = Producer(producer_conf)

# Sự kiện để dừng các luồng một cách an toàn
stop_event = Event()

# Đóng Kafka producer khi chương trình dừng
def close_producer():
    logging.info("🔴 Đang đóng Kafka producer...")
    producer.flush()
    logging.info("✅ Kafka producer đã đóng.")

atexit.register(close_producer)  # Đảm bảo producer được đóng khi chương trình kết thúc

def delivery_callback(err, msg):
    """Callback xử lý kết quả gửi tin."""
    if err:
        logging.warning(f"Lỗi khi gửi tin: {err}")
    else:
        logging.info(f"✅ Tin gửi thành công tới {msg.topic()} [partition {msg.partition()}] offset {msg.offset()}")

def send_to_kafka(data, retries=3):
    """Gửi dữ liệu JSON lên Kafka với retry nếu lỗi."""
    message_json = json.dumps(data)
    for attempt in range(retries):
        try:
            producer.produce(KAFKA_TOPIC, value=message_json, callback=delivery_callback)
            producer.poll(0)
            return  # Gửi thành công thì thoát
        except KafkaException as e:
            logging.warning(f"Kafka Error (lần {attempt+1}): {e}")
            time.sleep(2)  # Chờ trước khi thử lại
        except Exception as e:
            logging.error(f"Lỗi khi gửi lên Kafka: {e}")
            break  # Lỗi khác không phải Kafka thì không retry

def read_sensor_data(sensor_id, temperature_range=(20, 30), humidity_range=(40, 60)):
    """Giả lập dữ liệu cảm biến."""
    return {
        "sensor_id": sensor_id,
        "temperature": round(random.uniform(*temperature_range), 2),
        "humidity": round(random.uniform(*humidity_range), 2),
        "timestamp": datetime.utcnow().isoformat()
    }

def sensor_thread(sensor_id, temperature_range, humidity_range, interval):
    """Luồng cảm biến, dừng an toàn khi có tín hiệu stop_event."""
    while not stop_event.is_set():
        data = read_sensor_data(sensor_id, temperature_range, humidity_range)
        logging.info(f"📡 Đọc dữ liệu cảm biến: {data}")
        send_to_kafka(data)
        stop_event.wait(interval)  # Thay vì time.sleep() để có thể dừng an toàn

def main(sensors, interval):
    """Tạo luồng riêng cho từng cảm biến."""
    threads = []
    for sensor in sensors:
        t = Thread(target=sensor_thread, args=(sensor["sensor_id"], sensor["temperature_range"], sensor["humidity_range"], interval), daemon=False)
        threads.append(t)
        t.start()

    try:
        for t in threads:
            t.join()
    except KeyboardInterrupt:
        logging.info("🔴 Nhận tín hiệu dừng, kết thúc các luồng...")
        stop_event.set()
        for t in threads:
            t.join()
        close_producer()  # Đảm bảo Kafka producer được đóng
        logging.info("✅ Đã dừng tất cả các luồng.")
        import sys
        sys.exit(0)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Kafka Sensor Data Producer")
    parser.add_argument("--interval", type=int, default=5, help="Thời gian giữa các lần gửi (giây)")
    args = parser.parse_args()

    sensors = [
        {"sensor_id": f"sensor_farm1_{i+1}", "temperature_range": (20 + i, 25 + i), "humidity_range": (40 + i, 60 + i)}
        for i in range(10)
    ]
    main(sensors, args.interval)
