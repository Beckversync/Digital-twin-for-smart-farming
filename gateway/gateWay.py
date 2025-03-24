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
logger = logging.getLogger(__name__)

# Cấu hình Kafka
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:39092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "sensor_data")

# Kiểm tra cấu hình
if not KAFKA_BOOTSTRAP_SERVERS:
    logger.error("❌ KAFKA_BOOTSTRAP_SERVERS không được cấu hình!")
    exit(1)

# Cấu hình producer
producer_conf = {
    'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
    'compression.type': 'snappy',
    'linger.ms': 10,  # Thêm để tối ưu hiệu suất gửi
    'batch.size': 65536,  # Tăng batch size để cải thiện thông lượng
}
try:
    producer = Producer(producer_conf)
except KafkaException as e:
    logger.error(f"❌ Không thể khởi tạo Kafka Producer: {e}")
    exit(1)

# Sự kiện để dừng các luồng
stop_event = Event()

def close_producer():
    logger.info("🔴 Đang đóng Kafka producer...")
    producer.flush(timeout=5)  # Đợi tối đa 5 giây để gửi hết dữ liệu
    logger.info("✅ Kafka producer đã đóng.")

atexit.register(close_producer)

def delivery_callback(err, msg):
    if err:
        logger.warning(f"⚠️ Lỗi khi gửi tin: {err}")
    else:
        logger.debug(f"✅ Tin gửi thành công tới {msg.topic()} [partition {msg.partition()}] offset {msg.offset()}")

def send_to_kafka(data, retries=3):
    message_json = json.dumps(data)
    for attempt in range(retries):
        try:
            producer.produce(KAFKA_TOPIC, value=message_json.encode('utf-8'), callback=delivery_callback)
            producer.poll(1)  # Đợi 1 giây để đảm bảo tin được gửi
            return
        except KafkaException as e:
            logger.warning(f"⚠️ Kafka Error (lần {attempt+1}/{retries}): {e}")
            if attempt < retries - 1:
                time.sleep(2)

def read_sensor_data(sensor_id, temperature_range=(20, 30), humidity_range=(40, 60)):
    return {
        "sensor_id": sensor_id,
        "temperature": round(random.uniform(*temperature_range), 2),
        "humidity": round(random.uniform(*humidity_range), 2),
        "timestamp": datetime.utcnow().isoformat() + "Z"  # Thêm 'Z' để chuẩn hóa ISO 8601
    }

def sensor_thread(sensor_id, temperature_range, humidity_range, interval):
    while not stop_event.is_set():
        data = read_sensor_data(sensor_id, temperature_range, humidity_range)
        logger.info(f"📡 Đọc dữ liệu cảm biến: {data}")
        send_to_kafka(data)
        time.sleep(interval)  # Đơn giản hóa với time.sleep

from concurrent.futures import ThreadPoolExecutor

def main(sensors, interval):
    with ThreadPoolExecutor(max_workers=len(sensors)) as executor:
        futures = [executor.submit(sensor_thread, sensor["sensor_id"], sensor["temperature_range"], sensor["humidity_range"], interval) for sensor in sensors]

        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            logger.info("🔴 Nhận tín hiệu dừng, kết thúc các luồng...")
            stop_event.set()
            close_producer()
            logger.info("✅ Đã dừng tất cả các luồng.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Kafka Sensor Data Producer")
    parser.add_argument("--interval", type=int, default=5, help="Thời gian giữa các lần gửi (giây)")
    args = parser.parse_args()

    sensors = [
        {"sensor_id": f"sensor_farm1_{i+1}", "temperature_range": (20 + i, 25 + i), "humidity_range": (40 + i, 60 + i)}
        for i in range(10)
    ]
    main(sensors, args.interval)