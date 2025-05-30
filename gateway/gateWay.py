import os
import time
import json
import random
import argparse
import atexit
import logging
from threading import Event
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from confluent_kafka import Producer, KafkaException

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:39092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "sensor_data_farm1")

if not KAFKA_BOOTSTRAP_SERVERS:
    logger.error(" KAFKA_BOOTSTRAP_SERVERS không được cấu hình!")
    exit(1)

# Cấu hình Kafka Producer
PRODUCER_CONFIG = {
    'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
    'compression.type': 'snappy',
    'linger.ms': 10,
    'batch.size': 65536,
}

# Các tham số retry mặc định
DEFAULT_MAX_RETRIES = int(os.getenv("MAX_RETRIES", 3))
DEFAULT_POLL_TIMEOUT = float(os.getenv("POLL_TIMEOUT", 1))  # thời gian poll (giây)

def create_producer():
    """Tạo và trả về Kafka Producer."""
    try:
        return Producer(PRODUCER_CONFIG)
    except KafkaException as e:
        logger.error(f"❌ Không thể khởi tạo Kafka Producer: {e}")
        exit(1)

# Khởi tạo Kafka Producer toàn cục
producer = create_producer()

# Sự kiện để báo hiệu dừng các luồng
stop_event = Event()

def close_producer():
    """Flush các message chờ và đóng Kafka Producer."""
    logger.info("🔴 Đang đóng Kafka Producer...")
    producer.flush(timeout=5)
    logger.info("✅ Kafka Producer đã đóng.")

atexit.register(close_producer)

def delivery_callback(err, msg):
    """Callback khi gửi message đến Kafka."""
    if err:
        logger.warning(f"⚠️ Lỗi khi gửi tin: {err}")
    else:
        logger.debug(f"✅ Tin gửi thành công tới {msg.topic()} [partition {msg.partition()}] offset {msg.offset()}")

def send_to_kafka(data, retries=DEFAULT_MAX_RETRIES):
    """
    Gửi dữ liệu tới Kafka với cơ chế retry sử dụng exponential backoff.
    
    Args:
        data (dict): Dữ liệu cảm biến cần gửi.
        retries (int): Số lần thử gửi lại khi gặp lỗi.
    """
    message_json = json.dumps(data)
    for attempt in range(1, retries + 1):
        try:
            producer.produce(KAFKA_TOPIC, value=message_json.encode('utf-8'), callback=delivery_callback)
            producer.poll(DEFAULT_POLL_TIMEOUT)
            return
        except KafkaException as e:
            logger.warning(f"⚠️ Kafka Error (lần {attempt}/{retries}): {e}")
            if attempt < retries:
                sleep_time = (2 ** attempt) + random.uniform(0, 0.1)
                time.sleep(sleep_time)
    logger.error("❌ Gửi dữ liệu thất bại sau nhiều lần retry.")

def read_sensor_data(sensor_id, temperature_range=(20, 30), humidity_range=(40, 60)):
    """
    Mô phỏng việc đọc dữ liệu từ cảm biến.
    
    Args:
        sensor_id (str): ID của cảm biến.
        temperature_range (tuple): Khoảng nhiệt độ.
        humidity_range (tuple): Khoảng độ ẩm.
    
    Returns:
        dict: Dữ liệu cảm biến bao gồm nhiệt độ, độ ẩm và timestamp.
    """
    return {
        "sensor_id": sensor_id,
        "temperature": round(random.uniform(*temperature_range), 2),
        "humidity": round(random.uniform(*humidity_range), 2),
        "timestamp": datetime.utcnow().isoformat() + "Z"
    }

def sensor_loop(sensor_id, temperature_range, humidity_range, interval):
    """Vòng lặp đọc dữ liệu cảm biến và gửi tới Kafka theo khoảng thời gian cố định."""
    while not stop_event.is_set():
        data = read_sensor_data(sensor_id, temperature_range, humidity_range)
        logger.info(f"📡 [Farm1] Đọc dữ liệu cảm biến: {data}")
        send_to_kafka(data)
        time.sleep(interval)

def run_sensor_threads(sensors, interval, max_workers):
    """
    Khởi chạy các luồng đọc cảm biến song song.
    
    Args:
        sensors (list): Danh sách cấu hình cảm biến.
        interval (int): Khoảng thời gian giữa các lần gửi (giây).
        max_workers (int): Số luồng tối đa của ThreadPoolExecutor.
    """
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        for sensor in sensors:
            executor.submit(
                sensor_loop,
                sensor["sensor_id"],
                sensor["temperature_range"],
                sensor["humidity_range"],
                interval
            )
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            logger.info("🔴 Nhận tín hiệu dừng, kết thúc các luồng...")
            stop_event.set()
            close_producer()
            logger.info("✅ Đã dừng tất cả các luồng.")

def parse_arguments():
    """Phân tích các tham số dòng lệnh."""
    parser = argparse.ArgumentParser(description="Kafka Sensor Data Producer cho Farm 1")
    parser.add_argument("--interval", type=int, default=5, help="Thời gian giữa các lần gửi (giây)")
    parser.add_argument("--sensor_count", type=int, default=1000, help="Số lượng cảm biến cần mô phỏng")
    parser.add_argument("--max_workers", type=int, default=100, help="Số luồng tối đa của ThreadPoolExecutor")
    return parser.parse_args()

def generate_sensors(count):
    """
    Sinh ra danh sách cảm biến với các cấu hình nhiệt độ và độ ẩm khác nhau.
    
    Args:
        count (int): Số lượng cảm biến cần tạo.
    
    Returns:
        list: Danh sách các cấu hình cảm biến.
    """
    sensors = []
    for i in range(count):
        sensors.append({
            "sensor_id": f"sensor_farm1_{i + 1}",
            "temperature_range": (20 + i % 5, 25 + i % 5),
            "humidity_range": (40 + i % 10, 60 + i % 10)
        })
    return sensors

def main():
    """Hàm chính khởi chạy ứng dụng cho Farm 1."""
    args = parse_arguments()
    sensors = generate_sensors(args.sensor_count)
    run_sensor_threads(sensors, args.interval, args.max_workers)

if __name__ == "__main__":
    main()
    