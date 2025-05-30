#!/usr/bin/env python3
"""
Kafka Sensor Data Producer cho Farm1.
"""

import os
import sys
import json
import time
import random
import argparse
import atexit
import logging
from datetime import datetime
from threading import Event
from concurrent.futures import ThreadPoolExecutor
from typing import Tuple, List, Dict

from confluent_kafka import Producer, KafkaException

# Cấu hình logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Các khoảng mặc định cho cảm biến
DEFAULT_TEMP_RANGE: Tuple[int, int] = (20, 25)
DEFAULT_HUM_RANGE: Tuple[int, int] = (40, 60)

# Cấu hình Kafka
KAFKA_BOOTSTRAP_SERVERS: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:39092")
KAFKA_TOPIC: str = os.getenv("KAFKA_TOPIC", "sensor_data_farm1")

if not KAFKA_BOOTSTRAP_SERVERS:
    logger.error("KAFKA_BOOTSTRAP_SERVERS không được cấu hình!")
    sys.exit(1)

PRODUCER_CONFIG: Dict[str, object] = {
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    "compression.type": "snappy",
    "linger.ms": 10,
    "batch.size": 65536,
}

DEFAULT_MAX_RETRIES: int = int(os.getenv("MAX_RETRIES", 3))
DEFAULT_POLL_TIMEOUT: float = float(os.getenv("POLL_TIMEOUT", 1.0))

# Producer toàn cục và event dừng
producer: Producer
stop_event: Event = Event()


def create_producer() -> Producer:
    """Tạo Kafka Producer theo cấu hình."""
    try:
        return Producer(PRODUCER_CONFIG)
    except KafkaException as e:
        logger.error(f"Không thể khởi tạo Kafka Producer: {e}")
        sys.exit(1)


@atexit.register
def shutdown() -> None:
    """Hàm gọi khi ứng dụng thoát."""
    stop_event.set()
    logger.info("Đang đóng Kafka Producer...")
    producer.flush(timeout=5)
    logger.info("Kafka Producer đã đóng.")



def delivery_callback(err, msg) -> None:
    """Callback thông báo khi gửi message."""
    if err:
        logger.warning(f"Lỗi khi gửi message: {err}")
    else:
        logger.debug(
            f"Tin gửi thành công: {msg.topic()}"
            f" [partition {msg.partition()}] offset {msg.offset()}"
        )


def send_to_kafka(data: Dict, retries: int = DEFAULT_MAX_RETRIES) -> None:
    """Gửi dữ liệu tới Kafka với retry/backoff."""
    payload = json.dumps(data).encode("utf-8")
    for attempt in range(1, retries + 1):
        try:
            producer.produce(
                topic=KAFKA_TOPIC,
                value=payload,
                callback=delivery_callback,
            )
            producer.poll(DEFAULT_POLL_TIMEOUT)
            return
        except KafkaException as e:
            logger.warning(f"Kafka error (lần {attempt}/{retries}): {e}")
            if attempt < retries:
                backoff = 2 ** attempt + random.random()
                time.sleep(backoff)
    logger.error("Gửi dữ liệu thất bại sau nhiều lần retry.")


def read_sensor_data(
    sensor_id: str,
    temp_range: Tuple[int, int] = DEFAULT_TEMP_RANGE,
    hum_range: Tuple[int, int] = DEFAULT_HUM_RANGE,
) -> Dict:
    """Mô phỏng đọc dữ liệu cảm biến."""
    return {
        "sensor_id": sensor_id,
        "temperature": round(random.uniform(*temp_range), 2),
        "humidity": round(random.uniform(*hum_range), 2),
        "timestamp": datetime.utcnow().isoformat() + "Z",
    }


def sensor_loop(
    sensor_id: str,
    temp_range: Tuple[int, int],
    hum_range: Tuple[int, int],
    interval: int,
) -> None:
    """Vòng lặp lấy và gửi dữ liệu cảm biến."""
    while not stop_event.is_set():
        data = read_sensor_data(sensor_id, temp_range, hum_range)
        logger.info(f"[Farm1] {data}")
        send_to_kafka(data)
        time.sleep(interval)


def generate_sensors(count: int) -> List[Dict]:
    """Sinh danh sách cấu hình cảm biến."""
    return [
        {
            "sensor_id": f"sensor_farm1_{i+1}",
            "temp_range": (
                DEFAULT_TEMP_RANGE[0] + (i % 5),
                DEFAULT_TEMP_RANGE[1] + (i % 5),
            ),
            "hum_range": (
                DEFAULT_HUM_RANGE[0] + (i % 10),
                DEFAULT_HUM_RANGE[1] + (i % 10),
            ),
        }
        for i in range(count)
    ]


def parse_args() -> argparse.Namespace:
    """Phân tích tham số CLI."""
    parser = argparse.ArgumentParser(
        description="Kafka Sensor Data Producer cho Farm1"
    )
    parser.add_argument(
        "--interval", type=int, default=5, help="Khoảng thời gian gửi (giây)"
    )
    parser.add_argument(
        "--sensor_count", type=int, default=1000, help="Số lượng cảm biến"
    )
    parser.add_argument(
        "--max_workers", type=int, default=100, help="Số luồng tối đa"
    )
    return parser.parse_args()


def main() -> None:
    """Điểm khởi chạy chính của ứng dụng."""
    global producer
    args = parse_args()
    producer = create_producer()

    sensors = generate_sensors(args.sensor_count)
    with ThreadPoolExecutor(max_workers=args.max_workers) as executor:
        for sensor in sensors:
            executor.submit(
                sensor_loop,
                sensor["sensor_id"],
                sensor["temp_range"],
                sensor["hum_range"],
                args.interval,
            )
        try:
            while not stop_event.is_set():
                time.sleep(1)
        except KeyboardInterrupt:
            logger.info("Dừng chương trình theo tín hiệu Ctrl+C")


if __name__ == "__main__":
    main()
