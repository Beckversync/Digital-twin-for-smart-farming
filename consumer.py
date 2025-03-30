import os
import json
import logging
import time
from kafka import KafkaConsumer
from influxdb_client import InfluxDBClient, Point
from concurrent.futures import ThreadPoolExecutor
from threading import Lock

# Cấu hình logging
logging.basicConfig(level=logging.INFO, format='%(ascti me)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Cấu hình Kafka
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:39092").split(",")
TOPICS = ["sensor_data", "sensor_data_farm2"]
GROUP_ID = os.getenv("KAFKA_CONSUMER_GROUP", "sensor_group")

# Cấu hình InfluxDB
INFLUXDB_URL = os.getenv("INFLUXDB_URL", "http://localhost:8086")
INFLUXDB_TOKEN = os.getenv("INFLUXDB_TOKEN", "aGxrcSG1YPp59_vFu3eiOMJHDv3NmBeJlIPdWGqqEEOfBHluP60gHy__-X1EnIata_97n8YNaEGyDTUoy9BY0g==")
INFLUXDB_ORG = os.getenv("INFLUXDB_ORG", "Ho Chi Minh University of Technology")
INFLUXDB_BUCKET = os.getenv("INFLUXDB_BUCKET", "sensor_data")


def create_influx_client():
    """
    Tạo và trả về InfluxDB client cùng với write API.
    """
    try:
        client = InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG)
        write_api = client.write_api()
        return client, write_api
    except Exception as e:
        logger.error(f"❌ Không thể kết nối InfluxDB: {e}")
        exit(1)


def create_kafka_consumer():
    """
    Tạo và trả về Kafka Consumer.
    """
    try:
        consumer = KafkaConsumer(
            *TOPICS,
            bootstrap_servers=BOOTSTRAP_SERVERS,
            security_protocol='PLAINTEXT',
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            auto_offset_reset='earliest',
            group_id=GROUP_ID
        )
        return consumer
    except Exception as e:
        logger.error(f"❌ Không thể khởi tạo Kafka Consumer: {e}")
        exit(1)


# Khởi tạo thread pool và lock cho xử lý thread-safe
pool = ThreadPoolExecutor(max_workers=4)
last_values = {}
lock = Lock()


def process_message(data, write_api):
    """
    Xử lý một message từ Kafka và ghi dữ liệu vào InfluxDB nếu có thay đổi đáng kể.

    Args:
        data (dict): Dữ liệu cảm biến từ Kafka.
        write_api: InfluxDB Write API.
    """
    sensor_id = data.get("sensor_id")
    if sensor_id is None:
        logger.warning("🔄 Dữ liệu không hợp lệ, thiếu sensor_id.")
        return

    current_temp = data.get("temperature")
    with lock:
        if sensor_id in last_values and abs(last_values[sensor_id] - current_temp) < 0.5:
            logger.debug(f"🔄 Bỏ qua dữ liệu không thay đổi đáng kể: {data}")
            return
        last_values[sensor_id] = current_temp

    point = Point("sensor_data") \
        .tag("sensor_id", sensor_id) \
        .field("temperature", current_temp) \
        .field("humidity", data.get("humidity")) \
        .time(data.get("timestamp"))

    for attempt in range(1, 4):
        try:
            write_api.write(bucket=INFLUXDB_BUCKET, org=INFLUXDB_ORG, record=point)
            logger.info(f"📥 Dữ liệu đã lưu vào InfluxDB: {data}")
            break
        except Exception as e:
            logger.warning(f"⚠️ Lỗi khi ghi vào InfluxDB (lần {attempt}/3): {e}")
            if attempt < 3:
                time.sleep(1)


def consume_messages(consumer, write_api, influx_client):
    """
    Lắng nghe message từ Kafka và xử lý chúng song song.

    Args:
        consumer: Kafka Consumer instance.
        write_api: InfluxDB Write API.
        influx_client: InfluxDB Client instance.
    """
    logger.info(f"📡 Consumer đang lắng nghe trên các topics: {TOPICS}...")
    try:
        for message in consumer:
            data = message.value
            pool.submit(process_message, data, write_api)
    except KeyboardInterrupt:
        logger.warning("⚠️ Consumer dừng do người dùng yêu cầu.")
    finally:
        consumer.close()
        influx_client.close()
        pool.shutdown(wait=True)
        logger.info("🔌 Consumer đã đóng kết nối.")


def main():
    """
    Hàm chính khởi tạo các client và bắt đầu quá trình consume message.
    """
    influx_client, write_api = create_influx_client()
    consumer = create_kafka_consumer()
    consume_messages(consumer, write_api, influx_client)


if __name__ == "__main__":
    main()
