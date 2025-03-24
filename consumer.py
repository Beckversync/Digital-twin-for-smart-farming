import os
import json
import logging
import time
from kafka import KafkaConsumer
from influxdb_client import InfluxDBClient, Point
from concurrent.futures import ThreadPoolExecutor
from threading import Lock

# Cấu hình logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Cấu hình Kafka
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:39092").split(",")
TOPICS = ["sensor_data", "sensor_data_farm2"]
GROUP_ID = os.getenv("KAFKA_CONSUMER_GROUP", "sensor_group")

# Cấu hình InfluxDB
INFLUXDB_URL = os.getenv("INFLUXDB_URL", "http://localhost:8086")
INFLUXDB_TOKEN = os.getenv("INFLUXDB_TOKEN", "aGxrcSG1YPp59_vFu3eiOMJHDv3NmBeJlIPdWGqqEEOfBHluP60gHy__-X1EnIata_97n8YNaEGyDTUoy9BY0g==")  # Đồng bộ với Docker Compose
INFLUXDB_ORG = os.getenv("INFLUXDB_ORG", "Ho Chi Minh University of Technology")
INFLUXDB_BUCKET = os.getenv("INFLUXDB_BUCKET", "sensor_data")

# Kết nối InfluxDB
try:
    influx_client = InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG)
    write_api = influx_client.write_api()
except Exception as e:
    logger.error(f"❌ Không thể kết nối InfluxDB: {e}")
    exit(1)

# Kafka Consumer
try:
    consumer = KafkaConsumer(
        *TOPICS,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        security_protocol='PLAINTEXT',
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        auto_offset_reset='earliest',
        group_id=GROUP_ID
    )
except Exception as e:
    logger.error(f"❌ Không thể khởi tạo Kafka Consumer: {e}")
    exit(1)

# ThreadPool và Lock cho thread-safe
pool = ThreadPoolExecutor(max_workers=4)
last_values = {}
lock = Lock()

def process_message(data):
    sensor_id = data["sensor_id"]
    with lock:
        if sensor_id in last_values and abs(last_values[sensor_id] - data["temperature"]) < 0.5:
            logger.debug(f"🔄 Bỏ qua dữ liệu không thay đổi đáng kể: {data}")
            return
        last_values[sensor_id] = data["temperature"]

    point = Point("sensor_data").tag("sensor_id", sensor_id) \
        .field("temperature", data["temperature"]) \
        .field("humidity", data["humidity"]) \
        .time(data["timestamp"])

    for attempt in range(3):  # Retry 3 lần nếu ghi thất bại
        try:
            write_api.write(bucket=INFLUXDB_BUCKET, org=INFLUXDB_ORG, record=point)
            logger.info(f"📥 Dữ liệu đã lưu vào InfluxDB: {data}")
            break
        except Exception as e:
            logger.warning(f"⚠️ Lỗi khi ghi vào InfluxDB (lần {attempt+1}/3): {e}")
            if attempt < 2:
                time.sleep(1)

def consume_messages():
    logger.info(f"📡 Consumer đang lắng nghe trên các topics: {TOPICS}...")
    try:
        for message in consumer:
            data = message.value
            pool.submit(process_message, data)
    except KeyboardInterrupt:
        logger.warning("⚠️ Consumer dừng do người dùng yêu cầu.")
    finally:
        consumer.close()
        influx_client.close()
        pool.shutdown(wait=True)
        logger.info("🔌 Consumer đã đóng kết nối.")

if __name__ == "__main__":
    consume_messages()