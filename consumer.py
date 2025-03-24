import os
import json
import logging
from kafka import KafkaConsumer
from influxdb_client import InfluxDBClient, Point, WritePrecision
from concurrent.futures import ThreadPoolExecutor

# Cấu hình logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Lấy cấu hình từ biến môi trường hoặc dùng giá trị mặc định
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:39092").split(",")
TOPICS = ["sensor_data", "sensor_data_farm2"]  # Lắng nghe cả hai topic
GROUP_ID = os.getenv("KAFKA_CONSUMER_GROUP", "sensor_group")

# Cấu hình InfluxDB
INFLUXDB_URL = os.getenv("INFLUXDB_URL", "http://localhost:8086")
INFLUXDB_TOKEN = os.getenv("INFLUXDB_TOKEN", "52OGtNGg0FWwWUr2JoeG8lPJnQL13SyvICbGF3DyVx4gE8ZzB5KwhH4RpjJzHvolO3YtnQ_DBpVqYcnlXSHlWQ==")
INFLUXDB_ORG = os.getenv("INFLUXDB_ORG", "Ho Chi Minh University of Technology")
INFLUXDB_BUCKET = os.getenv("INFLUXDB_BUCKET", "sensor_data")

# Kết nối InfluxDB
influx_client = InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG)
write_api = influx_client.write_api()

# Kafka Consumer
consumer = KafkaConsumer(
    *TOPICS,
    bootstrap_servers=BOOTSTRAP_SERVERS,
    security_protocol='PLAINTEXT',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    auto_offset_reset='earliest',
    group_id=GROUP_ID
)

# Dùng ThreadPool để xử lý message song song
pool = ThreadPoolExecutor(max_workers=4)
last_values = {}


def process_message(data):
    """Xử lý và lưu dữ liệu vào InfluxDB, lọc dữ liệu không cần thiết."""
    sensor_id = data["sensor_id"]

    # Lọc dữ liệu nếu thay đổi không đáng kể
    if sensor_id in last_values and abs(last_values[sensor_id] - data["temperature"]) < 0.5:
        logger.info(f"🔄 Bỏ qua dữ liệu không thay đổi đáng kể: {data}")
        return

    last_values[sensor_id] = data["temperature"]
    point = Point("sensor_data").tag("sensor_id", sensor_id) \
        .field("temperature", data["temperature"]) \
        .field("humidity", data["humidity"]) \
        .time(data["timestamp"])

    write_api.write(bucket=INFLUXDB_BUCKET, org=INFLUXDB_ORG, record=point)
    logger.info(f"📥 Dữ liệu đã lưu vào InfluxDB: {data}")


def consume_messages():
    """Nhận dữ liệu từ Kafka và xử lý song song."""
    logger.info(f"📡 Consumer đang lắng nghe trên các topics: {TOPICS}...")
    try:
        for message in consumer:
            data = message.value
            pool.submit(process_message, data)  # Đẩy task vào thread pool
    except KeyboardInterrupt:
        logger.warning("⚠️ Consumer dừng do người dùng yêu cầu.")
    finally:
        consumer.close()
        influx_client.close()
        logger.info("🔌 Consumer đã đóng kết nối.")


if __name__ == "__main__":
    consume_messages()
