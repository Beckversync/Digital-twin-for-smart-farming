import os
import json
from kafka import KafkaConsumer
from influxdb_client import InfluxDBClient, Point, WritePrecision

# Lấy cấu hình từ biến môi trường hoặc dùng giá trị mặc định
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:39092").split(",")
TOPIC = os.getenv("KAFKA_TOPIC", "sensor_data")  # Đồng bộ với Gateway
GROUP_ID = os.getenv("KAFKA_CONSUMER_GROUP", "sensor_group")

# Cấu hình InfluxDB
INFLUXDB_URL = os.getenv("INFLUXDB_URL", "http://localhost:8086")
INFLUXDB_TOKEN = os.getenv("INFLUXDB_TOKEN", "yh3-4d56K_isFw40Ar7p9NQ0SYxV8qYCdZ3L_wLEvZf7xH-yL1PoeTwVGQxOyXJq9b_zSXdkKXADD64tvF4W6Q==")
INFLUXDB_ORG = os.getenv("INFLUXDB_ORG", "Ho Chi Minh University of Technology")
INFLUXDB_BUCKET = os.getenv("INFLUXDB_BUCKET", "sensor_data")

# Kết nối InfluxDB
influx_client = InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG)
from influxdb_client.client.write_api import SYNCHRONOUS
write_api = influx_client.write_api(write_options=SYNCHRONOUS)


# Kafka Consumer
consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=BOOTSTRAP_SERVERS,
    security_protocol='PLAINTEXT',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    auto_offset_reset='earliest',
    group_id=GROUP_ID
)

def consume_messages():
    """Nhận dữ liệu từ Kafka và lưu vào InfluxDB"""
    print(f"📡 Consumer đang lắng nghe trên topic: {TOPIC}...")
    try:
        for message in consumer:
            data = message.value
            print(f"✅ Received: {data}")

            # Chuyển đổi dữ liệu thành Point của InfluxDB
            point = Point("sensor_data").tag("sensor_id", data["sensor_id"]) \
                .field("temperature", data["temperature"]) \
                .field("humidity", data["humidity"]) \
                .time(data["timestamp"])

            # Ghi vào InfluxDB
            write_api.write(bucket=INFLUXDB_BUCKET, org=INFLUXDB_ORG, record=point)
            print("📥 Dữ liệu đã được lưu vào InfluxDB!")
    except KeyboardInterrupt:
        print("⚠️ Consumer dừng do người dùng yêu cầu.")
    finally:
        consumer.close()
        influx_client.close()
        print("🔌 Consumer đã đóng kết nối.")

if __name__ == "__main__":
    consume_messages()
