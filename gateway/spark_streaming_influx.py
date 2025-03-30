import os
import time
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# Thư viện InfluxDB (v2)
from influxdb_client import InfluxDBClient, Point
from pyspark.sql.streaming import ForeachWriter

# ---- 1) Khai báo schema cho JSON ----
sensor_schema = StructType([
    StructField("sensor_id", StringType(), True),
    StructField("temperature", DoubleType(), True),
    StructField("humidity", DoubleType(), True),
    StructField("timestamp", StringType(), True)
])

# ---- 2) Tạo SparkSession ----
spark = SparkSession.builder \
    .appName("StructuredStreaming-IoT") \
    .getOrCreate()

# ---- 3) Đọc dữ liệu từ Kafka ----
# Thay "broker:9092" thành địa chỉ Kafka thực tế của bạn
df_kafka = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "broker:9092") \
    .option("subscribe", "sensor_data") \
    .option("startingOffsets", "latest") \
    .load()

# Kafka trả về cột "key" và "value" dạng nhị phân, cần chuyển sang String
df_json = df_kafka.selectExpr("CAST(value AS STRING) AS json_str")

# Parse JSON thành các cột: sensor_id, temperature, humidity, timestamp
df_parsed = df_json.select(
    from_json(col("json_str"), sensor_schema).alias("data")
).select("data.*")

# ---- 4) Ví dụ xử lý / lọc logic (nếu muốn) ----
# Giả sử chỉ lấy record mà nhiệt độ > 25
df_filtered = df_parsed.filter(col("temperature") > 25)

# Nếu muốn xử lý logic “nhiệt độ thay đổi dưới 0.5 thì bỏ qua”
# Ta cần so sánh với giá trị cũ. Thường phải dùng stateful operation (cửa sổ / window).
# Ở ví dụ đơn giản này, ta bỏ qua hoặc làm sau.

# ---- 5) Tạo custom sink ghi InfluxDB ----
#    - Dùng ForeachWriter: open() -> process(row) -> close()
INFLUXDB_URL = os.getenv("INFLUXDB_URL", "http://localhost:8086")
INFLUXDB_TOKEN = os.getenv("INFLUXDB_TOKEN", "myToken")
INFLUXDB_ORG = os.getenv("INFLUXDB_ORG", "myOrg")
INFLUXDB_BUCKET = os.getenv("INFLUXDB_BUCKET", "sensor_data")


class InfluxSink(ForeachWriter):
    def open(self, partition_id, epoch_id):
        """Khởi tạo kết nối InfluxDB cho mỗi partition."""
        self.client = InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG)
        self.write_api = self.client.write_api()
        return True  # Trả về True nếu kết nối OK

    def process(self, row):
        """
        Ghi 1 record vào InfluxDB.
        row chính là 1 row của DataFrame (có sensor_id, temperature, humidity, timestamp).
        """
        point = Point("sensor_data") \
            .tag("sensor_id", row.sensor_id) \
            .field("temperature", row.temperature) \
            .field("humidity", row.humidity) \
            .time(row.timestamp)  # row.timestamp kiểu string ISO8601

        try:
            self.write_api.write(bucket=INFLUXDB_BUCKET, org=INFLUXDB_ORG, record=point)
        except Exception as e:
            print(f"Lỗi ghi Influx: {e}")

    def close(self, error):
        """Đóng kết nối."""
        if self.client:
            self.client.close()


# ---- 6) Khởi tạo query ghi stream --> InfluxDB ----
#    - outputMode("append") phù hợp với Kafka source
#    - format("console") thay bằng foreach(InfluxSink()) để ghi Influx
query = df_filtered.writeStream \
    .outputMode("append") \
    .foreach(InfluxSink()) \
    .start()

query.awaitTermination()

