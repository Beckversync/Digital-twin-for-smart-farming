# gateway.py
import time
import json
import random
from confluent_kafka import Producer

# Cấu hình Kafka (sửa lại cho phù hợp với cluster của bạn)
KAFKA_BOOTSTRAP_SERVERS = "kafka1:9092"  # hoặc IP server Kafka
KAFKA_TOPIC = "sensor_data_topic"

# Tạo Kafka Producer
producer_conf = {
    'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
    # nếu cần bảo mật TLS/SSL, user/pass, có thể thêm config ở đây
}
producer = Producer(producer_conf)

def send_to_kafka(data):
    """Gửi dữ liệu lên Kafka."""
    # Convert to JSON
    message_json = json.dumps(data)
    producer.produce(KAFKA_TOPIC, value=message_json)
    producer.flush()  # hoặc flush định kỳ

def read_sensor_data():
    """
    Hàm đọc dữ liệu cảm biến.
    Đây là ví dụ giả lập, mỗi lần gọi random 1 giá trị nhiệt độ.
    Thay bằng code đọc thực tế (I2C, Serial, ...).
    """
    # Ví dụ: random giá trị nhiệt độ
    temperature = random.uniform(20, 30)
    humidity = random.uniform(40, 60)
    return {
        "sensor_id": "sensor_01",
        "temperature": temperature,
        "humidity": humidity,
        "timestamp": int(time.time())
    }

def main():
    while True:
        data = read_sensor_data()
        print(f"Read sensor data: {data}")
        send_to_kafka(data)
        time.sleep(5)  # gửi dữ liệu 5s/lần, tuỳ nhu cầu

if __name__ == "__main__":
    main()

