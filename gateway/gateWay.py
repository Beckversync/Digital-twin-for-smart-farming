import os
import time
import json
import random
from confluent_kafka import Producer

# Lấy cấu hình từ biến môi trường (nếu có), nếu không sẽ dùng giá trị mặc định
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "sensor_data_topic")

# Cấu hình producer cho Kafka
producer_conf = {
    'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS
}
producer = Producer(producer_conf)

def send_to_kafka(data):
    """Gửi dữ liệu dạng JSON lên Kafka."""
    try:
        message_json = json.dumps(data)
        producer.produce(KAFKA_TOPIC, value=message_json)
        # Đợi cho đến khi message được gửi thành công
        producer.flush()
        print(f"✅ Sent to Kafka: {data}")
    except Exception as e:
        print(f"⚠️ Error sending to Kafka: {e}")

def read_sensor_data():
    """Giả lập dữ liệu cảm biến với nhiệt độ và độ ẩm ngẫu nhiên."""
    temperature = random.uniform(20, 30)
    humidity = random.uniform(40, 60)
    return {
        "sensor_id": "sensor_01",
        "temperature": temperature,
        "humidity": humidity,
        "timestamp": int(time.time())
    }

def main():
    """Chạy vòng lặp vô hạn để đọc và gửi dữ liệu cảm biến theo chu kỳ."""
    while True:
        data = read_sensor_data()
        print(f"Read sensor data: {data}")
        send_to_kafka(data)
        time.sleep(5)  # Tạm dừng 5 giây trước khi đọc dữ liệu mới

if __name__ == "__main__":
    main()
