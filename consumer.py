import os
import json
from kafka import KafkaConsumer

# Lấy cấu hình từ biến môi trường (nếu có) hoặc dùng giá trị mặc định
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092").split(",")
TOPIC = os.getenv("KAFKA_TOPIC", "my_msk_topic")
GROUP_ID = os.getenv("KAFKA_CONSUMER_GROUP", "my_group")

# Tạo Kafka consumer
consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=BOOTSTRAP_SERVERS,
    security_protocol='PLAINTEXT',  # Sử dụng kết nối không mã hoá (PLAINTEXT)
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    auto_offset_reset='earliest',   # Đọc từ offset đầu tiên nếu chưa có offset nào được lưu
    group_id=GROUP_ID
)

def consume_messages():
    """Lắng nghe và xử lý các message nhận được từ Kafka."""
    print(f"Consumer bắt đầu lắng nghe trên topic: {TOPIC}")
    try:
        for message in consumer:
            print(f"Received: {message.value}")
    except KeyboardInterrupt:
        print("Consumer dừng do người dùng yêu cầu.")
    finally:
        consumer.close()
        print("Consumer đã đóng kết nối.")

if __name__ == "__main__":
    consume_messages()
