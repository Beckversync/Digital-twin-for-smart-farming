from kafka import KafkaProducer
import json

# Kết nối tới broker đang chạy trong Docker
bootstrap_servers = ['localhost:29092']

# Tạo Kafka producer
producer = KafkaProducer(
    bootstrap_servers=bootstrap_servers,
    security_protocol='PLAINTEXT',  # Đổi thành PLAINTEXT
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Gửi message tới Kafka topic
for i in range(10):
    message = {'number': i}
    producer.send('my_msk_topic', value=message)  # Đảm bảo topic này đã được tạo
    print(f"Sent: {message}")

# Đóng producer
producer.close()
