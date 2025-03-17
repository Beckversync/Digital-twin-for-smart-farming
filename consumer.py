from kafka import KafkaConsumer
import json

# Replace with your MSK cluster bootstrap servers
# bootstrap_servers = ['<broker-1-endpoint>:9094', '<broker-2-endpoint>:9094']
bootstrap_servers = ['localhost:29092']

# Create a Kafka consumer
consumer = KafkaConsumer(
    'my_msk_topic',  # Thay bằng tên topic thật
    bootstrap_servers=bootstrap_servers,
    security_protocol='PLAINTEXT',  # Không dùng SSL, nên đổi thành PLAINTEXT
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    auto_offset_reset='earliest',
    group_id='my_group'
)
# Consume messages from the Kafka topic
for message in consumer:
    print(f"Received: {message.value}")

# Close the consumer
consumer.close()