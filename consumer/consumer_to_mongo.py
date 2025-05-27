from kafka import KafkaConsumer
from pymongo import MongoClient
import json

consumer = KafkaConsumer(
    'bigdata',
    bootstrap_servers='kafka:9092',
    auto_offset_reset='latest',
    enable_auto_commit=True,
    group_id='mongo-writer',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    key_deserializer=lambda k: k.decode('utf-8') if k else None
)

mongo_client = MongoClient("mongodb://mongo:27017")  # Docker service name
db = mongo_client['finnhub']
collection = db['quotes']

print("🚀 Consumer đang ghi dữ liệu vào MongoDB...")

for message in consumer:
    data = message.value
    data['symbol'] = message.key
    collection.insert_one(data)
    print(f"📥 Đã lưu {data['symbol']} tại thời điểm {data['timestamp']}")
