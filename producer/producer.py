from kafka import KafkaProducer
import finnhub
import json
import time

# Khởi tạo Finnhub client
finnhub_client = finnhub.Client(api_key="d0ktknpr01qhb0251gvgd0ktknpr01qhb0251h00")

# Khởi tạo Kafka producer
producer = KafkaProducer(
    bootstrap_servers='kafka:9092',  # Tên service Kafka trong docker-compose
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda k: k.encode('utf-8'),
    retries=5,
    acks='all'
)

symbols = ['RBBN', 'ACRE', 'TEF', 'RGLS', 'KULR']
topic = 'bigdata'

def fetch_and_send(symbol):
    try:
        quote = finnhub_client.quote(symbol)
        message = {
            'symbol': symbol,
            'price': quote['c'],
            'high': quote['h'],
            'low': quote['l'],
            'open': quote['o'],
            'prev_close': quote['pc'],
            'timestamp': int(time.time())
        }
        # Gửi dữ liệu đến Kafka
        producer.send(topic, key=symbol, value=message)
        producer.flush()
        print(f"✔ Sent data for {symbol}")
    except Exception as e:
        print(f"❌ Error for {symbol}: {e}")

# Gửi dữ liệu cho tất cả mã
while True:
    for symbol in symbols:
        fetch_and_send(symbol)
    time.sleep(30)  # gửi mỗi 30 giây

# producer.close()
# print("✅ Đã gửi xong toàn bộ dữ liệu.")