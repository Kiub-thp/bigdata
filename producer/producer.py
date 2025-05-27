from kafka import KafkaProducer
import requests
import json
import time

API_KEY = "4708a451a0424a31b61aa51b9ba5a540"
BASE_URL = "https://api.twelvedata.com"

symbols = ['AMC', 'GME', 'NIO', 'PLTR', 'AAPL']
topic = 'bigdata'

producer = KafkaProducer(
    bootstrap_servers='kafka:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda k: k.encode('utf-8'),
    retries=5,
    acks='all'
)

def fetch_and_send(symbol):
    try:
        # 1. Lấy dữ liệu mới nhất theo 1 phút
        ts_params = {
            "symbol": symbol,
            "interval": "1min",
            "apikey": API_KEY,
            "outputsize": 1
        }
        ts_res = requests.get(f"{BASE_URL}/time_series", params=ts_params)
        ts_data = ts_res.json()

        if "values" not in ts_data or len(ts_data["values"]) == 0:
            print(f"⚠️ No time_series data for {symbol}")
            return

        latest = ts_data["values"][0]

        # 2. Lấy previous_close từ endpoint quote
        quote_params = {
            "symbol": symbol,
            "apikey": API_KEY
        }
        quote_res = requests.get(f"{BASE_URL}/quote", params=quote_params)
        quote_data = quote_res.json()

        prev_close = float(quote_data["previous_close"]) if "previous_close" in quote_data else None

        message = {
            "symbol": symbol,
            "price": float(latest["close"]),
            "high": float(latest["high"]),
            "low": float(latest["low"]),
            "open": float(latest["open"]),
            "volume": float(latest["volume"]),
            "prev_close": prev_close,
            "timestamp": int(time.time())
        }

        producer.send(topic, key=symbol, value=message)
        producer.flush()
        print(f"✔ Sent data for {symbol}")

    except Exception as e:
        print(f"❌ Error for {symbol}: {e}")

while True:
    for symbol in symbols:
        fetch_and_send(symbol)
    time.sleep(60)
