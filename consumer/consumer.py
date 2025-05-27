from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'bigdata',
    bootstrap_servers='kafka:9092',
    auto_offset_reset='latest',
    enable_auto_commit=True,
    group_id='data-logger-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    key_deserializer=lambda k: k.decode('utf-8') if k else None
)

print("🚀 Consumer đã kết nối, đang theo dõi tăng trần / giảm sàn...\n")

for message in consumer:
    symbol = message.key
    data = message.value
    price = data['price']
    high = data['high']
    low = data['low']
    volume = data ['volume']

    print(f"[{symbol}] Giá hiện tại: {price} | Cao nhất: {high} | Thấp nhất: {low}")

    # Kiểm tra tăng trần / giảm sàn
    if price >= high:
        print(f"🚀🚀 {symbol} đang TĂNG TRẦN!")
    elif price <= low:
        print(f"🔻🔻 {symbol} đang GIẢM SÀN!")
    else: print("Không có bất thường")



# from pyspark.sql import SparkSession
# from pyspark.sql.functions import from_json, col, expr, when, lag
# from pyspark.sql.types import StructType, StringType, FloatType, TimestampType
# from pyspark.sql.window import Window

# spark = SparkSession.builder \
#     .appName("KafkaSparkStreamingAnalysis") \
#     .config("spark.jars.packages", 
#             "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1") \
#     .getOrCreate()

# spark.sparkContext.setLogLevel("WARN")

# # Schema mở rộng để chứa nhiều thông tin hơn
# schema = StructType() \
#     .add("symbol", StringType()) \
#     .add("price", FloatType()) \
#     .add("high", FloatType()) \
#     .add("low", FloatType()) \
#     .add("open", FloatType()) \
#     .add("prev_close", FloatType()) \
#     .add("timestamp", StringType())  # sẽ ép kiểu sang Timestamp sau

# # Đọc từ Kafka
# df_raw = spark.readStream \
#     .format("kafka") \
#     .option("kafka.bootstrap.servers", "kafka:9092") \
#     .option("subscribe", "bigdata") \
#     .option("startingOffsets", "latest") \
#     .load()

# # Parse JSON
# df_parsed = df_raw.selectExpr("CAST(value AS STRING) as json_str") \
#     .select(from_json(col("json_str"), schema).alias("data")) \
#     .select("data.*") \
#     .withColumn("event_time", expr("CAST(timestamp AS TIMESTAMP)"))

# # Tính toán giá thay đổi so với giá đóng cửa trước
# df_analyzed = df_parsed.withColumn(
#     "price_change", col("price") - col("prev_close")
# ).withColumn(
#     "change_percent", ((col("price") - col("prev_close")) / col("prev_close")) * 100
# )

# # Cửa sổ thời gian theo symbol
# window_spec = Window.partitionBy("symbol").orderBy("event_time")

# # Dữ liệu giá trước đó (so sánh để xác định đột biến)
# df_with_lag = df_analyzed.withColumn("prev_price", lag("price").over(window_spec)) \
#     .withColumn("price_jump_percent", ((col("price") - col("prev_price")) / col("prev_price")) * 100)

# # Đánh dấu các hiện tượng bất thường
# df_flagged = df_with_lag.withColumn(
#     "is_spike", when(col("price_jump_percent") > 5, "🚀 Tăng đột biến").when(col("price_jump_percent") < -5, "📉 Giảm đột biến")
# ).withColumn(
#     "is_limit_up", when(col("change_percent") >= 6.9, "⬆ Tăng trần")  # Tùy sàn, thường là ~7%
# ).withColumn(
#     "is_limit_down", when(col("change_percent") <= -6.9, "⬇ Giảm sàn")
# )

# # Hiển thị ra console
# query = df_flagged.select("symbol", "price", "change_percent", "is_spike", "is_limit_up", "is_limit_down", "event_time") \
#     .writeStream \
#     .outputMode("append") \
#     .format("console") \
#     .option("truncate", False) \
#     .start()

# query.awaitTermination()