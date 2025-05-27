from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg
from pyspark.sql.types import TimestampType
from pyspark.sql.window import Window

# 1. Tạo SparkSession
spark = SparkSession.builder \
    .appName("Volume Anomaly Detection") \
    .config("spark.mongodb.input.uri", "mongodb://mongo:27017/finnhub.quotes") \
    .config("spark.es.nodes", "elasticsearch") \
    .config("spark.es.port", "9200") \
    .config("spark.es.nodes.wan.only", "true") \
    .getOrCreate()

# 2. Đọc dữ liệu từ MongoDB
df = spark.read.format("mongo").load()

df = df.drop("_id")

# 3. Ép timestamp thành TimestampType
df = df.withColumn("timestamp", col("timestamp").cast(TimestampType()))

# 4. Kiểm tra các cột quan trọng có tồn tại không
required_cols = ["volume", "price", "prev_close"]
missing = [x for x in required_cols if x not in df.columns]
if missing:
    print(f"❗ Thiếu cột: {missing}")
    spark.stop()
    exit()

# 5. Cửa sổ theo thời gian để tính trung bình volume
windowSpec = Window.partitionBy("symbol").orderBy("timestamp")
df = df.withColumn("avg_volume", avg("volume").over(windowSpec.rowsBetween(-5, -1)))

# 6. Gắn cờ bất thường
df = df.withColumn("volume_anomaly", col("volume") > col("avg_volume") * 2)
df = df.withColumn("change_percent", ((col("price") - col("prev_close")) / col("prev_close")) * 100)
df = df.withColumn("is_limit_up", col("change_percent") >= 7)
df = df.withColumn("is_limit_down", col("change_percent") <= -7)

# 7. Ghi toàn bộ dữ liệu vào Elasticsearch
df.write.format("org.elasticsearch.spark.sql") \
    .option("es.resource", "volume-anomalies/_doc") \
    .mode("overwrite") \
    .save() 

print("✅ Ghi dữ liệu vào Elasticsearch thành công")
