# Dựa trên image Spark chính thức từ Bitnami
FROM bitnami/spark:3.4.1

# Đặt thư mục làm việc
WORKDIR /app

# Copy file consumer.py vào container
COPY consumer.py .

# Mặc định chạy file consumer.py với spark-submit và kèm gói Spark Kafka
CMD ["/opt/bitnami/spark/bin/spark-submit", "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1", "/app/consumer.py"]