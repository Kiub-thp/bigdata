# Dùng base image đã có Java và Python sẵn (hoặc bạn có thể dùng python base image)
FROM python:3.10-slim

# Cài wget (nếu cần) và các thư viện cần thiết
RUN apt-get update && apt-get install -y wget && apt-get clean

# Đặt thư mục làm việc trong container
WORKDIR /app

# Copy file requirements và cài thư viện Python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy toàn bộ code producer vào container
COPY . .

# Mặc định chạy file producer.py
CMD ["python", "producer.py"]