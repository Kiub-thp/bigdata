#Sử dụng image Spark
# FROM bitnami/spark:3.4.1

# USER root
# COPY run.sh /run.sh
# RUN chmod +x /run.sh
# USER 1001

# ENTRYPOINT ["/run.sh"]

FROM python:3.10-slim

ENV HOME=/root
WORKDIR /app

RUN apt-get update && apt-get install -y wget && apt-get clean

COPY . .

RUN chmod +x run.sh

CMD ["./run.sh"]
# WORKDIR /app

# # Cài Python lib nếu cần
# RUN pip install --no-cache-dir pymongo elasticsearch

# # Copy toàn bộ nội dung thư mục hiện tại vào container
# COPY . .
# RUN chmod +x /run.sh

# CMD ["/run.sh"]

# FROM bde2020/hadoop-base:2.0.0-hadoop3.2.1-java8

# MAINTAINER Ivan Ermilov <ivan.s.ermilov@gmail.com>

# COPY WordCount.jar /opt/hadoop/applications/WordCount.jar

# ENV JAR_FILEPATH="/opt/hadoop/applications/WordCount.jar"
# ENV CLASS_TO_RUN="WordCount"
# ENV PARAMS="/input /output"

# ADD run.sh /run.sh
# RUN chmod a+x /run.sh

# CMD ["/run.sh"]
