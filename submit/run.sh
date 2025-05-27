#!/bin/bash

#$HADOOP_HOME/bin/hadoop jar $JAR_FILEPATH $CLASS_TO_RUN $PARAMS
# /opt/bitnami/spark/bin/spark-submit \
#   --master spark://spark-master:7077 \
#   --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,org.mongodb.spark:mongo-spark-connector_2.12:10.2.1 \
#   /app/abnormal_volume_analysis.py
# /opt/bitnami/spark/bin/spark-submit \
#   --master spark://spark:7077 \
#   --packages org.mongodb.spark:mongo-spark-connector_2.12:10.2.0,org.elasticsearch:elasticsearch-spark-30_2.12:8.11.0 \
#   /app/abnormal_volume_analysis.py
/opt/bitnami/spark/bin/spark-submit \
  --master spark://spark:7077 \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  --packages org.mongodb.spark:mongo-spark-connector_2.12:3.0.1,org.elasticsearch:elasticsearch-spark-30_2.12:8.11.0 \
  /app/abnormal_volume_analysis.py