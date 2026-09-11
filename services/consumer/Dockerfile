# services/consumer/Dockerfile
FROM apache/spark:3.5.6-python3

USER root

# Python deps for preprocessing
RUN pip install --no-cache-dir \
    pyspark==3.5.0 \
    numpy \
    scipy

# Copy both scripts into the container
COPY preprocessing.py /app/preprocessing.py
COPY news_stream.py   /app/news_stream.py

WORKDIR /app

CMD ["spark-submit", \
     "--master", "local[*]", \
     "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0", \
     "--conf", "spark.sql.shuffle.partitions=4", \
     "/app/news_stream.py"]
