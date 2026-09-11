#!/bin/bash
set -e
echo "[consumer] Starting spark-submit..."
exec /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --jars /opt/spark/jars/spark-sql-kafka-0-10_2.12-3.5.3.jar,/opt/spark/jars/kafka-clients-3.4.1.jar,/opt/spark/jars/spark-token-provider-kafka-0-10_2.12-3.5.3.jar,/opt/spark/jars/commons-pool2-2.11.1.jar \
  /opt/spark/jobs/news_stream.py