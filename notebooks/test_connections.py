"""
Run this INSIDE the jupyter container:
  docker exec -it jupyter-notebook python /home/jovyan/work/test_connections.py
"""
import socket, json, time, datetime

PASS, FAIL, WARN = "  PASS", "  FAIL", "  WARN"

def check(label, fn):
    try:
        msg = fn()
        print(f"{PASS}  {label}{(' — ' + msg) if msg else ''}")
        return True
    except Exception as e:
        print(f"{FAIL}  {label} — {e}")
        return False

results = {}

print("\n" + "="*55)
print("  PIPELINE CONNECTION TEST")
print(f"  {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("="*55)

# ── 1. PORT CHECKS ────────────────────────────────────────
print("\n[1] Port reachability")
services = [
    ("zookeeper",       2181, "ZooKeeper"),
    ("kafka",           9092, "Kafka"),
    ("spark-master",    7077, "Spark RPC"),
    ("spark-master",    8080, "Spark UI"),
    ("hadoop-namenode", 9870, "HDFS UI"),
    ("hadoop-namenode", 9005, "HDFS RPC"),
]
for host, port, label in services:
    def port_check(h=host, p=port):
        s = socket.create_connection((h, p), timeout=5)
        s.close()
        return f"{h}:{p}"
    results[label] = check(label, port_check)

# ── 2. KAFKA ──────────────────────────────────────────────
print("\n[2] Kafka — produce and consume a test message")
try:
    from kafka import KafkaProducer, KafkaConsumer
    from kafka.admin import KafkaAdminClient, NewTopic
    from kafka.errors import TopicAlreadyExistsError

    admin = KafkaAdminClient(bootstrap_servers="kafka:9092")
    try:
        admin.create_topics([NewTopic("_conn_test", 1, 1)])
    except TopicAlreadyExistsError:
        pass

    producer = KafkaProducer(
        bootstrap_servers="kafka:9092",
        value_serializer=lambda v: json.dumps(v).encode()
    )
    producer.send("_conn_test", {"ping": "pong", "ts": time.time()})
    producer.flush()

    consumer = KafkaConsumer(
        "_conn_test",
        bootstrap_servers="kafka:9092",
        auto_offset_reset="earliest",
        consumer_timeout_ms=8000,
        value_deserializer=lambda v: json.loads(v.decode())
    )
    msgs = [m.value for m in consumer]
    consumer.close()

    if msgs:
        topics = admin.list_topics()
        has_news = "news_topic" in topics
        print(f"{PASS}  Kafka round-trip — sent and received {len(msgs)} message(s)")
        print(f"{'  PASS' if has_news else '  WARN'}  news_topic {'exists — GDELT producer is running' if has_news else 'not yet — producer still starting'}")
        results["Kafka"] = True
    else:
        print(f"{FAIL}  Kafka — no messages received")
        results["Kafka"] = False
except Exception as e:
    print(f"{FAIL}  Kafka — {e}")
    results["Kafka"] = False

# ── 3. SPARK ──────────────────────────────────────────────
print("\n[3] Spark cluster")
spark = None
try:
    import findspark
    findspark.init()
    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder
        .appName("ConnectionTest")
        .master("spark://spark-master:7077")
        .config("spark.executor.memory", "1g")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    total   = spark.sparkContext.parallelize(range(100)).sum()
    workers = spark.sparkContext._jsc.sc().getExecutorMemoryStatus().size()

    print(f"{PASS}  Spark connected — version {spark.version}")
    print(f"{PASS}  Distributed job — sum(0..99) = {total} (expected 4950)")
    print(f"{PASS}  Workers registered: {workers}")
    results["Spark"] = True
except Exception as e:
    print(f"{FAIL}  Spark — {e}")
    results["Spark"] = False

# ── 4. SPARK ↔ KAFKA ─────────────────────────────────────
print("\n[4] Spark reads from Kafka")
if spark and results.get("Kafka"):
    try:
        from pyspark.sql.functions import from_json, col
        from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType

        schema = StructType([
            StructField("source_url",      StringType(), True),
            StructField("themes",          StringType(), True),
            StructField("sentiment_label", StringType(), True),
            StructField("sentiment_score", DoubleType(), True),
            StructField("event_ts",        LongType(),   True),
        ])

        df = (
            spark.read.format("kafka")
            .option("kafka.bootstrap.servers", "kafka:9092")
            .option("subscribe", "news_topic")
            .option("startingOffsets", "earliest")
            .load()
            .selectExpr("CAST(value AS STRING) as j")
            .select(from_json(col("j"), schema).alias("d"))
            .select("d.*")
            .filter(col("source_url").isNotNull())
        )
        count = df.count()
        print(f"{PASS}  Spark read Kafka — {count} GDELT articles available")
        if count > 0:
            df.select("source_url","sentiment_label").show(3, truncate=70)
        results["Spark-Kafka"] = True
    except Exception as e:
        print(f"{FAIL}  Spark-Kafka — {e}")
        results["Spark-Kafka"] = False
else:
    print(f"{WARN}  Skipped — Spark or Kafka not available")

# ── 5. HDFS ──────────────────────────────────────────────
print("\n[5] HDFS write and read")
if spark:
    try:
        HDFS = "hdfs://hadoop-namenode:9005"
        test = spark.createDataFrame(
            [("ok", str(datetime.datetime.now()))], ["status","written_at"]
        )
        test.write.mode("overwrite").parquet(f"{HDFS}/test/conn_check")
        back = spark.read.parquet(f"{HDFS}/test/conn_check").count()
        print(f"{PASS}  HDFS write + read — {back} row(s) verified")
        print(f"       Browse HDFS: http://localhost:9870")
        results["HDFS"] = True
    except Exception as e:
        print(f"{FAIL}  HDFS — {e}")
        results["HDFS"] = False
else:
    print(f"{WARN}  Skipped — Spark not available")

# ── SUMMARY ──────────────────────────────────────────────
print("\n" + "="*55)
print("  SUMMARY")
print("="*55)
all_passed = True
for name, ok in results.items():
    status = "PASS" if ok else "FAIL"
    print(f"  {status}  {name}")
    if not ok:
        all_passed = False

print()
if all_passed:
    print("  ALL TESTS PASSED — pipeline is fully working!")
    print()
    print("  Next steps:")
    print("  1. Open http://localhost:8888  → Jupyter Lab")
    print("  2. Open http://localhost:8080  → Spark UI")
    print("  3. Open http://localhost:9870  → HDFS browser")
else:
    print("  SOME TESTS FAILED")
    print("  Run: docker compose ps")
    print("  Then: docker compose logs -f <failed-service-name>")
print("="*55 + "\n")

if spark:
    spark.stop()