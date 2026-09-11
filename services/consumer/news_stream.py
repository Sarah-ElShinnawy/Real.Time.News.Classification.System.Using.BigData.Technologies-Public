"""
news_stream.py  (replaces / upgrades services/consumer/news_stream.py)
======================================================================
Spark Structured Streaming Consumer

Behaviour
─────────
• Reads from Kafka topic 'gdelt-raw'
• On the FIRST micro-batch:
    – collects it as a DataFrame
    – trains + saves the TF-IDF Pipeline (metadata/ + stages/)
    – applies the model to that batch → writes to Parquet
• On subsequent batches:
    – loads the saved model
    – transforms the batch → writes to Parquet
• Writes output to /data/hdfs/processed/ in Parquet format (append mode)
• Also prints summaries to the console for debugging

Run inside Spark container:
  spark-submit \
    --master spark://spark-master:7077 \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0 \
    /app/news_stream.py
"""

import logging
import os
from pathlib import Path

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, FloatType, BooleanType, TimestampType
)

# Local module (copied into container via Dockerfile / volume mount)
from preprocessing import (
    clean_dataframe,
    build_text_column,
    build_ml_pipeline,
    fit_and_save_pipeline,
    load_pipeline_model,
    add_tone_label,
    get_spark,
    MODEL_ROOT_PATH,
)

# Defined locally to avoid import mismatch
MODEL_FLAG_FILE = "/data/models/.trained"

# ─────────────────────────────────────────────────────────────────────────────
# Config
# ─────────────────────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP  = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC_RAW  = os.getenv("KAFKA_TOPIC_RAW",         "gdelt-raw")
OUTPUT_PATH      = os.getenv("OUTPUT_PATH",              "/data/hdfs/processed")
CHECKPOINT_PATH  = os.getenv("CHECKPOINT_PATH",         "/data/checkpoints/stream")
TRIGGER_INTERVAL = os.getenv("TRIGGER_INTERVAL",         "60 seconds")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s"
)

# Defined locally to avoid import mismatch
MODEL_FLAG_FILE = "/data/models/.trained"
logger = logging.getLogger("news_stream")

Path(OUTPUT_PATH).mkdir(parents=True, exist_ok=True)
Path(CHECKPOINT_PATH).mkdir(parents=True, exist_ok=True)


# ─────────────────────────────────────────────────────────────────────────────
# Kafka JSON schema
# Must mirror what gdelt_producer.py sends
# ─────────────────────────────────────────────────────────────────────────────
GDELT_SCHEMA = StructType([
    StructField("record_id",      StringType(),  True),
    StructField("event_time",     StringType(),  True),   # ISO string → cast below
    StructField("source",         StringType(),  True),
    StructField("url",            StringType(),  True),
    StructField("themes",         StringType(),  True),
    StructField("locations",      StringType(),  True),
    StructField("persons",        StringType(),  True),
    StructField("organizations",  StringType(),  True),
    StructField("all_names",      StringType(),  True),
    StructField("tone_score",     FloatType(),   True),
    StructField("ingested_at",    StringType(),  True),
    StructField("batch_url",      StringType(),  True),
    StructField("is_first_batch", BooleanType(), True),
])


# ─────────────────────────────────────────────────────────────────────────────
# foreachBatch handler — called for every micro-batch
# ─────────────────────────────────────────────────────────────────────────────
# We hold the model in a module-level variable so it survives across batches
# without re-loading from disk every time.
_pipeline_model = None


def process_batch(batch_df: DataFrame, batch_id: int):
    """
    Called by Spark Structured Streaming for each micro-batch.
    """
    global _pipeline_model

    logger.info("─── Batch %d ───", batch_id)

    if batch_df.isEmpty():
        logger.info("Empty batch, skipping.")
        return

    # ── 1. Parse Kafka JSON value ──────────────────────────────────────────
    parsed = (
        batch_df
        .select(F.from_json(F.col("value").cast("string"), GDELT_SCHEMA).alias("d"))
        .select("d.*")
        .withColumn("event_time", F.to_timestamp("event_time"))
    )

    row_count = parsed.count()
    logger.info("Parsed %d rows in batch %d", row_count, batch_id)

    # ── 2. Light cleaning (already partially cleaned by producer) ──────────
    parsed = (
        parsed
        .dropna(subset=["url", "event_time"])
        .dropDuplicates(["url"])
    )
    parsed = add_tone_label(parsed)
    parsed = build_text_column(parsed)

    # ── 3. Model: load saved model or train fresh one ────────────────────
    if _pipeline_model is None:
        model_exists = Path(MODEL_ROOT_PATH).exists() and \
                       (Path(MODEL_ROOT_PATH) / "metadata").exists()

        if model_exists:
            try:
                logger.info("Loading existing pipeline model …")
                _pipeline_model = load_pipeline_model(MODEL_ROOT_PATH)
                logger.info("Model loaded successfully.")
            except Exception as e:
                logger.warning("Model load failed (%s) — retraining …", e)
                import shutil
                shutil.rmtree(MODEL_ROOT_PATH, ignore_errors=True)
                pipeline        = build_ml_pipeline()
                _pipeline_model = fit_and_save_pipeline(pipeline, parsed, MODEL_ROOT_PATH)
                logger.info("Model retrained and saved.")
        else:
            logger.info("No saved model — fitting new pipeline model …")
            pipeline        = build_ml_pipeline()
            _pipeline_model = fit_and_save_pipeline(pipeline, parsed, MODEL_ROOT_PATH)
            logger.info("Model trained and saved.")

    # ── 4. Transform ───────────────────────────────────────────────────────
    transformed = _pipeline_model.transform(parsed)

    # ── 5. Select output columns ───────────────────────────────────────────
    output = transformed.select(
        "record_id",
        "event_time",
        "source",
        "url",
        "themes",
        "persons",
        "organizations",
        "locations",
        "tone_score",
        "sentiment_label",
        "batch_url",
        F.lit(batch_id).alias("batch_id"),
    )
    # Note: 'features' (ML vector) excluded from Parquet — not human-readable

    # ── 6. Write to Parquet ────────────────────────────────────────────────
    (
        output
        .write
        .mode("append")
        .partitionBy("sentiment_label")
        .parquet(OUTPUT_PATH)
    )
    logger.info("Batch %d written to %s", batch_id, OUTPUT_PATH)

    # ── 7. Console preview ────────────────────────────────────────────────
    output.select("url", "event_time", "tone_score", "sentiment_label", "source") \
          .show(5, truncate=80)


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────
def main():
    spark = get_spark(app_name="GDELTNewsStream", kafka_bootstrap=KAFKA_BOOTSTRAP)

    logger.info("Reading from Kafka topic '%s' at %s", KAFKA_TOPIC_RAW, KAFKA_BOOTSTRAP)

    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe",               KAFKA_TOPIC_RAW)
        .option("startingOffsets",         "earliest")
        .option("failOnDataLoss",          "false")
        .option("maxOffsetsPerTrigger",    50_000)       # throttle per batch
        .load()
    )

    query = (
        raw_stream
        .writeStream
        .foreachBatch(process_batch)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .trigger(processingTime=TRIGGER_INTERVAL)
        .start()
    )

    logger.info("Streaming query started. Waiting for data …")
    query.awaitTermination()


if __name__ == "__main__":
    main()