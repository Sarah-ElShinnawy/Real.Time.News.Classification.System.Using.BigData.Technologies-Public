"""
gdelt_producer.py
=================
Real-Time GDELT News Producer for Kafka
Folder target: services/producer/gdelt_producer.py

What it does every 15 minutes (GDELT update interval):
  1. Fetch http://data.gdeltproject.org/gdeltv2/lastupdate.txt
  2. Download the .gkg.csv.zip (Global Knowledge Graph — richest table)
  3. Parse each row into a JSON news record
  4. Publish each record to Kafka topic 'gdelt-raw'
  5. On the FIRST batch, trigger preprocessing + model training via a flag file
"""

import csv
import gzip
import io
import json
import logging
import os
import sys
import time
import zipfile
from datetime import datetime
from pathlib import Path
from typing import Optional

import requests
from kafka import KafkaProducer
from kafka.errors import KafkaError

# ─────────────────────────────────────────────────────────────────────────────
# Config — override via env vars in docker-compose.yml
# ─────────────────────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP    = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC_RAW    = os.getenv("KAFKA_TOPIC_RAW",         "gdelt-raw")
KAFKA_TOPIC_STATUS = os.getenv("KAFKA_TOPIC_STATUS",      "gdelt-status")
GDELT_MASTER_URL   = "http://data.gdeltproject.org/gdeltv2/lastupdate.txt"
POLL_INTERVAL_SEC  = int(os.getenv("GDELT_POLL_INTERVAL", "900"))   # 15 min
RAW_DATA_DIR       = Path(os.getenv("RAW_DATA_DIR", "/data/raw"))
MODEL_FLAG_FILE    = Path(os.getenv("MODEL_FLAG_FILE", "/data/models/.trained"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s"
)
logger = logging.getLogger("gdelt_producer")

RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)


# ─────────────────────────────────────────────────────────────────────────────
# Kafka producer factory
# ─────────────────────────────────────────────────────────────────────────────
def make_kafka_producer() -> KafkaProducer:
    for attempt in range(10):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                acks="all",
                retries=5,
                batch_size=16_384,
                linger_ms=20,
                compression_type="gzip",
            )
            logger.info("Connected to Kafka at %s", KAFKA_BOOTSTRAP)
            return producer
        except KafkaError as e:
            logger.warning("Kafka not ready (attempt %d/10): %s", attempt + 1, e)
            time.sleep(10)
    raise RuntimeError("Could not connect to Kafka after 10 attempts.")


# ─────────────────────────────────────────────────────────────────────────────
# GDELT fetch helpers
# ─────────────────────────────────────────────────────────────────────────────
def fetch_latest_urls() -> dict:
    """
    Parse http://data.gdeltproject.org/gdeltv2/lastupdate.txt
    Returns: { 'export': url, 'mentions': url, 'gkg': url }
    """
    resp = requests.get(GDELT_MASTER_URL, timeout=30)
    resp.raise_for_status()
    urls = {}
    for line in resp.text.strip().splitlines():
        parts = line.split()
        if len(parts) == 3:
            url = parts[2]
            if ".export." in url:
                urls["export"] = url
            elif ".mentions." in url:
                urls["mentions"] = url
            elif ".gkg." in url:
                urls["gkg"] = url
    logger.info("Latest GDELT URLs: %s", urls)
    return urls


def download_and_extract_gkg(zip_url: str) -> Optional[bytes]:
    """
    Download a GDELT .gkg.csv.zip and return the raw CSV bytes.
    Also caches the file in RAW_DATA_DIR.
    """
    filename = zip_url.split("/")[-1].replace(".zip", "")
    cache_path = RAW_DATA_DIR / filename

    if cache_path.exists():
        logger.info("Cache hit: %s", cache_path)
        return cache_path.read_bytes()

    logger.info("Downloading %s …", zip_url)
    resp = requests.get(zip_url, timeout=120, stream=True)
    resp.raise_for_status()

    zip_bytes = b"".join(resp.iter_content(chunk_size=65_536))
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        csv_name = [n for n in zf.namelist() if n.endswith(".csv")][0]
        csv_bytes = zf.read(csv_name)

    cache_path.write_bytes(csv_bytes)
    logger.info("Saved %d bytes to %s", len(csv_bytes), cache_path)
    return csv_bytes


# ─────────────────────────────────────────────────────────────────────────────
# GKG row parser
# ─────────────────────────────────────────────────────────────────────────────
def parse_gkg_row(row: list) -> Optional[dict]:
    """
    Convert a raw GKG tab-separated row (list of strings) into a clean dict.
    Returns None if the row is malformed.
    """
    try:
        # GKG column indices (0-based)
        record_id = row[0]
        raw_date  = row[1]          # YYYYMMDDHHMMSS
        source    = row[4] if len(row) > 4  else ""
        url       = row[5] if len(row) > 5  else ""
        themes    = row[7] if len(row) > 7  else ""
        locations = row[9] if len(row) > 9  else ""
        persons   = row[11] if len(row) > 11 else ""
        orgs      = row[12] if len(row) > 12 else ""
        tone_raw  = row[15] if len(row) > 15 else ""
        all_names = row[23] if len(row) > 23 else ""

        if not url or not raw_date:
            return None

        # Parse timestamp
        try:
            event_time = datetime.strptime(raw_date[:14], "%Y%m%d%H%M%S").isoformat()
        except ValueError:
            event_time = None

        # Parse tone (first value = overall tone)
        tone_score = None
        if tone_raw:
            try:
                tone_score = float(tone_raw.split(",")[0])
            except (ValueError, IndexError):
                pass

        return {
            "record_id":     record_id,
            "event_time":    event_time,
            "source":        source,
            "url":           url,
            "themes":        themes.replace(";", "|"),
            "locations":     locations.replace(";", "|"),
            "persons":       persons.replace(";", "|"),
            "organizations": orgs.replace(";", "|"),
            "all_names":     all_names.replace(";", "|"),
            "tone_score":    tone_score,
            "ingested_at":   datetime.utcnow().isoformat(),
            "batch_url":     "",   # filled in by caller
        }
    except Exception as e:
        logger.debug("Row parse error: %s", e)
        return None


# ─────────────────────────────────────────────────────────────────────────────
# Publish a batch to Kafka
# ─────────────────────────────────────────────────────────────────────────────
def publish_batch(producer: KafkaProducer,
                  csv_bytes: bytes,
                  batch_url: str,
                  is_first_batch: bool) -> int:
    """
    Parse GKG CSV bytes and send each row as a Kafka message.
    Returns the number of messages sent.
    """
    sent = 0
    skipped = 0

    reader = csv.reader(
        io.StringIO(csv_bytes.decode("utf-8", errors="replace")),
        delimiter="\t",
        quoting=csv.QUOTE_NONE,
    )

    for row in reader:
        record = parse_gkg_row(row)
        if record is None:
            skipped += 1
            continue

        record["batch_url"]     = batch_url
        record["is_first_batch"] = is_first_batch

        try:
            producer.send(
                KAFKA_TOPIC_RAW,
                key=record["url"],
                value=record,
            )
            sent += 1
        except KafkaError as e:
            logger.error("Send error: %s", e)

    producer.flush()
    logger.info("Published %d records (%d skipped) from %s",
                sent, skipped, batch_url)
    return sent


def publish_status(producer: KafkaProducer, status: dict):
    """Send a status/control message to the status topic."""
    try:
        producer.send(KAFKA_TOPIC_STATUS, key="status", value=status)
        producer.flush()
    except Exception:
        pass


# ─────────────────────────────────────────────────────────────────────────────
# First-batch flag — signals the consumer to train the model
# ─────────────────────────────────────────────────────────────────────────────
def is_first_batch() -> bool:
    return not MODEL_FLAG_FILE.exists()


def mark_model_trained():
    MODEL_FLAG_FILE.parent.mkdir(parents=True, exist_ok=True)
    MODEL_FLAG_FILE.write_text(datetime.utcnow().isoformat())
    logger.info("Model flag written: %s", MODEL_FLAG_FILE)


# ─────────────────────────────────────────────────────────────────────────────
# Main loop
# ─────────────────────────────────────────────────────────────────────────────
def run():
    logger.info("GDELT Producer starting. Poll interval: %d s", POLL_INTERVAL_SEC)
    producer       = make_kafka_producer()
    last_batch_url = None
    first_batch    = is_first_batch()

    while True:
        try:
            urls      = fetch_latest_urls()
            gkg_url   = urls.get("gkg")

            if not gkg_url:
                logger.warning("No GKG URL found in lastupdate.txt")
            elif gkg_url == last_batch_url:
                logger.info("No new batch yet (same URL). Waiting …")
            else:
                csv_bytes = download_and_extract_gkg(gkg_url)

                if csv_bytes:
                    count = publish_batch(
                        producer, csv_bytes, gkg_url,
                        is_first_batch=first_batch
                    )

                    publish_status(producer, {
                        "event":        "batch_published",
                        "batch_url":    gkg_url,
                        "record_count": count,
                        "is_first":     first_batch,
                        "timestamp":    datetime.utcnow().isoformat(),
                    })

                    if first_batch:
                        mark_model_trained()
                        first_batch = False
                        logger.info("First batch complete — model training will be triggered.")

                    last_batch_url = gkg_url

        except requests.RequestException as e:
            logger.error("HTTP error fetching GDELT: %s", e)
        except Exception as e:
            logger.exception("Unexpected error: %s", e)

        logger.info("Sleeping %d seconds until next poll …", POLL_INTERVAL_SEC)
        time.sleep(POLL_INTERVAL_SEC)


if __name__ == "__main__":
    run()