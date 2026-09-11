import os, io, json, time, zipfile, random, requests
import pandas as pd
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

KAFKA_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC         = "news_topic"
INTERVAL_SECS = 15 * 60
SEND_DELAY    = 1.5
GDELT_MASTER  = "http://data.gdeltproject.org/gdeltv2/lastupdate.txt"

def connect_kafka(retries=15, delay=5):
    for attempt in range(retries):
        try:
            p = KafkaProducer(
                bootstrap_servers=KAFKA_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                acks="all", retries=3,
            )
            print(f"[producer] Connected to Kafka")
            return p
        except NoBrokersAvailable:
            print(f"[producer] Kafka not ready, retrying ({attempt+1}/{retries})")
            time.sleep(delay)
    raise RuntimeError("Could not connect to Kafka")

def get_latest_gdelt_url():
    resp = requests.get(GDELT_MASTER, timeout=30)
    resp.raise_for_status()
    for line in resp.text.strip().split("\n"):
        if "gkg.csv" in line:
            url = line.strip().split(" ")[-1]
            print(f"[producer] Latest file: {url}")
            return url
    raise ValueError("GKG URL not found")

def download_and_parse(url):
    print(f"[producer] Downloading...")
    resp = requests.get(url, timeout=120)
    resp.raise_for_status()
    with zipfile.ZipFile(io.BytesIO(resp.content)) as z:
        csv_name = [n for n in z.namelist() if n.endswith(".csv")][0]
        with z.open(csv_name) as f:
            df = pd.read_csv(f, sep="\t", header=None, on_bad_lines="skip", dtype=str)
    col_map = {0:"event_date",4:"source_url",11:"themes",15:"locations",23:"persons",24:"organizations",25:"tone_raw"}
    available = {k:v for k,v in col_map.items() if k < len(df.columns)}
    df = df[list(available.keys())].rename(columns=available).fillna("")
    def parse_tone(t):
        try:
            parts = t.split(",")
            s = float(parts[0]) - abs(float(parts[1]))
            return round(s,3), "positive" if s>0 else ("negative" if s<0 else "neutral")
        except: return 0.0, "neutral"
    df["sentiment_score"], df["sentiment_label"] = zip(*df["tone_raw"].map(parse_tone))
    df = df.drop(columns=["tone_raw"], errors="ignore")
    for c in ["themes","locations","persons","organizations"]:
        if c in df.columns:
            df[c] = df[c].str.replace(";",",").str.lower().str.strip(",")
    print(f"[producer] Parsed {len(df)} articles")
    return df

def run():
    producer = connect_kafka()
    sent_urls, last_url = set(), None
    print(f"[producer] Starting GDELT live producer")
    while True:
        try:
            url = get_latest_gdelt_url()
            if url != last_url:
                df = download_and_parse(url)
                new = 0
                for _, row in df.iterrows():
                    u = row.get("source_url","")
                    if u in sent_urls: continue
                    msg = row.to_dict()
                    msg["event_ts"] = int(time.time()*1000)
                    producer.send(TOPIC, msg)
                    sent_urls.add(u)
                    new += 1
                    print(f"[producer] Sent: {u[:80]}")
                    time.sleep(SEND_DELAY)
                print(f"[producer] Batch done — {new} new articles")
                last_url = url
                if len(sent_urls) > 50000:
                    sent_urls = set(list(sent_urls)[-10000:])
            else:
                print("[producer] No new file yet, waiting...")
        except Exception as e:
            print(f"[producer] ERROR: {e}")
        print(f"[producer] Sleeping 15 min...")
        time.sleep(INTERVAL_SECS)

if __name__ == "__main__":
    run()