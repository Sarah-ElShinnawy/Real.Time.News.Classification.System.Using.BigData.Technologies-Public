"""
preprocessing.py
================
Data Preprocessing Module for News Streaming Big Data Pipeline
Converted from Jupyter Notebook — Stage 1: Raw GDELT data cleaning & feature extraction

Folder target: services/consumer/preprocessing.py
              OR notebooks/preprocessing.py
"""

import re
import logging
from datetime import datetime
from typing import Optional

from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.feature import (
    RegexTokenizer,
    StopWordsRemover,
    HashingTF,
    IDF,
    VectorAssembler,
)
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, FloatType, LongType, TimestampType
)

# ─────────────────────────────────────────────────────────────────────────────
# Logging
# ─────────────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s"
)
logger = logging.getLogger("preprocessing")


# ─────────────────────────────────────────────────────────────────────────────
# GDELT GKG Schema (Global Knowledge Graph — the richest GDELT table)
# Only the columns we actually use are kept; the rest are dropped.
# ─────────────────────────────────────────────────────────────────────────────
GDELT_GKG_COLUMNS = [
    "GKGRECORDID",        # 0  unique record ID
    "DATE",               # 1  YYYYMMDDHHMMSS
    "SourceCommonName",   # 4  outlet name
    "DocumentIdentifier", # 5  article URL
    "Themes",             # 7  semicolon-separated themes
    "Locations",          # 9  semicolon-separated locations
    "Persons",            # 11 semicolon-separated persons
    "Organizations",      # 12 semicolon-separated orgs
    "Tone",               # 15 tone scores (comma-separated)
    "AllNames",           # 23 all named entities
]

# GDELT GKG has 27 tab-separated columns — we select by index below
GKG_TOTAL_COLS = 27


# ─────────────────────────────────────────────────────────────────────────────
# SparkSession factory
# ─────────────────────────────────────────────────────────────────────────────
def get_spark(app_name: str = "NewsPreprocessing",
              kafka_bootstrap: str = "kafka:9092") -> SparkSession:
    """
    Build (or retrieve existing) SparkSession with Kafka + NLP packages.
    Runs in local mode inside the container — avoids cluster resource contention.
    """
    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")                           # use all container CPUs
        # Kafka connector — must match Spark 3.5.x + Scala 2.12
        .config("spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")
        # Serialization
        .config("spark.serializer",
                "org.apache.spark.serializer.KryoSerializer")
        # Streaming
        .config("spark.sql.streaming.statefulOperator.checkCorrectness.enabled",
                "false")
        # Reduce shuffle partitions for small batches
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    logger.info("SparkSession created: %s", app_name)
    return spark


# ─────────────────────────────────────────────────────────────────────────────
# Stage 1 — Raw CSV → cleaned Spark DataFrame
# ─────────────────────────────────────────────────────────────────────────────
def load_gdelt_gkg_csv(spark: SparkSession, csv_path: str) -> DataFrame:
    """
    Read a GDELT GKG CSV file (tab-separated, no header) and return a
    DataFrame with human-readable column names.

    csv_path: local path or HDFS path, e.g.
              /data/raw/20260515234500.gkg.csv
              hdfs://namenode:8020/gdelt/raw/...
    """
    logger.info("Loading GDELT GKG CSV: %s", csv_path)

    raw = (
        spark.read
        .option("sep", "\t")
        .option("quote", "")          # GKG has no quoting
        .option("multiLine", "false")
        .csv(csv_path)
    )

    # GKG columns are positional — rename only the ones we need
    col_map = {
        "_c0":  "record_id",
        "_c1":  "raw_date",
        "_c4":  "source",
        "_c5":  "url",
        "_c7":  "themes",
        "_c9":  "locations",
        "_c11": "persons",
        "_c12": "organizations",
        "_c15": "tone_raw",
        "_c23": "all_names",
    }
    df = raw.select([F.col(c).alias(alias) for c, alias in col_map.items()])
    logger.info("Loaded %d rows", df.count())
    return df


# ─────────────────────────────────────────────────────────────────────────────
# Stage 2 — Field cleaning & type casting
# ─────────────────────────────────────────────────────────────────────────────
def clean_dataframe(df: DataFrame) -> DataFrame:
    """
    Apply field-level cleaning:
      • Parse GDELT date string → proper timestamp
      • Extract numeric tone score
      • Normalise text fields (lowercase, strip control chars)
      • Drop rows with null URL or date
    """
    logger.info("Cleaning dataframe …")

    df = (
        df
        # --- timestamp: YYYYMMDDHHMMSS → timestamp ---
        .withColumn(
            "event_time",
            F.to_timestamp(F.col("raw_date").cast("string"), "yyyyMMddHHmmss")
        )
        .drop("raw_date")

        # --- tone: first value before comma is the overall tone score ---
        .withColumn(
            "tone_score",
            F.split(F.col("tone_raw"), ",").getItem(0).cast(FloatType())
        )
        .drop("tone_raw")

        # --- normalise text lists (semicolons → pipe, strip whitespace) ---
        .withColumn("themes",        _clean_list_col("themes"))
        .withColumn("locations",     _clean_list_col("locations"))
        .withColumn("persons",       _clean_list_col("persons"))
        .withColumn("organizations", _clean_list_col("organizations"))
        .withColumn("all_names",     _clean_list_col("all_names"))

        # --- source domain only ---
        .withColumn(
            "source",
            F.regexp_extract(F.col("url"), r"https?://([^/]+)", 1)
        )

        # --- drop nulls ---
        .dropna(subset=["url", "event_time"])
        .dropDuplicates(["url"])
    )

    logger.info("Cleaning done. Schema:")
    df.printSchema()
    return df


def _clean_list_col(col_name: str):
    """Helper: replace semicolons with pipes, strip, lowercase."""
    return F.lower(
        F.regexp_replace(
            F.trim(F.col(col_name)),
            r";+", "|"
        )
    )


# ─────────────────────────────────────────────────────────────────────────────
# Stage 3 — Feature engineering (text → ML features)
# ─────────────────────────────────────────────────────────────────────────────
def build_text_column(df: DataFrame) -> DataFrame:
    """
    Concatenate themes + persons + organizations + all_names into a single
    'text' column that will be tokenised for TF-IDF.
    """
    df = df.withColumn(
        "text",
        F.concat_ws(
            " ",
            F.regexp_replace(F.col("themes"),        r"\|", " "),
            F.regexp_replace(F.col("persons"),        r"\|", " "),
            F.regexp_replace(F.col("organizations"),  r"\|", " "),
            F.regexp_replace(F.col("all_names"),      r"\|", " "),
        )
    )
    return df


def build_ml_pipeline(num_features: int = 10_000,
                       num_idf_features: int = 5_000) -> Pipeline:
    """
    Construct a Spark ML Pipeline:
      RegexTokenizer → StopWordsRemover → HashingTF → IDF

    Returns an *unfitted* Pipeline. Call .fit(training_df) to get a PipelineModel.
    """
    tokenizer = RegexTokenizer(
        inputCol="text",
        outputCol="tokens",
        pattern=r"\W+",
        minTokenLength=3,
    )
    remover = StopWordsRemover(
        inputCol="tokens",
        outputCol="filtered_tokens",
    )
    hashing_tf = HashingTF(
        inputCol="filtered_tokens",
        outputCol="raw_features",
        numFeatures=num_features,
    )
    idf = IDF(
        inputCol="raw_features",
        outputCol="features",
        minDocFreq=2,
    )

    pipeline = Pipeline(stages=[tokenizer, remover, hashing_tf, idf])
    logger.info("ML Pipeline built (TF-IDF, %d features)", num_idf_features)
    return pipeline


# ─────────────────────────────────────────────────────────────────────────────
# Stage 4 — Persist / load trained pipeline model
# ─────────────────────────────────────────────────────────────────────────────
MODEL_METADATA_PATH = "/data/models/metadata"
MODEL_STAGES_PATH   = "/data/models/stages"
MODEL_ROOT_PATH     = "/data/models"


def fit_and_save_pipeline(pipeline: Pipeline,
                           training_df: DataFrame,
                           path: str = MODEL_ROOT_PATH) -> PipelineModel:
    """
    Fit the pipeline on training_df and persist it to disk.
    Saves:
      {path}/metadata/  ← pipeline metadata (formerly your notebook)
      {path}/stages/    ← serialised stage models  (formerly your notebook)
    """
    logger.info("Fitting pipeline on %d rows …", training_df.count())
    model = pipeline.fit(training_df)
    model.write().overwrite().save(path)
    logger.info("Pipeline model saved to %s", path)
    return model


def load_pipeline_model(path: str = MODEL_ROOT_PATH) -> PipelineModel:
    """
    Load a previously fitted PipelineModel from disk.
    Reads from {path}/metadata/ and {path}/stages/ automatically.
    """
    logger.info("Loading pipeline model from %s …", path)
    model = PipelineModel.load(path)
    logger.info("Pipeline model loaded.")
    return model


# ─────────────────────────────────────────────────────────────────────────────
# Stage 5 — Transform new batch with loaded model
# ─────────────────────────────────────────────────────────────────────────────
def transform_batch(model: PipelineModel, df: DataFrame) -> DataFrame:
    """
    Apply a fitted PipelineModel to a new DataFrame.
    Returns a DataFrame with 'features' column added (ready for ML inference).
    """
    df = build_text_column(df)
    result = model.transform(df)
    logger.info("Transformed %d rows.", result.count())
    return result


# ─────────────────────────────────────────────────────────────────────────────
# Stage 6 — Tone-based label (used when no ground-truth labels available)
# ─────────────────────────────────────────────────────────────────────────────
def add_tone_label(df: DataFrame) -> DataFrame:
    """
    Derive a simple 3-class sentiment label from GDELT tone score:
      tone < -2  → 'negative'
      tone >  2  → 'positive'
      otherwise  → 'neutral'
    Useful for unsupervised / weakly supervised training.
    """
    df = df.withColumn(
        "sentiment_label",
        F.when(F.col("tone_score") < -2.0, "negative")
         .when(F.col("tone_score") >  2.0, "positive")
         .otherwise("neutral")
    )
    return df


# ─────────────────────────────────────────────────────────────────────────────
# Convenience: run full preprocessing on a CSV batch
# ─────────────────────────────────────────────────────────────────────────────
def preprocess_batch(spark: SparkSession,
                     csv_path: str,
                     model: Optional[PipelineModel] = None,
                     save_model_if_new: bool = True) -> DataFrame:
    """
    End-to-end preprocessing for a single GDELT GKG batch file.

    If `model` is None, a new pipeline is fitted on this batch and saved.
    If `model` is provided, it is used directly (streaming mode).

    Returns a transformed DataFrame with ML features.
    """
    raw_df    = load_gdelt_gkg_csv(spark, csv_path)
    clean_df  = clean_dataframe(raw_df)
    clean_df  = add_tone_label(clean_df)
    text_df   = build_text_column(clean_df)

    if model is None:
        pipeline = build_ml_pipeline()
        model    = fit_and_save_pipeline(pipeline, text_df)
    
    transformed = model.transform(text_df)
    return transformed


# ─────────────────────────────────────────────────────────────────────────────
# Main — standalone test
# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import sys
    csv_path = sys.argv[1] if len(sys.argv) > 1 else "/data/raw/sample.gkg.csv"

    spark = get_spark()
    result = preprocess_batch(spark, csv_path, model=None, save_model_if_new=True)
    result.select("url", "event_time", "tone_score", "sentiment_label", "source") \
          .show(20, truncate=80)
    spark.stop()