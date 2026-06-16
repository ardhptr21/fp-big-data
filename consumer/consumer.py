#!/usr/bin/env python3
"""
Kafka Consumer for Slum Analytics System.
Polls survey-events and secondary-batch topics,
writes to Bronze Delta Lake on HDFS, then triggers processing via API.
"""

import os
import json
import time
import uuid
import logging
import requests
from datetime import datetime

from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    FloatType, BooleanType, TimestampType
)
from pyspark.sql.functions import lit, current_timestamp
from delta import configure_spark_with_delta_pip

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

# ============================================================
# ENVIRONMENT CONFIG
# ============================================================
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC_SURVEY = os.environ.get("KAFKA_TOPIC_SURVEY", "survey-events")
TOPIC_SECONDARY = os.environ.get("KAFKA_TOPIC_SECONDARY", "secondary-batch")
HDFS_URL = os.environ.get("HDFS_URL", "hdfs://namenode:9000")
API_URL = os.environ.get("API_URL", "http://api:8000")
SPARK_MASTER = os.environ.get("SPARK_MASTER", "spark://spark:7077")

BRONZE_SURVEY_PATH = f"{HDFS_URL}/data/bronze/survey_events"
BRONZE_SECONDARY_PATH = f"{HDFS_URL}/data/bronze/secondary_sources"

# ============================================================
# SCHEMAS
# ============================================================
SURVEY_SCHEMA = StructType([
    StructField("event_id", StringType(), False),
    StructField("id_wilayah", StringType(), False),
    StructField("recorded_at", TimestampType(), True),
    StructField("recorded_by", StringType(), True),
    StructField("skor_bangunan", IntegerType(), True),
    StructField("skor_jalan", IntegerType(), True),
    StructField("skor_drainase", IntegerType(), True),
    StructField("skor_air_limbah", IntegerType(), True),
    StructField("skor_sampah", IntegerType(), True),
    StructField("skor_kebakaran", IntegerType(), True),
    StructField("skor_air_minum", IntegerType(), True),
    StructField("jumlah_kk", IntegerType(), True),
    StructField("jumlah_jiwa", IntegerType(), True),
    StructField("pernah_banjir", BooleanType(), True),
    StructField("frekuensi_banjir", IntegerType(), True),
    StructField("sosek_dominan", StringType(), True),
    StructField("catatan", StringType(), True),
])

SECONDARY_SCHEMA = StructType([
    StructField("batch_id", StringType(), False),
    StructField("source_type", StringType(), True),
    StructField("id_wilayah", StringType(), True),
    StructField("kelurahan", StringType(), True),
    StructField("kecamatan", StringType(), True),
    StructField("label_kumuh", IntegerType(), True),
    StructField("sumber_label", StringType(), True),
    StructField("tanggal_label", StringType(), True),
    StructField("kepadatan_jiwa_per_km2", FloatType(), True),
    StructField("jumlah_kejadian_banjir", IntegerType(), True),
    StructField("pct_akses_air_bersih", FloatType(), True),
    StructField("tahun_data", IntegerType(), True),
    StructField("raw_data", StringType(), True),
    StructField("ingested_at", TimestampType(), True),
])


def create_spark_session():
    """Create PySpark session with Delta Lake support."""
    builder = (
        SparkSession.builder
        .appName("SlumConsumer")
        .master("local[*]")  # Local mode - consumer runs in-process
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.hadoop.fs.defaultFS", HDFS_URL)
        .config("spark.hadoop.dfs.client.use.datanode.hostname", "true")
        .config("spark.driver.memory", "1g")
        .config("spark.executor.memory", "1g")
        .config("spark.ui.enabled", "false")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


def write_survey_to_bronze(spark, data: dict):
    """Write a survey event to Bronze Delta Lake (append-only)."""
    from pyspark.sql import Row
    from datetime import timezone

    recorded_at = datetime.fromisoformat(data.get("recorded_at", datetime.utcnow().isoformat()))
    row = Row(
        event_id=data.get("event_id", str(uuid.uuid4())),
        id_wilayah=data["id_wilayah"],
        recorded_at=recorded_at,
        recorded_by=data.get("recorded_by", ""),
        skor_bangunan=int(data.get("skor_bangunan", 0)),
        skor_jalan=int(data.get("skor_jalan", 0)),
        skor_drainase=int(data.get("skor_drainase", 0)),
        skor_air_limbah=int(data.get("skor_air_limbah", 0)),
        skor_sampah=int(data.get("skor_sampah", 0)),
        skor_kebakaran=int(data.get("skor_kebakaran", 0)),
        skor_air_minum=int(data.get("skor_air_minum", 0)),
        jumlah_kk=int(data.get("jumlah_kk", 0)),
        jumlah_jiwa=int(data.get("jumlah_jiwa", 0)),
        pernah_banjir=bool(data.get("pernah_banjir", False)),
        frekuensi_banjir=int(data.get("frekuensi_banjir", 0)),
        sosek_dominan=str(data.get("sosek_dominan", "")),
        catatan=str(data.get("catatan", "")),
    )

    df = spark.createDataFrame([row], schema=SURVEY_SCHEMA)

    # APPEND ONLY — never overwrite existing data
    df.write \
        .format("delta") \
        .mode("append") \
        .option("mergeSchema", "true") \
        .save(BRONZE_SURVEY_PATH)

    log.info(f"Written survey event {row.event_id} for {row.id_wilayah} to Bronze")


def write_secondary_to_bronze(spark, data: dict):
    """Write secondary data batch to Bronze Delta Lake (append-only)."""
    from pyspark.sql import Row

    row = Row(
        batch_id=data.get("batch_id", str(uuid.uuid4())),
        source_type=data.get("source_type", "unknown"),
        id_wilayah=data.get("id_wilayah", ""),
        kelurahan=data.get("kelurahan", ""),
        kecamatan=data.get("kecamatan", ""),
        label_kumuh=int(data["label_kumuh"]) if "label_kumuh" in data else None,
        sumber_label=data.get("sumber_label", ""),
        tanggal_label=data.get("tanggal_label", ""),
        kepadatan_jiwa_per_km2=float(data["kepadatan_jiwa_per_km2"]) if "kepadatan_jiwa_per_km2" in data else None,
        jumlah_kejadian_banjir=int(data["jumlah_kejadian_banjir"]) if "jumlah_kejadian_banjir" in data else None,
        pct_akses_air_bersih=float(data["pct_akses_air_bersih"]) if "pct_akses_air_bersih" in data else None,
        tahun_data=int(data["tahun_data"]) if "tahun_data" in data else None,
        raw_data=json.dumps(data),
        ingested_at=datetime.utcnow(),
    )

    df = spark.createDataFrame([row], schema=SECONDARY_SCHEMA)
    df.write \
        .format("delta") \
        .mode("append") \
        .option("mergeSchema", "true") \
        .save(BRONZE_SECONDARY_PATH)

    log.info(f"Written secondary batch {row.batch_id} (type={row.source_type}) to Bronze")


def trigger_processing():
    """Notify API to trigger Spark processing jobs."""
    try:
        resp = requests.post(f"{API_URL}/api/internal/trigger-processing", timeout=5)
        if resp.status_code == 200:
            log.info("Processing triggered successfully")
        else:
            log.warning(f"Trigger returned {resp.status_code}: {resp.text[:200]}")
    except Exception as e:
        log.warning(f"Could not trigger processing: {e}")


def wait_for_kafka(max_retries=30, delay=10):
    """Wait until Kafka is reachable."""
    for i in range(max_retries):
        try:
            consumer = KafkaConsumer(
                bootstrap_servers=[KAFKA_BOOTSTRAP],
                request_timeout_ms=5000,
                consumer_timeout_ms=1000,
            )
            consumer.close()
            log.info("Kafka is ready!")
            return True
        except NoBrokersAvailable:
            log.info(f"Kafka not ready, retry {i+1}/{max_retries}...")
            time.sleep(delay)
        except Exception as e:
            log.info(f"Kafka check error: {e}, retry {i+1}/{max_retries}...")
            time.sleep(delay)
    return False


def main():
    log.info("=== Slum Analytics Kafka Consumer Starting ===")

    if not wait_for_kafka():
        log.error("Kafka not available after max retries. Exiting.")
        return

    log.info("Creating Spark session...")
    spark = None
    for attempt in range(5):
        try:
            spark = create_spark_session()
            log.info("Spark session created successfully")
            break
        except Exception as e:
            log.warning(f"Spark session attempt {attempt+1}/5 failed: {e}")
            time.sleep(15)

    if spark is None:
        log.error("Could not create Spark session. Exiting.")
        return

    log.info(f"Subscribing to topics: {TOPIC_SURVEY}, {TOPIC_SECONDARY}")
    consumer = KafkaConsumer(
        TOPIC_SURVEY,
        TOPIC_SECONDARY,
        bootstrap_servers=[KAFKA_BOOTSTRAP],
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="slum-consumer-group",
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        session_timeout_ms=30000,
        heartbeat_interval_ms=10000,
        request_timeout_ms=40000,
    )

    log.info("Consumer started, polling for messages...")
    pending_trigger = False
    last_trigger = time.time()

    for message in consumer:
        topic = message.topic
        data = message.value
        log.info(f"Received message from topic={topic}, partition={message.partition}, offset={message.offset}")

        try:
            if topic == TOPIC_SURVEY:
                write_survey_to_bronze(spark, data)
            elif topic == TOPIC_SECONDARY:
                write_secondary_to_bronze(spark, data)
            pending_trigger = True
        except Exception as e:
            log.error(f"Error processing message: {e}", exc_info=True)

        # Trigger processing at most every 10 seconds (batch)
        now = time.time()
        if pending_trigger and (now - last_trigger) >= 10:
            trigger_processing()
            last_trigger = now
            pending_trigger = False


if __name__ == "__main__":
    main()
