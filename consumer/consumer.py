#!/usr/bin/env python3
"""
Kafka Consumer for Slum Analytics System.
Polls survey-events and secondary-batch topics,
writes to Bronze Delta Lake on HDFS, then triggers processing via API.
"""

import os
import glob
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
STORAGE_MODE = os.environ.get("STORAGE_MODE", "auto").lower()
LOCAL_LAKEHOUSE_DIR = os.environ.get("LOCAL_LAKEHOUSE_DIR", "/data/local_lakehouse")
API_URL = os.environ.get("API_URL", "http://api:8000")
SPARK_MASTER = os.environ.get("SPARK_MASTER", "spark://spark:7077")
SPARK_DRIVER_MEMORY = os.environ.get("SPARK_DRIVER_MEMORY", "512m")
SPARK_EXECUTOR_MEMORY = os.environ.get("SPARK_EXECUTOR_MEMORY", "512m")
SPARK_SHUFFLE_PARTITIONS = os.environ.get("SPARK_SHUFFLE_PARTITIONS", "2")
SPARK_SQL_FILES_MAX_PARTITION_BYTES = os.environ.get("SPARK_SQL_FILES_MAX_PARTITION_BYTES", "32m")
SPARK_ADVISORY_PARTITION_SIZE = os.environ.get("SPARK_ADVISORY_PARTITION_SIZE", "16m")
SPARK_DELTA_JARS = os.environ.get("SPARK_DELTA_JARS", "/opt/delta-jars/*")
try:
    HDFS_PROBE_RETRIES = max(1, int(os.environ.get("HDFS_PROBE_RETRIES", "12")))
except ValueError:
    HDFS_PROBE_RETRIES = 12
try:
    HDFS_PROBE_INTERVAL_SECONDS = max(0.1, float(os.environ.get("HDFS_PROBE_INTERVAL_SECONDS", "2.5")))
except ValueError:
    HDFS_PROBE_INTERVAL_SECONDS = 2.5

try:
    SPARK_LOCAL_THREADS = max(1, int(os.environ.get("SPARK_LOCAL_THREADS", "1")))
except ValueError:
    SPARK_LOCAL_THREADS = 1

SPARK_DEFAULT_PARALLELISM = os.environ.get("SPARK_DEFAULT_PARALLELISM", str(max(1, SPARK_LOCAL_THREADS)))

_lakehouse_root = None
_storage_backend = "unresolved"
BRONZE_SURVEY_PATH = None
BRONZE_SECONDARY_PATH = None

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


def resolve_delta_jars():
    jars = []
    for pattern in SPARK_DELTA_JARS.split(","):
        pattern = pattern.strip()
        if not pattern:
            continue
        matches = glob.glob(pattern)
        if matches:
            jars.extend(matches)
        elif os.path.exists(pattern):
            jars.append(pattern)
    return sorted(set(jars))


def configure_delta_builder(builder):
    jars = resolve_delta_jars()
    if jars:
        return builder.config("spark.jars", ",".join(jars))
    return configure_spark_with_delta_pip(builder)


def hdfs_accessible(spark) -> bool:
    if not HDFS_URL.startswith("hdfs://"):
        return False
    last_error = None
    for attempt in range(1, HDFS_PROBE_RETRIES + 1):
        try:
            jvm = spark._jvm
            conf = spark._jsc.hadoopConfiguration()
            uri = jvm.java.net.URI(HDFS_URL)
            fs = jvm.org.apache.hadoop.fs.FileSystem.get(uri, conf)
            if fs.exists(jvm.org.apache.hadoop.fs.Path("/")):
                return True
        except Exception as e:
            last_error = e
        if attempt < HDFS_PROBE_RETRIES:
            time.sleep(HDFS_PROBE_INTERVAL_SECONDS)
    log.warning("HDFS probe failed after retries, using local lakehouse fallback: %s", last_error)
    return False


def configure_lakehouse_paths(spark):
    global _lakehouse_root, _storage_backend, BRONZE_SURVEY_PATH, BRONZE_SECONDARY_PATH
    if _lakehouse_root:
        return

    local_root = os.path.abspath(LOCAL_LAKEHOUSE_DIR)
    local_marker = os.path.join(local_root, ".use_local_fallback")
    if STORAGE_MODE == "local":
        use_hdfs = False
    elif STORAGE_MODE == "hdfs":
        use_hdfs = True
    elif os.path.exists(local_marker):
        use_hdfs = False
    else:
        use_hdfs = hdfs_accessible(spark)

    if use_hdfs:
        _storage_backend = "hdfs"
        _lakehouse_root = f"{HDFS_URL.rstrip('/')}/data"
    else:
        _storage_backend = "local"
        os.makedirs(local_root, exist_ok=True)
        with open(local_marker, "a", encoding="utf-8"):
            pass
        _lakehouse_root = f"file://{local_root}"

    BRONZE_SURVEY_PATH = f"{_lakehouse_root.rstrip('/')}/bronze/survey_events"
    BRONZE_SECONDARY_PATH = f"{_lakehouse_root.rstrip('/')}/bronze/secondary_sources"
    log.info("Lakehouse storage backend=%s root=%s", _storage_backend, _lakehouse_root)


def create_spark_session():
    """Create PySpark session with Delta Lake support."""
    local_master = f"local[{SPARK_LOCAL_THREADS}]"
    builder = (
        SparkSession.builder
        .appName("SlumConsumer")
        .master(local_master)  # Local mode - consumer runs in-process
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
        .config("spark.hadoop.fs.defaultFS", HDFS_URL)
        .config("spark.hadoop.dfs.client.use.datanode.hostname", "true")
        .config("spark.driver.memory", SPARK_DRIVER_MEMORY)
        .config("spark.executor.memory", SPARK_EXECUTOR_MEMORY)
        .config("spark.driver.maxResultSize", "128m")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.kryoserializer.buffer.max", "128m")
        .config("spark.sql.shuffle.partitions", SPARK_SHUFFLE_PARTITIONS)
        .config("spark.default.parallelism", SPARK_DEFAULT_PARALLELISM)
        .config("spark.sql.files.maxPartitionBytes", SPARK_SQL_FILES_MAX_PARTITION_BYTES)
        .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", SPARK_ADVISORY_PARTITION_SIZE)
        .config("spark.python.worker.memory", "256m")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.adaptive.localShuffleReader.enabled", "true")
        .config("spark.locality.wait", "0")
        .config("spark.ui.enabled", "false")
    )
    spark = configure_delta_builder(builder).getOrCreate()
    configure_lakehouse_paths(spark)
    return spark


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


def trigger_processing(affected_id: str = "", train_model: bool = False, max_retries: int = 5):
    """Notify API to trigger Spark processing jobs."""
    payload = {"affected_ids": [affected_id]} if affected_id else {}
    if train_model:
        payload["train_model"] = True

    for attempt in range(1, max_retries + 1):
        try:
            resp = requests.post(f"{API_URL}/api/internal/trigger-processing", json=payload, timeout=10)
            if resp.status_code == 200:
                log.info("Processing triggered successfully")
                return True
            log.warning(f"Trigger attempt {attempt}/{max_retries} returned {resp.status_code}: {resp.text[:200]}")
        except Exception as e:
            log.warning(f"Trigger attempt {attempt}/{max_retries} failed: {e}")

        if attempt < max_retries:
            time.sleep(min(2 ** attempt, 15))

    log.error("Could not trigger processing after retries")
    return False


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
    for message in consumer:
        topic = message.topic
        data = message.value
        log.info(f"Received message from topic={topic}, partition={message.partition}, offset={message.offset}")

        try:
            affected_id = ""
            if topic == TOPIC_SURVEY:
                write_survey_to_bronze(spark, data)
                affected_id = data.get("id_wilayah", "")
            elif topic == TOPIC_SECONDARY:
                write_secondary_to_bronze(spark, data)
                affected_id = data.get("id_wilayah", "")
            trigger_processing(affected_id, train_model=(topic == TOPIC_SECONDARY))
        except Exception as e:
            log.error(f"Error processing message: {e}", exc_info=True)


if __name__ == "__main__":
    main()
