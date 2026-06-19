#!/usr/bin/env python3
"""
Spark Job 1: Bronze -> Silver
Transforms raw Bronze events into:
- silver/latest_indicators  : latest snapshot per wilayah
- silver/event_history      : full history (all events, enriched)
- silver/feature_matrix     : aggregated features per kelurahan for ML
"""

import glob
import os
import time
from typing import Iterable, List

from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import FloatType

HDFS_URL = os.environ.get("HDFS_URL", "hdfs://namenode:9000")
STORAGE_MODE = os.environ.get("STORAGE_MODE", "auto").lower()
LOCAL_LAKEHOUSE_DIR = os.environ.get("LOCAL_LAKEHOUSE_DIR", "/data/local_lakehouse")
try:
    SPARK_LOCAL_THREADS = max(1, int(os.environ.get("SPARK_LOCAL_THREADS", "1")))
except ValueError:
    SPARK_LOCAL_THREADS = 1

# Use bounded local mode for in-process execution on lower-memory machines.
SPARK_MASTER = os.environ.get("SPARK_MASTER", f"local[{SPARK_LOCAL_THREADS}]")
if SPARK_MASTER.startswith("spark://"):
    SPARK_MASTER = f"local[{SPARK_LOCAL_THREADS}]"

SPARK_DRIVER_MEMORY = os.environ.get("SPARK_DRIVER_MEMORY", "512m")
SPARK_EXECUTOR_MEMORY = os.environ.get("SPARK_EXECUTOR_MEMORY", "512m")
SPARK_SHUFFLE_PARTITIONS = os.environ.get("SPARK_SHUFFLE_PARTITIONS", "2")
SPARK_DEFAULT_PARALLELISM = os.environ.get("SPARK_DEFAULT_PARALLELISM", str(max(1, SPARK_LOCAL_THREADS)))
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

LAKEHOUSE_ROOT = None
STORAGE_BACKEND = "unresolved"

BRONZE_SURVEY = None
BRONZE_MASTER = None
BRONZE_SECONDARY = None

SILVER_LATEST = None
SILVER_HISTORY = None
SILVER_FEATURE = None
SILVER_GROUND_TRUTH = None
SILVER_POPULATION = None

INDICATORS = [
    "skor_bangunan", "skor_jalan", "skor_drainase",
    "skor_air_limbah", "skor_sampah", "skor_kebakaran", "skor_air_minum",
]


def parse_affected_ids() -> List[str]:
    raw = os.environ.get("AFFECTED_WILAYAH_IDS", "")
    return [part.strip() for part in raw.split(",") if part.strip()]


def truthy_env(name: str) -> bool:
    return os.environ.get(name, "").lower() in {"1", "true", "yes"}


def resolve_delta_jars() -> List[str]:
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


def configure_delta(builder):
    jars = resolve_delta_jars()
    if jars:
        return builder.config("spark.jars", ",".join(jars))

    # Fallback for local development where baked jars are not available.
    from delta import configure_spark_with_delta_pip
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
    print(f"HDFS probe failed after retries, using local lakehouse fallback: {last_error}")
    return False


def configure_lakehouse_paths(spark):
    global LAKEHOUSE_ROOT, STORAGE_BACKEND
    global BRONZE_SURVEY, BRONZE_MASTER, BRONZE_SECONDARY
    global SILVER_LATEST, SILVER_HISTORY, SILVER_FEATURE, SILVER_GROUND_TRUTH, SILVER_POPULATION

    if LAKEHOUSE_ROOT:
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
        STORAGE_BACKEND = "hdfs"
        LAKEHOUSE_ROOT = f"{HDFS_URL.rstrip('/')}/data"
    else:
        STORAGE_BACKEND = "local"
        os.makedirs(local_root, exist_ok=True)
        with open(local_marker, "a", encoding="utf-8"):
            pass
        LAKEHOUSE_ROOT = f"file://{local_root}"

    root = LAKEHOUSE_ROOT.rstrip("/")
    BRONZE_SURVEY = f"{root}/bronze/survey_events"
    BRONZE_MASTER = f"{root}/bronze/master_wilayah"
    BRONZE_SECONDARY = f"{root}/bronze/secondary_sources"
    SILVER_LATEST = f"{root}/silver/latest_indicators"
    SILVER_HISTORY = f"{root}/silver/event_history"
    SILVER_FEATURE = f"{root}/silver/feature_matrix"
    SILVER_GROUND_TRUTH = f"{root}/silver/ground_truth"
    SILVER_POPULATION = f"{root}/silver/population"
    print(f"Lakehouse storage backend={STORAGE_BACKEND} root={LAKEHOUSE_ROOT}")


def create_spark():
    builder = (
        SparkSession.builder
        .appName("SlumJob1-BronzeSilver")
        .master(SPARK_MASTER)
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
    spark = configure_delta(builder).getOrCreate()
    configure_lakehouse_paths(spark)
    return spark


def delta_exists(spark, path: str) -> bool:
    try:
        return DeltaTable.isDeltaTable(spark, path)
    except Exception:
        try:
            spark.read.format("delta").load(path).limit(1).collect()
            return True
        except Exception:
            return False


def df_is_empty(df) -> bool:
    return len(df.take(1)) == 0


def write_delta(df, path: str, partition_cols: Iterable[str] = ()):
    writer = (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
    )
    cols = [c for c in partition_cols if c in df.columns]
    if cols:
        writer = writer.partitionBy(*cols)
    writer.save(path)


def align_to_target_schema(spark, df, path: str):
    try:
        target_schema = spark.read.format("delta").load(path).schema
    except Exception:
        return df

    for field in target_schema:
        if field.name not in df.columns:
            df = df.withColumn(field.name, F.lit(None).cast(field.dataType))
    return df


def merge_delta(
    spark,
    df,
    path: str,
    condition: str,
    partition_cols: Iterable[str] = (),
    update_matches: bool = True,
):
    if df_is_empty(df):
        print(f"No rows to merge into {path}")
        return

    table_exists = delta_exists(spark, path)
    if not table_exists:
        print(f"Creating Delta table at {path}")
        write_delta(df, path, partition_cols)
        return

    df = align_to_target_schema(spark, df, path)
    merge = DeltaTable.forPath(spark, path).alias("t").merge(df.alias("s"), condition)
    if update_matches:
        merge = merge.whenMatchedUpdateAll()
    merge.whenNotMatchedInsertAll().execute()


def compute_risk_score(df):
    """Compute risk score 0-100 from 7 PUPR indicators."""
    present = [c for c in INDICATORS if c in df.columns]
    if not present:
        return df.withColumn("risk_score", F.lit(0.0))

    sum_expr = sum(F.coalesce(F.col(c).cast(FloatType()), F.lit(0.0)) for c in present)
    max_possible = 3.0 * len(present)
    return df.withColumn("risk_score", (sum_expr / max_possible * 100).cast(FloatType()))


def risk_level(df):
    """Add risk_level column from risk_score."""
    return df.withColumn(
        "risk_level",
        F.when(F.col("risk_score") < 25, "Ringan")
         .when(F.col("risk_score") < 50, "Sedang")
         .when(F.col("risk_score") < 75, "Berat")
         .otherwise("Sangat Berat")
    )


def enrich_survey(survey_df, master_df):
    if master_df is None:
        return survey_df.withColumn("kecamatan", F.lit("")).withColumn("kelurahan", F.lit(""))

    master_slim = master_df.select(
        "id_wilayah", "kecamatan", "kelurahan", "rw", "rt",
        "total_jiwa", "total_kk", "luas_m2", "geometry_wkt"
    )
    return survey_df.join(master_slim, on="id_wilayah", how="left")


def latest_per_wilayah(enriched):
    window_spec = Window.partitionBy("id_wilayah").orderBy(F.col("recorded_at").desc())
    latest_df = (
        enriched.withColumn("_rn", F.row_number().over(window_spec))
        .filter(F.col("_rn") == 1)
        .drop("_rn", "risk_score_saat_itu", "risk_level_saat_itu")
    )
    return risk_level(compute_risk_score(latest_df))


def build_feature_matrix(latest_df):
    agg_exprs = [F.avg(c).alias(f"avg_{c}") for c in INDICATORS if c in latest_df.columns]
    agg_exprs += [
        F.sum("jumlah_jiwa").alias("total_jiwa_kelurahan"),
        F.avg("frekuensi_banjir").alias("avg_frekuensi_banjir"),
        F.count("id_wilayah").alias("total_rt_surveyed"),
        F.avg("risk_score").alias("avg_risk_score"),
    ]

    return (
        latest_df.groupBy("kelurahan", "kecamatan")
        .agg(*agg_exprs)
        .withColumn("id_kelurahan", F.col("kelurahan"))
        .withColumn("updated_at", F.current_timestamp())
    )


def process_ground_truth(spark, incremental: bool):
    try:
        secondary_df = spark.read.format("delta").load(BRONZE_SECONDARY)
    except Exception as e:
        print(f"No secondary data yet: {e}")
        return

    gt_df = secondary_df.filter(F.col("source_type") == "ground_truth")
    if df_is_empty(gt_df):
        return

    if "ingested_at" in gt_df.columns:
        window_gt = Window.partitionBy("id_wilayah").orderBy(F.col("ingested_at").desc_nulls_last())
        gt_df = gt_df.withColumn("_rn", F.row_number().over(window_gt)).filter(F.col("_rn") == 1).drop("_rn")
    else:
        gt_df = gt_df.dropDuplicates(["id_wilayah"])

    if incremental:
        print("Merging silver/ground_truth...")
        merge_delta(spark, gt_df, SILVER_GROUND_TRUTH, "t.id_wilayah = s.id_wilayah")
    else:
        print("Writing silver/ground_truth...")
        write_delta(gt_df, SILVER_GROUND_TRUTH)


def process_population(master_df):
    if master_df is None:
        return None
    return (
        master_df.groupBy("kelurahan", "kecamatan")
        .agg(
            F.sum("total_jiwa").alias("total_jiwa"),
            F.sum("total_kk").alias("total_kk"),
            F.count("id_wilayah").alias("total_rt"),
        )
    )


def main():
    print("=== Job 1: Bronze -> Silver ===")
    spark = create_spark()
    spark.sparkContext.setLogLevel("WARN")

    affected_ids = parse_affected_ids()
    train_model_requested = truthy_env("TRAIN_MODEL_REQUESTED")

    try:
        survey_df = spark.read.format("delta").load(BRONZE_SURVEY)
    except Exception as e:
        print(f"No Bronze survey data yet: {e}")
        if train_model_requested:
            process_ground_truth(spark, incremental=delta_exists(spark, SILVER_GROUND_TRUTH))
        spark.stop()
        return

    try:
        master_df = spark.read.format("delta").load(BRONZE_MASTER)
    except Exception as e:
        print(f"No master wilayah: {e}")
        master_df = None

    core_tables_exist = all(delta_exists(spark, p) for p in (SILVER_HISTORY, SILVER_LATEST, SILVER_FEATURE))
    incremental = bool(affected_ids) and core_tables_exist
    print(f"Processing mode: {'incremental' if incremental else 'full'}")
    if affected_ids:
        print(f"Affected wilayah: {', '.join(affected_ids)}")

    survey_scope = survey_df
    if incremental:
        survey_scope = survey_df.filter(F.col("id_wilayah").isin(affected_ids))
        if df_is_empty(survey_scope):
            print("No Bronze survey rows matched affected wilayah; skipping survey Silver updates")
            if train_model_requested:
                process_ground_truth(spark, incremental=delta_exists(spark, SILVER_GROUND_TRUTH))
            spark.stop()
            return

    enriched = enrich_survey(survey_scope, master_df)
    enriched = risk_level(compute_risk_score(enriched))
    enriched = (
        enriched.withColumnRenamed("risk_score", "risk_score_saat_itu")
        .withColumnRenamed("risk_level", "risk_level_saat_itu")
    )

    history_df = enriched.dropDuplicates(["event_id"])
    if incremental:
        print("Merging silver/event_history by event_id...")
        merge_delta(
            spark,
            history_df,
            SILVER_HISTORY,
            "t.event_id = s.event_id",
            partition_cols=("kelurahan",),
            update_matches=False,
        )
    else:
        print("Writing silver/event_history...")
        write_delta(history_df, SILVER_HISTORY, partition_cols=("kelurahan",))

    latest_df = latest_per_wilayah(enriched)
    if incremental:
        print("Merging silver/latest_indicators by id_wilayah...")
        merge_delta(
            spark,
            latest_df,
            SILVER_LATEST,
            "t.id_wilayah = s.id_wilayah",
            partition_cols=("kelurahan",),
        )

        current_latest = spark.read.format("delta").load(SILVER_LATEST)
        affected_kelurahan = latest_df.select("kelurahan", "kecamatan").dropDuplicates()
        feature_scope = current_latest.join(F.broadcast(affected_kelurahan), ["kelurahan", "kecamatan"], "inner")
    else:
        print("Writing silver/latest_indicators...")
        write_delta(latest_df, SILVER_LATEST, partition_cols=("kelurahan",))
        feature_scope = latest_df

    feature_df = build_feature_matrix(feature_scope)
    if incremental:
        print("Merging silver/feature_matrix by kelurahan...")
        merge_delta(
            spark,
            feature_df,
            SILVER_FEATURE,
            "t.id_kelurahan = s.id_kelurahan AND t.kecamatan = s.kecamatan",
        )
    else:
        print("Writing silver/feature_matrix...")
        write_delta(feature_df, SILVER_FEATURE)

    if train_model_requested or not incremental:
        process_ground_truth(spark, incremental=incremental and delta_exists(spark, SILVER_GROUND_TRUTH))

    if not incremental:
        pop_df = process_population(master_df)
        if pop_df is not None:
            print("Writing silver/population...")
            write_delta(pop_df, SILVER_POPULATION)

    print("=== Job 1 Complete ===")
    spark.stop()


if __name__ == "__main__":
    main()
