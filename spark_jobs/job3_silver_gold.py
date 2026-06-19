#!/usr/bin/env python3
"""
Spark Job 3: Silver + Model -> Gold
Menghasilkan semua output analitik:
- gold/slum_risk_score        : risk score per wilayah + kelurahan
- gold/slum_prediction        : ML prediction labels + probabilitas
- gold/slum_trend             : delta skor antar events (temporal trend)
- gold/dominant_factors       : top-3 faktor per kelurahan
- gold/intervention_priority  : ranking prioritas intervensi
- gold/slum_clusters   [NEW]  : K-Means clustering wilayah berdasarkan 7 indikator PUPR
- gold/slum_forecast   [NEW]  : Prediksi risk_score 30 hari ke depan (linear regression)

Teknik analisis lanjutan:
  1. RandomForestClassifier (via Job 2 model)         — klasifikasi kumuh
  2. K-Means Clustering (Spark MLlib, K=4)            — pengelompokan spasial wilayah
  3. Linear Regression Forecasting (window functions) — prediksi risk score T+30 hari
"""

import glob
import math
import os
import sys
import time
from typing import Iterable, List

from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import FloatType, IntegerType, StringType

HDFS_URL = os.environ.get("HDFS_URL", "hdfs://namenode:9000")
STORAGE_MODE = os.environ.get("STORAGE_MODE", "auto").lower()
LOCAL_LAKEHOUSE_DIR = os.environ.get("LOCAL_LAKEHOUSE_DIR", "/data/local_lakehouse")
try:
    SPARK_LOCAL_THREADS = max(1, int(os.environ.get("SPARK_LOCAL_THREADS", "1")))
except ValueError:
    SPARK_LOCAL_THREADS = 1

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

# Jumlah cluster K-Means (sesuai 4 level risiko: Ringan/Sedang/Berat/Sangat Berat)
KMEANS_K = int(os.environ.get("KMEANS_K", "4"))

# Horizon forecasting dalam hari
FORECAST_HORIZON_DAYS = int(os.environ.get("FORECAST_HORIZON_DAYS", "30"))

LAKEHOUSE_ROOT = None
STORAGE_BACKEND = "unresolved"

SILVER_LATEST = None
SILVER_HISTORY = None
BRONZE_MASTER = None
MODELS_LATEST = None

GOLD_RISK = None
GOLD_PREDICTION = None
GOLD_TREND = None
GOLD_FACTORS = None
GOLD_PRIORITY = None
GOLD_CLUSTERS = None
GOLD_FORECAST = None

INDICATOR_LABELS = {
    "skor_bangunan": "Kondisi Bangunan",
    "skor_jalan": "Aksesibilitas Jalan",
    "skor_drainase": "Drainase Lingkungan",
    "skor_air_limbah": "Pengelolaan Air Limbah",
    "skor_sampah": "Pengelolaan Persampahan",
    "skor_kebakaran": "Proteksi Kebakaran",
    "skor_air_minum": "Penyediaan Air Minum",
}

# Label cluster berdasarkan rata-rata risk score centroid
CLUSTER_LEVEL_LABELS = [
    "Cluster Risiko Rendah",
    "Cluster Risiko Sedang",
    "Cluster Risiko Tinggi",
    "Cluster Risiko Sangat Tinggi",
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
    global SILVER_LATEST, SILVER_HISTORY, BRONZE_MASTER, MODELS_LATEST
    global GOLD_RISK, GOLD_PREDICTION, GOLD_TREND, GOLD_FACTORS, GOLD_PRIORITY
    global GOLD_CLUSTERS, GOLD_FORECAST

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
    SILVER_LATEST = f"{root}/silver/latest_indicators"
    SILVER_HISTORY = f"{root}/silver/event_history"
    BRONZE_MASTER = f"{root}/bronze/master_wilayah"
    MODELS_LATEST = f"{root}/models/rf_slum_latest"
    GOLD_RISK = f"{root}/gold/slum_risk_score"
    GOLD_PREDICTION = f"{root}/gold/slum_prediction"
    GOLD_TREND = f"{root}/gold/slum_trend"
    GOLD_FACTORS = f"{root}/gold/dominant_factors"
    GOLD_PRIORITY = f"{root}/gold/intervention_priority"
    GOLD_CLUSTERS = f"{root}/gold/slum_clusters"
    GOLD_FORECAST = f"{root}/gold/slum_forecast"
    print(f"Lakehouse storage backend={STORAGE_BACKEND} root={LAKEHOUSE_ROOT}")


def create_spark():
    builder = (
        SparkSession.builder
        .appName("SlumJob3-SilverGold")
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


def compute_risk_score_df(df):
    """Compute risk_score 0-100 from 7 PUPR indicators."""
    present = [c for c in INDICATOR_LABELS if c in df.columns]
    if not present:
        return df.withColumn("risk_score", F.lit(0.0))

    col_sum = sum(F.coalesce(F.col(c).cast(FloatType()), F.lit(0.0)) for c in present)
    max_val = 3.0 * len(present)
    return df.withColumn("risk_score", (col_sum / max_val * 100).cast(FloatType()))


def add_risk_level(df):
    return df.withColumn(
        "risk_level",
        F.when(F.col("risk_score") < 25, "Ringan")
         .when(F.col("risk_score") < 50, "Sedang")
         .when(F.col("risk_score") < 75, "Berat")
         .otherwise("Sangat Berat")
    )


def with_top_factors(df):
    present = [c for c in INDICATOR_LABELS if c in df.columns]
    if not present:
        return (
            df.withColumn("top_faktor_1", F.lit(""))
            .withColumn("top_faktor_2", F.lit(""))
            .withColumn("top_faktor_3", F.lit(""))
            .withColumn("skor_faktor_1", F.lit(0.0))
            .withColumn("skor_faktor_2", F.lit(0.0))
            .withColumn("skor_faktor_3", F.lit(0.0))
        )

    factors = F.array(*[
        F.struct(
            (-F.coalesce(F.col(col_name).cast(FloatType()), F.lit(0.0))).alias("sort_score"),
            F.lit(label).alias("label"),
            F.coalesce(F.col(col_name).cast(FloatType()), F.lit(0.0)).alias("score"),
        )
        for col_name, label in INDICATOR_LABELS.items()
        if col_name in df.columns
    ])
    ranked = df.withColumn("_ranked_factors", F.sort_array(factors, asc=True))
    return (
        ranked.withColumn("top_faktor_1", F.element_at("_ranked_factors", 1).getField("label"))
        .withColumn("top_faktor_2", F.element_at("_ranked_factors", 2).getField("label"))
        .withColumn("top_faktor_3", F.element_at("_ranked_factors", 3).getField("label"))
        .withColumn("skor_faktor_1", F.element_at("_ranked_factors", 1).getField("score"))
        .withColumn("skor_faktor_2", F.element_at("_ranked_factors", 2).getField("score"))
        .withColumn("skor_faktor_3", F.element_at("_ranked_factors", 3).getField("score"))
        .drop("_ranked_factors")
    )


# ===========================================================
# TEKNIK #2: K-Means Clustering (Spark MLlib)
# ===========================================================
def compute_kmeans_clusters(spark, latest_df):
    """
    Mengelompokkan wilayah menggunakan K-Means (K=4) berdasarkan 7 indikator PUPR.

    Return:
        cluster_assignment_df: DataFrame dengan kolom id_wilayah, cluster_id, cluster_label
        cluster_summary_df:    DataFrame dengan centroid tiap cluster
    """
    from pyspark.ml.clustering import KMeans
    from pyspark.ml.feature import VectorAssembler, StandardScaler

    print(f"=== K-Means Clustering (K={KMEANS_K}) ===")

    feature_cols = [c for c in INDICATOR_LABELS if c in latest_df.columns]
    if not feature_cols:
        print("K-Means: No indicator columns found, skipping")
        return None, None

    kmeans_input = latest_df.select(["id_wilayah", "kelurahan", "kecamatan"] + feature_cols).fillna(0)

    n_rows = kmeans_input.count()
    if n_rows < KMEANS_K:
        print(f"K-Means: Hanya {n_rows} wilayah, tidak cukup untuk K={KMEANS_K} — skip clustering")
        return None, None

    # Assemble + scale fitur
    assembler = VectorAssembler(inputCols=feature_cols, outputCol="_raw_features", handleInvalid="keep")
    scaler = StandardScaler(inputCol="_raw_features", outputCol="features", withMean=True, withStd=True)

    try:
        from pyspark.ml import Pipeline as MLPipeline
        prep_pipeline = MLPipeline(stages=[assembler, scaler])
        prep_model = prep_pipeline.fit(kmeans_input)
        scaled_df = prep_model.transform(kmeans_input)
    except Exception as e:
        print(f"K-Means scaling failed: {e}; falling back to unscaled features")
        assembler_only = VectorAssembler(inputCols=feature_cols, outputCol="features", handleInvalid="keep")
        scaled_df = assembler_only.transform(kmeans_input)

    # K-Means
    kmeans = KMeans(k=KMEANS_K, seed=42, maxIter=20, featuresCol="features", predictionCol="cluster_id")
    kmeans_model = kmeans.fit(scaled_df)
    clustered_df = kmeans_model.transform(scaled_df)

    # Hitung rata-rata risk_score per cluster untuk labeling
    if "risk_score" not in clustered_df.columns:
        clustered_df = compute_risk_score_df(clustered_df)

    cluster_stats = (
        clustered_df.groupBy("cluster_id")
        .agg(
            F.avg("risk_score").alias("avg_risk_score"),
            F.count("id_wilayah").alias("jumlah_wilayah"),
        )
        .orderBy("avg_risk_score")
    )

    # Beri label cluster sesuai urutan risk_score (ascending → Rendah dulu)
    cluster_stats_rows = cluster_stats.collect()
    cluster_label_map = {}
    for rank, row in enumerate(cluster_stats_rows):
        label_idx = min(rank, len(CLUSTER_LEVEL_LABELS) - 1)
        cluster_label_map[int(row["cluster_id"])] = CLUSTER_LEVEL_LABELS[label_idx]

    print(f"K-Means hasil cluster: {cluster_label_map}")

    # UDF untuk map cluster_id → cluster_label
    @F.udf(StringType())
    def map_cluster_label(cid):
        return cluster_label_map.get(int(cid), f"Cluster {cid}")

    cluster_assignment = (
        clustered_df.select("id_wilayah", "kelurahan", "kecamatan", "cluster_id")
        .withColumn("cluster_label", map_cluster_label(F.col("cluster_id").cast("int")))
        .withColumn("computed_at", F.current_timestamp())
    )

    # Centroid summary untuk gold/slum_clusters
    centroids = kmeans_model.clusterCenters()
    centroid_rows = []
    for i, center in enumerate(centroids):
        label = cluster_label_map.get(i, f"Cluster {i}")
        stats_row = next((r for r in cluster_stats_rows if int(r["cluster_id"]) == i), None)
        centroid_row = {
            "cluster_id": i,
            "cluster_label": label,
            "jumlah_wilayah": int(stats_row["jumlah_wilayah"]) if stats_row else 0,
            "avg_risk_score": float(stats_row["avg_risk_score"]) if stats_row else 0.0,
        }
        # Tambah centroid values per fitur
        for j, feat in enumerate(feature_cols):
            centroid_row[f"centroid_{feat}"] = float(center[j]) if j < len(center) else 0.0
        centroid_rows.append(centroid_row)

    cluster_summary_df = spark.createDataFrame(centroid_rows)
    cluster_summary_df = cluster_summary_df.withColumn("computed_at", F.current_timestamp())

    print(f"K-Means selesai: {len(centroid_rows)} clusters ditemukan")
    return cluster_assignment, cluster_summary_df


# ===========================================================
# TEKNIK #3: Linear Regression Forecasting
# ===========================================================
def compute_forecast(spark, history_df):
    """
    Prediksi risk_score T+30 hari menggunakan regresi linear sederhana.

    Metode:
    - Konversi recorded_at ke Unix timestamp (detik)
    - Hitung slope (b1) dan intercept (b0) menggunakan formula OLS:
        b1 = (Σ(x - x̄)(y - ȳ)) / Σ(x - x̄)²
        b0 = ȳ - b1 * x̄
    - Prediksi: forecast_score = b0 + b1 * (now + 30 hari dalam detik)

    Return:
        forecast_df: DataFrame dengan forecast per wilayah
    """
    from pyspark.sql.functions import (
        unix_timestamp, avg as spark_avg, sum as spark_sum, count as spark_count
    )

    print(f"=== Linear Regression Forecasting (horizon={FORECAST_HORIZON_DAYS} hari) ===")

    if df_is_empty(history_df):
        print("Forecast: No history data, skipping")
        return None

    # Hitung risk_score per event jika belum ada
    if "risk_score_saat_itu" in history_df.columns:
        ts_df = history_df.withColumn("risk_score_h", F.col("risk_score_saat_itu").cast(FloatType()))
    elif "risk_score" in history_df.columns:
        ts_df = history_df.withColumn("risk_score_h", F.col("risk_score").cast(FloatType()))
    else:
        ts_df = compute_risk_score_df(history_df)
        ts_df = ts_df.withColumn("risk_score_h", F.col("risk_score").cast(FloatType()))

    ts_df = ts_df.withColumn(
        "ts_unix",
        unix_timestamp(F.col("recorded_at").cast("timestamp")).cast(FloatType())
    ).filter(F.col("ts_unix").isNotNull() & F.col("risk_score_h").isNotNull())

    # Minimal 2 titik data per wilayah untuk regresi
    count_df = ts_df.groupBy("id_wilayah").agg(spark_count("*").alias("n_events"))
    ts_df = ts_df.join(count_df, on="id_wilayah", how="inner").filter(F.col("n_events") >= 2)

    if df_is_empty(ts_df):
        print("Forecast: Tidak cukup titik data (min 2 event per wilayah), skipping")
        return None

    # OLS linear regression per wilayah menggunakan window aggregations
    # b1 = Cov(x,y) / Var(x) = (n*Σxy - Σx*Σy) / (n*Σx² - (Σx)²)
    reg_df = ts_df.groupBy("id_wilayah").agg(
        spark_count("*").alias("n"),
        spark_sum(F.col("ts_unix") * F.col("risk_score_h")).alias("sum_xy"),
        spark_sum(F.col("ts_unix")).alias("sum_x"),
        spark_sum(F.col("risk_score_h")).alias("sum_y"),
        spark_sum(F.col("ts_unix") * F.col("ts_unix")).alias("sum_x2"),
        F.max("ts_unix").alias("last_ts"),
        F.max("risk_score_h").alias("last_risk_score"),
        spark_avg("risk_score_h").alias("avg_risk_score"),
    )

    # Hitung slope dan intercept via Spark SQL expressions
    reg_df = reg_df.withColumn(
        "denom",
        F.col("n") * F.col("sum_x2") - F.col("sum_x") * F.col("sum_x")
    )

    reg_df = reg_df.withColumn(
        "slope",
        F.when(F.abs(F.col("denom")) > 1e-6,
               (F.col("n") * F.col("sum_xy") - F.col("sum_x") * F.col("sum_y")) / F.col("denom")
        ).otherwise(F.lit(0.0)).cast(FloatType())
    )

    reg_df = reg_df.withColumn(
        "intercept",
        ((F.col("sum_y") - F.col("slope") * F.col("sum_x")) / F.col("n")).cast(FloatType())
    )

    # Prediksi T+30 hari
    horizon_secs = float(FORECAST_HORIZON_DAYS * 86400)
    reg_df = reg_df.withColumn(
        "forecast_ts",
        (F.col("last_ts") + F.lit(horizon_secs)).cast(FloatType())
    )

    reg_df = reg_df.withColumn(
        "forecast_score_raw",
        (F.col("intercept") + F.col("slope") * F.col("forecast_ts")).cast(FloatType())
    )

    # Clamp ke [0, 100]
    reg_df = reg_df.withColumn(
        "forecast_score",
        F.greatest(F.lit(0.0), F.least(F.lit(100.0), F.col("forecast_score_raw"))).cast(FloatType())
    )

    # Label trend
    reg_df = reg_df.withColumn(
        "forecast_trend",
        F.when(F.col("slope") > 0.5, "Memburuk")
         .when(F.col("slope") < -0.5, "Membaik")
         .otherwise("Stabil")
    )

    # Level forecasting
    reg_df = reg_df.withColumn(
        "forecast_risk_level",
        F.when(F.col("forecast_score") < 25, "Ringan")
         .when(F.col("forecast_score") < 50, "Sedang")
         .when(F.col("forecast_score") < 75, "Berat")
         .otherwise("Sangat Berat")
    )

    # Tanggal forecast (sebagai string ISO)
    reg_df = reg_df.withColumn(
        "forecast_at",
        F.from_unixtime(F.col("forecast_ts").cast("long")).cast("timestamp")
    )

    forecast_df = reg_df.select(
        "id_wilayah",
        F.col("n").alias("n_history_events"),
        F.col("last_risk_score").alias("current_risk_score"),
        "forecast_score",
        "forecast_at",
        "forecast_trend",
        "forecast_risk_level",
        F.round(F.col("slope") * 86400, 4).alias("slope_per_day"),  # perubahan skor per hari
        F.current_timestamp().alias("computed_at"),
    )

    n_forecasted = forecast_df.count()
    print(f"Forecast selesai: {n_forecasted} wilayah diforecast ({FORECAST_HORIZON_DAYS} hari ke depan)")
    return forecast_df


def main():
    print("=== Job 3: Silver -> Gold (Risk Score + ML + K-Means Clustering + Forecasting) ===")
    spark = create_spark()
    spark.sparkContext.setLogLevel("WARN")

    affected_ids = parse_affected_ids()
    train_model_requested = truthy_env("TRAIN_MODEL_REQUESTED")

    try:
        latest_df = spark.read.format("delta").load(SILVER_LATEST)
        if df_is_empty(latest_df):
            print("No data in silver/latest_indicators, exiting")
            spark.stop()
            return
    except Exception as e:
        print(f"Cannot read silver/latest_indicators: {e}")
        spark.stop()
        return

    gold_ready = all(delta_exists(spark, p) for p in (GOLD_RISK, GOLD_PREDICTION, GOLD_TREND, GOLD_FACTORS, GOLD_PRIORITY))
    incremental = bool(affected_ids) and gold_ready and not train_model_requested
    print(f"Processing mode: {'incremental' if incremental else 'full'}")
    if affected_ids:
        print(f"Affected wilayah: {', '.join(affected_ids)}")

    latest_scope = latest_df
    if incremental:
        latest_scope = latest_df.filter(F.col("id_wilayah").isin(affected_ids))
        if df_is_empty(latest_scope):
            print("No latest rows matched affected wilayah, exiting")
            spark.stop()
            return

    try:
        master_df = spark.read.format("delta").load(BRONZE_MASTER)
        has_master = True
    except Exception:
        master_df = None
        has_master = False

    scored_df = add_risk_level(compute_risk_score_df(latest_scope))

    # ML prediction
    try:
        from pyspark.ml import PipelineModel
        from pyspark.ml.functions import vector_to_array

        model = PipelineModel.load(MODELS_LATEST)
        feature_cols = [
            c for c in [
                "skor_bangunan", "skor_jalan", "skor_drainase",
                "skor_air_limbah", "skor_sampah", "skor_kebakaran", "skor_air_minum",
                "frekuensi_banjir", "jumlah_jiwa",
            ]
            if c in scored_df.columns
        ]
        pred_input = scored_df.fillna(0, subset=feature_cols)
        predictions = model.transform(pred_input)
        probability_array = vector_to_array(F.col("probability"))
        label_df = predictions.select(
            "id_wilayah",
            F.col("prediction").cast(IntegerType()).alias("label_prediksi"),
            F.round(F.element_at(probability_array, 2), 4).alias("proba_kumuh"),
        )
        print("ML predictions computed with Spark ML PipelineModel")
    except Exception as e:
        print(f"ML model or prediction failed: {e}", file=sys.stderr)
        spark.stop()
        sys.exit(1)

    gold_df = scored_df.join(label_df, on="id_wilayah", how="left")

    if has_master:
        if "geometry_wkt" in gold_df.columns:
            gold_df = gold_df.drop("geometry_wkt")
        if "jiwa_terdampak" in gold_df.columns:
            gold_df = gold_df.drop("jiwa_terdampak")
        master_geo = master_df.select(
            "id_wilayah",
            "geometry_wkt",
            F.col("total_jiwa").alias("jiwa_terdampak"),
        )
        gold_df = gold_df.join(master_geo, on="id_wilayah", how="left")
    else:
        gold_df = gold_df.withColumn("geometry_wkt", F.lit(""))
        if "total_jiwa" in gold_df.columns:
            gold_df = gold_df.withColumnRenamed("total_jiwa", "jiwa_terdampak")
        else:
            gold_df = gold_df.withColumn("jiwa_terdampak", F.lit(0))

    gold_df = with_top_factors(gold_df)
    gold_df = (
        gold_df.withColumn("last_updated", F.current_timestamp())
        .withColumn("priority_score", F.col("risk_score") * F.col("jiwa_terdampak").cast(FloatType()))
    )

    # =======================================================
    # TEKNIK #2: K-Means Clustering (selalu full-run)
    # =======================================================
    print("Menjalankan K-Means Clustering pada semua wilayah...")
    all_latest_for_cluster = latest_df if not incremental else latest_df
    cluster_assignment, cluster_summary = compute_kmeans_clusters(spark, compute_risk_score_df(all_latest_for_cluster))

    if cluster_assignment is not None:
        # Join cluster_id ke gold_df
        if "cluster_id" in gold_df.columns:
            gold_df = gold_df.drop("cluster_id", "cluster_label")
        cluster_slim = cluster_assignment.select("id_wilayah", "cluster_id", "cluster_label")
        gold_df = gold_df.join(cluster_slim, on="id_wilayah", how="left")
    else:
        gold_df = gold_df.withColumn("cluster_id", F.lit(None).cast(IntegerType()))
        gold_df = gold_df.withColumn("cluster_label", F.lit(""))

    print("Merging gold/slum_risk_score...")
    merge_delta(
        spark,
        gold_df,
        GOLD_RISK,
        "t.id_wilayah = s.id_wilayah",
        partition_cols=("kelurahan",),
    )

    prediction_df = gold_df.select(
        "id_wilayah",
        "kelurahan",
        "kecamatan",
        "label_prediksi",
        "proba_kumuh",
        "last_updated",
    )
    print("Merging gold/slum_prediction...")
    merge_delta(spark, prediction_df, GOLD_PREDICTION, "t.id_wilayah = s.id_wilayah")

    # Trend
    try:
        history_df = spark.read.format("delta").load(SILVER_HISTORY)
        if incremental:
            history_df = history_df.filter(F.col("id_wilayah").isin(affected_ids))
        history_scored = compute_risk_score_df(history_df)

        window_h = Window.partitionBy("id_wilayah").orderBy(F.col("recorded_at").desc())
        history_scored = history_scored.withColumn("_rn", F.row_number().over(window_h))

        latest_event = (
            history_scored.filter(F.col("_rn") == 1)
            .select(
                "id_wilayah",
                F.col("risk_score").alias("risk_score_latest"),
                F.col("recorded_at").alias("recorded_at_latest"),
            )
        )

        prev_event = (
            history_scored.filter(F.col("_rn") == 2)
            .select(
                "id_wilayah",
                F.col("risk_score").alias("risk_score_prev"),
                F.col("recorded_at").alias("recorded_at_prev"),
            )
        )

        trend_df = latest_event.join(prev_event, on="id_wilayah", how="left")
        trend_df = trend_df.withColumn(
            "delta_risk",
            F.col("risk_score_latest") - F.coalesce(F.col("risk_score_prev"), F.col("risk_score_latest")),
        )
        trend_df = trend_df.withColumn(
            "trend_label",
            F.when(F.col("delta_risk") > 2, "Memburuk")
             .when(F.col("delta_risk") < -2, "Membaik")
             .otherwise("Stabil"),
        ).withColumn("computed_at", F.current_timestamp())

        print("Merging gold/slum_trend...")
        merge_delta(spark, trend_df, GOLD_TREND, "t.id_wilayah = s.id_wilayah")
    except Exception as e:
        print(f"Cannot compute trend (need at least one event): {e}")

    # Dominant factors
    factors_df = gold_df.select(
        "id_wilayah",
        "kelurahan",
        "kecamatan",
        "top_faktor_1",
        "top_faktor_2",
        "top_faktor_3",
        "skor_faktor_1",
        "skor_faktor_2",
        "skor_faktor_3",
    ).withColumn("computed_at", F.current_timestamp())
    print("Merging gold/dominant_factors...")
    merge_delta(spark, factors_df, GOLD_FACTORS, "t.id_wilayah = s.id_wilayah")

    # Priority ranking
    current_gold = spark.read.format("delta").load(GOLD_RISK)
    ranked_gold = current_gold.withColumn(
        "priority_score",
        F.col("risk_score") * F.col("jiwa_terdampak").cast(FloatType()),
    )
    window_rank = Window.orderBy(F.col("priority_score").desc_nulls_last())
    ranked_gold = ranked_gold.withColumn("prioritas_rank", F.rank().over(window_rank)).cache()
    ranked_gold.count()

    print("Merging updated priority rank back into gold/slum_risk_score...")
    merge_delta(spark, ranked_gold, GOLD_RISK, "t.id_wilayah = s.id_wilayah")

    priority_df = ranked_gold.select(
        "prioritas_rank",
        "id_wilayah",
        "kelurahan",
        "kecamatan",
        "risk_score",
        "risk_level",
        "label_prediksi",
        "proba_kumuh",
        F.col("jiwa_terdampak").alias("jiwa_terdampak"),
        "top_faktor_1",
        "top_faktor_2",
        "top_faktor_3",
        "cluster_id",
        "cluster_label",
        "last_updated",
    )
    print("Merging gold/intervention_priority...")
    merge_delta(spark, priority_df, GOLD_PRIORITY, "t.id_wilayah = s.id_wilayah")

    ranked_gold.unpersist()

    # Simpan cluster summary
    if cluster_summary is not None:
        print("Writing gold/slum_clusters...")
        write_delta(cluster_summary, GOLD_CLUSTERS)

    # =======================================================
    # TEKNIK #3: Forecasting (selalu full-run dari semua history)
    # =======================================================
    try:
        full_history = spark.read.format("delta").load(SILVER_HISTORY)
        forecast_df = compute_forecast(spark, full_history)
        if forecast_df is not None:
            print("Merging gold/slum_forecast...")
            merge_delta(spark, forecast_df, GOLD_FORECAST, "t.id_wilayah = s.id_wilayah")
    except Exception as e:
        print(f"Forecasting failed (non-critical): {e}")

    print("=== Job 3 Complete ===")
    spark.stop()


if __name__ == "__main__":
    main()
