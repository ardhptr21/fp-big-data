#!/usr/bin/env python3
"""
Spark Job 3: Silver + Model -> Gold
Produces all analytics outputs:
- gold/slum_risk_score      : risk score per wilayah + kelurahan
- gold/slum_prediction      : ML prediction labels
- gold/slum_trend           : delta skor antar events
- gold/dominant_factors     : top-3 faktor per kelurahan
- gold/intervention_priority: ranking prioritas intervensi
"""

import glob
import os
import sys
import time
from typing import Iterable, List

from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import FloatType, IntegerType

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

INDICATOR_LABELS = {
    "skor_bangunan": "Kondisi Bangunan",
    "skor_jalan": "Aksesibilitas Jalan",
    "skor_drainase": "Drainase Lingkungan",
    "skor_air_limbah": "Pengelolaan Air Limbah",
    "skor_sampah": "Pengelolaan Persampahan",
    "skor_kebakaran": "Proteksi Kebakaran",
    "skor_air_minum": "Penyediaan Air Minum",
}


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


def main():
    print("=== Job 3: Silver -> Gold ===")
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

    # ML prediction is mandatory for Gold prediction output.
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

    # Priority rank is global, so compute it from the merged risk table. It is
    # still persisted via Delta MERGE instead of rewriting the whole Gold table.
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
        "last_updated",
    )
    print("Merging gold/intervention_priority...")
    merge_delta(spark, priority_df, GOLD_PRIORITY, "t.id_wilayah = s.id_wilayah")

    ranked_gold.unpersist()
    print("=== Job 3 Complete ===")
    spark.stop()


if __name__ == "__main__":
    main()
