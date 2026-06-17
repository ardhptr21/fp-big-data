#!/usr/bin/env python3
"""
Spark Job 1: Bronze → Silver
Transforms raw Bronze events into:
- silver/latest_indicators  : latest snapshot per wilayah
- silver/event_history      : full history (all events, enriched)
- silver/feature_matrix     : aggregated features per kelurahan for ML
"""

import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import FloatType, StringType, IntegerType
from delta import configure_spark_with_delta_pip

HDFS_URL = os.environ.get("HDFS_URL", "hdfs://namenode:9000")
try:
    SPARK_LOCAL_THREADS = max(1, int(os.environ.get("SPARK_LOCAL_THREADS", "1")))
except ValueError:
    SPARK_LOCAL_THREADS = 1

# Use bounded local mode for in-process execution on lower-memory machines.
SPARK_MASTER = os.environ.get("SPARK_MASTER", f"local[{SPARK_LOCAL_THREADS}]")
# For running from API container, use local mode to avoid driver hostname issues
if SPARK_MASTER.startswith("spark://"):
    SPARK_MASTER = f"local[{SPARK_LOCAL_THREADS}]"  # override to local for subprocess execution

SPARK_DRIVER_MEMORY = os.environ.get("SPARK_DRIVER_MEMORY", "768m")
SPARK_EXECUTOR_MEMORY = os.environ.get("SPARK_EXECUTOR_MEMORY", "768m")
SPARK_SHUFFLE_PARTITIONS = os.environ.get("SPARK_SHUFFLE_PARTITIONS", "4")

BRONZE_SURVEY = f"{HDFS_URL}/data/bronze/survey_events"
BRONZE_MASTER = f"{HDFS_URL}/data/bronze/master_wilayah"
BRONZE_SECONDARY = f"{HDFS_URL}/data/bronze/secondary_sources"

SILVER_LATEST = f"{HDFS_URL}/data/silver/latest_indicators"
SILVER_HISTORY = f"{HDFS_URL}/data/silver/event_history"
SILVER_FEATURE = f"{HDFS_URL}/data/silver/feature_matrix"
SILVER_GROUND_TRUTH = f"{HDFS_URL}/data/silver/ground_truth"
SILVER_POPULATION = f"{HDFS_URL}/data/silver/population"


def create_spark():
    builder = (
        SparkSession.builder
        .appName("SlumJob1-BronzeSilver")
        .master(SPARK_MASTER)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.hadoop.fs.defaultFS", HDFS_URL)
        .config("spark.hadoop.dfs.client.use.datanode.hostname", "true")
        .config("spark.driver.memory", SPARK_DRIVER_MEMORY)
        .config("spark.executor.memory", SPARK_EXECUTOR_MEMORY)
        .config("spark.driver.maxResultSize", "128m")
        .config("spark.sql.shuffle.partitions", SPARK_SHUFFLE_PARTITIONS)
        .config("spark.default.parallelism", SPARK_LOCAL_THREADS)
        .config("spark.python.worker.memory", "256m")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


def compute_risk_score(df):
    """Compute risk score 0-100 from 7 PUPR indicators (equal weights for now)."""
    indicators = ["skor_bangunan", "skor_jalan", "skor_drainase",
                  "skor_air_limbah", "skor_sampah", "skor_kebakaran", "skor_air_minum"]
    total_cols = [F.col(c).cast(FloatType()) for c in indicators if c in df.columns]
    if not total_cols:
        return df.withColumn("risk_score", F.lit(0.0))

    sum_expr = sum(total_cols)  # Use Python's sum on list of columns
    max_possible = 3.0 * len(total_cols)
    risk_score = (sum_expr / max_possible * 100).cast(FloatType())

    return df.withColumn("risk_score", risk_score)


def risk_level(df):
    """Add risk_level column from risk_score."""
    return df.withColumn(
        "risk_level",
        F.when(F.col("risk_score") < 25, "Ringan")
         .when(F.col("risk_score") < 50, "Sedang")
         .when(F.col("risk_score") < 75, "Berat")
         .otherwise("Sangat Berat")
    )


def main():
    print("=== Job 1: Bronze → Silver ===")
    spark = create_spark()
    spark.sparkContext.setLogLevel("WARN")

    # --------------------------------------------------------
    # Read Bronze survey events
    # --------------------------------------------------------
    try:
        survey_df = spark.read.format("delta").load(BRONZE_SURVEY)
        print(f"Bronze survey events: {survey_df.count()} rows")
    except Exception as e:
        print(f"No Bronze survey data yet: {e}")
        spark.stop()
        return

    # Read master wilayah
    try:
        master_df = spark.read.format("delta").load(BRONZE_MASTER)
        print(f"Master wilayah: {master_df.count()} rows")
    except Exception as e:
        print(f"No master wilayah: {e}")
        master_df = None

    # --------------------------------------------------------
    # Enrich survey events with wilayah info
    # --------------------------------------------------------
    if master_df is not None:
        master_slim = master_df.select(
            "id_wilayah", "kecamatan", "kelurahan", "rw", "rt",
            "total_jiwa", "total_kk", "luas_m2", "geometry_wkt"
        )
        enriched = survey_df.join(master_slim, on="id_wilayah", how="left")
    else:
        enriched = survey_df.withColumn("kecamatan", F.lit("")).withColumn("kelurahan", F.lit(""))

    # --------------------------------------------------------
    # Compute risk score for each event
    # --------------------------------------------------------
    enriched = compute_risk_score(enriched)
    enriched = risk_level(enriched)
    enriched = enriched.withColumnRenamed("risk_score", "risk_score_saat_itu") \
                       .withColumnRenamed("risk_level", "risk_level_saat_itu")

    # --------------------------------------------------------
    # Silver/event_history — logical full history from append-only Bronze.
    # Rebuild instead of appending all Bronze rows every run, otherwise each
    # pipeline duplicates prior events and grows Silver quadratically.
    # --------------------------------------------------------
    print("Writing silver/event_history (deduplicated overwrite)...")
    history_df = enriched.dropDuplicates(["event_id"])
    history_df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .partitionBy("kelurahan") \
        .save(SILVER_HISTORY)

    # --------------------------------------------------------
    # Silver/latest_indicators — LATEST per wilayah (overwrite)
    # --------------------------------------------------------
    print("Building silver/latest_indicators...")
    window_spec = Window.partitionBy("id_wilayah").orderBy(F.col("recorded_at").desc())
    latest_df = enriched.withColumn("_rn", F.row_number().over(window_spec)) \
                        .filter(F.col("_rn") == 1) \
                        .drop("_rn", "risk_score_saat_itu", "risk_level_saat_itu")

    # Recompute risk score for latest
    latest_df = compute_risk_score(latest_df)
    latest_df = risk_level(latest_df)

    print(f"Latest indicators: {latest_df.count()} wilayah")
    latest_df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .save(SILVER_LATEST)

    # --------------------------------------------------------
    # Silver/feature_matrix — aggregated per kelurahan for ML
    # --------------------------------------------------------
    print("Building silver/feature_matrix...")
    indicators = ["skor_bangunan", "skor_jalan", "skor_drainase",
                  "skor_air_limbah", "skor_sampah", "skor_kebakaran", "skor_air_minum"]

    agg_exprs = [F.avg(c).alias(f"avg_{c}") for c in indicators if c in latest_df.columns]
    agg_exprs += [
        F.sum("jumlah_jiwa").alias("total_jiwa_kelurahan"),
        F.avg("frekuensi_banjir").alias("avg_frekuensi_banjir"),
        F.count("id_wilayah").alias("total_rt_surveyed"),
        F.avg("risk_score").alias("avg_risk_score"),
    ]

    feature_df = latest_df.groupBy("kelurahan", "kecamatan").agg(*agg_exprs)
    feature_df = feature_df.withColumn("id_kelurahan", F.col("kelurahan"))
    feature_df = feature_df.withColumn("updated_at", F.current_timestamp())

    print(f"Feature matrix: {feature_df.count()} kelurahan")
    feature_df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .save(SILVER_FEATURE)

    # --------------------------------------------------------
    # Silver/ground_truth — from secondary sources
    # --------------------------------------------------------
    try:
        secondary_df = spark.read.format("delta").load(BRONZE_SECONDARY)
        gt_df = secondary_df.filter(F.col("source_type") == "ground_truth")
        if gt_df.count() > 0:
            print(f"Ground truth records: {gt_df.count()}")
            gt_df.write \
                .format("delta") \
                .mode("overwrite") \
                .option("overwriteSchema", "true") \
                .save(SILVER_GROUND_TRUTH)
    except Exception as e:
        print(f"No secondary data yet: {e}")

    # --------------------------------------------------------
    # Silver/population — from master wilayah
    # --------------------------------------------------------
    if master_df is not None:
        pop_df = master_df.groupBy("kelurahan", "kecamatan") \
                          .agg(
                              F.sum("total_jiwa").alias("total_jiwa"),
                              F.sum("total_kk").alias("total_kk"),
                              F.count("id_wilayah").alias("total_rt")
                          )
        pop_df.write \
            .format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .save(SILVER_POPULATION)

    print("=== Job 1 Complete ===")
    spark.stop()


if __name__ == "__main__":
    main()
