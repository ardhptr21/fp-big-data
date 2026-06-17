#!/usr/bin/env python3
"""
Spark Job 3: Silver + Model → Gold
Produces all analytics outputs:
- gold/slum_risk_score      : risk score per wilayah + kelurahan
- gold/slum_prediction      : ML prediction labels
- gold/slum_trend           : delta skor antar events
- gold/dominant_factors     : top-3 faktor per kelurahan
- gold/intervention_priority: ranking prioritas intervensi
"""

import os
import json
from datetime import datetime
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

SPARK_MASTER = os.environ.get("SPARK_MASTER", f"local[{SPARK_LOCAL_THREADS}]")
if SPARK_MASTER.startswith("spark://"):
    SPARK_MASTER = f"local[{SPARK_LOCAL_THREADS}]"

SPARK_DRIVER_MEMORY = os.environ.get("SPARK_DRIVER_MEMORY", "768m")
SPARK_EXECUTOR_MEMORY = os.environ.get("SPARK_EXECUTOR_MEMORY", "768m")
SPARK_SHUFFLE_PARTITIONS = os.environ.get("SPARK_SHUFFLE_PARTITIONS", "4")

SILVER_LATEST = f"{HDFS_URL}/data/silver/latest_indicators"
SILVER_HISTORY = f"{HDFS_URL}/data/silver/event_history"
SILVER_FEATURE = f"{HDFS_URL}/data/silver/feature_matrix"
BRONZE_MASTER = f"{HDFS_URL}/data/bronze/master_wilayah"
MODELS_LATEST = f"{HDFS_URL}/data/models/rf_slum_latest"

GOLD_RISK = f"{HDFS_URL}/data/gold/slum_risk_score"
GOLD_TREND = f"{HDFS_URL}/data/gold/slum_trend"
GOLD_FACTORS = f"{HDFS_URL}/data/gold/dominant_factors"
GOLD_PRIORITY = f"{HDFS_URL}/data/gold/intervention_priority"


def create_spark():
    builder = (
        SparkSession.builder
        .appName("SlumJob3-SilverGold")
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


def compute_risk_score_df(df):
    """Compute risk_score 0-100 from 7 PUPR indicators."""
    indicators = [
        "skor_bangunan", "skor_jalan", "skor_drainase",
        "skor_air_limbah", "skor_sampah", "skor_kebakaran", "skor_air_minum"
    ]
    present = [c for c in indicators if c in df.columns]
    if not present:
        return df.withColumn("risk_score", F.lit(0.0))

    col_sum = sum(F.col(c).cast(FloatType()) for c in present)
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


def get_top_factors(df):
    """
    Compute top-3 factors per kelurahan based on average indicator scores.
    Returns: DataFrame with id_wilayah, top_faktor_1, top_faktor_2, top_faktor_3
    """
    indicator_labels = {
        "skor_bangunan": "Kondisi Bangunan",
        "skor_jalan": "Aksesibilitas Jalan",
        "skor_drainase": "Drainase Lingkungan",
        "skor_air_limbah": "Pengelolaan Air Limbah",
        "skor_sampah": "Pengelolaan Persampahan",
        "skor_kebakaran": "Proteksi Kebakaran",
        "skor_air_minum": "Penyediaan Air Minum",
    }

    present = [c for c in indicator_labels if c in df.columns]

    rows = df.select("id_wilayah", "kelurahan", "kecamatan", *present).collect()
    result = []
    for row in rows:
        row_dict = row.asDict()
        scores = {indicator_labels[c]: (row_dict.get(c) or 0) for c in present}
        sorted_factors = sorted(scores.items(), key=lambda x: x[1], reverse=True)
        result.append({
            "id_wilayah": row_dict.get("id_wilayah", ""),
            "kelurahan": row_dict.get("kelurahan", ""),
            "kecamatan": row_dict.get("kecamatan", ""),
            "top_faktor_1": sorted_factors[0][0] if len(sorted_factors) > 0 else "",
            "top_faktor_2": sorted_factors[1][0] if len(sorted_factors) > 1 else "",
            "top_faktor_3": sorted_factors[2][0] if len(sorted_factors) > 2 else "",
            "skor_faktor_1": float(sorted_factors[0][1]) if len(sorted_factors) > 0 else 0.0,
            "skor_faktor_2": float(sorted_factors[1][1]) if len(sorted_factors) > 1 else 0.0,
            "skor_faktor_3": float(sorted_factors[2][1]) if len(sorted_factors) > 2 else 0.0,
        })
    return result


def main():
    print("=== Job 3: Silver → Gold ===")
    spark = create_spark()
    spark.sparkContext.setLogLevel("WARN")

    # --------------------------------------------------------
    # Load Silver latest
    # --------------------------------------------------------
    try:
        latest_df = spark.read.format("delta").load(SILVER_LATEST)
        latest_count = latest_df.count()
        print(f"Silver latest_indicators: {latest_count} rows")
        if latest_count == 0:
            print("No data in silver/latest_indicators, exiting")
            spark.stop()
            return
    except Exception as e:
        print(f"Cannot read silver/latest_indicators: {e}")
        spark.stop()
        return

    # Load master wilayah for geometry
    try:
        master_df = spark.read.format("delta").load(BRONZE_MASTER)
        has_master = True
    except Exception:
        has_master = False

    # --------------------------------------------------------
    # Compute risk score
    # --------------------------------------------------------
    scored_df = compute_risk_score_df(latest_df)
    scored_df = add_risk_level(scored_df)

    # --------------------------------------------------------
    # Try ML predictions if model exists
    # --------------------------------------------------------
    label_df = None
    try:
        from pyspark.ml import PipelineModel
        from pyspark.ml.functions import vector_to_array
        model = PipelineModel.load(MODELS_LATEST)
        feature_cols = [
            c for c in ["skor_bangunan", "skor_jalan", "skor_drainase",
                        "skor_air_limbah", "skor_sampah", "skor_kebakaran", "skor_air_minum",
                        "frekuensi_banjir", "jumlah_jiwa"]
            if c in scored_df.columns
        ]
        pred_input = scored_df.fillna(0, subset=feature_cols)
        predictions = model.transform(pred_input)
        probability_array = vector_to_array(F.col("probability"))
        label_df = predictions.select(
            "id_wilayah",
            F.col("prediction").cast(IntegerType()).alias("label_prediksi"),
            F.round(F.element_at(probability_array, 2), 4).alias("proba_kumuh")
        )
        print("ML predictions computed")
    except Exception as e:
        print(f"No ML model or prediction failed: {e}")
        # Fallback: predict kumuh if risk_score >= 50
        label_df = scored_df.select(
            "id_wilayah",
            F.when(F.col("risk_score") >= 50, 1).otherwise(0).alias("label_prediksi"),
            (F.col("risk_score") / 100).alias("proba_kumuh")
        )

    # --------------------------------------------------------
    # Build gold/slum_risk_score
    # --------------------------------------------------------
    gold_df = scored_df.join(label_df, on="id_wilayah", how="left")

    # Join with master for geometry and jiwa_terdampak
    if has_master:
        if "geometry_wkt" in gold_df.columns:
            gold_df = gold_df.drop("geometry_wkt")
        master_geo = master_df.select("id_wilayah", "geometry_wkt",
                                       F.col("total_jiwa").alias("jiwa_terdampak"))
        gold_df = gold_df.join(master_geo, on="id_wilayah", how="left")
    else:
        gold_df = gold_df.withColumn("geometry_wkt", F.lit(""))
        if "total_jiwa" in gold_df.columns:
            gold_df = gold_df.withColumnRenamed("total_jiwa", "jiwa_terdampak")
        else:
            gold_df = gold_df.withColumn("jiwa_terdampak", F.lit(0))

    # Add last_updated
    gold_df = gold_df.withColumn("last_updated", F.current_timestamp())

    # Add top factors
    top_factors_data = get_top_factors(gold_df)
    if top_factors_data:
        factors_df = spark.createDataFrame(top_factors_data)
        factors_select = factors_df.select("id_wilayah", "top_faktor_1", "top_faktor_2", "top_faktor_3")
        gold_df = gold_df.join(factors_select, on="id_wilayah", how="left")
    else:
        gold_df = gold_df.withColumn("top_faktor_1", F.lit("")) \
                         .withColumn("top_faktor_2", F.lit("")) \
                         .withColumn("top_faktor_3", F.lit(""))

    # Priority rank = risk_score × jiwa_terdampak (descending)
    gold_df = gold_df.withColumn(
        "priority_score",
        F.col("risk_score") * F.col("jiwa_terdampak").cast(FloatType())
    )
    window_rank = Window.orderBy(F.col("priority_score").desc())
    gold_df = gold_df.withColumn("prioritas_rank", F.rank().over(window_rank))

    print(f"Gold risk scores: {gold_df.count()} rows")
    gold_df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .save(GOLD_RISK)

    # --------------------------------------------------------
    # Build gold/slum_trend — compare last 2 events per wilayah
    # --------------------------------------------------------
    try:
        history_df = spark.read.format("delta").load(SILVER_HISTORY)
        history_scored = compute_risk_score_df(history_df)

        window_h = Window.partitionBy("id_wilayah").orderBy(F.col("recorded_at").desc())
        history_scored = history_scored.withColumn("_rn", F.row_number().over(window_h))

        latest_event = history_scored.filter(F.col("_rn") == 1) \
            .select("id_wilayah", F.col("risk_score").alias("risk_score_latest"),
                    F.col("recorded_at").alias("recorded_at_latest"))

        prev_event = history_scored.filter(F.col("_rn") == 2) \
            .select("id_wilayah", F.col("risk_score").alias("risk_score_prev"),
                    F.col("recorded_at").alias("recorded_at_prev"))

        trend_df = latest_event.join(prev_event, on="id_wilayah", how="left")
        trend_df = trend_df.withColumn(
            "delta_risk",
            (F.col("risk_score_latest") - F.coalesce(F.col("risk_score_prev"), F.col("risk_score_latest")))
        )
        trend_df = trend_df.withColumn(
            "trend_label",
            F.when(F.col("delta_risk") > 2, "Memburuk")
             .when(F.col("delta_risk") < -2, "Membaik")
             .otherwise("Stabil")
        )
        trend_df = trend_df.withColumn("computed_at", F.current_timestamp())

        trend_df.write \
            .format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .save(GOLD_TREND)
        print("Trend data written")
    except Exception as e:
        print(f"Cannot compute trend (need at least 2 events): {e}")

    # --------------------------------------------------------
    # Build gold/dominant_factors
    # --------------------------------------------------------
    if top_factors_data:
        factors_full_df = spark.createDataFrame(top_factors_data)
        factors_full_df = factors_full_df.withColumn("computed_at", F.current_timestamp())
        factors_full_df.write \
            .format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .save(GOLD_FACTORS)
        print("Dominant factors written")

    # --------------------------------------------------------
    # Build gold/intervention_priority
    # --------------------------------------------------------
    priority_df = gold_df.select(
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
        "last_updated"
    ).orderBy("prioritas_rank")

    priority_df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .save(GOLD_PRIORITY)

    print(f"Intervention priority: {priority_df.count()} rows")
    print("=== Job 3 Complete ===")
    spark.stop()


if __name__ == "__main__":
    main()
