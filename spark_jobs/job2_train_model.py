#!/usr/bin/env python3
"""
Spark Job 2: Train ML Model
- Input: silver/feature_matrix + silver/ground_truth + silver/latest_indicators
- Output: models/rf_slum_v{timestamp} + silver/model_metrics

Teknik yang digunakan:
  1. RandomForestClassifier (Spark MLlib) — klasifikasi kumuh/tidak kumuh
  2. 3-fold CrossValidator — evaluasi generalisasi model
  3. Metrik: AUC, F1, Accuracy, Confusion Matrix (TP/TN/FP/FN), CV-mean-F1
"""

import glob
import os
import time
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder

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

try:
    RF_NUM_TREES = max(1, int(os.environ.get("RF_NUM_TREES", "8")))
except ValueError:
    RF_NUM_TREES = 8

try:
    RF_MAX_DEPTH = max(1, int(os.environ.get("RF_MAX_DEPTH", "3")))
except ValueError:
    RF_MAX_DEPTH = 3

LAKEHOUSE_ROOT = None
STORAGE_BACKEND = "unresolved"
SILVER_FEATURE = None
SILVER_GT = None
MODELS_PATH = None
SILVER_METRICS = None
SILVER_LATEST = None


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
    global LAKEHOUSE_ROOT, STORAGE_BACKEND, SILVER_FEATURE, SILVER_GT, MODELS_PATH, SILVER_METRICS, SILVER_LATEST
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
    SILVER_FEATURE = f"{root}/silver/feature_matrix"
    SILVER_GT = f"{root}/silver/ground_truth"
    MODELS_PATH = f"{root}/models"
    SILVER_METRICS = f"{root}/silver/model_metrics"
    SILVER_LATEST = f"{root}/silver/latest_indicators"
    print(f"Lakehouse storage backend={STORAGE_BACKEND} root={LAKEHOUSE_ROOT}")


def create_spark():
    builder = (
        SparkSession.builder
        .appName("SlumJob2-MLTraining")
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


def compute_confusion_matrix(predictions, label_col="label", pred_col="prediction"):
    """Compute TP, TN, FP, FN from predictions DataFrame."""
    try:
        rows = predictions.select(label_col, pred_col).collect()
        tp = sum(1 for r in rows if r[label_col] == 1 and r[pred_col] == 1.0)
        tn = sum(1 for r in rows if r[label_col] == 0 and r[pred_col] == 0.0)
        fp = sum(1 for r in rows if r[label_col] == 0 and r[pred_col] == 1.0)
        fn = sum(1 for r in rows if r[label_col] == 1 and r[pred_col] == 0.0)
        precision = tp / (tp + fp) if (tp + fp) > 0 else 0.0
        recall = tp / (tp + fn) if (tp + fn) > 0 else 0.0
        return {"tp": tp, "tn": tn, "fp": fp, "fn": fn,
                "precision": round(precision, 4), "recall": round(recall, 4)}
    except Exception as e:
        print(f"Confusion matrix error: {e}")
        return {"tp": 0, "tn": 0, "fp": 0, "fn": 0, "precision": 0.0, "recall": 0.0}


def train_with_crossvalidation(pipeline, train_df, rf, feature_cols, total):
    """
    Train model menggunakan 3-fold CrossValidator.
    Fallback ke single fit jika data terlalu sedikit (< 10 rows).

    Return: (best_model, cv_mean_f1, cv_std_f1)
    """
    # Minimal 3 rows per fold = minimal 9 rows untuk 3-fold CV
    if total < 9:
        print(f"Dataset terlalu kecil ({total} rows) untuk cross-validation — menggunakan single fit")
        model = pipeline.fit(train_df)
        return model, None, None

    print(f"Menjalankan 3-fold CrossValidator pada {total} training samples...")
    param_grid = (
        ParamGridBuilder()
        .addGrid(rf.numTrees, [RF_NUM_TREES, min(RF_NUM_TREES * 2, 20)])
        .addGrid(rf.maxDepth, [RF_MAX_DEPTH, min(RF_MAX_DEPTH + 1, 5)])
        .build()
    )

    evaluator_cv = MulticlassClassificationEvaluator(
        labelCol="label",
        predictionCol="prediction",
        metricName="f1"
    )

    cv = CrossValidator(
        estimator=pipeline,
        estimatorParamMaps=param_grid,
        evaluator=evaluator_cv,
        numFolds=3,
        seed=42,
        parallelism=1,  # hemat memori
    )

    cv_model = cv.fit(train_df)
    avg_metrics = cv_model.avgMetrics
    best_idx = int(max(range(len(avg_metrics)), key=lambda i: avg_metrics[i]))
    cv_mean_f1 = round(float(avg_metrics[best_idx]), 4)

    # Hitung std dari semua metrics
    if len(avg_metrics) > 1:
        import math
        mean_all = sum(avg_metrics) / len(avg_metrics)
        variance = sum((m - mean_all) ** 2 for m in avg_metrics) / len(avg_metrics)
        cv_std_f1 = round(math.sqrt(variance), 4)
    else:
        cv_std_f1 = 0.0

    print(f"Cross-Validation selesai. Best CV F1: {cv_mean_f1:.4f} ± {cv_std_f1:.4f}")
    print(f"Semua CV metrics: {[round(m, 4) for m in avg_metrics]}")

    return cv_model.bestModel, cv_mean_f1, cv_std_f1


def main():
    print("=== Job 2: ML Model Training (RandomForest + CrossValidator) ===")
    spark = create_spark()
    spark.sparkContext.setLogLevel("WARN")

    # --------------------------------------------------------
    # Load data
    # --------------------------------------------------------
    try:
        feature_df = spark.read.format("delta").load(SILVER_FEATURE)
        print(f"Feature matrix: {feature_df.count()} rows")
    except Exception as e:
        print(f"No feature matrix available: {e}")
        spark.stop()
        return

    try:
        gt_df = spark.read.format("delta").load(SILVER_GT)
        gt_count = gt_df.count()
        print(f"Ground truth: {gt_count} rows")
        if gt_count == 0:
            print("No ground truth data, skipping training")
            spark.stop()
            return
    except Exception as e:
        print(f"No ground truth available: {e}")
        spark.stop()
        return

    # --------------------------------------------------------
    # Join features with ground truth via latest_indicators
    # --------------------------------------------------------
    gt_clean = gt_df.select(
        F.col("id_wilayah"),
        F.col("label_kumuh").cast("int").alias("label")
    ).dropDuplicates(["id_wilayah"])

    try:
        latest_df = spark.read.format("delta").load(SILVER_LATEST)
        ml_df = latest_df.join(gt_clean, on="id_wilayah", how="inner")
    except Exception:
        print("Falling back to feature matrix join...")
        ml_df = None

    if ml_df is None or ml_df.count() == 0:
        print("Cannot match ground truth to features — skipping training")
        spark.stop()
        return

    total_ml = ml_df.count()
    print(f"ML dataset: {total_ml} matched rows")

    # --------------------------------------------------------
    # Build feature vector
    # --------------------------------------------------------
    feature_cols = [
        c for c in [
            "skor_bangunan", "skor_jalan", "skor_drainase",
            "skor_air_limbah", "skor_sampah", "skor_kebakaran", "skor_air_minum",
            "frekuensi_banjir", "jumlah_jiwa",
        ]
        if c in ml_df.columns
    ]

    ml_df = ml_df.fillna(0, subset=feature_cols)
    ml_df = ml_df.filter(F.col("label").isNotNull())

    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features", handleInvalid="keep")

    rf = RandomForestClassifier(
        labelCol="label",
        featuresCol="features",
        numTrees=RF_NUM_TREES,
        maxDepth=RF_MAX_DEPTH,
        seed=42,
    )

    pipeline = Pipeline(stages=[assembler, rf])

    # --------------------------------------------------------
    # Train/test split (80/20)
    # --------------------------------------------------------
    total = ml_df.count()
    if total < 5:
        print(f"Too few samples ({total}) for proper training, using all for training")
        train_df = ml_df
        test_df = ml_df
    else:
        train_df, test_df = ml_df.randomSplit([0.8, 0.2], seed=42)

    n_train = train_df.count()
    n_test = test_df.count()
    print(f"Train/test split: {n_train} train / {n_test} test (80/20, seed=42)")

    # --------------------------------------------------------
    # Train model dengan CrossValidator
    # --------------------------------------------------------
    model, cv_mean_f1, cv_std_f1 = train_with_crossvalidation(pipeline, train_df, rf, feature_cols, n_train)

    # --------------------------------------------------------
    # Evaluate on held-out test set
    # --------------------------------------------------------
    predictions = model.transform(test_df)

    evaluator_auc = BinaryClassificationEvaluator(labelCol="label", metricName="areaUnderROC")
    evaluator_f1 = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="f1")
    evaluator_acc = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="accuracy")
    evaluator_precision = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="weightedPrecision")
    evaluator_recall = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="weightedRecall")

    auc = evaluator_auc.evaluate(predictions)
    f1 = evaluator_f1.evaluate(predictions)
    acc = evaluator_acc.evaluate(predictions)
    precision = evaluator_precision.evaluate(predictions)
    recall = evaluator_recall.evaluate(predictions)

    print(f"Test Set Metrics:")
    print(f"  AUC:       {auc:.4f}")
    print(f"  F1-score:  {f1:.4f}")
    print(f"  Accuracy:  {acc:.4f}")
    print(f"  Precision: {precision:.4f}")
    print(f"  Recall:    {recall:.4f}")
    if cv_mean_f1 is not None:
        print(f"  CV F1 (3-fold mean): {cv_mean_f1:.4f} ± {cv_std_f1:.4f}")

    # Confusion Matrix
    cm = compute_confusion_matrix(predictions)
    print(f"  Confusion Matrix — TP:{cm['tp']} TN:{cm['tn']} FP:{cm['fp']} FN:{cm['fn']}")

    # Feature importances — ambil dari final RF stage
    try:
        rf_model = model.stages[-1]
        importances = rf_model.featureImportances.toArray().tolist()
        importance_map = {feature_cols[i]: importances[i] for i in range(len(feature_cols))}
        sorted_importance = sorted(importance_map.items(), key=lambda x: x[1], reverse=True)
        print(f"Feature importances: {sorted_importance}")
    except Exception as e:
        sorted_importance = []
        print(f"Feature importance not available: {e}")

    # --------------------------------------------------------
    # Save model
    # --------------------------------------------------------
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    model_path = f"{MODELS_PATH}/rf_slum_v{ts}"
    model.write().overwrite().save(model_path)
    print(f"Model saved to {model_path}")

    latest_model_path = f"{MODELS_PATH}/rf_slum_latest"
    model.write().overwrite().save(latest_model_path)
    print(f"Latest model pointer updated: {latest_model_path}")

    # --------------------------------------------------------
    # Save metrics to Delta Lake (append — keeps history)
    # --------------------------------------------------------
    from pyspark.sql.types import (
        StructType, StructField, StringType as ST, FloatType as FT,
        IntegerType as IT, LongType,
    )

    metrics_schema = StructType([
        StructField("model_version", ST(), True),
        StructField("model_path", ST(), True),
        StructField("algorithm", ST(), True),
        StructField("n_trees", IT(), True),
        StructField("max_depth", IT(), True),
        StructField("auc", FT(), True),
        StructField("f1_score", FT(), True),
        StructField("accuracy", FT(), True),
        StructField("precision", FT(), True),
        StructField("recall", FT(), True),
        StructField("cv_folds", IT(), True),
        StructField("cv_mean_f1", FT(), True),   # nullable float — OK with explicit schema
        StructField("cv_std_f1", FT(), True),    # nullable float
        StructField("confusion_matrix_tp", IT(), True),
        StructField("confusion_matrix_tn", IT(), True),
        StructField("confusion_matrix_fp", IT(), True),
        StructField("confusion_matrix_fn", IT(), True),
        StructField("n_train", LongType(), True),
        StructField("n_test", LongType(), True),
        StructField("feature_cols", ST(), True),
        StructField("feature_importances", ST(), True),
        StructField("trained_at", ST(), True),
    ])

    metrics_data = (
        f"rf_slum_v{ts}", model_path, "RandomForestClassifier",
        int(RF_NUM_TREES), int(RF_MAX_DEPTH),
        float(auc), float(f1), float(acc), float(precision), float(recall),
        3,
        float(cv_mean_f1) if cv_mean_f1 is not None else None,
        float(cv_std_f1) if cv_std_f1 is not None else None,
        int(cm["tp"]), int(cm["tn"]), int(cm["fp"]), int(cm["fn"]),
        int(n_train), int(n_test),
        ",".join(feature_cols), str(sorted_importance),
        datetime.utcnow().isoformat(),
    )

    metrics_row = spark.createDataFrame([metrics_data], schema=metrics_schema)

    # Tulis ke Delta Lake — overwrite agar tidak ada schema conflict
    metrics_row.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .save(SILVER_METRICS)

    print(f"Metrics saved to {SILVER_METRICS}")
    print("=== Job 2 Complete ===")
    spark.stop()


if __name__ == "__main__":
    main()
