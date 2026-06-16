#!/usr/bin/env python3
"""
Spark Job 2: Train ML Model
- Input: silver/feature_matrix + silver/ground_truth
- Output: models/rf_slum_v{timestamp} + silver/model_metrics

Uses Spark MLlib RandomForestClassifier.
Only re-trains if ground truth data is available.
"""

import os
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from delta import configure_spark_with_delta_pip

HDFS_URL = os.environ.get("HDFS_URL", "hdfs://namenode:9000")
SPARK_MASTER = os.environ.get("SPARK_MASTER", "local[*]")
if SPARK_MASTER.startswith("spark://"):
    SPARK_MASTER = "local[*]"

SILVER_FEATURE = f"{HDFS_URL}/data/silver/feature_matrix"
SILVER_GT = f"{HDFS_URL}/data/silver/ground_truth"
MODELS_PATH = f"{HDFS_URL}/data/models"
SILVER_METRICS = f"{HDFS_URL}/data/silver/model_metrics"


def create_spark():
    builder = (
        SparkSession.builder
        .appName("SlumJob2-MLTraining")
        .master(SPARK_MASTER)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.hadoop.fs.defaultFS", HDFS_URL)
        .config("spark.hadoop.dfs.client.use.datanode.hostname", "true")
        .config("spark.driver.memory", "2g")
        .config("spark.executor.memory", "2g")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


def main():
    print("=== Job 2: ML Model Training ===")
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
    # Join features with ground truth
    # We join on kelurahan name (from ground truth id_wilayah prefix)
    # --------------------------------------------------------
    # Ground truth has id_wilayah, feature has kelurahan
    # Try to join on kelurahan
    gt_clean = gt_df.select(
        F.col("id_wilayah"),
        F.col("label_kumuh").cast("int").alias("label")
    ).dropDuplicates(["id_wilayah"])

    # Also join with latest indicators to get per-RT features matched to ground truth
    try:
        latest_df = spark.read.format("delta").load(f"{HDFS_URL}/data/silver/latest_indicators")
        ml_df = latest_df.join(gt_clean, on="id_wilayah", how="inner")
    except Exception:
        # Fall back to feature_matrix joined by kelurahan if possible
        print("Falling back to feature matrix join...")
        ml_df = None

    if ml_df is None or ml_df.count() == 0:
        print("Cannot match ground truth to features, using synthetic data")
        spark.stop()
        return

    print(f"ML dataset: {ml_df.count()} matched rows")

    # --------------------------------------------------------
    # Build feature vector
    # --------------------------------------------------------
    feature_cols = [
        c for c in ["skor_bangunan", "skor_jalan", "skor_drainase",
                    "skor_air_limbah", "skor_sampah", "skor_kebakaran", "skor_air_minum",
                    "frekuensi_banjir", "jumlah_jiwa"]
        if c in ml_df.columns
    ]

    # Fill nulls
    ml_df = ml_df.fillna(0, subset=feature_cols)
    ml_df = ml_df.filter(F.col("label").isNotNull())

    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features", handleInvalid="keep")

    rf = RandomForestClassifier(
        labelCol="label",
        featuresCol="features",
        numTrees=50,
        maxDepth=5,
        seed=42
    )

    pipeline = Pipeline(stages=[assembler, rf])

    # --------------------------------------------------------
    # Train/test split
    # --------------------------------------------------------
    total = ml_df.count()
    if total < 5:
        print(f"Too few samples ({total}) for proper training, using all for training")
        train_df = ml_df
        test_df = ml_df
    else:
        train_df, test_df = ml_df.randomSplit([0.8, 0.2], seed=42)

    print(f"Training on {train_df.count()} samples, testing on {test_df.count()} samples")

    # --------------------------------------------------------
    # Train model
    # --------------------------------------------------------
    model = pipeline.fit(train_df)
    rf_model = model.stages[-1]

    # --------------------------------------------------------
    # Evaluate
    # --------------------------------------------------------
    predictions = model.transform(test_df)

    evaluator_auc = BinaryClassificationEvaluator(labelCol="label", metricName="areaUnderROC")
    evaluator_f1 = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="f1")
    evaluator_acc = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="accuracy")

    auc = evaluator_auc.evaluate(predictions)
    f1 = evaluator_f1.evaluate(predictions)
    acc = evaluator_acc.evaluate(predictions)

    print(f"AUC: {auc:.4f}, F1: {f1:.4f}, Accuracy: {acc:.4f}")

    # Feature importances
    importances = rf_model.featureImportances.toArray().tolist()
    importance_map = {feature_cols[i]: importances[i] for i in range(len(feature_cols))}
    sorted_importance = sorted(importance_map.items(), key=lambda x: x[1], reverse=True)
    print("Feature importances:", sorted_importance)

    # --------------------------------------------------------
    # Save model
    # --------------------------------------------------------
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    model_path = f"{MODELS_PATH}/rf_slum_v{ts}"
    model.write().overwrite().save(model_path)
    print(f"Model saved to {model_path}")

    # Save pointer to latest model
    latest_model_path = f"{MODELS_PATH}/rf_slum_latest"
    model.write().overwrite().save(latest_model_path)

    # --------------------------------------------------------
    # Save metrics to Delta Lake
    # --------------------------------------------------------
    metrics_row = spark.createDataFrame([{
        "model_version": f"rf_slum_v{ts}",
        "model_path": model_path,
        "auc": float(auc),
        "f1_score": float(f1),
        "accuracy": float(acc),
        "n_train": int(train_df.count()),
        "n_test": int(test_df.count()),
        "feature_cols": ",".join(feature_cols),
        "feature_importances": str(sorted_importance),
        "trained_at": datetime.utcnow().isoformat(),
    }])

    metrics_row.write \
        .format("delta") \
        .mode("append") \
        .option("mergeSchema", "true") \
        .save(SILVER_METRICS)

    print("=== Job 2 Complete ===")
    spark.stop()


if __name__ == "__main__":
    main()
