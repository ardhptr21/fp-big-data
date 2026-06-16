#!/bin/bash
# ============================================================
# HDFS Initialization Script
# Creates all required directories for the Delta Lake Lakehouse
# ============================================================

set -e

echo "=== Waiting for HDFS to be ready ==="
sleep 15

HDFS_CMD="hdfs dfs"

echo "=== Creating HDFS directory structure ==="

# Bronze layer
$HDFS_CMD -mkdir -p /data/bronze/master_wilayah
$HDFS_CMD -mkdir -p /data/bronze/survey_events
$HDFS_CMD -mkdir -p /data/bronze/secondary_sources

# Silver layer
$HDFS_CMD -mkdir -p /data/silver/latest_indicators
$HDFS_CMD -mkdir -p /data/silver/event_history
$HDFS_CMD -mkdir -p /data/silver/feature_matrix
$HDFS_CMD -mkdir -p /data/silver/ground_truth
$HDFS_CMD -mkdir -p /data/silver/population

# Gold layer
$HDFS_CMD -mkdir -p /data/gold/slum_risk_score
$HDFS_CMD -mkdir -p /data/gold/slum_prediction
$HDFS_CMD -mkdir -p /data/gold/slum_trend
$HDFS_CMD -mkdir -p /data/gold/dominant_factors
$HDFS_CMD -mkdir -p /data/gold/intervention_priority

# Models
$HDFS_CMD -mkdir -p /data/models

# Set permissions
$HDFS_CMD -chmod -R 777 /data

echo "=== HDFS directory structure created successfully ==="
$HDFS_CMD -ls -R /data
