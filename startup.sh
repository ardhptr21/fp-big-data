#!/bin/bash
# =============================================================
# startup.sh — One-shot startup script for SlumMap Surabaya
# Runs docker compose up and waits for all services to be ready
# =============================================================

set -e

COMPOSE="docker compose"
PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"

echo "======================================================"
echo " SlumMap Surabaya — Big Data Lakehouse"
echo " Starting all services..."
echo "======================================================"

cd "$PROJECT_DIR"

# Pull images first
echo ""
echo "[1/5] Pulling Docker images..."
$COMPOSE pull --ignore-pull-failures 2>/dev/null || true

# Build custom images
echo ""
echo "[2/5] Building custom images (api, consumer, frontend)..."
$COMPOSE build --parallel

# Start infrastructure first
echo ""
echo "[3/5] Starting infrastructure (HDFS, Kafka, Spark)..."
$COMPOSE up -d namenode datanode zookeeper kafka spark spark-worker

echo "   Waiting for HDFS NameNode..."
until curl -sf http://localhost:9870 > /dev/null 2>&1; do
  printf "."
  sleep 5
done
echo " Ready!"

echo "   Waiting for Kafka..."
sleep 15

# Initialize HDFS directories
echo ""
echo "[4/5] Initializing HDFS directory structure..."
$COMPOSE up hdfs-init
echo "   HDFS initialized!"

# Start application services
echo ""
echo "[5/5] Starting application services (API, Consumer, Frontend)..."
$COMPOSE up -d consumer api frontend

echo ""
echo "   Waiting for API to be ready..."
until curl -sf http://localhost:8000/health > /dev/null 2>&1; do
  printf "."
  sleep 3
done
echo " Ready!"

# Seed initial data
echo ""
echo "======================================================"
echo " Seeding initial data..."
echo "======================================================"
$COMPOSE exec -T api pip install requests -q 2>/dev/null || true
docker run --rm --network "$(basename $PROJECT_DIR)_slum_net" \
  -e API_URL=http://api:8000 \
  -v "$PROJECT_DIR/init_scripts:/init_scripts" \
  python:3.11-slim \
  python /init_scripts/seed_wilayah.py 2>&1 | tail -30 || true

echo ""
echo "======================================================"
echo " ✅ All services are running!"
echo "======================================================"
echo ""
echo " Service URLs:"
echo "   🌐 Frontend (Web UI):  http://localhost:3000"
echo "   🔌 API (FastAPI):      http://localhost:8000"
echo "   📚 API Docs:           http://localhost:8000/docs"
echo "   🐘 HDFS Web UI:        http://localhost:9870"
echo "   ⚡ Spark Web UI:        http://localhost:8080"
echo "   📨 Kafka:              localhost:9092"
echo ""
echo " Quick Test:"
echo "   curl http://localhost:8000/health"
echo "   curl http://localhost:8000/api/wilayah"
echo ""
echo " To stop all services:"
echo "   docker compose down"
echo ""
echo " To view logs:"
echo "   docker compose logs -f api"
echo "   docker compose logs -f consumer"
echo ""
