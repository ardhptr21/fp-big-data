#!/usr/bin/env python3
"""
FastAPI Backend — Slum Analytics System
Geospatial Big Data Analytics untuk Pemetaan Permukiman Kumuh Surabaya
"""

import os
import json
import uuid
import asyncio
import logging
import subprocess
from datetime import datetime
from typing import Optional, List, Dict, Any
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, BackgroundTasks, UploadFile, File, Form
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel, Field

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

# ============================================================
# CONFIG
# ============================================================
HDFS_URL = os.environ.get("HDFS_URL", "hdfs://namenode:9000")
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC_SURVEY = os.environ.get("KAFKA_TOPIC_SURVEY", "survey-events")
TOPIC_SECONDARY = os.environ.get("KAFKA_TOPIC_SECONDARY", "secondary-batch")
SPARK_MASTER = os.environ.get("SPARK_MASTER", "spark://spark:7077")
SPARK_JOBS_PATH = os.environ.get("SPARK_JOBS_PATH", "/opt/spark_jobs")

# SSE event queue
sse_clients: List[asyncio.Queue] = []

# In-memory cache for spark results (to avoid repeated Spark reads)
_cache: Dict[str, Any] = {}
_cache_ts: Dict[str, float] = {}
CACHE_TTL = 30  # seconds

# ============================================================
# LIFESPAN
# ============================================================
@asynccontextmanager
async def lifespan(app: FastAPI):
    log.info("=== Slum Analytics API Starting ===")
    yield
    log.info("=== Slum Analytics API Shutting Down ===")


app = FastAPI(
    title="Slum Analytics API",
    description="Geospatial Big Data Analytics untuk Pemetaan Permukiman Kumuh Kota Surabaya",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ============================================================
# PYDANTIC MODELS
# ============================================================
class WilayahCreate(BaseModel):
    id_wilayah: str
    kota: str = "Surabaya"
    kecamatan: str
    kelurahan: str
    rw: str
    rt: str
    total_kk: int = 0
    total_jiwa: int = 0
    luas_m2: float = 0.0
    geometry_wkt: Optional[str] = None


class SurveyEvent(BaseModel):
    id_wilayah: str
    recorded_by: Optional[str] = ""
    skor_bangunan: int = Field(ge=0, le=3)
    skor_jalan: int = Field(ge=0, le=3)
    skor_drainase: int = Field(ge=0, le=3)
    skor_air_limbah: int = Field(ge=0, le=3)
    skor_sampah: int = Field(ge=0, le=3)
    skor_kebakaran: int = Field(ge=0, le=3)
    skor_air_minum: int = Field(ge=0, le=3)
    jumlah_kk: int = 0
    jumlah_jiwa: int = 0
    pernah_banjir: bool = False
    frekuensi_banjir: int = 0
    sosek_dominan: str = "menengah"
    catatan: Optional[str] = ""


# ============================================================
# HELPER: Kafka Producer
# ============================================================
def produce_kafka_message(topic: str, message: dict):
    """Send a message to Kafka topic."""
    try:
        from kafka import KafkaProducer
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BOOTSTRAP],
            value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
            request_timeout_ms=10000,
        )
        producer.send(topic, value=message)
        producer.flush()
        producer.close()
        log.info(f"Message sent to topic={topic}")
    except Exception as e:
        log.error(f"Kafka produce error: {e}")
        raise HTTPException(status_code=503, detail=f"Kafka unavailable: {e}")


# ============================================================
# HELPER: Spark / Delta Lake reads
# ============================================================
def get_spark():
    """Get or create SparkSession (lazy singleton)."""
    from pyspark.sql import SparkSession
    from delta import configure_spark_with_delta_pip

    builder = (
        SparkSession.builder
        .appName("SlumAPI")
        .master("local[2]")  # local mode - API runs Spark in-process
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.hadoop.fs.defaultFS", HDFS_URL)
        .config("spark.hadoop.dfs.client.use.datanode.hostname", "true")
        .config("spark.driver.memory", "512m")
        .config("spark.ui.enabled", "false")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


def read_delta(path: str) -> Optional[list]:
    """Read a Delta table and return as list of dicts. Returns None if table doesn't exist."""
    import time
    cache_key = path
    now = time.time()
    if cache_key in _cache and (now - _cache_ts.get(cache_key, 0)) < CACHE_TTL:
        return _cache[cache_key]

    try:
        spark = get_spark()
        df = spark.read.format("delta").load(path)
        result = [row.asDict() for row in df.collect()]
        _cache[cache_key] = result
        _cache_ts[cache_key] = now
        return result
    except Exception as e:
        log.warning(f"Cannot read Delta table at {path}: {e}")
        return None


def invalidate_cache():
    """Clear the in-memory cache after new data is processed."""
    _cache.clear()
    _cache_ts.clear()


# ============================================================
# HELPER: Broadcast SSE event
# ============================================================
async def broadcast_sse(event_type: str, data: dict = {}):
    """Send Server-Sent Event to all connected clients."""
    msg = json.dumps({"type": event_type, "timestamp": datetime.utcnow().isoformat(), **data})
    dead = []
    for q in sse_clients:
        try:
            await q.put(msg)
        except Exception:
            dead.append(q)
    for q in dead:
        if q in sse_clients:
            sse_clients.remove(q)


# ============================================================
# HELPER: Run Spark jobs
# ============================================================
def run_spark_job(job_file: str) -> bool:
    """Run a Spark job Python script directly (via python)."""
    job_path = os.path.join(SPARK_JOBS_PATH, job_file)
    if not os.path.exists(job_path):
        log.error(f"Spark job not found: {job_path}")
        return False

    env = os.environ.copy()
    env.update({
        "HDFS_URL": HDFS_URL,
        "SPARK_MASTER": SPARK_MASTER,
        "PYSPARK_PYTHON": "python3",
        "PYSPARK_DRIVER_PYTHON": "python3",
    })

    log.info(f"Running Spark job: {job_path}")
    try:
        result = subprocess.run(
            ["python3", job_path],
            capture_output=True,
            text=True,
            timeout=600,
            env=env,
        )
        if result.returncode == 0:
            log.info(f"Job {job_file} completed successfully")
            if result.stdout:
                log.info(f"Job stdout:\n{result.stdout[-1000:]}")
            return True
        else:
            log.error(f"Job {job_file} failed (exit {result.returncode}):\n{result.stderr[-2000:]}")
            return False
    except subprocess.TimeoutExpired:
        log.error(f"Job {job_file} timed out after 600s")
        return False
    except Exception as e:
        log.error(f"Job {job_file} exception: {e}")
        return False


# ============================================================
# BACKGROUND TASK: Full pipeline processing
# ============================================================
async def run_pipeline():
    """Run full processing pipeline: Bronze→Silver→Gold."""
    loop = asyncio.get_event_loop()
    await broadcast_sse("processing_started")
    log.info("=== Pipeline started ===")

    # Job 1: Bronze → Silver
    ok1 = await loop.run_in_executor(None, run_spark_job, "job1_bronze_silver.py")
    if ok1:
        log.info("Job 1 complete: Bronze → Silver")
    else:
        log.warning("Job 1 had issues, continuing...")

    # Job 2: Train ML model (only if ground truth exists)
    ok2 = await loop.run_in_executor(None, run_spark_job, "job2_train_model.py")
    if ok2:
        log.info("Job 2 complete: ML model trained")
    else:
        log.warning("Job 2 had issues (maybe no ground truth yet), continuing...")

    # Job 3: Silver → Gold
    ok3 = await loop.run_in_executor(None, run_spark_job, "job3_silver_gold.py")
    if ok3:
        log.info("Job 3 complete: Silver → Gold")
    else:
        log.warning("Job 3 had issues, continuing...")

    invalidate_cache()
    await broadcast_sse("map_updated", {"jobs_ok": [ok1, ok2, ok3]})
    log.info("=== Pipeline complete, SSE sent ===")


# ============================================================
# ROUTES: Health
# ============================================================
@app.get("/health")
async def health():
    return {"status": "ok", "timestamp": datetime.utcnow().isoformat()}


# ============================================================
# ROUTES: Wilayah (Master)
# ============================================================
@app.get("/api/wilayah")
async def get_wilayah():
    """Return all registered wilayah as tree structure."""
    path = f"{HDFS_URL}/data/bronze/master_wilayah"
    rows = read_delta(path)
    if rows is None:
        return {"wilayah": [], "total": 0}
    return {"wilayah": rows, "total": len(rows)}


@app.post("/api/wilayah", status_code=201)
async def create_wilayah(wilayah: WilayahCreate):
    """Register a new wilayah to master table."""
    try:
        from pyspark.sql import Row
        spark = get_spark()

        # Check if already exists
        path = f"{HDFS_URL}/data/bronze/master_wilayah"
        try:
            existing = spark.read.format("delta").load(path)
            count = existing.filter(existing.id_wilayah == wilayah.id_wilayah).count()
            if count > 0:
                raise HTTPException(status_code=409, detail=f"Wilayah {wilayah.id_wilayah} already exists")
        except Exception as e:
            if "409" in str(e):
                raise
            # Table doesn't exist yet — that's OK
            pass

        row = Row(
            id_wilayah=wilayah.id_wilayah,
            kota=wilayah.kota,
            kecamatan=wilayah.kecamatan,
            kelurahan=wilayah.kelurahan,
            rw=wilayah.rw,
            rt=wilayah.rt,
            total_kk=wilayah.total_kk,
            total_jiwa=wilayah.total_jiwa,
            luas_m2=wilayah.luas_m2,
            geometry_wkt=wilayah.geometry_wkt or "",
            created_at=datetime.utcnow(),
        )

        df = spark.createDataFrame([row])
        df.write.format("delta").mode("append").option("mergeSchema", "true").save(path)

        invalidate_cache()
        log.info(f"Wilayah registered: {wilayah.id_wilayah}")
        return {"status": "created", "id_wilayah": wilayah.id_wilayah}

    except HTTPException:
        raise
    except Exception as e:
        log.error(f"Error creating wilayah: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================
# ROUTES: Survey
# ============================================================
@app.post("/api/survey", status_code=201)
async def submit_survey(event: SurveyEvent, background_tasks: BackgroundTasks):
    """Submit a survey event — produces to Kafka, triggers pipeline."""
    event_id = str(uuid.uuid4())
    recorded_at = datetime.utcnow().isoformat()

    payload = event.model_dump()
    payload["event_id"] = event_id
    payload["recorded_at"] = recorded_at

    produce_kafka_message(TOPIC_SURVEY, payload)
    # Also trigger pipeline immediately for responsiveness
    background_tasks.add_task(run_pipeline)

    return {
        "status": "accepted",
        "event_id": event_id,
        "recorded_at": recorded_at,
        "message": "Data diterima, pipeline sedang berjalan..."
    }


@app.post("/api/secondary/upload", status_code=201)
async def upload_secondary(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...),
    source_type: str = Form("ground_truth")
):
    """Upload a secondary data CSV file (BPS, BMKG, PDAM, ground_truth)."""
    import csv
    import io

    content = await file.read()
    text = content.decode("utf-8")
    reader = csv.DictReader(io.StringIO(text))

    count = 0
    for row in reader:
        payload = dict(row)
        payload["batch_id"] = str(uuid.uuid4())
        payload["source_type"] = source_type
        payload["ingested_at"] = datetime.utcnow().isoformat()
        produce_kafka_message(TOPIC_SECONDARY, payload)
        count += 1

    background_tasks.add_task(run_pipeline)

    return {
        "status": "accepted",
        "rows_ingested": count,
        "source_type": source_type
    }


# ============================================================
# ROUTES: Map (GeoJSON for Leaflet.js)
# ============================================================
@app.get("/api/map/risk-score")
async def get_map_risk_score():
    """Return GeoJSON FeatureCollection with risk scores per wilayah."""
    gold_path = f"{HDFS_URL}/data/gold/slum_risk_score"
    wilayah_path = f"{HDFS_URL}/data/bronze/master_wilayah"

    gold_rows = read_delta(gold_path)
    wilayah_rows = read_delta(wilayah_path)

    if not wilayah_rows:
        return {"type": "FeatureCollection", "features": []}

    # Build a dict from gold data for quick lookup
    gold_map = {}
    if gold_rows:
        for r in gold_rows:
            gold_map[r.get("id_wilayah", "")] = r

    features = []
    for w in wilayah_rows:
        wid = w.get("id_wilayah", "")
        geom = w.get("geometry_wkt", "")
        if not geom:
            continue

        try:
            geometry = json.loads(geom)
        except Exception:
            continue

        gold = gold_map.get(wid, {})
        props = {
            "id_wilayah": wid,
            "kelurahan": w.get("kelurahan", ""),
            "kecamatan": w.get("kecamatan", ""),
            "rt": w.get("rt", ""),
            "rw": w.get("rw", ""),
            "total_jiwa": w.get("total_jiwa", 0),
            "risk_score": gold.get("risk_score", None),
            "risk_level": gold.get("risk_level", "Belum Didata"),
            "proba_kumuh": gold.get("proba_kumuh", None),
            "label_prediksi": gold.get("label_prediksi", None),
            "top_faktor_1": gold.get("top_faktor_1", ""),
            "top_faktor_2": gold.get("top_faktor_2", ""),
            "top_faktor_3": gold.get("top_faktor_3", ""),
            "last_updated": str(gold.get("last_updated", "")),
            "prioritas_rank": gold.get("prioritas_rank", None),
        }

        features.append({
            "type": "Feature",
            "geometry": geometry,
            "properties": props
        })

    return {"type": "FeatureCollection", "features": features}


@app.get("/api/map/prediction")
async def get_map_prediction():
    """Return GeoJSON with prediction labels."""
    return await get_map_risk_score()  # same data, frontend filters what to show


# ============================================================
# ROUTES: Analytics per Wilayah
# ============================================================
@app.get("/api/wilayah/{id_wilayah}/latest")
async def get_wilayah_latest(id_wilayah: str):
    """Get the latest survey indicators for a wilayah."""
    path = f"{HDFS_URL}/data/silver/latest_indicators"
    rows = read_delta(path)
    if rows is None:
        raise HTTPException(status_code=404, detail="No data available yet")
    found = [r for r in rows if r.get("id_wilayah") == id_wilayah]
    if not found:
        raise HTTPException(status_code=404, detail=f"No data for {id_wilayah}")
    return found[0]


@app.get("/api/wilayah/{id_wilayah}/history")
async def get_wilayah_history(id_wilayah: str):
    """Get ALL historical events for a wilayah — incremental log."""
    path = f"{HDFS_URL}/data/silver/event_history"
    rows = read_delta(path)
    if rows is None:
        # Try bronze directly
        path = f"{HDFS_URL}/data/bronze/survey_events"
        rows = read_delta(path)

    if rows is None:
        return {"id_wilayah": id_wilayah, "history": [], "total": 0}

    history = sorted(
        [r for r in rows if r.get("id_wilayah") == id_wilayah],
        key=lambda x: str(x.get("recorded_at", "")),
        reverse=True
    )
    return {
        "id_wilayah": id_wilayah,
        "history": history,
        "total": len(history)
    }


@app.get("/api/wilayah/{id_wilayah}/trend")
async def get_wilayah_trend(id_wilayah: str):
    """Get risk score trend over time for a wilayah."""
    path = f"{HDFS_URL}/data/gold/slum_trend"
    rows = read_delta(path)
    if rows is None:
        return {"id_wilayah": id_wilayah, "trend": []}
    trend = [r for r in rows if r.get("id_wilayah") == id_wilayah]
    return {"id_wilayah": id_wilayah, "trend": trend}


@app.get("/api/kelurahan/{id_kelurahan}/factors")
async def get_kelurahan_factors(id_kelurahan: str):
    """Get top-3 dominant factors for a kelurahan."""
    path = f"{HDFS_URL}/data/gold/dominant_factors"
    rows = read_delta(path)
    if rows is None:
        raise HTTPException(status_code=404, detail="No factor data available yet")
    found = [r for r in rows if r.get("id_kelurahan") == id_kelurahan or r.get("kelurahan") == id_kelurahan]
    if not found:
        raise HTTPException(status_code=404, detail=f"No factors for {id_kelurahan}")
    return found[0]


@app.get("/api/priority")
async def get_priority():
    """Get intervention priority ranking."""
    path = f"{HDFS_URL}/data/gold/intervention_priority"
    rows = read_delta(path)
    if rows is None:
        return {"priority": [], "total": 0}
    sorted_rows = sorted(rows, key=lambda x: x.get("prioritas_rank", 9999))
    return {"priority": sorted_rows, "total": len(sorted_rows)}


@app.get("/api/summary")
async def get_summary():
    """Dashboard summary statistics."""
    gold_path = f"{HDFS_URL}/data/gold/slum_risk_score"
    wilayah_path = f"{HDFS_URL}/data/bronze/master_wilayah"
    bronze_path = f"{HDFS_URL}/data/bronze/survey_events"

    gold_rows = read_delta(gold_path) or []
    wilayah_rows = read_delta(wilayah_path) or []
    survey_rows = read_delta(bronze_path) or []

    total_wilayah = len(wilayah_rows)
    total_kumuh = sum(1 for r in gold_rows if r.get("label_prediksi") == 1)
    total_jiwa = sum(r.get("jiwa_terdampak", 0) or 0 for r in gold_rows if r.get("label_prediksi") == 1)
    total_survey_events = len(survey_rows)

    level_counts = {}
    for r in gold_rows:
        lvl = r.get("risk_level", "Belum Didata")
        level_counts[lvl] = level_counts.get(lvl, 0) + 1

    return {
        "total_wilayah": total_wilayah,
        "total_kumuh": total_kumuh,
        "total_jiwa_terdampak": total_jiwa,
        "total_survey_events": total_survey_events,
        "risk_level_breakdown": level_counts,
        "last_updated": datetime.utcnow().isoformat(),
    }


# ============================================================
# ROUTES: Real-time SSE
# ============================================================
@app.get("/api/stream/updates")
async def stream_updates():
    """Server-Sent Events stream for real-time map updates."""
    queue: asyncio.Queue = asyncio.Queue()
    sse_clients.append(queue)

    async def event_generator():
        try:
            # Send initial connection event
            yield f"data: {json.dumps({'type': 'connected', 'timestamp': datetime.utcnow().isoformat()})}\n\n"
            while True:
                try:
                    msg = await asyncio.wait_for(queue.get(), timeout=30)
                    yield f"data: {msg}\n\n"
                except asyncio.TimeoutError:
                    # Heartbeat to keep connection alive
                    yield f"data: {json.dumps({'type': 'heartbeat', 'timestamp': datetime.utcnow().isoformat()})}\n\n"
        except asyncio.CancelledError:
            pass
        finally:
            if queue in sse_clients:
                sse_clients.remove(queue)

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        }
    )


# ============================================================
# ROUTES: Internal (called by consumer)
# ============================================================
@app.post("/api/internal/trigger-processing")
async def trigger_processing(background_tasks: BackgroundTasks):
    """Called by Kafka consumer after writing to Bronze. Triggers Spark pipeline."""
    background_tasks.add_task(run_pipeline)
    return {"status": "triggered", "timestamp": datetime.utcnow().isoformat()}
