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
import math
import time
from datetime import datetime
from typing import Optional, List, Dict, Any
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, UploadFile, File, Form, Query, Body
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
SPARK_DRIVER_MEMORY = os.environ.get("SPARK_DRIVER_MEMORY", "768m")
SPARK_EXECUTOR_MEMORY = os.environ.get("SPARK_EXECUTOR_MEMORY", "768m")
SPARK_SHUFFLE_PARTITIONS = os.environ.get("SPARK_SHUFFLE_PARTITIONS", "4")

try:
    SPARK_LOCAL_THREADS = max(1, int(os.environ.get("SPARK_LOCAL_THREADS", "1")))
except ValueError:
    SPARK_LOCAL_THREADS = 1

try:
    PIPELINE_DEBOUNCE_SECONDS = max(0.0, float(os.environ.get("PIPELINE_DEBOUNCE_SECONDS", "15")))
except ValueError:
    PIPELINE_DEBOUNCE_SECONDS = 15.0

# SSE event queue
sse_clients: List[asyncio.Queue] = []

# In-memory cache for spark results (to avoid repeated Spark reads)
_cache: Dict[str, Any] = {}
_cache_ts: Dict[str, float] = {}
CACHE_TTL = 30  # seconds

# Single-flight Spark pipeline state. Requests are debounced and coalesced so
# low-memory machines do not run multiple PySpark driver processes at once.
_pipeline_lock = asyncio.Lock()
_pipeline_task: Optional[asyncio.Task] = None
_pipeline_pending = False
_pipeline_last_requested = 0.0
_pipeline_affected_ids: set[str] = set()
_pipeline_state: Dict[str, Any] = {
    "state": "idle",
    "running": False,
    "pending": False,
    "version": 0,
    "last_requested_at": None,
    "last_started_at": None,
    "last_completed_at": None,
    "last_error": None,
    "jobs_ok": {},
    "affected_ids": [],
}

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


class PipelineTrigger(BaseModel):
    affected_ids: List[str] = Field(default_factory=list)


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
        .master(f"local[{SPARK_LOCAL_THREADS}]")  # local mode - API runs Spark in-process
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.hadoop.fs.defaultFS", HDFS_URL)
        .config("spark.hadoop.dfs.client.use.datanode.hostname", "true")
        .config("spark.driver.memory", "512m")
        .config("spark.driver.maxResultSize", "128m")
        .config("spark.sql.shuffle.partitions", SPARK_SHUFFLE_PARTITIONS)
        .config("spark.default.parallelism", max(1, int(SPARK_LOCAL_THREADS)))
        .config("spark.python.worker.memory", "256m")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
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


def stop_api_spark():
    """Stop the API-owned SparkSession before launching heavier pipeline jobs."""
    try:
        from pyspark.sql import SparkSession
        spark = SparkSession.getActiveSession()
        if spark is not None:
            log.info("Stopping API Spark session before pipeline run")
            spark.stop()
    except Exception as e:
        log.warning("Could not stop API Spark session: %s", e)


def filter_wilayah_rows(
    rows: list,
    kecamatan: Optional[str] = None,
    kelurahan: Optional[str] = None,
    rw: Optional[str] = None,
    rt: Optional[str] = None,
    q: Optional[str] = None,
) -> list:
    """Apply lightweight master wilayah filters after the cached Delta read."""
    filtered = rows
    for key, value in {
        "kecamatan": kecamatan,
        "kelurahan": kelurahan,
        "rw": rw,
        "rt": rt,
    }.items():
        if value:
            filtered = [r for r in filtered if str(r.get(key, "")) == str(value)]

    if q:
        needle = q.lower()
        filtered = [
            r for r in filtered
            if needle in " ".join(str(r.get(k, "")) for k in (
                "id_wilayah", "kecamatan", "kelurahan", "rw", "rt"
            )).lower()
        ]

    return filtered


def parse_bbox_param(bbox: Optional[str]) -> Optional[tuple]:
    """Parse bbox=minLng,minLat,maxLng,maxLat."""
    if not bbox:
        return None
    try:
        parts = [float(v.strip()) for v in bbox.split(",")]
    except ValueError:
        raise HTTPException(status_code=400, detail="bbox must contain numeric values")
    if len(parts) != 4:
        raise HTTPException(status_code=400, detail="bbox must be minLng,minLat,maxLng,maxLat")
    min_lng, min_lat, max_lng, max_lat = parts
    return (
        min(min_lng, max_lng),
        min(min_lat, max_lat),
        max(min_lng, max_lng),
        max(min_lat, max_lat),
    )


def iter_geometry_points(coords):
    """Yield (lng, lat) pairs from nested GeoJSON coordinates."""
    if not isinstance(coords, (list, tuple)):
        return
    if len(coords) >= 2 and all(isinstance(v, (int, float)) for v in coords[:2]):
        yield float(coords[0]), float(coords[1])
        return
    for item in coords:
        yield from iter_geometry_points(item)


def geometry_bounds(geometry: dict) -> Optional[tuple]:
    points = list(iter_geometry_points(geometry.get("coordinates", [])))
    if not points:
        return None
    lngs = [p[0] for p in points]
    lats = [p[1] for p in points]
    return min(lngs), min(lats), max(lngs), max(lats)


def point_to_box_geometry(lng: float, lat: float, area_m2: float = 0.0) -> dict:
    """Convert a clicked point to a small GeoJSON square polygon for choropleth display."""
    try:
        side_m = math.sqrt(float(area_m2)) if float(area_m2) > 0 else 450.0
    except (TypeError, ValueError):
        side_m = 450.0
    side_m = min(max(side_m, 120.0), 700.0)
    half_lat = (side_m / 2) / 111_320
    meters_per_lng = 111_320 * max(math.cos(math.radians(lat)), 0.2)
    half_lng = (side_m / 2) / meters_per_lng
    return {
        "type": "Polygon",
        "coordinates": [[
            [lng - half_lng, lat - half_lat],
            [lng + half_lng, lat - half_lat],
            [lng + half_lng, lat + half_lat],
            [lng - half_lng, lat + half_lat],
            [lng - half_lng, lat - half_lat],
        ]],
    }


def normalize_geojson_geometry(value: Any, area_m2: float = 0.0) -> dict:
    """Accept GeoJSON Point/Polygon text and normalize Points into polygons."""
    geometry = json.loads(value) if isinstance(value, str) else value
    if not isinstance(geometry, dict):
        raise ValueError("geometry must be a GeoJSON object")

    geom_type = geometry.get("type")
    if geom_type == "Point":
        coords = geometry.get("coordinates", [])
        if not isinstance(coords, list) or len(coords) < 2:
            raise ValueError("Point geometry must contain [lng, lat]")
        return point_to_box_geometry(float(coords[0]), float(coords[1]), area_m2)

    if geom_type in {"Polygon", "MultiPolygon"}:
        if not geometry_bounds(geometry):
            raise ValueError(f"{geom_type} geometry has no coordinates")
        return geometry

    raise ValueError("geometry must be GeoJSON Point, Polygon, or MultiPolygon")


def bbox_intersects(a: tuple, b: tuple) -> bool:
    return not (a[2] < b[0] or a[0] > b[2] or a[3] < b[1] or a[1] > b[3])


def haversine_km(lat1: float, lng1: float, lat2: float, lng2: float) -> float:
    radius = 6371.0
    p1 = math.radians(lat1)
    p2 = math.radians(lat2)
    dp = math.radians(lat2 - lat1)
    dl = math.radians(lng2 - lng1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * radius * math.atan2(math.sqrt(a), math.sqrt(1 - a))


# ============================================================
# HELPER: Broadcast SSE event
# ============================================================
async def broadcast_sse(event_type: str, data: Optional[dict] = None):
    """Send Server-Sent Event to all connected clients."""
    data = data or {}
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


def pipeline_status_payload() -> dict:
    payload = dict(_pipeline_state)
    payload["pending"] = _pipeline_pending
    payload["running"] = _pipeline_lock.locked() or payload.get("state") in {"queued", "running"}
    payload["affected_ids"] = sorted(set(payload.get("affected_ids") or []) | _pipeline_affected_ids)
    return payload


def set_pipeline_state(state: str, **extra):
    _pipeline_state["state"] = state
    _pipeline_state["last_event_at"] = datetime.utcnow().isoformat()
    if state in {"queued", "running"}:
        _pipeline_state["running"] = True
    if state == "queued":
        _pipeline_state["last_requested_at"] = _pipeline_state["last_event_at"]
    elif state == "running":
        _pipeline_state["last_started_at"] = _pipeline_state["last_event_at"]
        _pipeline_state["last_error"] = None
    elif state in {"succeeded", "failed"}:
        _pipeline_state["running"] = False
        _pipeline_state["last_completed_at"] = _pipeline_state["last_event_at"]
        _pipeline_state["version"] = int(_pipeline_state.get("version") or 0) + 1
    _pipeline_state.update(extra)


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
        "SPARK_LOCAL_THREADS": str(SPARK_LOCAL_THREADS),
        "SPARK_DRIVER_MEMORY": SPARK_DRIVER_MEMORY,
        "SPARK_EXECUTOR_MEMORY": SPARK_EXECUTOR_MEMORY,
        "SPARK_SHUFFLE_PARTITIONS": SPARK_SHUFFLE_PARTITIONS,
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


async def request_pipeline(reason: str, affected_ids: Optional[List[str]] = None) -> str:
    """Queue one debounced pipeline run and coalesce duplicate triggers."""
    global _pipeline_pending, _pipeline_last_requested, _pipeline_task

    _pipeline_pending = True
    _pipeline_last_requested = time.time()
    for wid in affected_ids or []:
        if wid:
            _pipeline_affected_ids.add(str(wid))

    if _pipeline_task is None or _pipeline_task.done():
        _pipeline_task = asyncio.create_task(pipeline_worker())
        status = "queued"
    elif _pipeline_lock.locked():
        status = "running"
    else:
        status = "queued"

    set_pipeline_state("queued", affected_ids=sorted(_pipeline_affected_ids))
    await broadcast_sse("processing_queued", {
        "reason": reason,
        "pipeline": pipeline_status_payload(),
    })
    log.info("Pipeline trigger accepted: reason=%s status=%s", reason, status)
    return status


async def pipeline_worker():
    """Debounce trigger bursts, then run at most one full Spark pipeline at a time."""
    global _pipeline_pending, _pipeline_task

    try:
        while True:
            while True:
                elapsed = time.time() - _pipeline_last_requested
                remaining = PIPELINE_DEBOUNCE_SECONDS - elapsed
                if remaining <= 0:
                    break
                await asyncio.sleep(min(remaining, PIPELINE_DEBOUNCE_SECONDS))

            async with _pipeline_lock:
                if not _pipeline_pending:
                    break
                _pipeline_pending = False
                await run_pipeline()

            if not _pipeline_pending:
                break
    finally:
        _pipeline_task = None
        if _pipeline_pending:
            _pipeline_task = asyncio.create_task(pipeline_worker())


# ============================================================
# BACKGROUND TASK: Full pipeline processing
# ============================================================
async def run_pipeline():
    """Run full processing pipeline: Bronze→Silver→Gold."""
    global _pipeline_affected_ids
    loop = asyncio.get_event_loop()
    current_affected_ids = sorted(_pipeline_affected_ids)
    set_pipeline_state("running", affected_ids=current_affected_ids)
    await broadcast_sse("processing_started", {"pipeline": pipeline_status_payload()})
    log.info("=== Pipeline started ===")

    await loop.run_in_executor(None, stop_api_spark)

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

    jobs_ok = {
        "job1_bronze_silver": ok1,
        "job2_train_model": ok2,
        "job3_silver_gold": ok3,
    }
    pipeline_success = bool(ok1 and ok3)
    set_pipeline_state(
        "succeeded" if pipeline_success else "failed",
        jobs_ok=jobs_ok,
        affected_ids=current_affected_ids,
        last_error=None if pipeline_success else "One or more required Spark jobs failed",
    )
    for wid in current_affected_ids:
        _pipeline_affected_ids.discard(wid)

    invalidate_cache()
    payload = {
        "jobs_ok": [ok1, ok2, ok3],
        "affected_ids": current_affected_ids,
        "pipeline": pipeline_status_payload(),
    }
    await broadcast_sse("map_updated" if pipeline_success else "processing_failed", payload)
    log.info("=== Pipeline complete, SSE sent ===")


# ============================================================
# ROUTES: Health
# ============================================================
@app.get("/health")
async def health():
    return {"status": "ok", "timestamp": datetime.utcnow().isoformat()}


@app.get("/api/pipeline/status")
async def get_pipeline_status():
    """Return the current pipeline queue/running/completion state."""
    return pipeline_status_payload()


# ============================================================
# ROUTES: Wilayah (Master)
# ============================================================
@app.get("/api/wilayah")
async def get_wilayah(
    kecamatan: Optional[str] = None,
    kelurahan: Optional[str] = None,
    rw: Optional[str] = None,
    rt: Optional[str] = None,
    q: Optional[str] = None,
    limit: int = Query(500, ge=1, le=5000),
    offset: int = Query(0, ge=0),
):
    """Return registered wilayah with optional filtering and pagination."""
    path = f"{HDFS_URL}/data/bronze/master_wilayah"
    rows = read_delta(path)
    if rows is None:
        return {"wilayah": [], "total": 0, "limit": limit, "offset": offset, "has_more": False}

    filtered = filter_wilayah_rows(rows, kecamatan, kelurahan, rw, rt, q)
    page = filtered[offset:offset + limit]
    return {
        "wilayah": page,
        "total": len(filtered),
        "limit": limit,
        "offset": offset,
        "has_more": offset + len(page) < len(filtered),
    }


@app.get("/api/wilayah/options")
async def get_wilayah_options(
    level: str = Query(..., pattern="^(kecamatan|kelurahan|rw|rt)$"),
    kecamatan: Optional[str] = None,
    kelurahan: Optional[str] = None,
    rw: Optional[str] = None,
):
    """Return distinct option values for cascading wilayah selectors."""
    path = f"{HDFS_URL}/data/bronze/master_wilayah"
    rows = read_delta(path) or []
    filtered = filter_wilayah_rows(rows, kecamatan=kecamatan, kelurahan=kelurahan, rw=rw)
    options = sorted({str(r.get(level, "")) for r in filtered if r.get(level) not in (None, "")})
    return {"level": level, "options": options, "total": len(options)}


@app.get("/api/wilayah/lookup")
async def lookup_wilayah(
    kecamatan: str,
    kelurahan: str,
    rw: str,
    rt: str,
):
    """Find one wilayah from cascading selector values."""
    path = f"{HDFS_URL}/data/bronze/master_wilayah"
    rows = read_delta(path) or []
    filtered = filter_wilayah_rows(rows, kecamatan=kecamatan, kelurahan=kelurahan, rw=rw, rt=rt)
    if not filtered:
        raise HTTPException(status_code=404, detail="Wilayah not found")
    return filtered[0]


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

        geometry_wkt = ""
        if wilayah.geometry_wkt:
            try:
                geometry_wkt = json.dumps(
                    normalize_geojson_geometry(wilayah.geometry_wkt, wilayah.luas_m2),
                    separators=(",", ":"),
                )
            except (TypeError, ValueError, json.JSONDecodeError) as e:
                raise HTTPException(status_code=400, detail=f"Invalid geometry_wkt: {e}")

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
            geometry_wkt=geometry_wkt,
            created_at=datetime.utcnow(),
        )

        df = spark.createDataFrame([row])
        df.write.format("delta").mode("append").option("mergeSchema", "true").save(path)

        invalidate_cache()
        await broadcast_sse("wilayah_registered", {
            "id_wilayah": wilayah.id_wilayah,
            "affected_ids": [wilayah.id_wilayah],
        })
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
async def submit_survey(event: SurveyEvent):
    """Submit a survey event. The consumer triggers processing after Bronze write."""
    event_id = str(uuid.uuid4())
    recorded_at = datetime.utcnow().isoformat()

    payload = event.model_dump()
    payload["event_id"] = event_id
    payload["recorded_at"] = recorded_at

    produce_kafka_message(TOPIC_SURVEY, payload)

    return {
        "status": "accepted",
        "event_id": event_id,
        "recorded_at": recorded_at,
        "message": "Data diterima, pipeline akan berjalan setelah data masuk Bronze..."
    }


@app.post("/api/secondary/upload", status_code=201)
async def upload_secondary(
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

    return {
        "status": "accepted",
        "rows_ingested": count,
        "source_type": source_type
    }


# ============================================================
# ROUTES: Map (GeoJSON for Leaflet.js)
# ============================================================
@app.get("/api/map/risk-score")
async def get_map_risk_score(
    bbox: Optional[str] = None,
    lat: Optional[float] = None,
    lng: Optional[float] = None,
    ids: Optional[str] = None,
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
):
    """Return paged GeoJSON FeatureCollection with risk scores per wilayah."""
    gold_path = f"{HDFS_URL}/data/gold/slum_risk_score"
    wilayah_path = f"{HDFS_URL}/data/bronze/master_wilayah"

    gold_rows = read_delta(gold_path)
    wilayah_rows = read_delta(wilayah_path)

    if not wilayah_rows:
        return {
            "type": "FeatureCollection",
            "features": [],
            "total": 0,
            "returned": 0,
            "limit": limit,
            "offset": offset,
            "has_more": False,
        }

    bbox_filter = parse_bbox_param(bbox)
    point = None
    if lat is not None or lng is not None:
        if lat is None or lng is None:
            raise HTTPException(status_code=400, detail="lat and lng must be provided together")
        point = (lat, lng)
    id_filter = {part.strip() for part in ids.split(",") if part.strip()} if ids else set()

    # Build a dict from gold data for quick lookup
    gold_map = {}
    if gold_rows:
        for r in gold_rows:
            gold_map[r.get("id_wilayah", "")] = r

    features = []
    for w in wilayah_rows:
        wid = w.get("id_wilayah", "")
        if id_filter and wid not in id_filter:
            continue
        geom = w.get("geometry_wkt", "")
        if not geom:
            continue

        try:
            geometry = normalize_geojson_geometry(geom, w.get("luas_m2", 0) or 0)
        except Exception:
            continue

        bounds = geometry_bounds(geometry)
        if not id_filter and bbox_filter and bounds and not bbox_intersects(bounds, bbox_filter):
            continue

        gold = gold_map.get(wid, {})
        props = {
            "id_wilayah": wid,
            "kelurahan": w.get("kelurahan", ""),
            "kecamatan": w.get("kecamatan", ""),
            "rt": w.get("rt", ""),
            "rw": w.get("rw", ""),
            "total_jiwa": w.get("total_jiwa", 0),
            "luas_m2": w.get("luas_m2", 0),
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

        if point and bounds:
            center_lng = (bounds[0] + bounds[2]) / 2
            center_lat = (bounds[1] + bounds[3]) / 2
            props["distance_km"] = round(haversine_km(point[0], point[1], center_lat, center_lng), 3)

        features.append({
            "type": "Feature",
            "geometry": geometry,
            "properties": props
        })

    if point:
        features.sort(key=lambda f: f["properties"].get("distance_km", float("inf")))

    total = len(features)
    page = features[offset:offset + limit]
    return {
        "type": "FeatureCollection",
        "features": page,
        "total": total,
        "returned": len(page),
        "limit": limit,
        "offset": offset,
        "has_more": offset + len(page) < total,
    }


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
            yield f"data: {json.dumps({'type': 'connected', 'timestamp': datetime.utcnow().isoformat(), 'pipeline': pipeline_status_payload()})}\n\n"
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
async def trigger_processing(payload: Optional[PipelineTrigger] = Body(None)):
    """Called by Kafka consumer after writing to Bronze. Debounces Spark pipeline."""
    affected_ids = payload.affected_ids if payload else []
    status = await request_pipeline("consumer", affected_ids)
    return {
        "status": status,
        "debounce_seconds": PIPELINE_DEBOUNCE_SECONDS,
        "affected_ids": sorted(_pipeline_affected_ids),
        "pipeline": pipeline_status_payload(),
        "timestamp": datetime.utcnow().isoformat(),
    }
