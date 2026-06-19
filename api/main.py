#!/usr/bin/env python3
"""
FastAPI Backend — Slum Analytics System
Geospatial Big Data Analytics untuk Pemetaan Permukiman Kumuh Surabaya
"""

import os
import glob
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
STORAGE_MODE = os.environ.get("STORAGE_MODE", "auto").lower()
LOCAL_LAKEHOUSE_DIR = os.environ.get("LOCAL_LAKEHOUSE_DIR", "/data/local_lakehouse")
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC_SURVEY = os.environ.get("KAFKA_TOPIC_SURVEY", "survey-events")
TOPIC_SECONDARY = os.environ.get("KAFKA_TOPIC_SECONDARY", "secondary-batch")
SPARK_MASTER = os.environ.get("SPARK_MASTER", "spark://spark:7077")
SPARK_JOBS_PATH = os.environ.get("SPARK_JOBS_PATH", "/opt/spark_jobs")
SPARK_DRIVER_MEMORY = os.environ.get("SPARK_DRIVER_MEMORY", "512m")
SPARK_EXECUTOR_MEMORY = os.environ.get("SPARK_EXECUTOR_MEMORY", "512m")
SPARK_SHUFFLE_PARTITIONS = os.environ.get("SPARK_SHUFFLE_PARTITIONS", "2")
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
    SPARK_LOCAL_THREADS = max(1, int(os.environ.get("SPARK_LOCAL_THREADS", "1")))
except ValueError:
    SPARK_LOCAL_THREADS = 1

SPARK_DEFAULT_PARALLELISM = os.environ.get("SPARK_DEFAULT_PARALLELISM", str(max(1, SPARK_LOCAL_THREADS)))

try:
    PIPELINE_DEBOUNCE_SECONDS = max(0.0, float(os.environ.get("PIPELINE_DEBOUNCE_SECONDS", "2")))
except ValueError:
    PIPELINE_DEBOUNCE_SECONDS = 2.0

FORCE_TRAIN_EVERY_RUN = os.environ.get("FORCE_TRAIN_EVERY_RUN", "false").lower() in {"1", "true", "yes"}

try:
    JOB1_EXPECTED_SECONDS = max(1, int(os.environ.get("JOB1_EXPECTED_SECONDS", "35")))
except ValueError:
    JOB1_EXPECTED_SECONDS = 35
try:
    JOB2_EXPECTED_SECONDS = max(1, int(os.environ.get("JOB2_EXPECTED_SECONDS", "55")))
except ValueError:
    JOB2_EXPECTED_SECONDS = 55
try:
    JOB3_EXPECTED_SECONDS = max(1, int(os.environ.get("JOB3_EXPECTED_SECONDS", "45")))
except ValueError:
    JOB3_EXPECTED_SECONDS = 45
try:
    PROGRESS_TICK_SECONDS = max(0.25, float(os.environ.get("PROGRESS_TICK_SECONDS", "0.75")))
except ValueError:
    PROGRESS_TICK_SECONDS = 0.75

# SSE event queue
sse_clients: List[asyncio.Queue] = []

# In-memory cache for spark results (to avoid repeated Spark reads)
_cache: Dict[str, Any] = {}
_cache_ts: Dict[str, float] = {}
CACHE_TTL = 30  # seconds
_lakehouse_root: Optional[str] = None
_storage_backend = "unresolved"

# Single-flight Spark pipeline state. Requests are debounced and coalesced so
# low-memory machines do not run multiple PySpark driver processes at once.
_pipeline_lock = asyncio.Lock()
_pipeline_task: Optional[asyncio.Task] = None
_pipeline_pending = False
_pipeline_last_requested = 0.0
_pipeline_affected_ids: set[str] = set()
_pipeline_running_affected_ids: set[str] = set()
_pipeline_train_requested = False
_pipeline_cancel_requested = False
_current_job_process: Optional[subprocess.Popen] = None
_pipeline_state: Dict[str, Any] = {
    "state": "idle",
    "running": False,
    "pending": False,
    "version": 0,
    "phase": "idle",
    "phase_label": "Siap menerima pembaruan",
    "progress": 0,
    "last_requested_at": None,
    "last_started_at": None,
    "last_completed_at": None,
    "last_error": None,
    "jobs_ok": {},
    "affected_ids": [],
    "pending_affected_ids": [],
    "running_affected_ids": [],
    "train_model_requested": False,
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
    idempotency_key: Optional[str] = None
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
    train_model: bool = False


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


def configure_delta_builder(builder):
    jars = resolve_delta_jars()
    if jars:
        return builder.config("spark.jars", ",".join(jars))

    # Local development fallback. The Docker image bakes these jars so API and
    # Spark jobs do not resolve Maven packages at runtime.
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
    log.warning("HDFS probe failed after retries, using local lakehouse fallback: %s", last_error)
    return False


def configure_lakehouse_root(spark) -> str:
    """Resolve HDFS-first lakehouse root, falling back to shared local storage."""
    global _lakehouse_root, _storage_backend
    if _lakehouse_root:
        return _lakehouse_root

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
        _storage_backend = "hdfs"
        _lakehouse_root = f"{HDFS_URL.rstrip('/')}/data"
    else:
        _storage_backend = "local"
        os.makedirs(local_root, exist_ok=True)
        with open(local_marker, "a", encoding="utf-8"):
            pass
        _lakehouse_root = f"file://{local_root}"

    log.info("Lakehouse storage backend=%s root=%s", _storage_backend, _lakehouse_root)
    return _lakehouse_root


def lakehouse_path(relative_path: str) -> str:
    """Return an absolute Delta path under the resolved lakehouse root."""
    spark = get_spark()
    root = configure_lakehouse_root(spark).rstrip("/")
    return f"{root}/{relative_path.lstrip('/')}"


def get_spark():
    """Get or create SparkSession (lazy singleton)."""
    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder
        .appName("SlumAPI")
        .master(f"local[{SPARK_LOCAL_THREADS}]")  # local mode - API runs Spark in-process
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
        .config("spark.hadoop.fs.defaultFS", HDFS_URL)
        .config("spark.hadoop.dfs.client.use.datanode.hostname", "true")
        .config("spark.driver.memory", SPARK_DRIVER_MEMORY)
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
    spark = configure_delta_builder(builder).getOrCreate()
    configure_lakehouse_root(spark)
    return spark


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
    pending_ids = sorted(_pipeline_affected_ids)
    running_ids = sorted(_pipeline_running_affected_ids)
    payload["pending"] = _pipeline_pending
    payload["running"] = payload.get("state") == "running"
    payload["pending_affected_ids"] = pending_ids
    payload["running_affected_ids"] = running_ids
    if payload["running"]:
        payload["affected_ids"] = running_ids
    elif payload["pending"]:
        payload["affected_ids"] = pending_ids
    else:
        payload["affected_ids"] = sorted(payload.get("affected_ids") or [])
    payload["all_affected_ids"] = sorted(set(payload.get("affected_ids") or []) | set(pending_ids) | set(running_ids))
    payload["train_model_requested"] = bool(payload.get("train_model_requested") or _pipeline_train_requested)
    return payload


def set_pipeline_state(state: str, **extra):
    previous_state = _pipeline_state.get("state")
    _pipeline_state["state"] = state
    _pipeline_state["last_event_at"] = datetime.utcnow().isoformat()
    if state in {"queued", "running"}:
        _pipeline_state["running"] = True
    if state == "queued":
        _pipeline_state["last_requested_at"] = _pipeline_state["last_event_at"]
    elif state == "running":
        if previous_state != "running":
            _pipeline_state["last_started_at"] = _pipeline_state["last_event_at"]
        _pipeline_state["last_error"] = None
    elif state in {"succeeded", "failed", "cancelled"}:
        _pipeline_state["running"] = False
        _pipeline_state["last_completed_at"] = _pipeline_state["last_event_at"]
        if state in {"succeeded", "failed"}:
            _pipeline_state["version"] = int(_pipeline_state.get("version") or 0) + 1
    _pipeline_state.update(extra)


async def publish_pipeline_progress(phase: str, label: str, progress: float):
    bounded_progress = round(max(0.0, min(100.0, float(progress))), 1)
    set_pipeline_state(
        "running",
        phase=phase,
        phase_label=label,
        progress=bounded_progress,
        affected_ids=sorted(_pipeline_running_affected_ids),
        pending_affected_ids=sorted(_pipeline_affected_ids),
        running_affected_ids=sorted(_pipeline_running_affected_ids),
    )
    await broadcast_sse("processing_progress", {"pipeline": pipeline_status_payload()})


# ============================================================
# HELPER: Run Spark jobs
# ============================================================
def run_spark_job(job_file: str) -> bool:
    """Run a Spark job Python script directly (via python)."""
    global _current_job_process
    job_path = os.path.join(SPARK_JOBS_PATH, job_file)
    if not os.path.exists(job_path):
        log.error(f"Spark job not found: {job_path}")
        return False

    configure_lakehouse_root(get_spark())
    active_storage_mode = _storage_backend if _storage_backend in {"hdfs", "local"} else STORAGE_MODE

    env = os.environ.copy()
    env.update({
        "HDFS_URL": HDFS_URL,
        "STORAGE_MODE": active_storage_mode,
        "LOCAL_LAKEHOUSE_DIR": LOCAL_LAKEHOUSE_DIR,
        "SPARK_MASTER": SPARK_MASTER,
        "SPARK_LOCAL_THREADS": str(SPARK_LOCAL_THREADS),
        "SPARK_DRIVER_MEMORY": SPARK_DRIVER_MEMORY,
        "SPARK_EXECUTOR_MEMORY": SPARK_EXECUTOR_MEMORY,
        "SPARK_SHUFFLE_PARTITIONS": SPARK_SHUFFLE_PARTITIONS,
        "SPARK_DEFAULT_PARALLELISM": SPARK_DEFAULT_PARALLELISM,
        "SPARK_SQL_FILES_MAX_PARTITION_BYTES": SPARK_SQL_FILES_MAX_PARTITION_BYTES,
        "SPARK_ADVISORY_PARTITION_SIZE": SPARK_ADVISORY_PARTITION_SIZE,
        "SPARK_DELTA_JARS": SPARK_DELTA_JARS,
        "AFFECTED_WILAYAH_IDS": ",".join(sorted(_pipeline_running_affected_ids)),
        "TRAIN_MODEL_REQUESTED": "true" if _pipeline_state.get("train_model_requested") else "false",
        "PYSPARK_PYTHON": "python3",
        "PYSPARK_DRIVER_PYTHON": "python3",
    })

    log.info(f"Running Spark job: {job_path}")
    proc = None
    try:
        proc = subprocess.Popen(
            ["python3", job_path],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            env=env,
        )
        _current_job_process = proc
        started_at = time.time()
        while True:
            try:
                stdout, stderr = proc.communicate(timeout=1)
                break
            except subprocess.TimeoutExpired:
                if _pipeline_cancel_requested:
                    log.warning("Terminating Spark job %s after cancel request", job_file)
                    proc.terminate()
                    try:
                        stdout, stderr = proc.communicate(timeout=10)
                    except subprocess.TimeoutExpired:
                        proc.kill()
                        stdout, stderr = proc.communicate()
                    return False
                if time.time() - started_at > 600:
                    log.error("Job %s timed out after 600s", job_file)
                    proc.kill()
                    stdout, stderr = proc.communicate()
                    return False

        if proc.returncode == 0:
            log.info(f"Job {job_file} completed successfully")
            if stdout:
                log.info(f"Job stdout:\n{stdout[-1000:]}")
            return True
        else:
            log.error(f"Job {job_file} failed (exit {proc.returncode}):\n{stderr[-2000:]}")
            return False
    except Exception as e:
        log.error(f"Job {job_file} exception: {e}")
        return False
    finally:
        if _current_job_process is proc:
            _current_job_process = None


async def run_job_with_progress(
    job_file: str,
    phase: str,
    label: str,
    start_progress: int,
    end_progress: int,
    expected_seconds: int,
) -> bool:
    loop = asyncio.get_event_loop()
    future = loop.run_in_executor(None, run_spark_job, job_file)
    started_at = time.time()
    await publish_pipeline_progress(phase, label, start_progress)
    last_progress = round(float(start_progress), 1)

    while not future.done():
        if _pipeline_cancel_requested:
            await asyncio.sleep(PROGRESS_TICK_SECONDS)
            continue
        elapsed = time.time() - started_at
        fraction = min(0.96, elapsed / max(expected_seconds, 1))
        progress = round(start_progress + (end_progress - start_progress) * fraction, 1)
        if progress != last_progress:
            last_progress = progress
            await publish_pipeline_progress(phase, label, progress)
        await asyncio.sleep(PROGRESS_TICK_SECONDS)

    ok = await future
    if ok:
        await publish_pipeline_progress(phase, label, end_progress)
    return ok


async def request_pipeline(
    reason: str,
    affected_ids: Optional[List[str]] = None,
    train_model: bool = False,
) -> str:
    """Queue one debounced pipeline run and coalesce duplicate triggers."""
    global _pipeline_pending, _pipeline_last_requested, _pipeline_task, _pipeline_train_requested

    _pipeline_pending = True
    _pipeline_last_requested = time.time()
    _pipeline_train_requested = _pipeline_train_requested or train_model
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

    if status == "running":
        _pipeline_state.update({
            "last_event_at": datetime.utcnow().isoformat(),
            "pending": True,
            "pending_affected_ids": sorted(_pipeline_affected_ids),
            "running_affected_ids": sorted(_pipeline_running_affected_ids),
            "train_model_requested": _pipeline_train_requested,
        })
    else:
        set_pipeline_state(
            "queued",
            phase="queued",
            phase_label="Menunggu data siap diproses",
            progress=8,
            affected_ids=sorted(_pipeline_affected_ids),
            pending_affected_ids=sorted(_pipeline_affected_ids),
            running_affected_ids=[],
            train_model_requested=_pipeline_train_requested,
        )
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


async def finish_pipeline_cancelled(affected_ids: List[str]):
    global _pipeline_running_affected_ids
    set_pipeline_state(
        "cancelled",
        phase="cancelled",
        phase_label="Pemrosesan dibatalkan",
        progress=100,
        affected_ids=affected_ids,
        pending_affected_ids=sorted(_pipeline_affected_ids),
        running_affected_ids=[],
        train_model_requested=False,
        last_error="Pipeline cancelled by user",
    )
    _pipeline_running_affected_ids.clear()
    await broadcast_sse("processing_cancelled", {
        "affected_ids": affected_ids,
        "pipeline": pipeline_status_payload(),
    })
    log.info("=== Pipeline cancelled ===")


# ============================================================
# BACKGROUND TASK: Full pipeline processing
# ============================================================
async def run_pipeline():
    """Run full processing pipeline: Bronze→Silver→Gold."""
    global _pipeline_affected_ids, _pipeline_running_affected_ids, _pipeline_train_requested, _pipeline_cancel_requested
    _pipeline_cancel_requested = False
    _pipeline_running_affected_ids = set(_pipeline_affected_ids)
    _pipeline_affected_ids.clear()
    current_affected_ids = sorted(_pipeline_running_affected_ids)
    train_model = _pipeline_train_requested or FORCE_TRAIN_EVERY_RUN
    _pipeline_train_requested = False
    set_pipeline_state(
        "running",
        phase="preparing",
        phase_label="Menyiapkan data untuk diproses",
        progress=15,
        affected_ids=current_affected_ids,
        pending_affected_ids=[],
        running_affected_ids=current_affected_ids,
        train_model_requested=train_model,
    )
    await broadcast_sse("processing_started", {"pipeline": pipeline_status_payload()})
    log.info("=== Pipeline started ===")

    await publish_pipeline_progress("preparing", "Menyiapkan data untuk diproses", 18)
    stop_api_spark()

    # Job 1: Bronze → Silver
    ok1 = await run_job_with_progress(
        "job1_bronze_silver.py",
        "validating",
        "Memeriksa dan merapikan data survei",
        18,
        48 if train_model else 58,
        JOB1_EXPECTED_SECONDS,
    )
    if ok1:
        log.info("Job 1 complete: Bronze → Silver")
    else:
        log.warning("Job 1 had issues, continuing...")

    if _pipeline_cancel_requested:
        await finish_pipeline_cancelled(current_affected_ids)
        return

    ok2 = True
    if train_model:
        ok2 = await run_job_with_progress(
            "job2_train_model.py",
            "analyzing",
            "Memperbarui analisis wilayah",
            48,
            70,
            JOB2_EXPECTED_SECONDS,
        )
        if ok2:
            log.info("Job 2 complete: ML model training step finished")
        else:
            log.warning("Job 2 had issues (maybe no ground truth yet), continuing to model-backed Gold step...")

    if _pipeline_cancel_requested:
        await finish_pipeline_cancelled(current_affected_ids)
        return

    # Job 3: Silver → Gold
    ok3 = await run_job_with_progress(
        "job3_silver_gold.py",
        "publishing",
        "Menyiapkan hasil peta terbaru",
        70 if train_model else 58,
        96,
        JOB3_EXPECTED_SECONDS,
    )
    if ok3:
        log.info("Job 3 complete: Silver → Gold")
    else:
        log.warning("Job 3 had issues, continuing...")

    if _pipeline_cancel_requested:
        await finish_pipeline_cancelled(current_affected_ids)
        return

    if not ok3 and not train_model:
        log.warning("Gold step failed without model refresh; training model once, then retrying Gold")
        ok2 = await run_job_with_progress(
            "job2_train_model.py",
            "analyzing",
            "Memperbarui analisis wilayah",
            58,
            76,
            JOB2_EXPECTED_SECONDS,
        )
        if _pipeline_cancel_requested:
            await finish_pipeline_cancelled(current_affected_ids)
            return
        ok3 = await run_job_with_progress(
            "job3_silver_gold.py",
            "publishing",
            "Menyiapkan hasil peta terbaru",
            76,
            96,
            JOB3_EXPECTED_SECONDS,
        )

    jobs_ok = {
        "job1_bronze_silver": ok1,
        "job2_train_model": ok2,
        "job3_silver_gold": ok3,
    }
    pipeline_success = bool(ok1 and ok3)
    set_pipeline_state(
        "succeeded" if pipeline_success else "failed",
        phase="completed" if pipeline_success else "failed",
        phase_label="Pembaruan selesai" if pipeline_success else "Pemrosesan belum berhasil",
        progress=100,
        jobs_ok=jobs_ok,
        affected_ids=current_affected_ids,
        pending_affected_ids=sorted(_pipeline_affected_ids),
        running_affected_ids=[],
        train_model_requested=False,
        last_error=None if pipeline_success else "One or more required Spark jobs failed",
    )
    _pipeline_running_affected_ids.clear()

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


@app.post("/api/pipeline/cancel")
async def cancel_pipeline():
    """Cancel the active or queued pipeline work."""
    global _pipeline_cancel_requested, _pipeline_pending
    active = pipeline_status_payload()
    if not (active.get("running") or active.get("pending")):
        return {"status": "idle", "pipeline": active}

    _pipeline_cancel_requested = True
    _pipeline_pending = False
    _pipeline_affected_ids.clear()

    proc = _current_job_process
    if not active.get("running"):
        await finish_pipeline_cancelled(active.get("affected_ids") or [])
        return {"status": "cancelled", "pipeline": pipeline_status_payload()}

    if proc is not None and proc.poll() is None:
        log.warning("Terminating active Spark job after user cancel")
        proc.terminate()

    set_pipeline_state(
        "running",
        phase="cancelling",
        phase_label="Membatalkan pemrosesan",
        progress=max(float(_pipeline_state.get("progress") or 0), 1.0),
        pending_affected_ids=[],
        running_affected_ids=sorted(_pipeline_running_affected_ids),
    )
    await broadcast_sse("processing_progress", {"pipeline": pipeline_status_payload()})
    return {"status": "cancelling", "pipeline": pipeline_status_payload()}


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
    path = lakehouse_path("bronze/master_wilayah")
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
    path = lakehouse_path("bronze/master_wilayah")
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
    path = lakehouse_path("bronze/master_wilayah")
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
        path = lakehouse_path("bronze/master_wilayah")
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
    event_id = str(uuid.uuid5(uuid.NAMESPACE_URL, f"survey:{event.idempotency_key}")) if event.idempotency_key else str(uuid.uuid4())
    recorded_at = datetime.utcnow().isoformat()

    payload = event.model_dump(exclude={"idempotency_key"})
    payload["event_id"] = event_id
    payload["recorded_at"] = recorded_at

    produce_kafka_message(TOPIC_SURVEY, payload)

    return {
        "status": "accepted",
        "event_id": event_id,
        "recorded_at": recorded_at,
        "message": "Data diterima. Pemrosesan akan berjalan setelah data siap."
    }


@app.post("/api/secondary/upload", status_code=201)
async def upload_secondary(
    file: UploadFile = File(...),
    source_type: str = Form("ground_truth"),
    idempotency_key: Optional[str] = Form(None),
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
        payload["batch_id"] = str(uuid.uuid5(uuid.NAMESPACE_URL, f"secondary:{idempotency_key}:{count}")) if idempotency_key else str(uuid.uuid4())
        payload["source_type"] = source_type
        payload["ingested_at"] = datetime.utcnow().isoformat()
        produce_kafka_message(TOPIC_SECONDARY, payload)
        count += 1

    return {
        "status": "accepted",
        "rows_ingested": count,
        "source_type": source_type,
        "accepted_at": datetime.utcnow().isoformat(),
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
    gold_path = lakehouse_path("gold/slum_risk_score")
    wilayah_path = lakehouse_path("bronze/master_wilayah")

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
async def get_map_prediction(
    bbox: Optional[str] = None,
    lat: Optional[float] = None,
    lng: Optional[float] = None,
    ids: Optional[str] = None,
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
):
    """Return paged GeoJSON with prediction labels."""
    return await get_map_risk_score(
        bbox=bbox,
        lat=lat,
        lng=lng,
        ids=ids,
        limit=limit,
        offset=offset,
    )


# ============================================================
# ROUTES: Analytics per Wilayah
# ============================================================
@app.get("/api/wilayah/{id_wilayah}/latest")
async def get_wilayah_latest(id_wilayah: str):
    """Get the latest survey indicators for a wilayah."""
    path = lakehouse_path("silver/latest_indicators")
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
    path = lakehouse_path("silver/event_history")
    rows = read_delta(path)
    if rows is None:
        # Try bronze directly
        path = lakehouse_path("bronze/survey_events")
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
    path = lakehouse_path("gold/slum_trend")
    rows = read_delta(path)
    if rows is None:
        return {"id_wilayah": id_wilayah, "trend": []}
    trend = [r for r in rows if r.get("id_wilayah") == id_wilayah]
    return {"id_wilayah": id_wilayah, "trend": trend}


@app.get("/api/kelurahan/{id_kelurahan}/factors")
async def get_kelurahan_factors(id_kelurahan: str):
    """Get top-3 dominant factors for a kelurahan."""
    path = lakehouse_path("gold/dominant_factors")
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
    path = lakehouse_path("gold/intervention_priority")
    rows = read_delta(path)
    if rows is None:
        return {"priority": [], "total": 0}
    sorted_rows = sorted(rows, key=lambda x: x.get("prioritas_rank", 9999))
    return {"priority": sorted_rows, "total": len(sorted_rows)}


@app.get("/api/summary")
async def get_summary():
    """Dashboard summary statistics."""
    gold_path = lakehouse_path("gold/slum_risk_score")
    wilayah_path = lakehouse_path("bronze/master_wilayah")
    bronze_path = lakehouse_path("bronze/survey_events")

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
    train_model = bool(payload.train_model) if payload else False
    status = await request_pipeline("consumer", affected_ids, train_model=train_model)
    return {
        "status": status,
        "debounce_seconds": PIPELINE_DEBOUNCE_SECONDS,
        "affected_ids": sorted(_pipeline_affected_ids),
        "pipeline": pipeline_status_payload(),
        "timestamp": datetime.utcnow().isoformat(),
    }
