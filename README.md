# Geospatial Big Data Analytics — Pemetaan Permukiman Kumuh Surabaya

> **Final Project Big Data** — Sistem pemetaan permukiman kumuh berbasis Big Data Lakehouse dengan visualisasi geospasial real-time.

## 🏗️ Arsitektur Sistem

```
[Web Form / Upload] → [FastAPI] → [Kafka] → [Consumer]
                                               ↓
                                      [Bronze Delta Lake (HDFS)]
                                               ↓ (Spark Job 1)
                                      [Silver Delta Lake]
                                               ↓ (Spark Job 2 + 3)
                                      [Gold Delta Lake]
                                               ↓
                          [FastAPI REST API] + [SSE Real-time]
                                               ↓
                                    [Frontend Leaflet.js]
```

## 📦 Stack Teknologi

| Komponen | Image | Peran |
|---|---|---|
| Hadoop HDFS | `apache/hadoop:3` | Distributed storage (NameNode + DataNode) |
| Kafka | `bitnami/kafka:3.6` | Message broker ingest |
| Spark | `apache/spark:3.5` / local PySpark | Processing engine (PySpark + MLlib) |
| Delta Lake | `delta-spark` 3.2 | Lakehouse layer + time travel |
| FastAPI | `python:3.11-slim` | Backend REST API + SSE streaming |
| Frontend | `nginx:alpine` | Web UI + Leaflet.js peta |

## 🚀 Cara Menjalankan

### Prerequisite
- Docker Engine ≥ 24.0
- Docker Compose ≥ 2.0
- RAM minimal 4-6GB untuk mode lokal rendah memori (direkomendasikan 8GB+)
- Port bebas: 3000, 8000, 9000, 9092, 9870

### Full startup (direkomendasikan)
```bash
cd fp-big-data
bash startup.sh
```

### Manual step-by-step
```bash
# 1. Build semua images
docker compose build

# 2. Start infrastructure
docker compose up -d namenode datanode kafka

# 3. Init HDFS directories (tunggu namenode ready)
docker compose up hdfs-init

# 4. Start aplikasi
docker compose up -d consumer api frontend

# 5. Seed data awal
make seed
```

Standalone Spark master/worker bersifat opsional untuk debugging Spark UI:

```bash
docker compose --profile spark-standalone up -d spark spark-worker
```

### Menggunakan Makefile
```bash
make up          # Start semua services
make down        # Stop semua services
make logs        # Lihat logs api + consumer
make seed        # Seed data wilayah awal
make health      # Health check API + summary
make trigger     # Trigger Spark pipeline manual
make test-survey # Submit survey event test
make sse         # Subscribe SSE stream
make clean       # Hapus semua data (destructive!)
```

## 🌐 URL Services

| Service | URL |
|---|---|
| **Frontend (Web UI)** | http://localhost:3000 |
| **API (FastAPI)** | http://localhost:8000 |
| **API Documentation** | http://localhost:8000/docs |
| **HDFS Web UI** | http://localhost:9870 |
| **Spark Web UI** | http://localhost:8080 (opsional: profile `spark-standalone`) |

## 📄 Halaman Web UI

| Halaman | URL | Fungsi |
|---|---|---|
| Peta Utama | `/` | Choropleth real-time dengan SSE |
| Input Data | `/input.html` | Form 7 indikator PUPR |
| Riwayat Wilayah | `/wilayah.html?id={id}` | Log incremental per wilayah |
| Prioritas | `/prioritas.html` | Ranking intervensi + chart |
| Daftar Wilayah | `/register.html` | Register wilayah baru |

## 🔌 API Endpoints

```
GET  /api/wilayah                    # Daftar semua wilayah
POST /api/wilayah                    # Daftarkan wilayah baru
POST /api/survey                     # Submit data survei
POST /api/secondary/upload           # Upload CSV data sekunder
GET  /api/map/risk-score             # GeoJSON untuk peta choropleth
GET  /api/wilayah/{id}/history       # Riwayat event per wilayah
GET  /api/wilayah/{id}/latest        # Kondisi terbaru per wilayah
GET  /api/priority                   # Ranking prioritas intervensi
GET  /api/summary                    # Dashboard summary stats
GET  /api/stream/updates             # SSE real-time stream
```

## 📂 Struktur Direktori

```
fp-big-data/
├── docker-compose.yml
├── startup.sh              ← Full startup script
├── Makefile
├── api/                    ← FastAPI backend
│   ├── Dockerfile
│   ├── main.py
│   └── requirements.txt
├── consumer/               ← Kafka consumer (PySpark)
│   ├── Dockerfile
│   ├── consumer.py
│   └── requirements.txt
├── spark_jobs/             ← PySpark processing jobs
│   ├── job1_bronze_silver.py
│   ├── job2_train_model.py
│   └── job3_silver_gold.py
├── frontend/               ← Nginx + HTML/JS
│   ├── Dockerfile
│   ├── nginx.conf
│   └── public/
│       ├── index.html      ← Peta utama
│       ├── input.html
│       ├── wilayah.html
│       ├── prioritas.html
│       ├── register.html
│       ├── css/style.css
│       └── js/
│           ├── map.js
│           ├── charts.js
│           ├── input.js
│           └── wilayah.js
└── init_scripts/
    ├── init_hdfs.sh        ← Init HDFS directories
    └── seed_wilayah.py     ← Seed 5 kecamatan sample data
```

## 🗃️ Delta Lake Structure (HDFS)

```
/data/
├── bronze/
│   ├── master_wilayah/     ← Referensi wilayah (insert-only)
│   ├── survey_events/      ← LOG semua event survei (append-only)
│   └── secondary_sources/  ← Data BPS/BMKG/PDAM (append-only)
├── silver/
│   ├── latest_indicators/  ← Snapshot terbaru per wilayah
│   ├── event_history/      ← Full history enriched
│   ├── feature_matrix/     ← Fitur per kelurahan untuk ML
│   ├── ground_truth/       ← Label kumuh
│   └── population/         ← Populasi per kelurahan
├── gold/
│   ├── slum_risk_score/    ← ⭐ OUTPUT UTAMA: Risk score + prediksi
│   ├── slum_trend/         ← Delta skor antar event
│   ├── dominant_factors/   ← Top-3 faktor per wilayah
│   └── intervention_priority/ ← Ranking prioritas
└── models/
    └── rf_slum_latest/     ← Random Forest model terbaru
```

## 🔄 Alur Data (End-to-End)

1. **User submit** form survei di `/input.html`
2. **FastAPI** validasi → produce ke Kafka topic `survey-events`
3. **Kafka Consumer** poll → write ke `bronze/survey_events` (Delta Lake, append-only)
4. **Consumer** trigger → `POST /api/internal/trigger-processing`
5. **FastAPI** jalankan Spark jobs di background:
   - Job 1: Bronze → Silver (ETL, latest_indicators, event_history)
   - Job 2: Train/load ML model (RandomForest)
   - Job 3: Silver → Gold (risk score, prediksi, trend, prioritas)
6. **FastAPI** emit SSE event: `{"type": "map_updated"}`
7. **Frontend** (yang subscribe `/api/stream/updates`) auto-fetch `/api/map/risk-score`
8. **Leaflet.js** update choropleth layer tanpa reload halaman

## 📊 Prinsip Data Incremental (Log-Based)

Sistem menggunakan **append-only log** untuk semua data survei:
- Setiap submit = event baru dengan UUID dan timestamp
- Data lama **tidak pernah dihapus atau ditimpa**
- `silver/event_history` menyimpan seluruh history
- `silver/latest_indicators` = snapshot terbaru (dicompute dari history)
- Delta Lake **time travel** memungkinkan query data di titik waktu manapun
- Halaman `/wilayah.html` menampilkan seluruh log perubahan per wilayah

## 📈 Model ML

- **Algoritma**: Random Forest Classifier (Spark MLlib)
- **Fitur**: 7 skor PUPR + frekuensi banjir + jumlah jiwa (total 9 fitur)
- **Label**: Kumuh (1) / Tidak Kumuh (0) dari ground truth KOTAKU/SK Pemkot
- **Fallback**: Jika model belum ditraining, gunakan rule-based (risk_score ≥ 50 = kumuh)

## 🛠️ Troubleshooting

```bash
# HDFS tidak mau start
docker compose logs namenode

# Kafka Consumer error
docker compose logs consumer

# Spark job gagal
docker compose logs api | grep "Job"

# Reset semua (HAPUS DATA)
make clean && make up
```
