# Prompt Arsitektur Sistem: Geospatial Big Data Analytics pada Data Lakehouse untuk Pemetaan Permukiman Kumuh Kota Surabaya

## Konteks Proyek

Kamu adalah arsitek sistem big data untuk proyek tugas kuliah dengan topik:
**"Geospatial Big Data Analytics pada Data Lakehouse untuk Pemetaan Permukiman Kumuh Kota Surabaya"**

Sistem ini harus bersifat **MVP (Minimum Viable Product)** — ringan, tidak over-engineered, namun tetap menggunakan stack teknologi berikut yang semuanya dijalankan via **Docker Compose**:

| Komponen | Image Docker | Peran |
|---|---|---|
| Hadoop HDFS | `apache/hadoop:3` | Penyimpanan distributed (NameNode + DataNode) |
| Apache Kafka | `bitnami/kafka:latest` | Message broker ingest |
| Apache Spark | `bitnami/spark:latest` | Processing engine (PySpark) |
| Delta Lake | library `delta-spark` di atas Spark | Lakehouse layer |
| FastAPI | `python:3.11-slim` (custom) | Backend REST API |
| Frontend | `nginx:alpine` (custom) | Web UI + peta |

---

## Prinsip Desain Utama

### 1. Data sebagai log (append-only, incremental)

Setiap kali data baru dimasukkan untuk suatu wilayah (misal RT 003/RW 005 Kelurahan Rawa Buaya), sistem **TIDAK menimpa data lama**. Data baru ditambahkan sebagai entri baru dengan timestamp. Ini berarti:

- Setiap baris di Bronze layer adalah **event**, bukan snapshot
- Silver dan Gold layer dibangun dengan **agregasi latest + history keduanya tersedia**
- Delta Lake menyimpan seluruh history via **transaction log** — query bisa mengambil kondisi wilayah pada titik waktu manapun
- Frontend dapat menampilkan "kondisi terbaru" sekaligus "riwayat perubahan" per wilayah

Contoh ilustrasi log untuk satu wilayah:
```
event_id | id_wilayah | recorded_at          | skor_drainase | skor_bangunan | ...
---------|------------|----------------------|---------------|---------------|----
uuid-001 | RT003-RW05 | 2024-03-01 10:00:00  | 2             | 1             | ...
uuid-002 | RT003-RW05 | 2024-07-15 09:30:00  | 3             | 1             | ...  ← data baru masuk
uuid-003 | RT003-RW05 | 2025-01-20 14:00:00  | 3             | 2             | ...  ← data baru lagi
```
History lama tetap ada. "Kondisi terbaru" = baris dengan `recorded_at` paling baru per wilayah.

### 2. Containerization (Docker Compose)

Seluruh sistem berjalan dengan satu perintah `docker compose up`. Tidak ada instalasi manual Hadoop, Kafka, atau Spark di host machine.

### 3. Peta geospasial real-time di frontend

Web UI menampilkan peta choropleth Surabaya yang **auto-refresh** setiap kali data baru masuk. Setelah user submit form input data, pipeline berjalan dan peta diperbarui tanpa perlu reload halaman manual.

---

## Alur Sistem (6 Tahap)

```
[1. Registrasi Wilayah] → [2. Input Data] → [3. Ingest] → [4. Processing] → [5. Saving] → [6. Serving]
```

---

## Tahap 1 — Registrasi Wilayah

**Tujuan:** Mendefinisikan hierarki wilayah administratif sebelum data apapun diinput.

**Hierarki:** Kota → Kecamatan → Kelurahan → RW → RT

Wilayah disimpan di tabel master Delta Lake (`/bronze/master_wilayah`) dengan skema:

```
id_wilayah   STRING   PK  (contoh: "SBY-TGL-RWABY-05-003" → Surabaya-Tegalsari-RawaBuaya-RW05-RT003)
kota         STRING
kecamatan    STRING
kelurahan    STRING
rw           STRING
rt           STRING
total_kk     INTEGER  (estimasi jumlah kepala keluarga)
total_jiwa   INTEGER  (estimasi jumlah jiwa)
luas_m2      FLOAT
geometry_wkt STRING   (format GeoJSON string atau WKT POLYGON/POINT)
created_at   TIMESTAMP
```

- Tabel ini bersifat **master/referensi** — wilayah didaftarkan sekali, tidak berubah
- `geometry_wkt` berisi koordinat poligon batas RT/RW — digunakan untuk render peta
- Untuk MVP: daftarkan 3–5 kecamatan di Surabaya sebagai sampel
- Sumber geometri: export dari OpenStreetMap atau gambar manual di QGIS lalu export GeoJSON

---

## Tahap 2 — Input Data

**Tujuan:** Mencatat data survei kondisi wilayah. Setiap pengisian form menghasilkan **satu event baru** — tidak menimpa data sebelumnya.

### 2A. Data Primer (input manual via web form)

Form diisi per RT/RW. Satu submit = satu event baru di log:

**7 Indikator Kekumuhan PUPR** (nilai integer 0–3 per indikator):

| # | Indikator | Keterangan nilai |
|---|---|---|
| 1 | Kondisi bangunan | 0=baik, 1=sebagian rusak, 2=mayoritas rusak, 3=tidak layak huni |
| 2 | Aksesibilitas jalan | 0=baik, 1=sempit/rusak sebagian, 2=mayoritas rusak, 3=tidak ada akses |
| 3 | Drainase lingkungan | 0=baik, 1=sebagian mampet, 2=mayoritas tidak berfungsi, 3=tidak ada drainase |
| 4 | Pengelolaan air limbah | 0=semua septik tank, 1=sebagian, 2=mayoritas tidak ada, 3=open defecation |
| 5 | Pengelolaan persampahan | 0=terangkut rutin, 1=sebagian, 2=mayoritas tidak terangkut, 3=tidak ada |
| 6 | Proteksi kebakaran | 0=akses hydrant/damkar ada, 1=terbatas, 2=sulit, 3=tidak ada akses sama sekali |
| 7 | Penyediaan air minum | 0=PDAM/sumur layak, 1=sebagian, 2=mayoritas tidak layak, 3=tidak ada akses |

**Fitur Pendukung:**
```
jumlah_kk           INTEGER
jumlah_jiwa         INTEGER
pernah_banjir       BOOLEAN
frekuensi_banjir    INTEGER   (kejadian per tahun, 0 jika tidak pernah)
sosek_dominan       STRING    (rendah / menengah / tinggi)
catatan             TEXT      (opsional, keterangan tambahan petugas)
recorded_at         TIMESTAMP (auto-fill saat submit, tidak bisa diubah user)
recorded_by         STRING    (nama petugas, opsional)
```

### 2B. Data Sekunder (upload file batch)

Upload via form file di web UI:

| Sumber | Format | Kolom minimum |
|---|---|---|
| BPS Surabaya | CSV | `kelurahan, kepadatan_jiwa_per_km2, tahun_data` |
| BMKG/BPBD | CSV | `kelurahan, jumlah_kejadian_banjir, tahun` |
| PDAM | CSV | `kelurahan, pct_akses_air_bersih` |
| Ground truth | CSV | `id_wilayah, label_kumuh (0/1), sumber_label, tanggal_label` |

Ground truth bersumber dari: SK penetapan kawasan kumuh Pemkot Surabaya, atau peta KOTAKU (Kota Tanpa Kumuh) PUPR. Label ini digunakan untuk training dan validasi model ML.

---

## Tahap 3 — Ingest

**Tujuan:** Mengalirkan data dari form/upload ke Kafka, kemudian disimpan ke Bronze layer sebagai log event.

### Arsitektur ingest

```
Web Form / File Upload
        ↓
  FastAPI (container: api)
  - Validasi input
  - Tambahkan event_id (UUID), recorded_at (now())
        ↓
  Apache Kafka (container: kafka)
  ├── topic: survey-events        ← data primer, per RT/RW submit
  └── topic: secondary-batch      ← data BPS, BMKG, PDAM, ground truth
        ↓
  Kafka Consumer (container: consumer — Python script loop)
  - Poll setiap 5 detik
  - Tulis ke Bronze layer Delta Lake via PySpark
  - Emit SSE event ke FastAPI setelah selesai tulis (trigger refresh frontend)
        ↓
  Delta Lake Bronze Layer (HDFS)
  ├── /bronze/survey_events       ← append-only, setiap baris = 1 event submit
  └── /bronze/secondary_sources   ← append-only, setiap baris = 1 baris file upload
```

### Aturan Bronze layer

- **Tidak ada UPDATE, tidak ada DELETE** di Bronze — hanya APPEND
- Setiap baris menyimpan `event_id` (UUID), `id_wilayah`, dan `recorded_at`
- Jika data yang sama (wilayah yang sama) disubmit lagi, hasilnya adalah **baris baru** — bukan overwrite
- Kolom `recorded_at` adalah timestamp server saat event diterima API, bukan input user

---

## Tahap 4 — Processing

**Tujuan:** Transformasi data Bronze menjadi Silver (bersih, terstruktur) dan Gold (output analitik), dijalankan sebagai Spark jobs.

### Trigger processing

Processing dipicu secara otomatis setelah Kafka Consumer berhasil menulis ke Bronze. Bisa menggunakan:
- **Opsi A (lebih sederhana):** Kafka Consumer memanggil endpoint FastAPI `/api/internal/trigger-processing` setelah tulis Bronze selesai
- **Opsi B:** Spark Structured Streaming yang listen ke Kafka topic secara langsung (lebih kompleks, skip untuk MVP)

→ Gunakan **Opsi A** untuk MVP.

### Job 1 — Bronze → Silver (ETL + Agregasi)

```
Input  : /bronze/survey_events (semua event, termasuk history)
Output : /silver/latest_indicators   ← kondisi TERBARU per wilayah (1 baris per id_wilayah)
         /silver/event_history       ← SEMUA event history, lengkap (mirror Bronze + enrichment)
         /silver/feature_matrix      ← fitur per kelurahan untuk ML (agregasi dari RT/RW terbaru)
         /silver/ground_truth        ← label kumuh dari upload file
         /silver/population          ← estimasi jiwa per kelurahan

Logika "terbaru per wilayah":
  SELECT *, ROW_NUMBER() OVER (PARTITION BY id_wilayah ORDER BY recorded_at DESC) AS rn
  FROM bronze.survey_events
  WHERE rn = 1
  → Ini menjadi "snapshot terkini" yang dirender di peta

Logika event_history:
  SELECT * FROM bronze.survey_events ORDER BY id_wilayah, recorded_at
  → Semua history tersedia, digunakan untuk halaman "riwayat perubahan"

Agregasi ke kelurahan (untuk feature_matrix ML):
  - Ambil data terbaru tiap RT/RW di kelurahan tsb
  - Rata-rata 7 skor indikator
  - Join dengan data sekunder (kepadatan BPS, banjir BMKG, akses air PDAM)
  - Hasilkan 1 baris per kelurahan → menjadi input model ML
```

### Job 2 — Silver → ML (Training model)

```
Input  : /silver/feature_matrix + /silver/ground_truth
Output : /models/rf_slum_v{timestamp}    ← model tersimpan dengan versi timestamp
         /silver/model_metrics           ← akurasi, F1, confusion matrix, feature importance

Model  : Spark MLlib RandomForestClassifier
Fitur  : 7 skor PUPR + kepadatan_jiwa + frekuensi_banjir + pct_sosek_rendah + akses_air (total ~12 fitur)
Split  : 80% train / 20% test, stratified
Validasi: 5-fold cross-validation, metrik F1-score ≥ 0.85
Output feature importance disimpan → digunakan sebagai "faktor dominan"

Catatan: Job ini hanya dijalankan ulang jika ada data ground_truth baru.
Untuk prediksi rutin (setiap ada data baru), cukup load model versi terakhir.
```

### Job 3 — Silver + Model → Gold (Output Analitik)

```
Input  : /silver/latest_indicators + /silver/feature_matrix + /models/rf_slum_v{latest}
Output : /gold/slum_risk_score          ← skor risiko 0–100 + risk level per kelurahan
         /gold/slum_prediction          ← label prediksi kumuh + probabilitas per kelurahan
         /gold/slum_trend               ← perubahan skor dari event sebelumnya ke terbaru
         /gold/dominant_factors         ← top-3 faktor per kelurahan (dari feature importance)
         /gold/intervention_priority    ← ranking prioritas = risk_score × jiwa_terdampak

Risk score formula:
  risk_score = Σ (skor_indikator_i × bobot_i) × 100 / (3 × 7)
  bobot_i = feature importance dari model RF untuk indikator ke-i

Trend formula:
  Untuk tiap wilayah: ambil 2 event terbaru dari event_history
  delta_risk = risk_score_terbaru - risk_score_sebelumnya
  → positif = memburuk, negatif = membaik
```

---

## Tahap 5 — Saving (Delta Lake Schema)

**Tujuan:** Semua data tersimpan di HDFS menggunakan Delta Lake dengan prinsip append-only dan time travel.

### Struktur direktori HDFS

```
/data/
├── bronze/
│   ├── master_wilayah/         ← tabel referensi wilayah (insert-only, jarang berubah)
│   ├── survey_events/          ← LOG semua event submit form (append-only, tidak pernah dihapus)
│   └── secondary_sources/      ← data BPS, BMKG, PDAM, ground truth (append-only)
│
├── silver/
│   ├── latest_indicators/      ← snapshot terbaru per RT/RW (di-refresh setiap processing)
│   ├── event_history/          ← mirror bronze + enrichment, seluruh history
│   ├── feature_matrix/         ← fitur per kelurahan untuk ML
│   ├── ground_truth/           ← label kumuh historis
│   └── population/             ← estimasi jiwa per kelurahan
│
├── gold/
│   ├── slum_risk_score/        ← OUTPUT UTAMA: skor risiko per kelurahan
│   ├── slum_prediction/        ← prediksi label kumuh + probabilitas
│   ├── slum_trend/             ← delta skor antar dua event terbaru per wilayah
│   ├── dominant_factors/       ← top-3 faktor penyebab per kelurahan
│   └── intervention_priority/  ← ranking prioritas intervensi
│
└── models/
    └── rf_slum_v{timestamp}/   ← model Spark MLlib tersimpan, versi by timestamp
```

### Aturan penyimpanan per tabel

| Tabel | Mode tulis | Alasan |
|---|---|---|
| `bronze/survey_events` | APPEND only | Log event — tidak boleh diubah |
| `bronze/secondary_sources` | APPEND only | Log batch upload |
| `silver/latest_indicators` | OVERWRITE partisi per `id_wilayah` | Selalu reflect snapshot terbaru |
| `silver/event_history` | APPEND only | History lengkap untuk analisis temporal |
| `silver/feature_matrix` | OVERWRITE per `id_kelurahan` | Fitur ML selalu fresh |
| `gold/*` | OVERWRITE penuh | Output analitik diregenerasi setiap processing |

### Fitur Delta Lake yang wajib digunakan

- **Transaction log** — setiap operasi tulis terekam; Bronze layer tidak pernah kehilangan data
- **Time travel** — `SELECT * FROM delta.\`/data/silver/event_history\` TIMESTAMP AS OF '2024-03-01'` untuk lihat kondisi wilayah di masa lalu
- **Schema enforcement** — tolak data yang kolom atau tipe datanya tidak sesuai
- **Partisi** — `silver/event_history` dan `silver/latest_indicators` dipartisi by `kelurahan` agar query per wilayah cepat
- **MERGE/upsert** — digunakan di `silver/latest_indicators` untuk update snapshot terbaru tanpa duplikasi

---

## Tahap 6 — Serving

**Tujuan:** Web UI yang menampilkan peta geospasial real-time dan data analitik.

### Backend (FastAPI, container: api)

**Endpoint publik:**

```
--- Wilayah ---
GET  /api/wilayah                         ← daftar semua wilayah terdaftar (tree: kec→kel→rw→rt)
POST /api/wilayah                         ← daftarkan wilayah baru

--- Input data ---
POST /api/survey                          ← submit form survei → produce ke Kafka
POST /api/secondary/upload                ← upload file CSV data sekunder

--- Peta (untuk Leaflet.js) ---
GET  /api/map/risk-score                  ← GeoJSON semua kelurahan + risk_score (untuk choropleth)
GET  /api/map/prediction                  ← GeoJSON kelurahan + label prediksi kumuh

--- Analitik per wilayah ---
GET  /api/wilayah/{id}/latest             ← kondisi terbaru (7 skor indikator)
GET  /api/wilayah/{id}/history            ← SEMUA riwayat event wilayah tersebut (log lengkap)
GET  /api/wilayah/{id}/trend              ← delta skor dari waktu ke waktu
GET  /api/kelurahan/{id}/factors          ← top-3 faktor dominan
GET  /api/priority                        ← ranking prioritas intervensi

--- Dashboard summary ---
GET  /api/summary                         ← total kelurahan kumuh, total jiwa terdampak, dll

--- Real-time (SSE) ---
GET  /api/stream/updates                  ← Server-Sent Events; emit event setiap ada data baru selesai diproses
```

**Endpoint internal (dipanggil consumer, bukan user):**
```
POST /api/internal/trigger-processing    ← trigger Spark jobs setelah Bronze selesai ditulis
```

### Real-time update ke frontend

Mekanisme yang digunakan: **Server-Sent Events (SSE)** — lebih ringan dari WebSocket untuk use case ini.

Alur real-time:
```
User submit form
    ↓
FastAPI → Kafka producer
    ↓
Kafka Consumer menulis ke Bronze Delta
    ↓
Consumer panggil POST /api/internal/trigger-processing
    ↓
FastAPI jalankan Spark jobs (Bronze→Silver→Gold) secara async (background task)
    ↓
Setelah job selesai, FastAPI emit SSE event: { "type": "map_updated", "timestamp": "..." }
    ↓
Frontend (Leaflet.js) yang sudah subscribe /api/stream/updates
→ auto fetch ulang /api/map/risk-score
→ update layer choropleth peta tanpa reload halaman
```

### Frontend (Nginx + HTML/JS, container: frontend)

**Halaman 1 — Peta Utama (`/`)**
- Peta Surabaya via Leaflet.js
- Layer choropleth per kelurahan: warna berdasarkan `risk_score` (hijau → kuning → oranye → merah)
- Legend: 4 level risiko (Ringan / Sedang / Berat / Sangat Berat)
- Klik kelurahan → popup: nama, risk score, top-3 faktor, jumlah jiwa terdampak, link ke detail
- Subscribe ke `/api/stream/updates` → auto-refresh layer saat ada data baru
- Tombol toggle layer: "Kondisi Terkini" vs "Prediksi Berpotensi Kumuh"

**Halaman 2 — Input Data (`/input`)**
- Dropdown hierarki: pilih Kecamatan → Kelurahan → RW → RT
- Form 7 indikator (radio button 0–3 per indikator)
- Form fitur tambahan (jiwa, banjir, sosek)
- Setelah submit: tampilkan notifikasi "Data diterima, peta sedang diperbarui..."
- Peta kecil di samping form yang menghighlight wilayah yang sedang diisi

**Halaman 3 — Riwayat Wilayah (`/wilayah/:id`)**
- Header: nama lengkap wilayah (RT/RW/Kelurahan/Kecamatan)
- Tabel semua event historis dari `/api/wilayah/{id}/history` — diurutkan terbaru di atas
- Kolom: tanggal input, 7 skor, risk score, catatan
- Line chart: perubahan risk score dari waktu ke waktu (tiap event = satu titik di grafik)
- Menunjukkan dengan jelas bahwa data lama tidak ditimpa — semua tercatat

**Halaman 4 — Prioritas Intervensi (`/prioritas`)**
- Tabel ranking: rank, kelurahan, risk level, risk score, jiwa terdampak, top faktor
- Filter by kecamatan
- Export CSV (opsional)

**Halaman 5 — Daftarkan Wilayah (`/wilayah/baru`)**
- Form pendaftaran wilayah baru ke master tabel
- Input geometry: paste GeoJSON atau klik di peta untuk mark titik koordinat

---

## Docker Compose — Struktur Services

```yaml
# docker-compose.yml (referensi, bukan final — sesuaikan dengan kebutuhan)

services:

  namenode:
    image: apache/hadoop:3
    hostname: namenode
    environment:
      - ENSURE_NAMENODE_DIR=/tmp/hadoop-root/dfs/name
    ports:
      - "9870:9870"   # HDFS Web UI
    volumes:
      - hadoop_namenode:/tmp/hadoop-root/dfs/name

  datanode:
    image: apache/hadoop:3
    hostname: datanode
    environment:
      - SERVICE_PRECONDITION=namenode:9870
    volumes:
      - hadoop_datanode:/tmp/hadoop-root/dfs/data
    depends_on:
      - namenode

  zookeeper:
    image: bitnami/zookeeper:latest
    environment:
      - ALLOW_ANONYMOUS_LOGIN=yes

  kafka:
    image: bitnami/kafka:latest
    ports:
      - "9092:9092"
    environment:
      - KAFKA_BROKER_ID=1
      - KAFKA_CFG_ZOOKEEPER_CONNECT=zookeeper:2181
      - KAFKA_CFG_LISTENERS=PLAINTEXT://:9092
      - KAFKA_CFG_ADVERTISED_LISTENERS=PLAINTEXT://kafka:9092
      - ALLOW_PLAINTEXT_LISTENER=yes
    depends_on:
      - zookeeper

  spark:
    image: bitnami/spark:latest
    environment:
      - SPARK_MODE=master
    ports:
      - "8080:8080"   # Spark Web UI
    volumes:
      - ./spark_jobs:/opt/spark_jobs   # mount folder berisi file .py jobs

  spark-worker:
    image: bitnami/spark:latest
    environment:
      - SPARK_MODE=worker
      - SPARK_MASTER_URL=spark://spark:7077
    depends_on:
      - spark

  consumer:
    build: ./consumer                  # Dockerfile Python custom
    environment:
      - KAFKA_BOOTSTRAP=kafka:9092
      - HDFS_URL=hdfs://namenode:9000
      - API_URL=http://api:8000
    depends_on:
      - kafka
      - namenode

  api:
    build: ./api                       # Dockerfile FastAPI custom
    ports:
      - "8000:8000"
    environment:
      - HDFS_URL=hdfs://namenode:9000
      - KAFKA_BOOTSTRAP=kafka:9092
      - SPARK_MASTER=spark://spark:7077
    depends_on:
      - kafka
      - namenode
      - spark

  frontend:
    build: ./frontend                  # Nginx + static HTML/JS
    ports:
      - "3000:80"
    depends_on:
      - api

volumes:
  hadoop_namenode:
  hadoop_datanode:
```

**Folder struktur proyek:**
```
project/
├── docker-compose.yml
├── api/
│   ├── Dockerfile
│   ├── main.py              ← FastAPI app
│   ├── routers/
│   └── requirements.txt
├── consumer/
│   ├── Dockerfile
│   ├── consumer.py          ← Kafka consumer loop
│   └── requirements.txt
├── spark_jobs/
│   ├── job1_bronze_silver.py
│   ├── job2_train_model.py
│   └── job3_silver_gold.py
└── frontend/
    ├── Dockerfile
    ├── nginx.conf
    └── public/
        ├── index.html       ← halaman peta utama
        ├── input.html
        ├── wilayah.html
        ├── prioritas.html
        └── js/
            ├── map.js       ← Leaflet.js choropleth + SSE listener
            └── charts.js    ← Chart.js untuk line chart trend
```

---

## Skema Data Utama

### `bronze/survey_events` (append-only log)
```
event_id         STRING   (UUID, generated by API)
id_wilayah       STRING   FK → master_wilayah
recorded_at      TIMESTAMP (server time saat API terima request)
recorded_by      STRING
skor_bangunan    INTEGER  (0–3)
skor_jalan       INTEGER  (0–3)
skor_drainase    INTEGER  (0–3)
skor_air_limbah  INTEGER  (0–3)
skor_sampah      INTEGER  (0–3)
skor_kebakaran   INTEGER  (0–3)
skor_air_minum   INTEGER  (0–3)
jumlah_kk        INTEGER
jumlah_jiwa      INTEGER
pernah_banjir    BOOLEAN
frekuensi_banjir INTEGER
sosek_dominan    STRING
catatan          STRING
```

### `silver/event_history` (full history, append-only)
```
-- semua kolom dari survey_events +
risk_score_saat_itu   FLOAT   (dihitung saat processing, bukan saat input)
risk_level_saat_itu   STRING
kelurahan             STRING  (join dari master_wilayah)
kecamatan             STRING
```

### `silver/latest_indicators` (snapshot terbaru per wilayah)
```
id_wilayah       STRING   PK
kelurahan        STRING
kecamatan        STRING
last_event_id    STRING   (event_id dari baris terbaru)
last_recorded_at TIMESTAMP
skor_bangunan    FLOAT
... (7 skor)
jumlah_jiwa      INTEGER
pernah_banjir    BOOLEAN
frekuensi_banjir INTEGER
sosek_dominan    STRING
```

### `gold/slum_risk_score` (output utama untuk peta)
```
id_kelurahan       STRING
kelurahan          STRING
kecamatan          STRING
risk_score         FLOAT    (0–100)
risk_level         STRING   (Ringan / Sedang / Berat / Sangat Berat)
proba_kumuh        FLOAT    (0.0–1.0)
label_prediksi     INTEGER  (0 / 1)
jiwa_terdampak     INTEGER
top_faktor_1       STRING
top_faktor_2       STRING
top_faktor_3       STRING
geometry_wkt       STRING   (dari master_wilayah, untuk render peta)
last_updated       TIMESTAMP
prioritas_rank     INTEGER
```

---

## Batasan MVP (yang TIDAK perlu diimplementasikan)

- Tidak perlu multi-node Hadoop — single NameNode + single DataNode sudah cukup
- Tidak perlu Spark Structured Streaming — trigger berbasis HTTP sudah cukup
- Tidak perlu autentikasi user / login
- Tidak perlu GeoSpark/Sedona — koordinat disimpan sebagai WKT/GeoJSON string biasa
- Tidak perlu lebih dari satu model ML — Random Forest saja
- Tidak perlu message replay Kafka (retention default sudah cukup)

---

## Urutan Pengembangan yang Disarankan

```
Sprint 1 — Setup & infrastruktur (3–4 hari)
  → Buat docker-compose.yml dengan semua services
  → Verifikasi Hadoop HDFS bisa tulis/baca, Kafka bisa produce/consume
  → Setup PySpark + delta-spark di container Spark
  → Test: python script tulis Delta table ke HDFS, baca kembali

Sprint 2 — Registrasi wilayah + input form (3–4 hari)
  → Buat endpoint POST/GET /api/wilayah (FastAPI)
  → Buat halaman /wilayah/baru (form pendaftaran wilayah + koordinat)
  → Buat halaman /input (form 7 indikator)
  → Test: submit form → data masuk Kafka → consumer tulis ke Bronze Delta

Sprint 3 — Pipeline processing + Gold layer (4–5 hari)
  → Buat job1_bronze_silver.py (ETL + latest_indicators + event_history)
  → Masukkan data ground truth CSV → buat job2_train_model.py
  → Buat job3_silver_gold.py (risk score, prediksi, faktor, prioritas)
  → Test: trigger jobs via POST /api/internal/trigger-processing

Sprint 4 — Peta real-time + serving (3–4 hari)
  → Buat halaman peta utama (Leaflet.js choropleth dari /api/map/risk-score)
  → Implementasi SSE di FastAPI + listener di frontend
  → Test end-to-end: submit form → peta auto-update tanpa reload
  → Buat halaman riwayat wilayah (tabel history + line chart trend)
  → Buat halaman prioritas intervensi

Sprint 5 — Polish + validasi akurasi (2–3 hari)
  → Evaluasi model: F1-score, confusion matrix, feature importance
  → Dokumentasi API
  → Test seluruh alur dari awal
```