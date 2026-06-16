#!/usr/bin/env python3
"""
Seed script: Populate master_wilayah with sample data from 5 kecamatan in Surabaya.
Also creates ground_truth CSV for ML training.
Run this AFTER the API is up: python seed_wilayah.py
"""

import json
import csv
import os
import requests
import time

API_URL = os.environ.get("API_URL", "http://localhost:8000")

# =============================================================
# SAMPLE WILAYAH DATA - 5 Kecamatan Surabaya
# Using approximate polygon coordinates (GeoJSON format)
# Real coordinates sourced from OpenStreetMap / Surabaya city data
# =============================================================

WILAYAH_DATA = [
    # ===================== TAMBAKSARI =====================
    {
        "id_wilayah": "SBY-TBS-GADING-01-001",
        "kota": "Surabaya",
        "kecamatan": "Tambaksari",
        "kelurahan": "Gading",
        "rw": "01",
        "rt": "001",
        "total_kk": 120,
        "total_jiwa": 480,
        "luas_m2": 15000.0,
        "geometry_wkt": json.dumps({
            "type": "Polygon",
            "coordinates": [[[112.7520, -7.2450], [112.7560, -7.2450],
                              [112.7560, -7.2480], [112.7520, -7.2480], [112.7520, -7.2450]]]
        })
    },
    {
        "id_wilayah": "SBY-TBS-GADING-01-002",
        "kota": "Surabaya",
        "kecamatan": "Tambaksari",
        "kelurahan": "Gading",
        "rw": "01",
        "rt": "002",
        "total_kk": 95,
        "total_jiwa": 380,
        "luas_m2": 12000.0,
        "geometry_wkt": json.dumps({
            "type": "Polygon",
            "coordinates": [[[112.7560, -7.2450], [112.7600, -7.2450],
                              [112.7600, -7.2480], [112.7560, -7.2480], [112.7560, -7.2450]]]
        })
    },
    {
        "id_wilayah": "SBY-TBS-PEGIRIAN-02-001",
        "kota": "Surabaya",
        "kecamatan": "Tambaksari",
        "kelurahan": "Pegirian",
        "rw": "02",
        "rt": "001",
        "total_kk": 200,
        "total_jiwa": 800,
        "luas_m2": 22000.0,
        "geometry_wkt": json.dumps({
            "type": "Polygon",
            "coordinates": [[[112.7480, -7.2490], [112.7530, -7.2490],
                              [112.7530, -7.2530], [112.7480, -7.2530], [112.7480, -7.2490]]]
        })
    },
    # ===================== SIMOKERTO =====================
    {
        "id_wilayah": "SBY-SMK-SIMOLAWANG-01-001",
        "kota": "Surabaya",
        "kecamatan": "Simokerto",
        "kelurahan": "Simolawang",
        "rw": "01",
        "rt": "001",
        "total_kk": 150,
        "total_jiwa": 600,
        "luas_m2": 18000.0,
        "geometry_wkt": json.dumps({
            "type": "Polygon",
            "coordinates": [[[112.7400, -7.2390], [112.7450, -7.2390],
                              [112.7450, -7.2430], [112.7400, -7.2430], [112.7400, -7.2390]]]
        })
    },
    {
        "id_wilayah": "SBY-SMK-TAMBAKREJO-03-002",
        "kota": "Surabaya",
        "kecamatan": "Simokerto",
        "kelurahan": "Tambakrejo",
        "rw": "03",
        "rt": "002",
        "total_kk": 180,
        "total_jiwa": 720,
        "luas_m2": 20000.0,
        "geometry_wkt": json.dumps({
            "type": "Polygon",
            "coordinates": [[[112.7350, -7.2410], [112.7400, -7.2410],
                              [112.7400, -7.2450], [112.7350, -7.2450], [112.7350, -7.2410]]]
        })
    },
    # ===================== SEMAMPIR =====================
    {
        "id_wilayah": "SBY-SMP-UJUNG-05-003",
        "kota": "Surabaya",
        "kecamatan": "Semampir",
        "kelurahan": "Ujung",
        "rw": "05",
        "rt": "003",
        "total_kk": 220,
        "total_jiwa": 880,
        "luas_m2": 25000.0,
        "geometry_wkt": json.dumps({
            "type": "Polygon",
            "coordinates": [[[112.7300, -7.2250], [112.7360, -7.2250],
                              [112.7360, -7.2300], [112.7300, -7.2300], [112.7300, -7.2250]]]
        })
    },
    {
        "id_wilayah": "SBY-SMP-WONOKUSUMO-04-001",
        "kota": "Surabaya",
        "kecamatan": "Semampir",
        "kelurahan": "Wonokusumo",
        "rw": "04",
        "rt": "001",
        "total_kk": 190,
        "total_jiwa": 760,
        "luas_m2": 21000.0,
        "geometry_wkt": json.dumps({
            "type": "Polygon",
            "coordinates": [[[112.7260, -7.2280], [112.7310, -7.2280],
                              [112.7310, -7.2320], [112.7260, -7.2320], [112.7260, -7.2280]]]
        })
    },
    # ===================== KENJERAN =====================
    {
        "id_wilayah": "SBY-KNJ-BULAK-02-001",
        "kota": "Surabaya",
        "kecamatan": "Kenjeran",
        "kelurahan": "Bulak",
        "rw": "02",
        "rt": "001",
        "total_kk": 160,
        "total_jiwa": 640,
        "luas_m2": 19000.0,
        "geometry_wkt": json.dumps({
            "type": "Polygon",
            "coordinates": [[[112.7800, -7.2150], [112.7850, -7.2150],
                              [112.7850, -7.2200], [112.7800, -7.2200], [112.7800, -7.2150]]]
        })
    },
    {
        "id_wilayah": "SBY-KNJ-TANAH-KALi-06-002",
        "kota": "Surabaya",
        "kecamatan": "Kenjeran",
        "kelurahan": "Tanah Kali Kedinding",
        "rw": "06",
        "rt": "002",
        "total_kk": 210,
        "total_jiwa": 840,
        "luas_m2": 24000.0,
        "geometry_wkt": json.dumps({
            "type": "Polygon",
            "coordinates": [[[112.7850, -7.2100], [112.7900, -7.2100],
                              [112.7900, -7.2150], [112.7850, -7.2150], [112.7850, -7.2100]]]
        })
    },
    # ===================== GUNUNG ANYAR =====================
    {
        "id_wilayah": "SBY-GNA-RUNGKUT-KIDUL-01-001",
        "kota": "Surabaya",
        "kecamatan": "Gunung Anyar",
        "kelurahan": "Rungkut Kidul",
        "rw": "01",
        "rt": "001",
        "total_kk": 130,
        "total_jiwa": 520,
        "luas_m2": 16000.0,
        "geometry_wkt": json.dumps({
            "type": "Polygon",
            "coordinates": [[[112.8050, -7.3350], [112.8100, -7.3350],
                              [112.8100, -7.3400], [112.8050, -7.3400], [112.8050, -7.3350]]]
        })
    },
]

# =============================================================
# GROUND TRUTH DATA (label kumuh dari SK Pemkot / KOTAKU)
# =============================================================
GROUND_TRUTH = [
    {"id_wilayah": "SBY-TBS-GADING-01-001", "label_kumuh": 0, "sumber_label": "SK Pemkot 2023", "tanggal_label": "2023-01-15"},
    {"id_wilayah": "SBY-TBS-GADING-01-002", "label_kumuh": 0, "sumber_label": "SK Pemkot 2023", "tanggal_label": "2023-01-15"},
    {"id_wilayah": "SBY-TBS-PEGIRIAN-02-001", "label_kumuh": 1, "sumber_label": "KOTAKU PUPR 2023", "tanggal_label": "2023-03-01"},
    {"id_wilayah": "SBY-SMK-SIMOLAWANG-01-001", "label_kumuh": 1, "sumber_label": "KOTAKU PUPR 2023", "tanggal_label": "2023-03-01"},
    {"id_wilayah": "SBY-SMK-TAMBAKREJO-03-002", "label_kumuh": 1, "sumber_label": "SK Pemkot 2023", "tanggal_label": "2023-02-10"},
    {"id_wilayah": "SBY-SMP-UJUNG-05-003", "label_kumuh": 1, "sumber_label": "KOTAKU PUPR 2022", "tanggal_label": "2022-08-20"},
    {"id_wilayah": "SBY-SMP-WONOKUSUMO-04-001", "label_kumuh": 1, "sumber_label": "KOTAKU PUPR 2022", "tanggal_label": "2022-08-20"},
    {"id_wilayah": "SBY-KNJ-BULAK-02-001", "label_kumuh": 0, "sumber_label": "SK Pemkot 2023", "tanggal_label": "2023-01-15"},
    {"id_wilayah": "SBY-KNJ-TANAH-KALi-06-002", "label_kumuh": 1, "sumber_label": "KOTAKU PUPR 2023", "tanggal_label": "2023-04-05"},
    {"id_wilayah": "SBY-GNA-RUNGKUT-KIDUL-01-001", "label_kumuh": 0, "sumber_label": "SK Pemkot 2023", "tanggal_label": "2023-01-15"},
]

def wait_for_api(max_retries=30, delay=5):
    """Wait until the API is responsive."""
    print(f"Waiting for API at {API_URL}...")
    for i in range(max_retries):
        try:
            resp = requests.get(f"{API_URL}/health", timeout=5)
            if resp.status_code == 200:
                print("API is ready!")
                return True
        except Exception:
            pass
        print(f"  Retry {i+1}/{max_retries}...")
        time.sleep(delay)
    return False


def seed_wilayah():
    """POST each wilayah to the API."""
    print("\n=== Seeding wilayah data ===")
    success = 0
    for w in WILAYAH_DATA:
        try:
            resp = requests.post(f"{API_URL}/api/wilayah", json=w, timeout=10)
            if resp.status_code in (200, 201):
                print(f"  ✓ {w['id_wilayah']}")
                success += 1
            elif resp.status_code == 409:
                print(f"  ~ {w['id_wilayah']} already exists")
                success += 1
            else:
                print(f"  ✗ {w['id_wilayah']}: {resp.status_code} {resp.text[:100]}")
        except Exception as e:
            print(f"  ✗ {w['id_wilayah']}: {e}")
    print(f"  Seeded {success}/{len(WILAYAH_DATA)} wilayah")


def save_ground_truth_csv():
    """Save ground truth CSV for upload."""
    path = "/init_scripts/ground_truth.csv"
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["id_wilayah", "label_kumuh", "sumber_label", "tanggal_label"])
        writer.writeheader()
        writer.writerows(GROUND_TRUTH)
    print(f"\n=== Ground truth CSV saved to {path} ===")

    # Upload via API
    try:
        with open(path, "rb") as f:
            resp = requests.post(
                f"{API_URL}/api/secondary/upload",
                files={"file": ("ground_truth.csv", f, "text/csv")},
                data={"source_type": "ground_truth"},
                timeout=30
            )
        if resp.status_code in (200, 201):
            print("  ✓ Ground truth uploaded to API")
        else:
            print(f"  ~ Ground truth upload: {resp.status_code} {resp.text[:100]}")
    except Exception as e:
        print(f"  ~ Could not upload ground truth via API: {e}")


def seed_sample_survey_data():
    """Seed some sample survey events so we have data to visualize."""
    print("\n=== Seeding sample survey events ===")
    # Initial survey data with varied slum scores
    sample_events = [
        {
            "id_wilayah": "SBY-TBS-PEGIRIAN-02-001",
            "skor_bangunan": 2, "skor_jalan": 2, "skor_drainase": 3,
            "skor_air_limbah": 2, "skor_sampah": 2, "skor_kebakaran": 3, "skor_air_minum": 1,
            "jumlah_kk": 200, "jumlah_jiwa": 800,
            "pernah_banjir": True, "frekuensi_banjir": 3,
            "sosek_dominan": "rendah", "catatan": "Data awal survei 2024",
            "recorded_by": "Petugas Seed"
        },
        {
            "id_wilayah": "SBY-SMK-SIMOLAWANG-01-001",
            "skor_bangunan": 2, "skor_jalan": 1, "skor_drainase": 2,
            "skor_air_limbah": 2, "skor_sampah": 2, "skor_kebakaran": 2, "skor_air_minum": 1,
            "jumlah_kk": 150, "jumlah_jiwa": 600,
            "pernah_banjir": True, "frekuensi_banjir": 2,
            "sosek_dominan": "rendah", "catatan": "Data awal survei 2024",
            "recorded_by": "Petugas Seed"
        },
        {
            "id_wilayah": "SBY-SMK-TAMBAKREJO-03-002",
            "skor_bangunan": 3, "skor_jalan": 2, "skor_drainase": 3,
            "skor_air_limbah": 3, "skor_sampah": 2, "skor_kebakaran": 3, "skor_air_minum": 2,
            "jumlah_kk": 180, "jumlah_jiwa": 720,
            "pernah_banjir": True, "frekuensi_banjir": 5,
            "sosek_dominan": "rendah", "catatan": "Wilayah sangat kumuh",
            "recorded_by": "Petugas Seed"
        },
        {
            "id_wilayah": "SBY-SMP-UJUNG-05-003",
            "skor_bangunan": 2, "skor_jalan": 2, "skor_drainase": 2,
            "skor_air_limbah": 2, "skor_sampah": 3, "skor_kebakaran": 2, "skor_air_minum": 2,
            "jumlah_kk": 220, "jumlah_jiwa": 880,
            "pernah_banjir": True, "frekuensi_banjir": 4,
            "sosek_dominan": "rendah", "catatan": "Area dekat laut, risiko tinggi",
            "recorded_by": "Petugas Seed"
        },
        {
            "id_wilayah": "SBY-SMP-WONOKUSUMO-04-001",
            "skor_bangunan": 1, "skor_jalan": 1, "skor_drainase": 2,
            "skor_air_limbah": 1, "skor_sampah": 2, "skor_kebakaran": 2, "skor_air_minum": 1,
            "jumlah_kk": 190, "jumlah_jiwa": 760,
            "pernah_banjir": False, "frekuensi_banjir": 0,
            "sosek_dominan": "rendah", "catatan": "Butuh perbaikan drainase",
            "recorded_by": "Petugas Seed"
        },
        {
            "id_wilayah": "SBY-TBS-GADING-01-001",
            "skor_bangunan": 0, "skor_jalan": 0, "skor_drainase": 1,
            "skor_air_limbah": 0, "skor_sampah": 1, "skor_kebakaran": 1, "skor_air_minum": 0,
            "jumlah_kk": 120, "jumlah_jiwa": 480,
            "pernah_banjir": False, "frekuensi_banjir": 0,
            "sosek_dominan": "menengah", "catatan": "Kondisi cukup baik",
            "recorded_by": "Petugas Seed"
        },
        {
            "id_wilayah": "SBY-KNJ-BULAK-02-001",
            "skor_bangunan": 1, "skor_jalan": 0, "skor_drainase": 1,
            "skor_air_limbah": 1, "skor_sampah": 1, "skor_kebakaran": 1, "skor_air_minum": 0,
            "jumlah_kk": 160, "jumlah_jiwa": 640,
            "pernah_banjir": False, "frekuensi_banjir": 0,
            "sosek_dominan": "menengah", "catatan": "",
            "recorded_by": "Petugas Seed"
        },
        {
            "id_wilayah": "SBY-KNJ-TANAH-KALi-06-002",
            "skor_bangunan": 2, "skor_jalan": 1, "skor_drainase": 2,
            "skor_air_limbah": 2, "skor_sampah": 2, "skor_kebakaran": 2, "skor_air_minum": 1,
            "jumlah_kk": 210, "jumlah_jiwa": 840,
            "pernah_banjir": True, "frekuensi_banjir": 2,
            "sosek_dominan": "rendah", "catatan": "Perlu monitoring rutin",
            "recorded_by": "Petugas Seed"
        },
    ]

    for event in sample_events:
        try:
            resp = requests.post(f"{API_URL}/api/survey", json=event, timeout=10)
            if resp.status_code in (200, 201):
                print(f"  ✓ Survey event for {event['id_wilayah']}")
            else:
                print(f"  ✗ {event['id_wilayah']}: {resp.status_code} {resp.text[:100]}")
        except Exception as e:
            print(f"  ✗ {event['id_wilayah']}: {e}")
        time.sleep(0.5)


if __name__ == "__main__":
    if not wait_for_api():
        print("API not available, exiting.")
        exit(1)

    seed_wilayah()
    save_ground_truth_csv()
    time.sleep(2)
    seed_sample_survey_data()

    print("\n=== Seeding complete! ===")
    print("Wait ~30 seconds for Spark processing to complete, then open the frontend.")
