.PHONY: up down logs seed clean restart status

# Start all services
up:
	docker compose up -d
	@echo "Waiting for API..."
	@sleep 20
	@curl -sf http://localhost:8000/health || echo "API not ready yet, check: docker compose logs api"
	@echo ""
	@echo "🌐 Frontend: http://localhost:3000"
	@echo "🔌 API Docs: http://localhost:8000/docs"
	@echo "🐘 HDFS UI:  http://localhost:9870"
	@echo "⚡ Spark UI:  http://localhost:8080"

# Full startup with init
start:
	bash startup.sh

# Stop all services
down:
	docker compose down

# Rebuild and restart
restart:
	docker compose down
	docker compose build
	docker compose up -d

# View logs
logs:
	docker compose logs -f api consumer

# Seed initial data
seed:
	docker run --rm --network fp-big-data_slum_net \
		-e API_URL=http://api:8000 \
		-v $(PWD)/init_scripts:/init_scripts \
		python:3.11-slim \
		python /init_scripts/seed_wilayah.py

# Initialize HDFS
init-hdfs:
	docker compose up hdfs-init

# Trigger pipeline manually
trigger:
	curl -X POST http://localhost:8000/api/internal/trigger-processing

# Clean volumes (destructive!)
clean:
	docker compose down -v
	@echo "⚠️  All volumes removed. Data has been cleared."

# Show service status
status:
	docker compose ps

# Health check
health:
	@echo "=== API Health ==="
	@curl -sf http://localhost:8000/health | python3 -m json.tool || echo "API not responding"
	@echo ""
	@echo "=== Summary ==="
	@curl -sf http://localhost:8000/api/summary | python3 -m json.tool || echo "No summary data"

# Test SSE stream (ctrl+c to stop)
sse:
	curl -N http://localhost:8000/api/stream/updates

# Submit a test survey event
test-survey:
	curl -X POST http://localhost:8000/api/survey \
		-H "Content-Type: application/json" \
		-d '{"id_wilayah":"SBY-TBS-PEGIRIAN-02-001","skor_bangunan":2,"skor_jalan":2,"skor_drainase":3,"skor_air_limbah":2,"skor_sampah":2,"skor_kebakaran":3,"skor_air_minum":1,"jumlah_kk":200,"jumlah_jiwa":800,"pernah_banjir":true,"frekuensi_banjir":3,"sosek_dominan":"rendah","catatan":"Test submission","recorded_by":"Make Test"}'
