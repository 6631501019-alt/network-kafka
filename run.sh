#!/usr/bin/env bash

# ========================================================
# Network Telemetry & Automation System Launcher
# (FastAPI + Kafka + Paramiko Worker + Web Dashboard)
# ========================================================

set -e

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$PROJECT_DIR"

echo "========================================================"
echo " Starting Network Automation & Telemetry Environment..."
echo "========================================================"

# 1. Check & start Kafka containers
echo "[1/4] Checking Apache Kafka & Kafka UI Docker containers..."
export DOCKER_CONFIG=/tmp/docker_empty
docker compose up -d

# Wait briefly for Kafka to be healthy
echo "Waiting for Kafka broker to initialize..."
sleep 3

# 2. Check virtual environment
if [ ! -d "$PROJECT_DIR/venv" ]; then
    echo "[2/4] Setting up Python virtual environment..."
    python3 -m venv "$PROJECT_DIR/venv"
    "$PROJECT_DIR/venv/bin/pip install" -r "$PROJECT_DIR/backend/requirements.txt" -r "$PROJECT_DIR/worker/requirements.txt"
else
    echo "[2/4] Python virtual environment ready."
fi

PYTHON_BIN="$PROJECT_DIR/venv/bin/python3"

# 3. Start Paramiko Worker in background
echo "[3/4] Starting Paramiko Worker (Kafka Consumer)..."
PYTHONPATH="$PROJECT_DIR/worker" $PYTHON_BIN "$PROJECT_DIR/worker/worker.py" &
WORKER_PID=$!

# 4. Start FastAPI Backend in background
echo "[4/4] Starting FastAPI Backend (Kafka Producer & Web UI)..."
PYTHONPATH="$PROJECT_DIR/backend" $PYTHON_BIN -m uvicorn app:app --app-dir "$PROJECT_DIR/backend" --host 0.0.0.0 --port 8000 &
BACKEND_PID=$!

cleanup() {
    echo ""
    echo "Stopping background services..."
    kill $WORKER_PID 2>/dev/null || true
    kill $BACKEND_PID 2>/dev/null || true
    echo "Done."
    exit 0
}

trap cleanup SIGINT SIGTERM

echo ""
echo "========================================================"
echo " >> ALL SERVICES STARTED SUCCESSFULLY! <<"
echo "========================================================"
echo " - Web UI Dashboard:   http://localhost:8000"
echo " - Kafka UI Inspector: http://localhost:8085"
echo " - API Documentation:  http://localhost:8000/docs"
echo "========================================================"
echo "Press Ctrl+C to stop all services."
echo ""

# Keep running
wait
