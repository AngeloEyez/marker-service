#!/usr/bin/env bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

echo "=================================================="
echo "    Updating Marker Microservice to Latest Version "
echo "=================================================="

cd "$PROJECT_DIR"

TARGET_VERSION="${1:-latest}"
echo "[1/4] Target Marker version: $TARGET_VERSION"

echo "[2/4] Rebuilding Docker image without cache to pull latest packages..."
docker compose build --no-cache

echo "[3/4] Recreating and starting Marker service..."
docker compose up -d --force-recreate

echo "[4/4] Waiting for service to become healthy..."
sleep 5
for i in {1..12}; do
    if curl -s -f http://127.0.0.1:8090/health > /dev/null; then
        echo "=================================================="
        echo "Marker service updated and healthy!"
        curl -s http://127.0.0.1:8090/health | python3 -m json.tool || true
        exit 0
    fi
    echo "Waiting for health check... ($i/12)"
    sleep 3
done

echo "[Warning] Service did not respond within timeout. Check logs with: docker compose logs -f"
