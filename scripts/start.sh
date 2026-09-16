#!/usr/bin/env bash

set -euo pipefail

# Move to project root no matter where this script is called from
cd "$(dirname "$0")/.."

echo "=== Starting CDC Pipeline ==="

# Create .env from template if it does not exist
if [ ! -f ".env" ]; then
    cp .env.example .env
    echo ".env created from .env.example"
fi

# Build custom Flink images
docker compose build

# Start infrastructure
docker compose up -d \
    mysql \
    postgres \
    kafka \
    kafka-init \
    connect \
    flink-jobmanager \
    flink-taskmanager

# Create Python virtual environment if needed
if [ ! -d ".venv" ]; then
    python3 -m venv .venv
fi

source .venv/bin/activate

python -m pip install --upgrade pip
python -m pip install -r requirements.txt

# Register Debezium MySQL connector
python scripts/register_connector.py

# Submit Flink SQL streaming job
docker compose run --rm flink-sql \
    bin/sql-client.sh -f /opt/flink/sql/cdc_pipeline.sql

echo "=== CDC Pipeline Started ==="
echo "Flink UI: http://localhost:8081"
echo "Kafka Connect: http://localhost:8083/connectors/mysql-shop-connector/status"