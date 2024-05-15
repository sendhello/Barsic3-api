#!/usr/bin/env sh
export PYTHONPATH=src:$PYTHONPATH

echo "Migration..."
alembic upgrade head

echo "Starting barsic_web..."
gunicorn main:app --workers 1 --worker-class uvicorn.workers.UvicornWorker --timeout 300 --bind 0.0.0.0:8000
