#!/usr/bin/env sh
export PYTHONPATH=src:$PYTHONPATH

echo "Migration..."
alembic upgrade head

echo "Starting barsic_web..."
uvicorn main:app --host "0.0.0.0" --port "8000" --reload
