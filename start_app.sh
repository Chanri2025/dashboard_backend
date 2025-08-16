#!/bin/bash

APP_NAME="fastapi_app"
APP_MODULE="main:app"
HOST="0.0.0.0"
PORT=8000
WORKERS=4

# Activate venv if you are using one
# source venv/bin/activate

# Run with Gunicorn + Uvicorn workers
exec gunicorn $APP_MODULE \
    --workers $WORKERS \
    --worker-class uvicorn.workers.UvicornWorker \
    --bind $HOST:$PORT \
    --timeout 120 \
    --log-level info