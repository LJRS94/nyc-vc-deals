#!/bin/bash
# Initialize DB then start gunicorn with CORRECT settings for SQLite
python -c "from database import restore_if_empty, init_db; restore_if_empty(); init_db()"

# SQLite only supports 1 writer process — always use 1 worker + threads
exec gunicorn api_server:app \
    --bind "0.0.0.0:${PORT:-8080}" \
    --workers 1 \
    --threads 8 \
    --timeout 120
