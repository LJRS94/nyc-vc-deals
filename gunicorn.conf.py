# Gunicorn configuration
# SQLite only supports one writer — use 1 worker with multiple threads
workers = 1
threads = 8
timeout = 120
