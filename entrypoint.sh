#!/bin/bash

# Start the Celery worker in the background
celery -A tasks worker --loglevel=info &

# Start the FastAPI application
uvicorn app:app --host 0.0.0.0 --port 8000
