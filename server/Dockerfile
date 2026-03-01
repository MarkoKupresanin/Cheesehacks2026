# Base image
FROM python:3.10-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    libgl1 \
    libglib2.0-0 \
    libsm6 \
    libxext6 \
    libxrender1 \
    libgomp1 \
    && rm -rf /var/lib/apt/lists/*

# Install ALL Python dependencies (worker + aggregator combined)
COPY requirements_worker.txt .
COPY requirements_aggregator.txt .
RUN pip install --no-cache-dir -r requirements_worker.txt -r requirements_aggregator.txt

# Copy ALL scripts — both worker and aggregator use this same image
COPY worker.py .
COPY aggregator.py .

# Environment variables (overridden at runtime via docker-compose)
ENV NODE_ID=0
ENV MODEL_TYPE=yolo
ENV MALICIOUS=false
ENV REDPANDA_BROKER=redpanda:9092
ENV INPUT_TOPIC=verified_frames
ENV OUTPUT_TOPIC=swarm_telemetry

# Default command (aggregator overrides this in docker-compose.yml)
CMD ["python", "worker.py"]
