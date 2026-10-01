# ==============================================================================
# Marker PDF Microservice Dockerfile
# Base: NVIDIA CUDA 12.4 Runtime on Ubuntu 22.04
# ==============================================================================
FROM nvidia/cuda:12.4.1-runtime-ubuntu22.04

LABEL maintainer="Antigravity Marker Team"
LABEL description="Marker Document-to-Markdown Microservice with CUDA GPU Acceleration"

ENV DEBIAN_FRONTEND=noninteractive \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    HF_HUB_ENABLE_HF_TRANSFER=0

# Install system dependencies for document processing (PDF, OCR, Office, Images)
RUN apt-get update && apt-get install -y --no-install-recommends \
    python3 \
    python3-pip \
    python3-dev \
    poppler-utils \
    ghostscript \
    tesseract-ocr \
    tesseract-ocr-chi-tra \
    tesseract-ocr-chi-sim \
    libgl1 \
    libglib2.0-0 \
    libgomp1 \
    curl \
    git \
    build-essential \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Upgrade pip and install PyTorch with CUDA 12.6 support (providing torch 2.14.1+cu126 for Marker >=2.7.0 compatibility)
RUN pip3 install --upgrade pip setuptools wheel && \
    pip3 install torch torchvision --index-url https://download.pytorch.org/whl/cu126

# Copy requirements and install python packages
COPY requirements.txt .
RUN pip3 install --extra-index-url https://download.pytorch.org/whl/cu126 -r requirements.txt

# Create application directories
RUN mkdir -p /app/server /app/scripts /app/doc /data/cache/huggingface /data/cache/torch /data/cache/datalab /tmp/marker_uploads

# Copy application code
COPY server/ /app/server/
COPY scripts/ /app/scripts/
COPY doc/ /app/doc/
COPY .env.example /app/.env.example

# Set permissions
RUN chmod +x /app/scripts/*.sh

EXPOSE 8090

ENTRYPOINT ["/app/scripts/entrypoint.sh"]
