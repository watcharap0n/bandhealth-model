# Brand Health Pipeline Dockerfile
# Production-ready container for Brand Health modeling pipeline

# Use official Python 3.11 slim image as base
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# Install system dependencies (if needed for pyarrow or other packages)
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements file first (for better layer caching)
COPY requirements.txt .

# Install Python dependencies
RUN pip install --upgrade pip && \
    pip install -r requirements.txt

# Copy source code
COPY src/ ./src/
COPY run_pipeline.py .
COPY run_pipeline_hops.py .
COPY README.md .

# Create necessary directories for pipeline outputs
RUN mkdir -p datasets reports outputs artifacts

# Set default command to show help
CMD ["python", "run_pipeline.py", "--help"]
