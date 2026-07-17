FROM python:3.12-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt && \
    pip install "pyiceberg[pyiceberg-core,sql-sqlite]" flask-cors && \
    pip uninstall -y lance || true

# Copy application code
COPY backend/ ./backend/
COPY frontend/ ./frontend/
COPY data/images/ ./data/images/

# Set environment variables for Docker mode (connect to real services)
ENV USE_MOCK_SERVICES=false
ENV DOCKER_MODE=true
ENV PYTHONUTF8=1
ENV TZ=Asia/Shanghai
ENV HF_HOME=/root/.cache/huggingface

# Expose ports
EXPOSE 8888

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s \
  CMD curl -f http://localhost:8888/api/health || exit 1

# Start the application
WORKDIR /app
CMD ["python", "backend/main.py"]
