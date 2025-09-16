# Multi-stage Docker build for FastAPI application
FROM python:3.11-slim AS base

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# Install system dependencies and clean up in a single layer
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    curl && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/* /var/cache/apt/* /tmp/*

# Create non-root user
RUN useradd --create-home --shell /bin/bash app

# Set work directory
WORKDIR /app

# Copy requirements file
COPY requirements.txt ./

# Development stage
FROM base AS development

# Install dependencies with offline cache and retries
RUN pip install --retries 5 --timeout 30 -r requirements.txt

# Copy source code
COPY --chown=app:app . .

# Switch to non-root user
USER app

# Expose port
EXPOSE 8001

# Run development server
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8001", "--reload"]

# Production stage
FROM base AS production

# Install dependencies
RUN pip install --retries 5 --timeout 30 -r requirements.txt

# Copy source code
COPY --chown=app:app . .

# Make startup script executable
# USER root
# RUN chmod +x /app/scripts/railway_start.sh
# USER app

# Expose port (Railway will override this with PORT env var)
EXPOSE 8001

# Health check (only for API service)
HEALTHCHECK --interval=30s --timeout=30s --start-period=5s --retries=3 \
    CMD if [ "$RAILWAY_SERVICE_TYPE" != "worker" ]; then curl -f http://localhost:${PORT:-8001}/health || exit 1; else exit 0; fi

# Use the startup script that checks RAILWAY_SERVICE_TYPE
CMD ["/bin/bash", "/app/scripts/railway_start.sh"]