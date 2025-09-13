#!/bin/bash

echo "Setting up local development environment for Celery..."

# Check if Docker is installed
if ! command -v docker &> /dev/null; then
    echo "Docker is not installed. Please install Docker first."
    exit 1
fi

# Start local Valkey instance
echo "Starting local Valkey (Redis) instance..."
docker run -d \
    --name energyexe-valkey \
    -p 6379:6379 \
    valkey/valkey:7-alpine \
    valkey-server --appendonly yes

# Wait for Valkey to be ready
echo "Waiting for Valkey to be ready..."
for i in {1..10}; do
    if docker exec energyexe-valkey valkey-cli ping &> /dev/null; then
        echo "Valkey is ready!"
        break
    fi
    echo "Waiting... ($i/10)"
    sleep 2
done

# Create local .env file if it doesn't exist
if [ ! -f .env ]; then
    echo "Creating .env file for local development..."
    cat > .env << EOF
# Local Development Configuration
DATABASE_URL=postgresql+asyncpg://postgres:RwaN9FJDCgP2AhuALxZ4Wa7QfvbKXQ647AAickORJ0rq5N6lUG19UneFJJTJ9Jnv@146.235.201.245:5432/energyexe_db

# API Keys
ENTSOE_API_KEY=3b00489d-a886-48a4-95ad-981da57f7b62
ELEXON_API_KEY=ytitiohgylom033
EIA_API_KEY=bLXfqlf12SKY6t6kIz03IKGgoTfTBxr9pOLKiZeZ

# Local Valkey/Redis
VALKEY_PUBLIC_HOST=localhost
VALKEY_PUBLIC_PORT=6379
VALKEY_PASSWORD=
VALKEY_USER=default

# Celery uses local Redis
CELERY_BROKER_URL=redis://localhost:6379/0
CELERY_RESULT_BACKEND=redis://localhost:6379/1
EOF
    echo ".env file created!"
else
    echo ".env file already exists. Updating Valkey configuration..."
    # Update existing .env to use local Redis
    sed -i.bak 's/VALKEY_PUBLIC_HOST=.*/VALKEY_PUBLIC_HOST=localhost/' .env
    sed -i.bak 's/VALKEY_PASSWORD=.*/VALKEY_PASSWORD=/' .env
fi

echo ""
echo "Local development environment is ready!"
echo ""
echo "To start services:"
echo "  1. API Server:    make run-api"
echo "  2. Celery Worker: make run-worker"
echo "  3. Flower:        make run-flower"
echo ""
echo "To stop Valkey:"
echo "  docker stop energyexe-valkey && docker rm energyexe-valkey"
echo ""