#!/bin/bash
# Start all development services in tmux

echo "🚀 Starting EnergyExe Development Environment"
echo ""

# Check if Redis is running
if ! redis-cli ping > /dev/null 2>&1; then
    echo "❌ Redis is not running!"
    echo "Please start Redis first:"
    echo "  Option 1: redis-server"
    echo "  Option 2: docker run -d -p 6379:6379 redis:7-alpine"
    exit 1
fi

echo "✅ Redis is running"

# Check if tmux is installed
if ! command -v tmux &> /dev/null; then
    echo "❌ tmux is not installed"
    echo "Install with: brew install tmux"
    exit 1
fi

# Kill existing session if it exists
tmux kill-session -t energyexe 2>/dev/null

# Create new tmux session
tmux new-session -d -s energyexe -n 'energyexe'

# Split into 3 panes
tmux split-window -h -t energyexe
tmux split-window -v -t energyexe

# Pane 0: Celery Worker
tmux send-keys -t energyexe:0.0 'cd energyexe-core-backend' C-m
tmux send-keys -t energyexe:0.0 'echo "🔄 Starting Celery Worker..."' C-m
tmux send-keys -t energyexe:0.0 'poetry run python scripts/run_celery_worker.py' C-m

# Pane 1: FastAPI Server
tmux send-keys -t energyexe:0.1 'cd energyexe-core-backend' C-m
tmux send-keys -t energyexe:0.1 'echo "🌐 Starting FastAPI Server..."' C-m
tmux send-keys -t energyexe:0.1 'sleep 3' C-m  # Wait for Celery to start
tmux send-keys -t energyexe:0.1 'poetry run python scripts/start.py' C-m

# Pane 2: Logs/Commands
tmux send-keys -t energyexe:0.2 'cd energyexe-core-backend' C-m
tmux send-keys -t energyexe:0.2 'echo "📊 Monitoring Pane"' C-m
tmux send-keys -t energyexe:0.2 'echo ""' C-m
tmux send-keys -t energyexe:0.2 'echo "Useful commands:"' C-m
tmux send-keys -t energyexe:0.2 'echo "  - Check Celery tasks: celery -A app.celery_app inspect active"' C-m
tmux send-keys -t energyexe:0.2 'echo "  - Monitor queue: redis-cli llen celery"' C-m
tmux send-keys -t energyexe:0.2 'echo "  - Clear queue: redis-cli del celery"' C-m
tmux send-keys -t energyexe:0.2 'echo ""' C-m

# Attach to the session
echo ""
echo "✨ All services started in tmux session 'energyexe'"
echo ""
echo "To attach: tmux attach -t energyexe"
echo "To detach: Ctrl+b, then d"
echo "To kill: tmux kill-session -t energyexe"
echo ""

tmux attach -t energyexe
