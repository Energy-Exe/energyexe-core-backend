#!/bin/bash
# Start the backend API in a tmux session (local development).
#
# There is no worker process any more: scheduled jobs run in AWS EventBridge
# (docs/operations/scheduled-jobs.md). Valkey is optional locally — the report
# cache and the brain-agent rate limit fail open without it.

set -e

echo "Starting EnergyExe backend (tmux session 'energyexe')"
echo ""

if ! command -v tmux &> /dev/null; then
    echo "tmux is not installed. Install with: brew install tmux"
    exit 1
fi

if command -v redis-cli &> /dev/null && redis-cli ping > /dev/null 2>&1; then
    echo "Valkey/Redis: running"
else
    echo "Valkey/Redis: not running (optional; start with 'docker compose up -d valkey' if you want the cache)"
fi

# Kill existing session if it exists
tmux kill-session -t energyexe 2>/dev/null || true

tmux new-session -d -s energyexe -n 'energyexe'
tmux split-window -h -t energyexe

# Pane 0: FastAPI server (PORT from .env, default 8001; local convention is 8002)
tmux send-keys -t energyexe:0.0 'echo "Starting FastAPI server..."' C-m
tmux send-keys -t energyexe:0.0 'poetry run python scripts/start.py' C-m

# Pane 1: shell for commands
tmux send-keys -t energyexe:0.1 'echo "Useful commands:"' C-m
tmux send-keys -t energyexe:0.1 'echo "  poetry run pytest"' C-m
tmux send-keys -t energyexe:0.1 'echo "  poetry run alembic upgrade head"' C-m
tmux send-keys -t energyexe:0.1 'echo "  curl http://localhost:${PORT:-8001}/health"' C-m

echo ""
echo "To attach: tmux attach -t energyexe   |  detach: Ctrl+b, d   |  kill: tmux kill-session -t energyexe"
echo ""

tmux attach -t energyexe
