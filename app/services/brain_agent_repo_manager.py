"""Source code access for the Brain Agent.

The agent may read the backend source only: in development that is the
checked-out repo, in the container it is /app/. Frontend repos are not
cloned or exposed (removed 2026-10-12 — the agent has no GitHub access).
"""

from pathlib import Path
from typing import List


def _find_backend_root() -> Path:
    """Return the root of the backend repo (where app/ lives)."""
    # In Docker: /app/  In dev: .../energyexe-core-backend/
    # Walk up from this file until we find app/main.py
    current = Path(__file__).resolve().parent  # app/services/
    for _ in range(5):
        if (current / "app" / "main.py").exists():
            return current
        current = current.parent
    # Fallback: assume /app/ (Docker default)
    return Path("/app")


def get_repo_dirs() -> List[str]:
    """Return the repo directory paths the brain agent may read (backend only)."""
    return [str(_find_backend_root())]
