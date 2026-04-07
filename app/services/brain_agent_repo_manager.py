"""Manages source code repo access for the Brain Agent.

In development, the three EnergyExe repos are siblings on disk and used directly.
In production, the backend code is already in the container (/app/) and the two
frontend repos are shallow-cloned from GitHub on startup.
"""

import fcntl
import shutil
import subprocess
from pathlib import Path
from typing import List

import structlog

from app.core.config import get_settings

logger = structlog.get_logger(__name__)

# Repos to clone in production (backend is already in the container)
_FRONTEND_REPOS = [
    ("faisal-energyexe/energyexe-admin-ui", "energyexe-admin-ui"),
    ("faisal-energyexe/energyexe-client-ui", "energyexe-client-ui"),
]

# Backend repo directory name (for sibling detection in dev)
_BACKEND_REPO_NAME = "energyexe-core-backend"


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


def _is_dev_mode() -> bool:
    """Check if we're running in local dev (repos are siblings on disk)."""
    backend_root = _find_backend_root()
    parent = backend_root.parent
    # In dev, the parent contains all three repos as siblings
    return (
        (parent / "energyexe-admin-ui" / "src").exists()
        and (parent / "energyexe-client-ui" / "src").exists()
    )


def ensure_repos() -> None:
    """Clone frontend repos if running in production. Called once at startup.

    Uses a file lock to prevent multiple Uvicorn workers from cloning
    concurrently (production runs 4 workers).
    """
    if _is_dev_mode():
        logger.info("brain_agent_repos_dev_mode", msg="Using local sibling directories")
        return

    settings = get_settings()
    repos_dir = Path(settings.CODE_REPOS_DIR)
    repos_dir.mkdir(parents=True, exist_ok=True)

    token = settings.GITHUB_TOKEN
    if not token:
        logger.warning(
            "brain_agent_repos_no_token",
            msg="GITHUB_TOKEN not set — skipping frontend repo cloning. "
            "Brain agent will not have access to frontend source code.",
        )
        return

    # Acquire an exclusive file lock so only one worker clones at a time.
    lock_path = repos_dir / ".clone.lock"
    try:
        lock_fd = open(lock_path, "w")
        fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except (IOError, OSError):
        logger.info("brain_agent_repos_skip", msg="Another worker is already cloning repos")
        return

    try:
        for repo_slug, dir_name in _FRONTEND_REPOS:
            dest = repos_dir / dir_name
            if dest.exists():
                # Verify checkout is healthy (src/ should exist for frontend repos)
                if not (dest / "src").exists():
                    logger.warning("brain_agent_repo_broken", repo=dir_name, msg="Missing src/ — removing and re-cloning")
                    shutil.rmtree(dest, ignore_errors=True)
                else:
                    logger.info("brain_agent_repo_exists", repo=dir_name, path=str(dest))
                    # Pull latest
                    try:
                        subprocess.run(
                            ["git", "-C", str(dest), "pull", "--ff-only"],
                            capture_output=True,
                            timeout=30,
                        )
                    except Exception as e:
                        logger.warning("brain_agent_repo_pull_failed", repo=dir_name, error=str(e))
                    continue

            clone_url = f"https://x-access-token:{token}@github.com/{repo_slug}.git"
            logger.info("brain_agent_cloning_repo", repo=dir_name)
            try:
                subprocess.run(
                    [
                        "git", "clone",
                        "--depth", "1",
                        "--single-branch",
                        clone_url,
                        str(dest),
                    ],
                    capture_output=True,
                    timeout=60,
                    check=True,
                )
                logger.info("brain_agent_repo_cloned", repo=dir_name, path=str(dest))
            except subprocess.CalledProcessError as e:
                logger.error(
                    "brain_agent_repo_clone_failed",
                    repo=dir_name,
                    stderr=e.stderr.decode(errors="replace")[:500] if e.stderr else "",
                )
            except Exception as e:
                logger.error("brain_agent_repo_clone_error", repo=dir_name, error=str(e))
    finally:
        fcntl.flock(lock_fd, fcntl.LOCK_UN)
        lock_fd.close()


def get_repo_dirs() -> List[str]:
    """Return list of repo directory paths available for the brain agent."""
    backend_root = _find_backend_root()
    dirs: List[str] = [str(backend_root)]

    if _is_dev_mode():
        # Dev: use local sibling directories
        parent = backend_root.parent
        for _, dir_name in _FRONTEND_REPOS:
            repo_path = parent / dir_name
            if repo_path.exists():
                dirs.append(str(repo_path))
    else:
        # Prod: use cloned repos
        settings = get_settings()
        repos_dir = Path(settings.CODE_REPOS_DIR)
        for _, dir_name in _FRONTEND_REPOS:
            repo_path = repos_dir / dir_name
            if repo_path.exists():
                dirs.append(str(repo_path))

    return dirs
