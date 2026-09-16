"""Run only the local preview, dropping inherited service credentials first.

SFE_PREVIEW_DATABASE_URL=postgresql+asyncpg://localhost-user@127.0.0.1:5432/energyexe_sfe_preview \
  .venv/bin/python scripts/start_sfe_preview.py
"""

import os
import sys
from pathlib import Path


def main():
    root = Path(__file__).resolve().parents[1]
    allowed = {"PATH", "LANG", "LC_ALL", "LC_CTYPE", "TMPDIR", "SYSTEMROOT"}
    allowed.update({"SFE_PREVIEW_DATABASE_URL", "SFE_PREVIEW_HOST", "SFE_PREVIEW_PORT"})
    clean_env = {key: value for key, value in os.environ.items() if key in allowed}
    os.chdir(root)
    os.execve(sys.executable, [sys.executable, "-m", "app.scada_preview.main"], clean_env)


if __name__ == "__main__":
    main()
