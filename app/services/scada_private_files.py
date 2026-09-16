"""Local private export boundaries shared conceptually with the offline preparer."""

from __future__ import annotations

import json
import os
import stat
from pathlib import Path

BACKEND_ROOT = Path(__file__).resolve().parents[2]
WORKSPACE_ROOT = BACKEND_ROOT.parent
ROOT_ENV_VARS = (
    "SCADA_DATA_ROOT",
    "SCADA_OPPORTUNITIES_ROOT",
    "ENERGYEXE_OPP_ROOT",
    "ENERGYEXE_DATA_ROOT",
    "SCADA_SYNC_ROOT",
    "SCADA_SYNC_ROOTS",
    "SCADA_PRIVATE_FORBIDDEN_ROOTS",
    "DROPBOX",
    "DROPBOX_PATH",
    "DROPBOX_ROOT",
    "ONEDRIVE",
    "ONEDRIVE_ROOT",
    "OneDrive",
    "OneDriveCommercial",
    "OneDriveConsumer",
    "GOOGLE_DRIVE_ROOT",
    "GOOGLE_DRIVE_PATH",
    "ICLOUD_DRIVE_ROOT",
)


def private_output_root(path: Path, *, extra_forbidden: tuple[Path, ...] = ()) -> Path:
    """Resolve symlinks before rejecting repositories, SCADA data and sync roots."""
    root = path.expanduser().resolve()
    home = Path.home()
    forbidden = [
        WORKSPACE_ROOT,
        home / "Library/CloudStorage",
        home / "Library/Mobile Documents",
        home / "Dropbox",
        home / "OneDrive",
        home / "Google Drive",
        *extra_forbidden,
    ]
    for name in ROOT_ENV_VARS:
        forbidden.extend(
            Path(p).expanduser() for p in os.environ.get(name, "").split(os.pathsep) if p
        )
    for forbidden_root in forbidden:
        resolved = forbidden_root.resolve()
        if root == resolved or root.is_relative_to(resolved):
            raise ValueError(
                "Private output cannot be inside repository, shared data or sync roots"
            )
    if any((parent / ".git").exists() for parent in (root, *root.parents)):
        raise ValueError("Private output cannot be inside a Git worktree")
    if root.exists():
        info = root.stat()
        if (
            not stat.S_ISDIR(info.st_mode)
            or info.st_uid != os.getuid()
            or stat.S_IMODE(info.st_mode) != 0o700
        ):
            raise ValueError(
                "Existing private output directory must be operator-owned with mode 0700"
            )
    # Create every new directory privately; mkdir(parents=True) uses a looser mode for parents.
    missing = []
    current = root
    while not current.exists():
        missing.append(current)
        current = current.parent
    for directory in reversed(missing):
        directory.mkdir(mode=0o700)
    return root


def write_private_json(root: Path, name: str, payload: dict) -> Path:
    """Never follow/overwrite a pre-existing output file."""
    if Path(name).name != name:
        raise ValueError("Output name must be a basename")
    destination = root / name
    content = json.dumps(payload, indent=2, ensure_ascii=False, allow_nan=False) + "\n"
    fd = os.open(destination, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    with os.fdopen(fd, "w", encoding="utf-8") as output:
        output.write(content)
        output.flush()
        os.fsync(output.fileno())
    return destination
