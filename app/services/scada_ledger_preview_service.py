"""Ledger preview (internal sandbox page): reads the value-ledger sandbox's exported results.

The results are static JSON files shipped with the app (``app/data/scada_ledger_preview/``, one per
farm and asset, schema ``ledger-preview/v1``). No database and no S3: the page is a preview of a
method, not a register. See the folder's README for where the files come from.
"""

from __future__ import annotations

import json
import re
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, List

SCHEMA = "ledger-preview/v1"
DATA_DIR = Path(__file__).resolve().parent.parent / "data" / "scada_ledger_preview"
_NAME = re.compile(r"^(?P<farm>[a-z0-9_]+)__(?P<asset>[A-Z0-9]+)\.json$")


class LedgerPreviewNotFound(LookupError):
    """No preview file for that farm (and asset)."""


class LedgerPreviewInvalid(ValueError):
    """A preview file is not the expected schema, or is not stamped as a sandbox."""


@lru_cache(maxsize=32)
def _load(path: str, mtime: float) -> Dict[str, Any]:
    doc = json.loads(Path(path).read_text(encoding="utf-8"))
    if doc.get("schema") != SCHEMA:
        raise LedgerPreviewInvalid(
            f"{Path(path).name}: schema {doc.get('schema')!r}, expected {SCHEMA}"
        )
    if "SANDBOX" not in str(doc.get("meta", {}).get("stamp", "")):
        raise LedgerPreviewInvalid(f"{Path(path).name}: not stamped as a sandbox result")
    return doc


class ScadaLedgerPreviewService:
    def __init__(self, data_dir: Path = DATA_DIR) -> None:
        self.data_dir = Path(data_dir)

    def _files(self) -> List[tuple[str, str, Path]]:
        if not self.data_dir.is_dir():
            return []
        out = []
        for p in sorted(self.data_dir.iterdir()):
            m = _NAME.match(p.name)
            if m:
                out.append((m["farm"], m["asset"], p))
        return out

    def assets(self, farm: str) -> List[Dict[str, Any]]:
        rows = []
        for f, a, p in self._files():
            if f != farm:
                continue
            meta = self._doc(p)["meta"]
            rows.append(
                {
                    "farm": f,
                    "asset_code": a,
                    "currency": meta["currency"],
                    "window_from": meta["window_from"],
                    "window_to": meta["window_to"],
                }
            )
        return rows

    def get(self, farm: str, asset: str | None = None) -> Dict[str, Any]:
        hits = [(a, p) for f, a, p in self._files() if f == farm and (asset is None or a == asset)]
        if not hits:
            raise LedgerPreviewNotFound(
                f"No ledger preview for farm {farm!r}" + (f" asset {asset!r}" if asset else "")
            )
        return self._doc(hits[0][1])

    @staticmethod
    def _doc(p: Path) -> Dict[str, Any]:
        return _load(str(p), p.stat().st_mtime)
