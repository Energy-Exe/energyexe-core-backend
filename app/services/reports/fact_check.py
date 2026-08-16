"""Numeric fact-check for the Pass-2 executive summary (EPR-82).

Third layer of the "no new claims" contract (after input restriction and the
citation-constrained output schema): every number the summary cites must appear
in the sections it cites. The LLM is allowed to round — a value matches within
a small relative tolerance — but a number with no plausible source fails the
whole summary so a fabricated figure can never render first in the report.
"""

import re
from typing import Iterable, List, Optional

# "1,234.5", "83.3", "-4", optionally followed by a magnitude word. The
# lookbehind keeps digits embedded in identifiers (P50, MKT_05, EPR-88) from
# being read as numeric claims.
_NUMBER_RE = re.compile(
    r"(?<![A-Za-z_0-9.\-])(-?\d[\d,]*\.?\d*)\s*(million|billion|thousand|mn|bn|k\b|m\b)?",
    re.IGNORECASE,
)

_MAGNITUDE = {
    "k": 1_000,
    "thousand": 1_000,
    "m": 1_000_000,
    "mn": 1_000_000,
    "million": 1_000_000,
    "bn": 1_000_000_000,
    "billion": 1_000_000_000,
}

# Relative tolerance for rounding ("€1.4M" vs 1,437,000 → 2.6% off is fine).
_REL_TOLERANCE = 0.06
_ABS_TOLERANCE = 0.051  # one-decimal rounding of small values


def _to_float(token: str, magnitude: Optional[str]) -> Optional[float]:
    try:
        value = float(token.replace(",", ""))
    except ValueError:
        return None
    if magnitude:
        value *= _MAGNITUDE.get(magnitude.lower(), 1)
    return value


def extract_numbers(text: str) -> List[float]:
    """All numeric claims in a sentence, magnitude words applied."""
    out: List[float] = []
    for token, magnitude in _NUMBER_RE.findall(text):
        if not token or token in ("-",):
            continue
        value = _to_float(token, magnitude or None)
        if value is not None:
            out.append(value)
    return out


def harvest_numbers(node) -> List[float]:
    """Every number reachable in a section payload — raw values and numbers
    embedded in formatted strings ("83.3%", "€1.2M") alike."""
    found: List[float] = []
    if isinstance(node, bool):
        return found
    if isinstance(node, (int, float)):
        found.append(float(node))
    elif isinstance(node, str):
        found.extend(extract_numbers(node))
    elif isinstance(node, dict):
        for value in node.values():
            found.extend(harvest_numbers(value))
    elif isinstance(node, (list, tuple)):
        for item in node:
            found.extend(harvest_numbers(item))
    return found


def _matches(claim: float, sources: Iterable[float]) -> bool:
    for source in sources:
        if abs(claim - source) <= max(_REL_TOLERANCE * abs(source), _ABS_TOLERANCE):
            return True
        # The model may quote a large value without its magnitude word
        # ("1.4" for 1.4M) or with a % sign the data stores as a ratio.
        for scale in (1_000, 1_000_000, 100):
            if source and abs(claim * scale - source) <= _REL_TOLERANCE * abs(source):
                return True
            if claim and abs(claim - source * scale) <= _REL_TOLERANCE * abs(claim):
                return True
    return False


def verify_bullet(text: str, cited_payloads: List) -> List[float]:
    """Numbers in ``text`` with no source in the cited payloads (empty = pass).

    Calendar years pass automatically — the report period makes them
    ubiquitous and they carry no analytical claim.
    """
    sources = []
    for payload in cited_payloads:
        sources.extend(harvest_numbers(payload))
    unverified = []
    for claim in extract_numbers(text):
        if 1990 <= claim <= 2100 and float(claim).is_integer():
            continue
        if not _matches(claim, sources):
            unverified.append(claim)
    return unverified
