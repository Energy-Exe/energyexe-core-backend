"""House terminology for AI-written report narratives (EPR-117, comment 2).

Kasper (2026-08-31): the Opportunity report's executive summary called the P50
target the "bankable P50 case" — a phrase used nowhere else on the platform.
The model had not invented it: our own FIN-01 one-liner and evidence labels
said "bankable", and the digest prompt instructed it. The client's rule is that
P50 is referred to ONLY in association with the farm's sourced P50 target,
which is called the **Generation target**, with performance expressed as
attainment against it.

This module is the single source of truth for both halves of the fix:

* :data:`PROMPT_RULES` — the rules block substituted into every narrative
  prompt as ``$terminology_rules`` (see ``narrative_service``).
* :func:`find_violations` — the banned-phrase lint the generation loop runs on
  the model's text; a hit earns one corrective retry, after which the output is
  accepted and the violation logged (client decision: a wording slip must not
  fail the section the way a numeric fact-check failure does).

The deterministic labels the model reads (schema one-liners, evidence labels,
scorecard rows) are covered by a test that runs the same lint over them, so
the banned vocabulary cannot creep back into model input.
"""

import re
from typing import Iterable, List, Optional

GENERATION_TARGET = "Generation target"
GENERATION_TARGET_FIRST_MENTION = "Generation target (P50)"

PROMPT_RULES = """Terminology — mandatory, applies to every sentence:
- The farm's sourced P50 target is the "Generation target": write "Generation
  target (P50)" on first mention, then "Generation target". Performance against
  it is "attainment vs the Generation target". Never call it the bankable case,
  bankable P50, base case, budget, P50 case, or any other name of your own.
- "Weather-adjusted attainment" and the "wind-normalised performance index"
  compare output with the farm's own power-curve expectation. They are NOT the
  Generation target: never equate them and never label them "P50".
- Use metric, schema and finding names exactly as they appear in the provided
  data; do not rename or paraphrase them into new terms."""

# Phrases a narrative must never contain (case-insensitive). Bare "budget" is
# allowed — an action plan may legitimately ask for budget approval; only the
# generation-budget sense is banned.
_BANNED_PATTERNS = [
    re.compile(p, re.IGNORECASE)
    for p in (
        r"\bbankable\b",
        r"\bP50 case\b",
        r"\bbase case\b",
        r"\b(?:P50|generation|production|yield|energy) budget\b",
        r"\bbudget(?:ed)? (?:generation|production|yield|case|P50)\b",
        r"\bP50 (?:baseline|scenario|forecast|estimate|expectation)\b",
        r"\byield shortfall vs\b",
    )
]


def find_violations(text: Optional[str]) -> List[str]:
    """Distinct banned phrases found in ``text`` (as written), in order."""
    if not text:
        return []
    seen: set = set()
    out: List[str] = []
    for pattern in _BANNED_PATTERNS:
        for match in pattern.finditer(text):
            key = match.group(0).lower()
            if key not in seen:
                seen.add(key)
                out.append(match.group(0))
    return out


def find_violations_in(texts: Iterable[Optional[str]]) -> List[str]:
    """:func:`find_violations` over several strings, de-duplicated."""
    seen: set = set()
    out: List[str] = []
    for text in texts:
        for term in find_violations(text):
            if term.lower() not in seen:
                seen.add(term.lower())
                out.append(term)
    return out


def correction_message(terms: List[str]) -> str:
    """The retry instruction appended to the system prompt after a lint hit."""
    quoted = ", ".join(f'"{t}"' for t in terms)
    return (
        f"Banned terminology used: {quoted}. The farm's sourced P50 target must be "
        f'called the "{GENERATION_TARGET}" (first mention "{GENERATION_TARGET_FIRST_MENTION}") '
        "and never the bankable case, base case, budget or P50 case; rewrite every "
        "affected sentence using only the names present in the provided data."
    )
