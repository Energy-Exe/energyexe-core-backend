"""Shared unit-resolution logic for the generation aggregation pipeline.

Used by all aggregation entrypoints (daily, monthly, ELEXON standalone) to
pick the correct generation_unit when raw data references it by source:code.

Key invariants:
- Caches must contain only `is_active=True` units. Inactive (decommissioned,
  expanded, or otherwise retired) units must never receive new attributions.
- `end_date` is treated as exclusive — sequential phases that share a
  boundary day don't both match.
- When multiple active units share a source:code AND overlapping date windows
  (e.g., EIA's plant-level code with multiple generator rows), resolution
  prefers a `preferred_unit_id` from the raw record, then falls back to
  min(id) with a one-shot warning per (windfarm, source, code).
"""

from datetime import datetime
from typing import Dict, List, Optional, Set, Tuple, Union
import logging

logger = logging.getLogger(__name__)


def is_unit_operational(unit_info: Dict, check_date: datetime) -> bool:
    """Check whether a cached unit was operational at the given hour.

    Uses `first_power_date` if set (when the unit first generated power),
    otherwise `start_date`. `end_date` is exclusive — a unit ending on
    2003-11-01 is NOT operational at any hour on or after 2003-11-01,
    so a sequentially-following unit starting 2003-11-01 is unambiguous.
    """
    if not unit_info:
        return False

    if hasattr(check_date, 'date'):
        check_date_naive = check_date.replace(tzinfo=None) if check_date.tzinfo else check_date
    else:
        check_date_naive = check_date

    effective_start = unit_info.get('first_power_date') or unit_info.get('start_date')
    if effective_start:
        if not isinstance(effective_start, datetime):
            effective_start = datetime.combine(effective_start, datetime.min.time())
        if hasattr(effective_start, 'tzinfo') and effective_start.tzinfo:
            effective_start = effective_start.replace(tzinfo=None)
        if check_date_naive < effective_start:
            return False

    end_date = unit_info.get('end_date')
    if end_date:
        # Exclusive upper bound: a unit ending YYYY-MM-DD is not operational at
        # any hour on or after that date.
        if not isinstance(end_date, datetime):
            end_date = datetime.combine(end_date, datetime.min.time())
        if hasattr(end_date, 'tzinfo') and end_date.tzinfo:
            end_date = end_date.replace(tzinfo=None)
        if check_date_naive >= end_date:
            return False

    return True


def resolve_operational_unit(
    cache_entry: Union[Dict, List[Dict], None],
    check_date: datetime,
    preferred_unit_id: Optional[int] = None,
    ambiguous_keys_warned: Optional[Set[Tuple]] = None,
) -> Optional[Dict]:
    """Resolve which generation unit a record belongs to. Three-tier:

    1. `preferred_unit_id` (e.g., NVE ingest pre-tags this) — wins if it's in
       the active cache and operational at the hour.
    2. Date-window match against the active cache.
    3. If multiple active units still match (EIA's multi-generator-per-plant
       pattern), pick `min(id)` deterministically and log a single WARN per
       (windfarm_id, source, code) per run. Never drop the record.

    Args:
        cache_entry: A unit dict, a list of dicts, or None.
        check_date: Hour to resolve.
        preferred_unit_id: Optional pre-tagged unit_id from the raw record.
        ambiguous_keys_warned: Caller-managed set used to dedupe WARN logs
            across multiple calls. Pass the same set across a run.

    Returns:
        The matched unit dict, or None if no active unit is operational at the hour.
    """
    if not cache_entry:
        return None

    candidates = [cache_entry] if isinstance(cache_entry, dict) else cache_entry

    # Tier 1: honour pre-tagged id if present and active+operational.
    if preferred_unit_id is not None:
        for unit_info in candidates:
            if unit_info.get('id') == preferred_unit_id and is_unit_operational(unit_info, check_date):
                return unit_info
        # Tagged id not in active cache (likely deactivated since ingest); fall through.

    # Tier 2: date-window match.
    operational = [u for u in candidates if is_unit_operational(u, check_date)]
    if not operational:
        return None
    if len(operational) == 1:
        return operational[0]

    # Tier 3: tie-break on min(id), warn once per (windfarm, source, code).
    chosen = min(operational, key=lambda u: u['id'])
    if ambiguous_keys_warned is not None:
        warn_key = (chosen.get('windfarm_id'), chosen.get('source'), chosen.get('code'))
        if warn_key not in ambiguous_keys_warned:
            ambiguous_keys_warned.add(warn_key)
            candidate_ids = sorted(u['id'] for u in operational)
            logger.warning(
                f"Ambiguous unit resolution for windfarm_id={warn_key[0]} "
                f"source={warn_key[1]} code={warn_key[2]}: {len(operational)} "
                f"active units match (ids={candidate_ids}). Picking min(id)={chosen['id']}. "
                f"Tag the raw record with a generation_unit_id at ingest to disambiguate."
            )
    return chosen
