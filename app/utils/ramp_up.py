"""Ramp-up period detection utility.

Determines whether a generation data record falls within the ramp-up period
of a generation unit. The ramp-up period spans from first power through
the end of commissioning/early operations.

Cascade logic for boundaries:
  Start: unit.first_power_date > windfarm.first_power_date > unit.start_date
  End:   unit.ramp_up_end_date > windfarm.ramp_up_end_date
         > unit.commercial_operational_date > windfarm.commercial_operational_date
         (if COD found, default to COD + 2 months when no explicit end date)
"""

from datetime import date, datetime
from typing import Union

from dateutil.relativedelta import relativedelta

DEFAULT_RAMP_UP_MONTHS = 2


def is_in_ramp_up_period(unit_info: dict, record_date: Union[date, datetime]) -> bool:
    """Check if a record date falls within the ramp-up period.

    Args:
        unit_info: Dict with date fields from generation unit and windfarm.
            Recognized keys:
              - first_power_date, start_date (unit-level ramp start)
              - windfarm_first_power_date (windfarm-level ramp start)
              - unit_ramp_up_end_date, commercial_operational_date (unit-level ramp end)
              - windfarm_ramp_up_end_date, windfarm_commercial_operational_date (windfarm-level)
        record_date: The hour/date of the generation data record.

    Returns:
        True if the record is within the ramp-up period, False otherwise.
    """
    # Normalize record_date to date
    if isinstance(record_date, datetime):
        record_date = record_date.date()

    # --- Resolve ramp start (cascade: unit first_power > windfarm first_power > unit start_date) ---
    ramp_start = (
        _to_date(unit_info.get('first_power_date'))
        or _to_date(unit_info.get('windfarm_first_power_date'))
        or _to_date(unit_info.get('start_date'))
    )

    if ramp_start is None:
        # Can't determine when ramp-up began → not ramp-up
        return False

    # --- Resolve ramp end (cascade: unit end > windfarm end > unit COD > windfarm COD) ---
    ramp_end = (
        _to_date(unit_info.get('unit_ramp_up_end_date'))
        or _to_date(unit_info.get('windfarm_ramp_up_end_date'))
    )

    if ramp_end is None:
        # No explicit ramp_up_end_date — fall back to COD + default months
        cod = (
            _to_date(unit_info.get('commercial_operational_date'))
            or _to_date(unit_info.get('unit_commercial_operational_date'))
            or _to_date(unit_info.get('windfarm_commercial_operational_date'))
        )
        if cod:
            ramp_end = cod + relativedelta(months=DEFAULT_RAMP_UP_MONTHS)

    if ramp_end is None:
        # No ramp end can be determined → not ramp-up
        return False

    # Record is ramp-up if: ramp_start <= record_date < ramp_end
    return ramp_start <= record_date < ramp_end


def _to_date(val) -> date | None:
    """Convert a value to date, handling datetime and None."""
    if val is None:
        return None
    if isinstance(val, datetime):
        return val.date()
    if isinstance(val, date):
        return val
    return None
