"""Tests for ramp-up period flagging system.

Tests the is_in_ramp_up_period() utility with cascade logic,
default 2-month post-COD window, explicit overrides, and
unit vs windfarm priority.
"""

from datetime import date

import pytest

from app.utils.ramp_up import is_in_ramp_up_period


class TestRampUpCascadeLogic:
    """Test is_in_ramp_up_period cascade logic."""

    def test_ramp_up_before_cod_is_flagged(self):
        """Record between first_power_date and COD should be ramp-up."""
        unit_info = {
            'first_power_date': date(2024, 1, 1),
            'commercial_operational_date': date(2024, 6, 1),
        }
        # Record during commissioning → ramp-up
        assert is_in_ramp_up_period(unit_info, date(2024, 3, 15)) is True
        # Record after COD + 2 months default → NOT ramp-up
        assert is_in_ramp_up_period(unit_info, date(2024, 9, 1)) is False

    def test_ramp_up_default_2_months_after_cod(self):
        """Without explicit ramp_up_end_date, default to COD + 2 months."""
        unit_info = {
            'first_power_date': date(2024, 1, 1),
            'commercial_operational_date': date(2024, 6, 1),
            # No ramp_up_end_date set
        }
        # 1 month after COD → still ramp-up
        assert is_in_ramp_up_period(unit_info, date(2024, 7, 1)) is True
        # Exactly 2 months after COD → no longer ramp-up (end is exclusive)
        assert is_in_ramp_up_period(unit_info, date(2024, 8, 1)) is False

    def test_ramp_up_explicit_end_date_overrides_default(self):
        """Explicit ramp_up_end_date overrides the 2-month default."""
        unit_info = {
            'first_power_date': date(2024, 1, 1),
            'commercial_operational_date': date(2024, 6, 1),
            'unit_ramp_up_end_date': date(2024, 12, 1),  # 6 months after COD
        }
        # 3 months after COD → still ramp-up (explicit end is Dec)
        assert is_in_ramp_up_period(unit_info, date(2024, 9, 1)) is True
        # After explicit end → NOT ramp-up
        assert is_in_ramp_up_period(unit_info, date(2024, 12, 15)) is False

    def test_ramp_up_unit_dates_override_windfarm(self):
        """Unit-level dates take precedence over windfarm-level."""
        unit_info = {
            'first_power_date': date(2024, 3, 1),           # unit first_power
            'windfarm_first_power_date': date(2024, 1, 1),   # windfarm (ignored)
            'unit_ramp_up_end_date': date(2024, 9, 1),       # unit end
            'windfarm_ramp_up_end_date': date(2024, 12, 1),  # windfarm (ignored)
        }
        # Before unit's first_power_date → NOT ramp-up
        assert is_in_ramp_up_period(unit_info, date(2024, 2, 15)) is False
        # Between unit dates → ramp-up
        assert is_in_ramp_up_period(unit_info, date(2024, 5, 1)) is True
        # After unit ramp_up_end → NOT ramp-up
        assert is_in_ramp_up_period(unit_info, date(2024, 10, 1)) is False

    def test_no_dates_means_no_ramp_up(self):
        """If no dates set, nothing is flagged as ramp-up."""
        unit_info = {}
        assert is_in_ramp_up_period(unit_info, date(2024, 6, 1)) is False

    def test_windfarm_fallback_when_unit_dates_missing(self):
        """Use windfarm dates when unit dates are not set."""
        unit_info = {
            'windfarm_first_power_date': date(2024, 1, 1),
            'windfarm_commercial_operational_date': date(2024, 6, 1),
        }
        # Between windfarm first power and COD + 2 months → ramp-up
        assert is_in_ramp_up_period(unit_info, date(2024, 3, 15)) is True
        # After COD + 2 months → NOT ramp-up
        assert is_in_ramp_up_period(unit_info, date(2024, 8, 1)) is False

    def test_only_cod_no_first_power(self):
        """If only COD is set (no first_power), no ramp start → no ramp-up."""
        unit_info = {
            'commercial_operational_date': date(2024, 6, 1),
        }
        # Can't determine ramp start without first_power or start_date
        assert is_in_ramp_up_period(unit_info, date(2024, 3, 15)) is False

    def test_start_date_fallback_for_ramp_start(self):
        """unit start_date is used when first_power_date is missing."""
        unit_info = {
            'start_date': date(2024, 1, 1),
            'commercial_operational_date': date(2024, 6, 1),
        }
        assert is_in_ramp_up_period(unit_info, date(2024, 3, 15)) is True
        assert is_in_ramp_up_period(unit_info, date(2024, 8, 1)) is False

    def test_windfarm_ramp_up_end_fallback(self):
        """Use windfarm ramp_up_end_date when unit doesn't have one."""
        unit_info = {
            'first_power_date': date(2024, 1, 1),
            'windfarm_ramp_up_end_date': date(2024, 10, 1),
        }
        # Between first_power and windfarm ramp_up_end → ramp-up
        assert is_in_ramp_up_period(unit_info, date(2024, 5, 1)) is True
        # After windfarm ramp_up_end → NOT ramp-up
        assert is_in_ramp_up_period(unit_info, date(2024, 11, 1)) is False

    def test_datetime_input_converted_to_date(self):
        """Function should handle datetime inputs by converting to date."""
        from datetime import datetime, timezone
        unit_info = {
            'first_power_date': date(2024, 1, 1),
            'commercial_operational_date': date(2024, 6, 1),
        }
        # Pass datetime instead of date
        record_dt = datetime(2024, 3, 15, 12, 0, 0, tzinfo=timezone.utc)
        assert is_in_ramp_up_period(unit_info, record_dt) is True

    def test_exactly_on_ramp_start_is_ramp_up(self):
        """Record exactly on ramp start date should be ramp-up (inclusive)."""
        unit_info = {
            'first_power_date': date(2024, 1, 1),
            'commercial_operational_date': date(2024, 6, 1),
        }
        assert is_in_ramp_up_period(unit_info, date(2024, 1, 1)) is True

    def test_exactly_on_ramp_end_is_not_ramp_up(self):
        """Record exactly on ramp end date should NOT be ramp-up (exclusive)."""
        unit_info = {
            'first_power_date': date(2024, 1, 1),
            'unit_ramp_up_end_date': date(2024, 6, 1),
        }
        assert is_in_ramp_up_period(unit_info, date(2024, 6, 1)) is False
