"""The Events calendar's daily figure: no -935 % on a calm day (SCADA UI tidy, stage 4)."""

from datetime import date
from types import SimpleNamespace

from app.services.scada_service import calendar_day, calendar_potential_floor


def _row(act, pot, dt=0.0, ct=0.0, pf=0.0):
    return SimpleNamespace(date_utc=date(2025, 6, 1), act=act, pot=pot, dt=dt, ct=ct, pf=pf)


def test_floor_is_two_percent_of_the_best_day_and_at_least_one_mwh():
    assert calendar_potential_floor([1000.0, 20.0]) == 20.0
    assert calendar_potential_floor([10.0]) == 1.0
    assert calendar_potential_floor([]) == 1.0


def test_calm_day_with_power_drawn_has_no_figure():
    # the case that showed -935 %: a few MWh of potential, the turbines drawing power
    day = calendar_day(_row(act=-46.8, pot=5.0), floor=20.0)
    assert day["perf"] is None
    assert day["act"] == -46.8


def test_negative_output_counts_as_nothing_produced():
    day = calendar_day(_row(act=-3.0, pot=400.0, dt=1000.0), floor=20.0)
    assert day["perf"] == 0.0
    assert day["cause"] == "dt"


def test_ordinary_day():
    day = calendar_day(_row(act=300.0, pot=400.0, pf=5.0), floor=20.0)
    assert day["perf"] == 75.0
    assert day["cause"] == "pf"
    assert calendar_day(_row(act=0, pot=0), floor=1.0)["cause"] is None
