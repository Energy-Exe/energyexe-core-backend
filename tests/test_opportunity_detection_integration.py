"""Characterization (behaviour-lock) harness for the current 6 opportunity schemas.

Issue #91 (M1 · "Migrate existing 6 — NO behaviour change"). This is the
**behaviour-preservation gate** for the OPS-01/02/03 + MKT-01/02/03 detectors.
It freezes the *current* output of the legacy engine — including the three known
bugs — as a byte-identical snapshot so that:

    * #92 (migrate OPS-01/02/03 verbatim) and #93 (migrate MKT-01/02/03 verbatim
      + cut the live path over to ``run_for_windfarm``) MUST keep
      ``EXPECTED_SNAPSHOT`` unchanged; and
    * #94–#98 (the deliberate bug-fixes / recalibrations) update specific entries
      with a documented delta (see the per-scenario "POST-FIX" notes below).

Why this approach (read before touching it)
============================================
There is **no Postgres** in the test environment: ``tests/conftest.py`` only
creates the auth tables on in-memory SQLite, and the ``opportunities`` /
``windfarms`` / ``generation_data`` tables use Postgres ``JSONB`` and cannot be
created on SQLite. So ``detect_all()`` cannot be run end-to-end against a real
database here. Instead we exercise the legacy assembly logic directly:

    For each named SCENARIO we mock ONLY the data-access seam — the
    ``_calc_monthly_availability`` / ``_calc_seasonal_capture`` /
    ``_calc_capture_rate_gap`` / ``_calc_cannibalisation_index`` /
    ``_load_ppa_info`` coroutine methods — to return canned inputs, then run the
    REAL ``OpportunityDetectionService._detect_windfarm`` against a ``FakeSession``
    (records ``add()``, assigns ids on ``flush()`` — the same pattern as
    ``tests/opportunity_schemas/test_registry.py``).

This locks everything that is *not* a raw DB query: the severity classification
(``determine_*_severity``), branch selection (``select_*_branch``), suppression
(``check_*_suppression``), the graceful-degradation force-downgrades, the
``data_slots`` / ``missing_slots`` assembly, and the cross-schema dependency
wiring (OPS-03→OPS-01, MKT-02→MKT-01) incl. ``triggered_by_id``.

How the snapshot survives the #92/#93 cutover WITHOUT modification
------------------------------------------------------------------
The data-access methods mocked here map 1:1 onto the ``DetectionContext``
accessors that the registry detectors will call after the cutover:

    legacy ``_calc_monthly_availability``  →  ``ctx.load_monthly_performance()``
    legacy ``_calc_capture_rate_gap``      →  ``ctx.load_capture_rate()``
    legacy ``_calc_cannibalisation_index`` →  ``ctx.load_cannibalisation_index()``
    legacy ``_load_ppa_info``              →  ``ctx.load_ppa_info()``
    legacy ``_calc_seasonal_capture``      →  (OPS-02's accessor; same row shape)

Both the legacy inline detectors and the post-cutover registry detectors consume
the *same* injected dicts and run the *same* pure functions. The two harness
helpers below — :func:`_run_legacy` (live path today) and the documented
:func:`_run_registry_when_available` seam — produce the same tuples. #92/#93 add
a parametrization over both paths and assert identical results; until then we
lock the legacy path, which is the live one.

The frozen snapshot itself (``EXPECTED_SNAPSHOT``) is path-independent: it is a
plain mapping of scenario-name → tuple, so it does not change at cutover.

The three bugs locked here (each gets an explicit ``test_bug_*``)
-----------------------------------------------------------------
1. **OPS-01 can NEVER reach CONFIRMED** — ``wind_resource_index`` is hardcoded
   into ``missing_slots`` and a CONFIRMED severity is then force-downgraded to
   INDICATIVE (service.py ``:255`` + ``:268``). Locked by ``BUG_ops01_*``.
2. **OPS-02 only ever reaches WATCH** — a CONFIRMED/INDICATIVE severity is forced
   to WATCH whenever ``wind_resource_index_monthly`` is missing (always), and the
   detector also requires the structurally-impossible "summer CF > winter CF"
   inversion (``low_wind_cf > high_wind_cf``) just to fire (service.py
   ``:305`` + ``:329``). Locked by ``BUG_ops02_*``.
3. **MKT-01 never fires** — ``compare_capture_rates_by_bidzone()`` omits
   ``zone_average_capture_rate`` (price_analytics_service.py ``:635-641``), so
   ``_calc_capture_rate_gap`` reads ``None`` and returns ``None`` → MKT-01 (and
   therefore its dependent MKT-02) never produce a row. Locked by
   ``BUG_mkt01_*`` (which drives the REAL ``_calc_capture_rate_gap`` against the
   real buggy return shape).
"""

from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple
from unittest.mock import AsyncMock, MagicMock

import pytest

from app.models.opportunity import OpportunityStatus, SchemaCode, Severity
from app.services.opportunity_detection_service import OpportunityDetectionService

# Fixed detection window used for every scenario (period string is part of the
# data_slots but NOT part of the snapshot tuple, so its exact value is irrelevant
# to behaviour-locking; we still pin it for determinism).
START = datetime(2024, 1, 1)
END = datetime(2026, 1, 1)
WINDFARM_ID = 101
DETECTION_RUN_ID = 555


# ───────────────────────── DB-free harness plumbing ─────────────────────────


class _FakeSession:
    """Minimal AsyncSession stand-in (same contract as test_registry.FakeSession).

    Records ``add()``ed objects and assigns a sequential ``id`` on each
    ``flush()`` so ``triggered_by_id`` wiring (parent.id read after flush) works
    without Postgres. ``execute`` raises by default: scenarios that need it (only
    the MKT-01 bug test, which drives the real ``_calc_capture_rate_gap``) inject
    their own ``execute`` mock.
    """

    def __init__(self) -> None:
        self.added: list = []
        self._next_id = 1

    def add(self, obj) -> None:
        self.added.append(obj)

    async def flush(self) -> None:
        for obj in self.added:
            if getattr(obj, "id", None) is None:
                obj.id = self._next_id
                self._next_id += 1


def _make_service(
    *,
    monthly: Optional[List[dict]] = None,
    seasonal: Optional[dict] = None,
    capture_gap: Optional[dict] = None,
    cannibalisation: Optional[dict] = None,
    ppa: Optional[dict] = None,
    db: Optional[Any] = None,
) -> OpportunityDetectionService:
    """Build a service whose data-access seam is fully mocked.

    Only the five data accessors (the seam that maps onto ``DetectionContext``
    accessors post-cutover) are mocked; ALL assembly / pure logic runs for real.
    """
    svc = OpportunityDetectionService.__new__(OpportunityDetectionService)
    svc.db = db if db is not None else _FakeSession()
    svc.price_analytics = MagicMock()
    svc._calc_monthly_availability = AsyncMock(return_value=monthly)
    svc._calc_seasonal_capture = AsyncMock(return_value=seasonal)
    svc._calc_capture_rate_gap = AsyncMock(return_value=capture_gap)
    svc._calc_cannibalisation_index = AsyncMock(return_value=cannibalisation)
    svc._load_ppa_info = AsyncMock(return_value=ppa if ppa is not None else {})
    return svc


def _outcome_tuple(opp) -> Tuple[str, str, Optional[str], str, tuple, tuple]:
    """Reduce an Opportunity ORM object to the frozen snapshot tuple.

    Tuple shape (per issue #91):
        (schema_code, severity, branch, status,
         sorted(missing_slots), sorted(data_slots.keys()))
    Enum members are normalised to their string ``.value`` so the snapshot is a
    plain-data structure (path- and ORM-independent).
    """

    def _v(x):
        return x.value if hasattr(x, "value") else x

    return (
        _v(opp.schema_code),
        _v(opp.severity),
        opp.branch,
        _v(opp.status),
        tuple(sorted(opp.missing_slots)),
        tuple(sorted(opp.data_slots.keys())),
    )


async def _run_legacy(svc: OpportunityDetectionService) -> List:
    """Run the LIVE legacy detection path for one windfarm.

    This is the ``_detect_windfarm`` the service uses in production today. After
    #93 cuts the live path over to ``run_for_windfarm`` this helper stays valid
    (it still exercises the legacy inline detectors, kept in place behind the
    registry seam), and #92/#93 add a second parametrization over the registry
    path asserting identical tuples — see ``_run_registry_when_available``.
    """
    return await svc._detect_windfarm(WINDFARM_ID, START, END, DETECTION_RUN_ID)


# ─── #92/#93 cutover seam (documented, not yet active) ───
#
# When the six detectors are registered (#92/#93), the same scenarios can be run
# through the registry path by building a DetectionContext whose ``prefetched``
# cache is the scenario inputs (keys: "monthly_performance", "capture_rate",
# "cannibalisation_index", "ppa_info") and calling ``run_for_windfarm(ctx)``.
# Because the detectors consume the same dicts and call the same pure functions,
# the resulting tuples MUST equal ``EXPECTED_SNAPSHOT``. #92/#93 should add:
#
#     @pytest.mark.parametrize("runner", [_run_legacy, _run_registry])
#
# to ``test_characterization_snapshot_current_six_schemas`` and keep this file's
# EXPECTED_SNAPSHOT byte-identical. (Left as a comment to avoid importing an
# empty registry path that would currently return [].)


# ──────────────────────────── Scenario inputs ───────────────────────────────
#
# Each scenario is a dict of injected data-accessor return values, chosen to
# drive specific schemas to specific tiers AND to exercise the three known bugs.
# Keep these labelled constants stable: #92/#93 re-run them unchanged; #94–#98
# update the matching EXPECTED_SNAPSHOT entries (and, where the *inputs* gate the
# bug — e.g. MKT-01 — the scenario too) with a documented delta.


def _months(*specs: Tuple[str, float]) -> List[dict]:
    """Build monthly availability rows from (month, availability_pct) pairs."""
    return [{"month": m, "availability_pct": pct} for m, pct in specs]


# OPS-01 fires at every tier off the count of months below ODI_THRESHOLD_PCT
# (95.0). Below: 8 low months across two years (2024+2025).
SCENARIO_INPUTS: Dict[str, Dict[str, Any]] = {
    # OPS-01 "should be CONFIRMED but is force-downgraded to INDICATIVE" (BUG 1).
    # 8 months < 95% across 2 years → determine_ops01_severity(8)=CONFIRMED, but
    # wind_resource_index is hardcoded missing → forced to INDICATIVE. PPA empty.
    "ops01_should_be_confirmed_is_indicative": {
        "monthly": _months(
            ("2024-01", 80.0),
            ("2024-02", 82.0),
            ("2024-03", 70.0),
            ("2024-11", 88.0),
            ("2025-01", 79.0),
            ("2025-02", 81.0),
            ("2025-03", 60.0),
            ("2025-11", 90.0),
            ("2025-12", 99.0),
        ),
        "ppa": {},
    },
    # OPS-01 WATCH (1 low month, single year) + OPS-03 follows it. Merchant PPA,
    # no availability penalties → OPS-03 fires at WATCH branch A.
    "ops01_watch_with_ops03_followon": {
        "monthly": _months(("2025-03", 80.0), ("2025-12", 99.0)),
        "ppa": {
            "ppa_status": "active",
            "contract_type": "merchant",
            "has_availability_penalties": False,
            "ppa_duration_years": 3,
        },
    },
    # OPS-02 "should be CONFIRMED but is forced to WATCH" (BUG 2). Requires the
    # structurally-impossible inversion (low_wind_cf > high_wind_cf) just to fire,
    # then forces WATCH because wind_resource_index_monthly is always missing.
    "ops02_should_be_confirmed_is_watch": {
        "seasonal": {"high_wind_cf": 0.30, "low_wind_cf": 0.45, "years_with_inversion": 2},
        "ppa": {},
    },
    # MKT-03 CONFIRMED (CI 1.25, 2 years sustained, worsening trend → branch A).
    "mkt03_confirmed": {
        "cannibalisation": {
            "ci_latest": 1.25,
            "ci_by_year": {"2024": 1.22, "2025": 1.25},
            "ci_trend": 0.03,
            "years_above_threshold": 2,
            "bidzone_code": "NO2",
        },
        "ppa": {},
    },
    # MKT-03 graceful-degradation: CONFIRMED-eligible CI but no trend (single
    # year) → downgraded to INDICATIVE, branch C.
    "mkt03_confirmed_downgraded_no_trend": {
        "cannibalisation": {
            "ci_latest": 1.25,
            "ci_by_year": {"2025": 1.25},
            "ci_trend": None,
            "years_above_threshold": 2,
            "bidzone_code": "NO2",
        },
        "ppa": {},
    },
    # MKT-01 "never fires" (BUG 3) — modelled at the data layer: with the real
    # buggy ``compare_capture_rates_by_bidzone`` the gap comes back None, so
    # ``_calc_capture_rate_gap`` returns None and MKT-01 (and dependent MKT-02)
    # produce NO rows. Here capture_gap=None reproduces that at the assembly
    # level; ``test_bug_mkt01_never_fires_via_real_calc`` proves it from the real
    # query-shape source. No CI either → no MKT-03. Result: ZERO opportunities.
    "mkt01_never_fires_no_opportunities": {
        "capture_gap": None,
        "cannibalisation": None,
        "ppa": {"ppa_status": "active"},
    },
    # Reference scenario documenting what MKT-01 WOULD produce if the zone-average
    # bug were fixed and data present (7pp gap → INDICATIVE, branch C, plus its
    # dependent MKT-02 at WATCH). This is NOT part of today's reachable behaviour
    # via detect_all (the bug blocks it); it is locked so #94 has an exact target
    # to flip the "never fires" scenario toward. Kept under a distinct name so the
    # snapshot stays unambiguous about which entries are "live" vs "would-be".
    "mkt01_would_fire_if_zone_average_present": {
        "capture_gap": {
            "capture_rate": 0.62,
            "zone_avg": 0.69,
            "gap_pp": 7.0,
            "bidzone_code": "NO2",
        },
        "cannibalisation": None,
        "ppa": {"ppa_status": "active"},
    },
}


# ─────────────────────────── THE FROZEN SNAPSHOT ────────────────────────────
#
# scenario-name → ordered tuple of outcome-tuples (one per Opportunity produced,
# in detection order). Outcome-tuple shape:
#   (schema_code, severity, branch, status,
#    sorted(missing_slots), sorted(data_slots.keys()))
#
# !!! BEHAVIOUR LOCK !!!
# #92 and #93 (verbatim migration + live-path cutover) MUST leave this constant
# byte-identical. Only #94–#98 update entries, each with a one-line "POST-FIX"
# delta noted inline. Captured bugs are annotated [BUG n].
EXPECTED_SNAPSHOT: Dict[str, Tuple[tuple, ...]] = {
    # [BUG 1] OPS-01 force-downgraded CONFIRMED→INDICATIVE; wind_resource_index
    # always in missing_slots. POST-FIX(#95): OPS-01 severity becomes CONFIRMED and
    # wind_resource_index stays listed in missing_slots (no longer caps severity);
    # the dependent OPS-03 row (no PPA → WATCH, branch C) may then re-tier (#97).
    "ops01_should_be_confirmed_is_indicative": (
        (
            "OPS_01",
            "INDICATIVE",
            "C",
            "ACTIVE",
            ("maintenance_schedule", "peer_odi_p50", "ppa_status", "wind_resource_index"),
            (
                "disruption_month_list",
                "odi_months_below_threshold",
                "odi_pct",
                "odi_threshold",
                "period",
                "ppa_status",
            ),
        ),
        # OPS-01 firing (non-None) opens the OPS-03 dependency gate. With no PPA,
        # contract_type is None → WATCH, branch C, and contract_type /
        # contract_penalty_clauses join the missing_slots.
        (
            "OPS_03",
            "WATCH",
            "C",
            "ACTIVE",
            (
                "am_location",
                "asset_age_years",
                "contract_penalty_clauses",
                "contract_type",
                "insource_benchmark",
                "oem_response_time",
                "peer_odi_p50",
            ),
            ("contract_type", "has_availability_penalties", "odi_pct", "period", "ppa_status"),
        ),
    ),
    # OPS-01 WATCH + dependent OPS-03 WATCH (triggered_by OPS-01). POST-FIX(#97):
    # OPS-03 severity logic changes (inherits OPS-01), but WATCH here is unchanged.
    "ops01_watch_with_ops03_followon": (
        (
            "OPS_01",
            "WATCH",
            "A",
            "ACTIVE",
            ("maintenance_schedule", "peer_odi_p50", "wind_resource_index"),
            (
                "disruption_month_list",
                "odi_months_below_threshold",
                "odi_pct",
                "odi_threshold",
                "period",
                "ppa_status",
            ),
        ),
        (
            "OPS_03",
            "WATCH",
            "A",
            "ACTIVE",
            (
                "am_location",
                "asset_age_years",
                "insource_benchmark",
                "oem_response_time",
                "peer_odi_p50",
            ),
            ("contract_type", "has_availability_penalties", "odi_pct", "period", "ppa_status"),
        ),
    ),
    # [BUG 2] OPS-02 forced to WATCH despite CONFIRMED-eligible gap+years.
    # POST-FIX(#96): full HODI+SSR rewrite — severity reaches CONFIRMED, the
    # inversion-only firing condition is removed, data_slots/missing_slots change.
    "ops02_should_be_confirmed_is_watch": (
        (
            "OPS_02",
            "WATCH",
            "C",
            "ACTIVE",
            (
                "cannibalisation_index_seasonal",
                "maintenance_calendar",
                "revenue_uplift_potential_eur",
                "turbine_scatter_spread",
                "wind_resource_index_monthly",
            ),
            (
                "high_wind_season_capture",
                "low_wind_season_capture",
                "period",
                "seasonal_gap_pp",
                "years_with_inversion",
            ),
        ),
    ),
    # MKT-03 CONFIRMED, branch A. POST-FIX(#98): recalibrated thresholds + trend
    # downgrade rules may change tier/branch for some CI inputs.
    "mkt03_confirmed": (
        (
            "MKT_03",
            "CONFIRMED",
            "A",
            "ACTIVE",
            (
                "alternative_zone_assets",
                "peer_zone_ci",
                "portfolio_zone_correlation",
                "revenue_impact_eur",
                "zone_renewable_penetration_pct",
            ),
            (
                "cannibalisation_index",
                "ci_trend_yoy",
                "ci_values_by_year",
                "period",
                "ppa_status",
                "price_zone",
            ),
        ),
    ),
    # MKT-03 CONFIRMED→INDICATIVE via no-trend graceful degradation, branch C.
    "mkt03_confirmed_downgraded_no_trend": (
        (
            "MKT_03",
            "INDICATIVE",
            "C",
            "ACTIVE",
            (
                "alternative_zone_assets",
                "peer_zone_ci",
                "portfolio_zone_correlation",
                "revenue_impact_eur",
                "zone_renewable_penetration_pct",
            ),
            (
                "cannibalisation_index",
                "ci_trend_yoy",
                "ci_values_by_year",
                "period",
                "ppa_status",
                "price_zone",
            ),
        ),
    ),
    # [BUG 3] MKT-01 never fires → no MKT-01, no dependent MKT-02, no MKT-03.
    # ZERO opportunities. POST-FIX(#94): the zone-average fix makes MKT-01 fire;
    # this scenario's inputs + expected tuple are updated to the would-fire shape.
    "mkt01_never_fires_no_opportunities": (),
    # Documented would-be behaviour (NOT live today; the bug blocks it). MKT-01
    # INDICATIVE branch C + dependent MKT-02 WATCH branch C. #94 uses this as the
    # target shape when flipping the bug scenario above.
    "mkt01_would_fire_if_zone_average_present": (
        (
            "MKT_01",
            "INDICATIVE",
            "C",
            "ACTIVE",
            (
                "cannibalisation_index",
                "high_wind_capture_delta",
                "pcc_slope",
                "peer_capture_p50",
                "ppa_expiry_date",
                "revenue_impact_eur",
            ),
            (
                "cannibalisation_index",
                "capture_rate",
                "gap_pp",
                "period",
                "ppa_expiry_date",
                "ppa_status",
                "price_zone",
                "zone_avg_capture",
            ),
        ),
        (
            "MKT_02",
            "WATCH",
            "C",
            "ACTIVE",
            (
                "bess_revenue_potential_eur",
                "grid_headroom_mw",
                "intraday_price_spread",
                "mfrr_eligible",
                "optimal_bess_size_mwh",
            ),
            ("mkt01_severity", "period", "ppa_status", "price_zone", "storage_present"),
        ),
    ),
}


async def _compute_outcomes(scenario_name: str) -> Tuple[tuple, ...]:
    """Run one scenario through the live legacy path and reduce to tuples."""
    svc = _make_service(**SCENARIO_INPUTS[scenario_name])
    opps = await _run_legacy(svc)
    return tuple(_outcome_tuple(o) for o in opps)


# ───────────────────────────── The lock test ────────────────────────────────


@pytest.mark.asyncio
async def test_characterization_snapshot_current_six_schemas():
    """Every scenario's computed outcome equals the frozen EXPECTED_SNAPSHOT.

    This is the M1 behaviour-preservation gate. If this fails after #92/#93, the
    verbatim migration changed behaviour — investigate, do NOT edit the snapshot.
    Only #94–#98 update EXPECTED_SNAPSHOT, each with a documented delta.
    """
    computed: Dict[str, Tuple[tuple, ...]] = {}
    for name in SCENARIO_INPUTS:
        computed[name] = await _compute_outcomes(name)

    # Compare the whole mapping at once for a single, legible diff on failure.
    assert computed == EXPECTED_SNAPSHOT


@pytest.mark.asyncio
@pytest.mark.parametrize("scenario_name", list(SCENARIO_INPUTS))
async def test_each_scenario_matches_snapshot(scenario_name):
    """Per-scenario lock (sharper failure messages than the aggregate test)."""
    assert await _compute_outcomes(scenario_name) == EXPECTED_SNAPSHOT[scenario_name]


# ─────────────────── Explicit "the bug is present" asserts ───────────────────
#
# These pin the three bugs independently of the snapshot so #94–#98 have an
# unambiguous "before" to flip. Each test name says what the CURRENT (buggy)
# behaviour is.


@pytest.mark.asyncio
async def test_bug_ops01_never_reaches_confirmed_and_force_downgrades():
    """BUG 1: 8 months below threshold → determine=CONFIRMED, but the assembled
    opportunity is INDICATIVE and wind_resource_index is always in missing_slots."""
    # The pure function CAN reach CONFIRMED on its own ...
    assert OpportunityDetectionService.determine_ops01_severity(8) == Severity.CONFIRMED
    # ... but the assembled detector force-downgrades it.
    svc = _make_service(**SCENARIO_INPUTS["ops01_should_be_confirmed_is_indicative"])
    opps = await _run_legacy(svc)
    ops01 = next(o for o in opps if o.schema_code == SchemaCode.OPS_01)
    assert ops01.severity == Severity.INDICATIVE  # NOT CONFIRMED — the bug
    assert "wind_resource_index" in ops01.missing_slots


@pytest.mark.asyncio
async def test_bug_ops02_only_ever_reaches_watch():
    """BUG 2: CONFIRMED-eligible seasonal gap (≥8pp, 2 years) is forced to WATCH
    because wind_resource_index_monthly is always missing."""
    assert OpportunityDetectionService.determine_ops02_severity(15.0, 2) == Severity.CONFIRMED
    svc = _make_service(**SCENARIO_INPUTS["ops02_should_be_confirmed_is_watch"])
    opps = await _run_legacy(svc)
    ops02 = next(o for o in opps if o.schema_code == SchemaCode.OPS_02)
    assert ops02.severity == Severity.WATCH  # forced down — the bug


@pytest.mark.asyncio
async def test_bug_ops02_requires_impossible_inversion_to_fire():
    """BUG 2 (firing condition): OPS-02 only fires when low-wind season CF EXCEEDS
    high-wind season CF — a structurally-impossible inversion. A normal farm
    (high-wind CF > low-wind CF) produces NO OPS-02 row at all."""
    svc = _make_service(
        seasonal={"high_wind_cf": 0.45, "low_wind_cf": 0.30, "years_with_inversion": 0},
        ppa={},
    )
    opps = await _run_legacy(svc)
    assert not any(o.schema_code == SchemaCode.OPS_02 for o in opps)


@pytest.mark.asyncio
async def test_bug_mkt01_never_fires_via_real_calc():
    """BUG 3 at the source: the REAL ``_calc_capture_rate_gap`` returns None
    because the real ``compare_capture_rates_by_bidzone`` omits
    ``zone_average_capture_rate`` → MKT-01 can never fire.

    Drives the actual legacy ``_calc_capture_rate_gap`` (NOT a mock of it) against
    a PriceAnalyticsService whose return shape matches today's buggy production
    code (see price_analytics_service.py :635-641 — no zone_average key)."""

    class _ScalarResult:
        def __init__(self, scalar):
            self._scalar = scalar

        def scalar_one_or_none(self):
            return self._scalar

    svc = OpportunityDetectionService.__new__(OpportunityDetectionService)
    db = MagicMock()
    # First execute() = windfarm bidzone_id lookup → 42. The Bidzone.code lookup
    # is never reached because zone_avg resolves to None first.
    db.execute = AsyncMock(side_effect=[_ScalarResult(42)])
    svc.db = db

    pa = MagicMock()
    pa.calculate_capture_rate = AsyncMock(
        return_value={"overall": {"capture_rate": 0.62}, "periods": []}
    )
    # Faithful reproduction of the CURRENT buggy return: no zone_average_capture_rate.
    pa.compare_capture_rates_by_bidzone = AsyncMock(
        return_value={
            "bidzone_id": 42,
            "bidzone_code": "NO2",
            "start_date": START.isoformat(),
            "end_date": END.isoformat(),
            "windfarms": [{"windfarm_id": WINDFARM_ID, "capture_rate": 0.62}],
        }
    )
    svc.price_analytics = pa

    gap = await svc._calc_capture_rate_gap(WINDFARM_ID, START, END)
    assert gap is None  # the bug — no zone average → no gap → MKT-01 never fires


@pytest.mark.asyncio
async def test_bug_mkt01_absent_from_detect_windfarm():
    """BUG 3 at the assembly level: with capture_gap None (the bug's effect),
    ``_detect_windfarm`` emits NO MKT-01 row — and therefore no dependent MKT-02."""
    svc = _make_service(**SCENARIO_INPUTS["mkt01_never_fires_no_opportunities"])
    opps = await _run_legacy(svc)
    codes = {o.schema_code for o in opps}
    assert SchemaCode.MKT_01 not in codes
    assert SchemaCode.MKT_02 not in codes


# ─────────────── Cross-schema dependency wiring (locked) ────────────────────


@pytest.mark.asyncio
async def test_ops03_triggered_by_ops01_row_id():
    """OPS-03 links to the OPS-01 row via triggered_by_id (dependency wiring)."""
    svc = _make_service(**SCENARIO_INPUTS["ops01_watch_with_ops03_followon"])
    opps = await _run_legacy(svc)
    ops01 = next(o for o in opps if o.schema_code == SchemaCode.OPS_01)
    ops03 = next(o for o in opps if o.schema_code == SchemaCode.OPS_03)
    assert ops01.id is not None
    assert ops03.triggered_by_id == ops01.id


@pytest.mark.asyncio
async def test_all_produced_rows_are_active():
    """Every produced opportunity is ACTIVE (supersede is handled in detect_all,
    not in _detect_windfarm)."""
    for name in SCENARIO_INPUTS:
        svc = _make_service(**SCENARIO_INPUTS[name])
        opps = await _run_legacy(svc)
        for o in opps:
            assert o.status == OpportunityStatus.ACTIVE
