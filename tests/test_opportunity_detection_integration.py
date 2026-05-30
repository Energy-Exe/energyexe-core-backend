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

Through #92/#93 both the legacy inline detectors and the post-cutover registry
detectors consumed the *same* injected dicts and ran the *same* pure functions,
so a SINGLE path-independent ``EXPECTED_SNAPSHOT`` locked both. From **#94** the
M2 fixes land on the LIVE registry path only — the retained legacy ``_detect_*``
staticmethods deliberately keep the buggy M1 behaviour (so the 57 legacy unit
tests stay green). The two snapshots therefore split:

    * ``EXPECTED_SNAPSHOT``  — the EVOLVING live (registry) snapshot; #94–#98 each
      bump the entries they change with an inline ``# CHANGED #<n>`` delta.
    * ``M1_LEGACY_BASELINE`` — a FROZEN snapshot of pre-#94 legacy behaviour, run
      against ``M1_LEGACY_INPUTS``; the completed #93 proof-of-cutover artifact.

See the "Snapshot-evolution pattern (established #94)" block below for details.

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
from app.services.opportunity_schemas.context import DetectionContext
from app.services.opportunity_schemas.registry import run_for_windfarm

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
    """Run the LEGACY inline detection assembly for one windfarm.

    Before #93, this *was* the live ``_detect_windfarm`` body. #93 cut the live
    path over to the registry (``_detect_windfarm`` → ``_run_registry`` →
    ``run_for_windfarm``), but the legacy ``_detect_opsXX`` / ``_detect_mktXX``
    methods were RETAINED. This helper drives those retained methods directly
    (reproducing the pre-cutover ``_detect_windfarm`` assembly: same order, same
    dependency gating) so the characterization snapshot still locks the legacy
    behaviour byte-for-byte. The companion :func:`_run_registry` runs the SAME
    scenario inputs through the new registry path; both must yield identical
    tuples (see ``test_characterization_snapshot_current_six_schemas``).
    """
    wf, ps, pe, rid = WINDFARM_ID, START, END, DETECTION_RUN_ID
    opps: List = []

    ppa_info = await svc._load_ppa_info(wf)

    ops01 = await svc._detect_ops01(wf, ps, pe, ppa_info, rid)
    if ops01:
        opps.append(ops01)

    ops02 = await svc._detect_ops02(wf, ps, pe, ppa_info, rid)
    if ops02:
        opps.append(ops02)

    if ops01:
        ops03 = await svc._detect_ops03(wf, ps, pe, ppa_info, ops01, rid)
        if ops03:
            opps.append(ops03)

    mkt01 = await svc._detect_mkt01(wf, ps, pe, ppa_info, rid)
    if mkt01:
        opps.append(mkt01)

    mkt03 = await svc._detect_mkt03(wf, ps, pe, ppa_info, rid)
    if mkt03:
        opps.append(mkt03)

    if mkt01:
        mkt02 = await svc._detect_mkt02(wf, ps, pe, ppa_info, mkt01, rid)
        if mkt02:
            opps.append(mkt02)

    return opps


# ─── #93 cutover: the registry runner ───
#
# The data-access methods mocked by ``_make_service`` map 1:1 onto the
# ``DetectionContext`` accessors that the registry detectors call after the
# cutover:
#
#     legacy ``_calc_monthly_availability``  →  ctx cache key "monthly_performance"
#     legacy ``_calc_seasonal_capture``      →  ctx cache key "seasonal_capture"
#     legacy ``_calc_capture_rate_gap``      →  ctx cache key "capture_rate"
#     legacy ``_calc_cannibalisation_index`` →  ctx cache key "cannibalisation_index"
#     legacy ``_load_ppa_info``              →  ctx cache key "ppa_info"
#
# So the SAME scenario input dicts can be injected straight into a
# ``DetectionContext(prefetched=...)``; ``run_for_windfarm`` then runs the exact
# same pure functions the legacy methods do. The tuples MUST equal
# ``EXPECTED_SNAPSHOT`` — the snapshot is path-independent.


def _ctx_from_scenario(scenario_name: str) -> DetectionContext:
    """Build a DB-free DetectionContext from a scenario's injected accessor data.

    Keys present (even with a ``None`` value) short-circuit the DB query in the
    matching accessor, so ``db`` is never touched — exactly mirroring the legacy
    ``_calc_*`` mocks. Note: a key whose scenario value is ``None`` (e.g.
    ``capture_gap=None``) is still inserted so the accessor returns ``None``
    without hitting Postgres.
    """
    inputs = SCENARIO_INPUTS[scenario_name]
    prefetched: Dict[str, Any] = {
        "monthly_performance": inputs.get("monthly"),
        "seasonal_capture": inputs.get("seasonal"),
        "capture_rate": inputs.get("capture_gap"),
        "cannibalisation_index": inputs.get("cannibalisation"),
        "ppa_info": inputs["ppa"] if inputs.get("ppa") is not None else {},
        # #94: MKT-01 reads curtailment for grid-driven suppression. No scenario
        # exercises it (suppression is unit-tested in the MKT-01 module tests), so
        # inject None — the accessor short-circuits without touching the DB and
        # suppression never triggers.
        "curtailment_pct": inputs.get("curtailment_pct"),
    }
    return DetectionContext(
        db=_FakeSession(),
        windfarm=WINDFARM_ID,
        period_start=START,
        period_end=END,
        prefetched=prefetched,
    )


async def _run_registry(scenario_name: str) -> List:
    """Run a scenario through the LIVE registry path (``run_for_windfarm``)."""
    ctx = _ctx_from_scenario(scenario_name)
    return await run_for_windfarm(ctx, detection_run_id=DETECTION_RUN_ID)


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
    # OPS-02 "should be CONFIRMED but is forced to WATCH" (BUG 2). The LEGACY path
    # (M1_LEGACY_INPUTS below) keeps only the ``seasonal`` input: it requires the
    # structurally-impossible inversion (low_wind_cf > high_wind_cf) just to fire,
    # then forces WATCH because wind_resource_index_monthly is always missing.
    # CHANGED #96: the LIVE path no longer reads seasonal CF — OPS-02 is now the
    #   HODI+SSR detector reading monthly ODI-underperformance via
    #   load_monthly_performance(). The ``monthly`` rows below are a full 12-month
    #   year with high-wind-season (Oct–Mar) underperformance concentrated in a
    #   single 54% spike (availability 46%) and zero elsewhere: HODI = 54/6 = 9.0,
    #   HODI_all = 54/12 = 4.5 → SSR = 2.0 → (>=9.0, >=1.30) CONFIRMED. Only ONE
    #   month is below the OPS-01 95% ODI threshold, so OPS-01 (which shares this
    #   accessor) does NOT fire and OPS-02 is the sole finding. ``seasonal`` is
    #   retained purely so the FROZEN legacy runner still reproduces its WATCH
    #   tuple (M1_LEGACY_INPUTS strips ``monthly`` for this scenario).
    "ops02_should_be_confirmed_is_watch": {
        "monthly": _months(
            ("2024-10", 46.0),  # high-wind spike: 54% underperf (the only low month)
            ("2024-11", 100.0),
            ("2024-12", 100.0),
            ("2025-01", 100.0),
            ("2025-02", 100.0),
            ("2025-03", 100.0),
            ("2024-04", 100.0),
            ("2024-05", 100.0),
            ("2024-06", 100.0),
            ("2024-07", 100.0),
            ("2024-08", 100.0),
            ("2024-09", 100.0),
        ),
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
    # MKT-01 fires after the #94 zone-average fix. PRE-#94 this scenario had
    # ``capture_gap=None`` (the never-fires bug surfacing at the data layer) and
    # produced ZERO opportunities; ``test_bug_mkt01_never_fires_via_real_calc``
    # still proves the bug from the real query-shape source. POST-#94 the data
    # layer resolves a 7pp gap → MKT-01 INDICATIVE branch C + dependent MKT-02
    # WATCH. No CI → no MKT-03. The inputs below mirror
    # ``mkt01_would_fire_if_zone_average_present`` (the would-be target #94 flips
    # toward).
    "mkt01_never_fires_no_opportunities": {
        "capture_gap": {
            "capture_rate": 0.62,
            "zone_avg": 0.69,
            "gap_pp": 7.0,
            "bidzone_code": "NO2",
        },
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
    # [BUG 1 — FIXED #95] OPS-01 no longer force-downgraded; reaches CONFIRMED.
    # CHANGED #95: OPS_01 "INDICATIVE" -> "CONFIRMED" (reason: the
    #   wind_resource_index force-downgrade is removed — 8 low months over 2 years
    #   classifies as CONFIRMED and avg ODI 81.0% is below the 97% soft cap.
    #   wind_resource_index STAYS in missing_slots, it just no longer caps
    #   severity). The dependent OPS-03 row is UNCHANGED here: with no PPA,
    #   contract_type is None so OPS-03's severity logic still yields WATCH,
    #   branch C regardless of the OPS-01 tier (#97 revisits OPS-03 inheritance).
    # CHANGED #97: OPS-03 row reviewed under the new named
    #   ``classify_contracting_severity`` helper (OPS-01 tier inheritance). With
    #   OPS-01 now CONFIRMED (post-#95) the helper CAN reach CONFIRMED, but only
    #   for a KNOWN, no-penalty contract. This scenario has no PPA →
    #   contract_type is None → the unknown-contract branch yields WATCH, branch
    #   C — so this OPS-03 tuple is DELIBERATELY unchanged (byte-identical). No
    #   other scenario pairs a CONFIRMED OPS-01 with a known no-penalty contract,
    #   so no EXPECTED_SNAPSHOT OPS-03 entry changes value under #97.
    "ops01_should_be_confirmed_is_indicative": (
        (
            "OPS_01",
            "CONFIRMED",
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
    # CHANGED #95: old ((OPS_01 WATCH A ...), (OPS_03 WATCH A ...)) -> () (reason:
    #   the scenario has only ONE month below the ODI threshold; under the new spec
    #   thresholds 1 low month is no longer a finding (WATCH now starts at 2 low
    #   months), so OPS-01 returns None and its dependent OPS-03 is gated out. The
    #   single-low-month WATCH tier was an artifact of the old 1/2/3-month bands.
    #   The legacy runner still produces the WATCH+followon pair against the frozen
    #   1/2/3 thresholds — see M1_LEGACY_BASELINE, which stays byte-identical.
    "ops01_watch_with_ops03_followon": (),
    # [BUG 2 — FIXED #96] OPS-02 was forced to WATCH despite a CONFIRMED-eligible
    # signal AND required a structurally-impossible inversion to fire at all.
    # CHANGED #96: old ((OPS_02 "WATCH" "C" ... seasonal-CF data_slots ...),) ->
    #   ((OPS_02 "CONFIRMED" "C" ... HODI+SSR data_slots ...),) (reason: full
    #   HODI+SSR rewrite. The detector now reads monthly ODI-underperformance, not
    #   seasonal CF: HODI = 9.0 (mean high-wind underperf), SSR = 2.0 → both meet
    #   the CONFIRMED floors (>=9.0, >=1.30) → CONFIRMED. The inversion-only firing
    #   condition and the wind_resource_index_monthly WATCH force-cap are removed,
    #   so the result stays CONFIRMED. data_slots change from the seasonal-CF set
    #   to {hodi_pct, ssr, high_wind_months, high_wind_months_observed,
    #   months_observed, period}; missing_slots are unchanged. OPS-01 does NOT
    #   co-fire: only one month is below the 95% ODI threshold). The FROZEN legacy
    #   runner still produces the old WATCH tuple — see M1_LEGACY_BASELINE, which
    #   strips ``monthly`` for this scenario and stays byte-identical.
    "ops02_should_be_confirmed_is_watch": (
        (
            "OPS_02",
            "CONFIRMED",
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
                "high_wind_months",
                "high_wind_months_observed",
                "hodi_pct",
                "months_observed",
                "period",
                "ssr",
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
    # [BUG 3 — FIXED #94] MKT-01 now fires after the zone-average fix.
    # CHANGED #94: old () -> new ((MKT_01 INDICATIVE C ...), (MKT_02 WATCH C ...))
    #   (reason: compare_capture_rates_by_bidzone now returns
    #    zone_average_capture_rate, so ctx.load_capture_rate() resolves a 7pp gap
    #    instead of None; gap 7pp > MKT01_GAP_INDICATIVE_PP(6.0) → INDICATIVE
    #    branch C (no CI), and the dependent MKT-02 follows at WATCH branch C).
    # This is now identical to the would-fire reference scenario below; the legacy
    # runner's frozen M1 baseline for THIS scenario stays () (see M1_LEGACY_BASELINE).
    "mkt01_never_fires_no_opportunities": (
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


# ───────────────── Snapshot-evolution pattern (established #94) ──────────────
#
# Through #92/#93 a SINGLE frozen snapshot served BOTH runners (the legacy
# retained assembly and the live registry path) because the cutover changed which
# path is live but NOT the output. From #94 on the two paths legitimately diverge:
# the live registry detectors carry the M2 fixes, while the retained legacy
# ``_detect_*`` staticmethods deliberately keep the buggy M1 behaviour (so the 57
# legacy unit tests stay green). So we split into:
#
#   * EXPECTED_SNAPSHOT  — the EVOLVING **live (registry)** snapshot. Each M2
#     issue (#94–#98) updates the entries it changes, with an inline
#     ``# CHANGED #<n>: old -> new (reason)`` delta. Non-changed scenarios stay
#     byte-identical.
#   * M1_LEGACY_BASELINE — a FROZEN snapshot of the pre-#94 legacy behaviour,
#     run against M1_LEGACY_INPUTS (the original pre-#94 scenario inputs). This
#     keeps the legacy retained assembly under test as a regression guard WITHOUT
#     forcing it to track the live fixes. It is the completed proof-of-cutover
#     artifact from #93; do NOT evolve it.
#
# #94's only divergence is MKT-01/02: the legacy MKT-01 still uses the legacy
# thresholds AND the never-fires inputs, so its M1 baseline for
# ``mkt01_never_fires_no_opportunities`` stays () while the live path now fires.
# (#95–#98 will diverge on OPS-01/02/03 + MKT-03 the same way: bump
# EXPECTED_SNAPSHOT for the live path, leave M1_LEGACY_BASELINE frozen.)

# Frozen pre-#94 inputs for the legacy runner. Identical to SCENARIO_INPUTS at
# the #93 freeze EXCEPT ``mkt01_never_fires_no_opportunities`` keeps its original
# ``capture_gap=None`` (the data-layer never-fires bug) — the one input #94 flips
# for the live path.
M1_LEGACY_INPUTS: Dict[str, Dict[str, Any]] = {
    **SCENARIO_INPUTS,
    "mkt01_never_fires_no_opportunities": {
        "capture_gap": None,
        "cannibalisation": None,
        "ppa": {"ppa_status": "active"},
    },
    # #96: the legacy OPS-02 detector reads ``seasonal`` (not ``monthly``); the
    # ``monthly`` rows are a LIVE-path-only input (the HODI+SSR rewrite). Strip
    # ``monthly`` here so the frozen legacy runner reproduces its WATCH-only
    # behaviour from the seasonal inversion (and so legacy OPS-01 stays None for
    # this scenario, exactly as pre-#96).
    "ops02_should_be_confirmed_is_watch": {
        "seasonal": {"high_wind_cf": 0.30, "low_wind_cf": 0.45, "years_with_inversion": 2},
        "ppa": {},
    },
}

# Frozen pre-#94 legacy outputs. This baseline is the completed #93
# proof-of-cutover artifact and MUST stay byte-identical: the legacy ``_detect_*``
# staticmethods keep the M1 (pre-fix) behaviour so the 57 legacy unit tests stay
# green. As later M2 issues bump EXPECTED_SNAPSHOT (the live path) the two
# snapshots diverge, so every scenario the live path changes must be PINNED here
# to its original frozen tuple (overriding the ``**EXPECTED_SNAPSHOT`` spread).
# Differences from the live EXPECTED_SNAPSHOT, by issue:
#   #94: ``mkt01_never_fires_no_opportunities`` stays () (the never-fires bug — the
#        legacy data layer returns capture_gap=None per M1_LEGACY_INPUTS).
#   #95: ``ops01_should_be_confirmed_is_indicative`` keeps the OPS-01 force-downgrade
#        (INDICATIVE, not CONFIRMED); ``ops01_watch_with_ops03_followon`` keeps the
#        old 1-low-month WATCH + OPS-03 WATCH pair (legacy 1/2/3-month thresholds).
#   #96: ``ops02_should_be_confirmed_is_watch`` keeps the OPS-02 WATCH force-cap +
#        seasonal-CF data_slots (the legacy detector reads ``seasonal`` and force-caps
#        to WATCH; the HODI+SSR CONFIRMED result is LIVE-path only).
M1_LEGACY_BASELINE: Dict[str, Tuple[tuple, ...]] = {
    **EXPECTED_SNAPSHOT,
    "mkt01_never_fires_no_opportunities": (),
    # #96: legacy OPS-02 force-capped to WATCH with seasonal-CF data_slots.
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
    # #95: legacy OPS-01 force-downgrade (CONFIRMED→INDICATIVE) preserved.
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
    # #95: legacy OPS-01 1-low-month WATCH (legacy thresholds) + dependent OPS-03.
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
}


async def _compute_outcomes_legacy(scenario_name: str) -> Tuple[tuple, ...]:
    """Run one scenario through the retained legacy assembly and reduce to tuples."""
    svc = _make_service(**M1_LEGACY_INPUTS[scenario_name])
    opps = await _run_legacy(svc)
    return tuple(_outcome_tuple(o) for o in opps)


async def _compute_outcomes_registry(scenario_name: str) -> Tuple[tuple, ...]:
    """Run one scenario through the live registry path and reduce to tuples."""
    opps = await _run_registry(scenario_name)
    return tuple(_outcome_tuple(o) for o in opps)


# ───────────────────────────── The lock tests ───────────────────────────────


@pytest.mark.asyncio
async def test_characterization_snapshot_live_registry_path():
    """The LIVE registry path reproduces the EVOLVING EXPECTED_SNAPSHOT.

    This is the post-#93 behaviour gate for the live path. #94–#98 each update the
    entries they change (with an inline ``# CHANGED #<n>`` delta); non-changed
    scenarios MUST stay byte-identical.
    """
    computed: Dict[str, Tuple[tuple, ...]] = {}
    for name in SCENARIO_INPUTS:
        computed[name] = await _compute_outcomes_registry(name)

    assert computed == EXPECTED_SNAPSHOT


@pytest.mark.asyncio
async def test_characterization_snapshot_frozen_m1_legacy_baseline():
    """The retained legacy assembly reproduces the FROZEN M1_LEGACY_BASELINE.

    The legacy ``_detect_*`` staticmethods keep the M1 (pre-fix) behaviour so the
    57 legacy unit tests stay green; this locks them as a regression guard. Do NOT
    evolve M1_LEGACY_BASELINE — it is the completed #93 proof-of-cutover artifact.
    """
    computed: Dict[str, Tuple[tuple, ...]] = {}
    for name in M1_LEGACY_INPUTS:
        computed[name] = await _compute_outcomes_legacy(name)

    assert computed == M1_LEGACY_BASELINE


@pytest.mark.asyncio
@pytest.mark.parametrize("scenario_name", list(SCENARIO_INPUTS))
async def test_each_scenario_matches_live_snapshot(scenario_name):
    """Per-scenario live-path lock (sharper failure messages than the aggregate)."""
    assert await _compute_outcomes_registry(scenario_name) == EXPECTED_SNAPSHOT[scenario_name]


@pytest.mark.asyncio
@pytest.mark.parametrize("scenario_name", list(M1_LEGACY_INPUTS))
async def test_each_scenario_matches_legacy_baseline(scenario_name):
    """Per-scenario frozen-legacy lock (sharper failure messages than aggregate)."""
    assert await _compute_outcomes_legacy(scenario_name) == M1_LEGACY_BASELINE[scenario_name]


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
    # The FROZEN legacy runner reads ``seasonal`` only — M1_LEGACY_INPUTS strips the
    # ``monthly`` rows that the #96 LIVE HODI+SSR detector now consumes.
    svc = _make_service(**M1_LEGACY_INPUTS["ops02_should_be_confirmed_is_watch"])
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
    """BUG 3 at the assembly level (legacy path): with the frozen pre-#94
    ``capture_gap=None`` input, the legacy ``_detect_windfarm`` assembly emits NO
    MKT-01 row — and therefore no dependent MKT-02. The LIVE path is fixed in #94
    (see ``test_characterization_snapshot_live_registry_path``)."""
    svc = _make_service(**M1_LEGACY_INPUTS["mkt01_never_fires_no_opportunities"])
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
    for name in M1_LEGACY_INPUTS:
        svc = _make_service(**M1_LEGACY_INPUTS[name])
        opps = await _run_legacy(svc)
        for o in opps:
            assert o.status == OpportunityStatus.ACTIVE
