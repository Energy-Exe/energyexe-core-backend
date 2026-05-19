"""Unit tests for MapPerformanceService helpers (client-ui #44b BE).

Pure helpers — no database, no async fixtures. Validates:
- 5-bucket bucketing against peer p10/p50/p90 thresholds
- Coverage aggregation across NO + UK with the asymmetric flag
- Year-row selection from financial ratio rows
- Interpretation-prompt composition
"""

from datetime import date
from types import SimpleNamespace

import pytest

from app.schemas.map import (
    MapPerformanceScore,
    MapStateFilter,
    MapStatePayload,
)
from app.services.map_performance_service import (
    ASYMMETRIC_THRESHOLD,
    MIN_PEER_COUNT,
    MapPerformanceService,
    _PeerThresholds,
    _bucket_for,
    _compute_coverage,
    _pick_year_row,
)


# ─── _bucket_for ──────────────────────────────────────────────────────


class TestBucketFor:
    def _thresholds(self, p10=0.5, p50=0.8, p90=1.1, count=10):
        return _PeerThresholds(p10=p10, p50=p50, p90=p90, count=count)

    def test_under_p10_is_bucket_1(self):
        bucket, has_data = _bucket_for(0.4, self._thresholds())
        assert bucket == 1 and has_data is True

    def test_between_p10_and_mid_low_is_bucket_2(self):
        # mid_low = (0.5 + 0.8)/2 = 0.65
        bucket, _ = _bucket_for(0.6, self._thresholds())
        assert bucket == 2

    def test_around_p50_is_bucket_3(self):
        # mid_low=0.65, mid_high=(0.8+1.1)/2=0.95 → 0.8 falls in bucket 3
        bucket, _ = _bucket_for(0.8, self._thresholds())
        assert bucket == 3

    def test_between_mid_high_and_p90_is_bucket_4(self):
        bucket, _ = _bucket_for(1.0, self._thresholds())
        assert bucket == 4

    def test_at_or_above_p90_is_bucket_5(self):
        bucket, _ = _bucket_for(1.1, self._thresholds())
        assert bucket == 5
        bucket, _ = _bucket_for(2.0, self._thresholds())
        assert bucket == 5

    def test_value_none_returns_no_data(self):
        bucket, has_data = _bucket_for(None, self._thresholds())
        assert bucket is None and has_data is False

    def test_thresholds_none_returns_no_data(self):
        bucket, has_data = _bucket_for(0.8, None)
        assert bucket is None and has_data is False

    def test_too_few_peers_returns_no_data(self):
        thresholds = self._thresholds(count=MIN_PEER_COUNT - 1)
        bucket, has_data = _bucket_for(0.8, thresholds)
        assert bucket is None and has_data is False

    def test_missing_percentile_returns_no_data(self):
        thresholds = _PeerThresholds(p10=None, p50=0.8, p90=1.1, count=10)
        bucket, has_data = _bucket_for(0.8, thresholds)
        assert bucket is None and has_data is False


# ─── _compute_coverage ────────────────────────────────────────────────


def _make_score(
    wf_id: int,
    country_code: str,
    has_gen: bool = True,
    has_com: bool = True,
) -> MapPerformanceScore:
    return MapPerformanceScore(
        windfarm_id=wf_id,
        country_code=country_code,
        has_commercial_data=has_com,
        has_generation_data=has_gen,
        period_type="year",
        period_year=2025,
    )


class TestCoverage:
    def test_empty_scores(self):
        cov = _compute_coverage([], [])
        assert cov.total_count == 0
        assert cov.asymmetric is False

    def test_balanced_no_uk(self):
        scores = [
            _make_score(1, "NOR", has_gen=True),
            _make_score(2, "NOR", has_gen=True),
            _make_score(3, "GBR", has_gen=True),
            _make_score(4, "GBR", has_gen=True),
        ]
        cov = _compute_coverage(scores, [])
        assert cov.no_count == 2
        assert cov.uk_count == 2
        assert cov.no_coverage_pct == 1.0
        assert cov.uk_coverage_pct == 1.0
        assert cov.asymmetric is False

    def test_asymmetric_when_no_coverage_drops(self):
        # Build a portfolio where NO has 50% coverage and UK has 100%.
        scores = [
            _make_score(1, "NOR", has_gen=True),
            _make_score(2, "NOR", has_gen=False),
            _make_score(3, "GBR", has_gen=True),
            _make_score(4, "GBR", has_gen=True),
        ]
        cov = _compute_coverage(scores, [])
        assert cov.no_coverage_pct == 0.5
        assert cov.uk_coverage_pct == 1.0
        # |1.0 - 0.5| = 0.5 ≥ 0.15 → asymmetric
        assert abs(cov.no_coverage_pct - cov.uk_coverage_pct) >= ASYMMETRIC_THRESHOLD
        assert cov.asymmetric is True

    def test_not_asymmetric_when_one_country_absent(self):
        # If only NO is present, asymmetric should be False (no UK to compare to)
        scores = [
            _make_score(1, "NOR", has_gen=True),
            _make_score(2, "NOR", has_gen=False),
        ]
        cov = _compute_coverage(scores, [])
        assert cov.no_count == 2
        assert cov.uk_count == 0
        assert cov.asymmetric is False

    def test_commercial_and_generation_counts_independent(self):
        scores = [
            _make_score(1, "NOR", has_gen=True, has_com=False),
            _make_score(2, "GBR", has_gen=False, has_com=True),
        ]
        cov = _compute_coverage(scores, [])
        assert cov.generation_count == 1
        assert cov.commercial_count == 1


# ─── _pick_year_row ───────────────────────────────────────────────────


def _row(period_start, period_end):
    return SimpleNamespace(period_start=period_start, period_end=period_end)


class TestPickYearRow:
    def test_none_when_empty(self):
        assert _pick_year_row([], 2025) is None

    def test_picks_row_covering_year(self):
        rows = [
            _row(date(2023, 1, 1), date(2023, 12, 31)),
            _row(date(2024, 1, 1), date(2024, 12, 31)),
            _row(date(2025, 1, 1), date(2025, 12, 31)),
        ]
        chosen = _pick_year_row(rows, 2024)
        assert chosen.period_start.year == 2024

    def test_falls_back_to_most_recent_when_no_overlap(self):
        rows = [
            _row(date(2020, 1, 1), date(2020, 12, 31)),
            _row(date(2022, 1, 1), date(2022, 12, 31)),
        ]
        chosen = _pick_year_row(rows, 2025)
        assert chosen.period_start.year == 2022


# ─── build_interpretation_prompt ──────────────────────────────────────


class TestInterpretationPrompt:
    def test_includes_basic_scope(self):
        payload = MapStatePayload(
            windfarm_ids=[1, 2, 3],
            view="generation",
            period_year=2025,
        )
        svc = MapPerformanceService(db=None)
        prompt = svc.build_interpretation_prompt(payload, scores=None)
        assert "View: generation" in prompt
        assert "Period: year 2025" in prompt
        assert "Wind farms in view: 3" in prompt
        # Without scores we should still emit the closing guidance.
        assert "interpret" in prompt.lower()

    def test_summarises_filters(self):
        payload = MapStatePayload(
            windfarm_ids=[1, 2],
            view="commercial",
            period_year=2025,
            filters=MapStateFilter(
                countries=["NO", "GB"],
                statuses=["operational"],
                capacity_min=50,
                capacity_max=500,
            ),
        )
        prompt = MapPerformanceService(db=None).build_interpretation_prompt(payload, scores=None)
        assert "countries=NO,GB" in prompt
        assert "statuses=operational" in prompt
        assert "capacity=50-500MW" in prompt

    def test_summarises_scores_when_present(self):
        payload = MapStatePayload(
            windfarm_ids=[1, 2, 3, 4, 5, 6, 7],
            view="generation",
            period_year=2025,
        )
        scores = [
            MapPerformanceScore(
                windfarm_id=i,
                period_type="year",
                period_year=2025,
                generation_value=val,
                has_generation_data=True,
            )
            for i, val in enumerate([0.6, 0.7, 0.8, 0.9, 1.0, 1.1, 1.2], start=1)
        ]
        prompt = MapPerformanceService(db=None).build_interpretation_prompt(payload, scores=scores)
        assert "Generation score range: 0.60–1.20" in prompt
        assert "Lowest 5 wind farms" in prompt
        assert "Highest 5 wind farms" in prompt
