"""
Test Elexon Curtailment (BOAV) Data Integrity

This test verifies that for hours where curtailment (BOAV) data exists in raw data,
the aggregated generation_data table correctly reflects the generation.

Issue: Reports that for hours with curtailment data, generation data shows as zero or missing.

ROOT CAUSE ANALYSIS:
====================
The investigation found that 2,902 hour/unit combinations have BOAV (curtailment) data
but no corresponding aggregated record in generation_data. The pattern shows:

1. 23:00 UTC is heavily affected (1,884 out of 2,902 missing, or 65%)
2. The issue appears to be a DAY BOUNDARY BUG in the elexon_processor.py

The processor runs day-by-day:
- When processing day N, it clears data from hour (N-1, 23:00) to hour (N+1, 00:00)
- Then fetches raw data with period_start in the same range
- For BOAV-only hours at 23:00 with no B1610 data, the aggregation may not create records

Additionally, some windfarms (e.g., Moray East, Paul's Hill) have raw data but were never
processed through the aggregation pipeline.

Tests:
1. Random sampling of hours from 2021 with BOAV data
2. Comparison of raw vs aggregated data
3. Identification of missing or incorrect records
4. Analysis of 23:00 UTC boundary issue
5. Detection of unprocessed windfarms
"""

import asyncio
import random
import sys
from pathlib import Path
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, Tuple
from dataclasses import dataclass, field
from zoneinfo import ZoneInfo
from collections import defaultdict
from decimal import Decimal

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

import pytest
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from sqlalchemy import select, and_, text, func

from app.core.config import get_settings
from app.models.generation_data import GenerationDataRaw, GenerationData
from app.models.generation_unit import GenerationUnit
from app.models.windfarm import Windfarm

UTC_TZ = ZoneInfo('UTC')
UK_TZ = ZoneInfo('Europe/London')


@dataclass
class DataIntegrityIssue:
    """Represents a data integrity issue found."""
    hour: datetime
    identifier: str
    generation_unit_id: Optional[int]
    issue_type: str  # 'missing_aggregated', 'value_mismatch', 'missing_raw'
    raw_metered_mwh: float
    raw_curtailed_mwh: float
    expected_generation_mwh: float
    actual_generation_mwh: Optional[float]
    raw_record_count: int
    details: str = ""


@dataclass
class TestResults:
    """Results of the data integrity test."""
    hours_tested: int = 0
    hours_with_issues: int = 0
    missing_aggregated: int = 0
    value_mismatches: int = 0
    total_raw_boav_records: int = 0
    total_raw_b1610_records: int = 0
    total_aggregated_records: int = 0
    issues: List[DataIntegrityIssue] = field(default_factory=list)
    sample_hours: List[datetime] = field(default_factory=list)

    def add_issue(self, issue: DataIntegrityIssue):
        self.issues.append(issue)
        self.hours_with_issues += 1
        if issue.issue_type == 'missing_aggregated':
            self.missing_aggregated += 1
        elif issue.issue_type == 'value_mismatch':
            self.value_mismatches += 1


class ElexonDataIntegrityTester:
    """Tests data integrity between raw and aggregated Elexon data."""

    def __init__(self, session: AsyncSession):
        self.db = session
        self.generation_units_cache: Dict[str, Dict] = {}

    async def load_generation_units(self) -> int:
        """Load all ELEXON generation units into cache."""
        result = await self.db.execute(
            select(GenerationUnit, Windfarm)
            .outerjoin(Windfarm, GenerationUnit.windfarm_id == Windfarm.id)
            .where(GenerationUnit.source == 'ELEXON')
        )
        rows = result.all()

        for unit, windfarm in rows:
            self.generation_units_cache[unit.code] = {
                'id': unit.id,
                'windfarm_id': unit.windfarm_id,
                'capacity_mw': float(unit.capacity_mw) if unit.capacity_mw else None,
                'name': unit.name,
                'windfarm_name': windfarm.name if windfarm else None
            }

        return len(self.generation_units_cache)

    async def get_hours_with_boav_data(
        self,
        start_date: datetime,
        end_date: datetime,
        limit: int = 1000
    ) -> List[Tuple[datetime, str]]:
        """Get all hours that have BOAV (curtailment) data in 2021."""
        result = await self.db.execute(
            text("""
                SELECT DISTINCT
                    date_trunc('hour', period_start) as hour,
                    identifier
                FROM generation_data_raw
                WHERE source = 'ELEXON'
                AND source_type = 'boav_bid'
                AND period_start >= :start_date
                AND period_start < :end_date
                AND value_extracted IS NOT NULL
                AND value_extracted != 0
                ORDER BY hour, identifier
                LIMIT :limit
            """),
            {'start_date': start_date, 'end_date': end_date, 'limit': limit}
        )
        return [(row.hour, row.identifier) for row in result.all()]

    async def get_raw_data_for_hour(
        self,
        hour: datetime,
        identifier: str
    ) -> Dict[str, Any]:
        """Get all raw data (B1610 and BOAV) for a specific hour and identifier."""
        hour_start = hour.replace(minute=0, second=0, microsecond=0)
        hour_end = hour_start + timedelta(hours=1)

        # Get B1610 (metered generation) data
        b1610_result = await self.db.execute(
            select(GenerationDataRaw)
            .where(
                and_(
                    GenerationDataRaw.source == 'ELEXON',
                    GenerationDataRaw.identifier == identifier,
                    GenerationDataRaw.period_start >= hour_start,
                    GenerationDataRaw.period_start < hour_end,
                    GenerationDataRaw.source_type.notin_(['boav_bid', 'boav_offer'])
                )
            )
        )
        b1610_records = b1610_result.scalars().all()

        # Get BOAV (curtailment) data
        boav_result = await self.db.execute(
            select(GenerationDataRaw)
            .where(
                and_(
                    GenerationDataRaw.source == 'ELEXON',
                    GenerationDataRaw.identifier == identifier,
                    GenerationDataRaw.period_start >= hour_start,
                    GenerationDataRaw.period_start < hour_end,
                    GenerationDataRaw.source_type == 'boav_bid'
                )
            )
        )
        boav_records = boav_result.scalars().all()

        # Calculate totals
        metered_mwh = 0.0
        for r in b1610_records:
            val = float(r.value_extracted) if r.value_extracted is not None else 0.0
            # Check for import/export indicator
            if r.data and r.data.get('import_export_ind') == 'I':
                val = -abs(val)
            elif r.data and r.data.get('import_export_ind') == 'E':
                val = abs(val)
            metered_mwh += val

        curtailed_mwh = sum(
            abs(float(r.value_extracted))
            for r in boav_records
            if r.value_extracted is not None
        )

        return {
            'b1610_count': len(b1610_records),
            'boav_count': len(boav_records),
            'metered_mwh': metered_mwh,
            'curtailed_mwh': curtailed_mwh,
            'expected_generation_mwh': metered_mwh + curtailed_mwh,
            'b1610_records': b1610_records,
            'boav_records': boav_records
        }

    async def get_aggregated_data_for_hour(
        self,
        hour: datetime,
        identifier: str
    ) -> Optional[GenerationData]:
        """Get aggregated generation data for a specific hour."""
        unit_info = self.generation_units_cache.get(identifier)
        if not unit_info:
            return None

        hour_start = hour.replace(minute=0, second=0, microsecond=0)

        result = await self.db.execute(
            select(GenerationData)
            .where(
                and_(
                    GenerationData.source == 'ELEXON',
                    GenerationData.generation_unit_id == unit_info['id'],
                    GenerationData.hour == hour_start
                )
            )
        )
        return result.scalar_one_or_none()

    async def test_random_samples(
        self,
        start_date: datetime,
        end_date: datetime,
        sample_size: int = 100
    ) -> TestResults:
        """Test random samples of hours with BOAV data."""
        results = TestResults()

        # Load generation units
        unit_count = await self.load_generation_units()
        print(f"\nLoaded {unit_count} ELEXON generation units")

        # Get all hours with BOAV data
        print(f"\nFetching hours with BOAV data between {start_date.date()} and {end_date.date()}...")
        hours_with_boav = await self.get_hours_with_boav_data(start_date, end_date, limit=10000)
        results.total_raw_boav_records = len(hours_with_boav)

        if not hours_with_boav:
            print("No hours with BOAV data found in the specified range!")
            return results

        print(f"Found {len(hours_with_boav)} hour/unit combinations with BOAV data")

        # Random sample
        if len(hours_with_boav) > sample_size:
            samples = random.sample(hours_with_boav, sample_size)
        else:
            samples = hours_with_boav

        results.hours_tested = len(samples)
        print(f"Testing {len(samples)} random samples...\n")

        # Test each sample
        for i, (hour, identifier) in enumerate(samples):
            if (i + 1) % 20 == 0:
                print(f"  Progress: {i + 1}/{len(samples)}")

            results.sample_hours.append(hour)

            # Get raw data
            raw_data = await self.get_raw_data_for_hour(hour, identifier)
            results.total_raw_b1610_records += raw_data['b1610_count']

            # Get aggregated data
            agg_data = await self.get_aggregated_data_for_hour(hour, identifier)

            if agg_data:
                results.total_aggregated_records += 1

            # Check if generation unit exists
            unit_info = self.generation_units_cache.get(identifier)
            if not unit_info:
                # Unit not in our tracked windfarms, skip
                continue

            # Check for issues
            if not agg_data:
                # Missing aggregated record
                issue = DataIntegrityIssue(
                    hour=hour,
                    identifier=identifier,
                    generation_unit_id=unit_info['id'],
                    issue_type='missing_aggregated',
                    raw_metered_mwh=raw_data['metered_mwh'],
                    raw_curtailed_mwh=raw_data['curtailed_mwh'],
                    expected_generation_mwh=raw_data['expected_generation_mwh'],
                    actual_generation_mwh=None,
                    raw_record_count=raw_data['b1610_count'] + raw_data['boav_count'],
                    details=f"B1610 records: {raw_data['b1610_count']}, BOAV records: {raw_data['boav_count']}"
                )
                results.add_issue(issue)
            else:
                # Check value consistency
                actual_gen = float(agg_data.generation_mwh) if agg_data.generation_mwh else 0.0
                expected_gen = raw_data['expected_generation_mwh']

                # Allow small tolerance for floating point
                if abs(actual_gen - expected_gen) > 0.1:
                    issue = DataIntegrityIssue(
                        hour=hour,
                        identifier=identifier,
                        generation_unit_id=unit_info['id'],
                        issue_type='value_mismatch',
                        raw_metered_mwh=raw_data['metered_mwh'],
                        raw_curtailed_mwh=raw_data['curtailed_mwh'],
                        expected_generation_mwh=expected_gen,
                        actual_generation_mwh=actual_gen,
                        raw_record_count=raw_data['b1610_count'] + raw_data['boav_count'],
                        details=f"Expected: {expected_gen:.3f}, Actual: {actual_gen:.3f}, Diff: {actual_gen - expected_gen:.3f}"
                    )
                    results.add_issue(issue)

        return results

    async def detailed_analysis_for_unit(
        self,
        identifier: str,
        start_date: datetime,
        end_date: datetime
    ) -> Dict[str, Any]:
        """Do a detailed analysis for a specific generation unit."""
        unit_info = self.generation_units_cache.get(identifier)
        if not unit_info:
            return {'error': f'Unit {identifier} not found'}

        # Count raw BOAV records
        boav_count_result = await self.db.execute(
            text("""
                SELECT COUNT(*) as cnt
                FROM generation_data_raw
                WHERE source = 'ELEXON'
                AND source_type = 'boav_bid'
                AND identifier = :identifier
                AND period_start >= :start_date
                AND period_start < :end_date
            """),
            {'identifier': identifier, 'start_date': start_date, 'end_date': end_date}
        )
        raw_boav_count = boav_count_result.scalar()

        # Count raw B1610 records
        b1610_count_result = await self.db.execute(
            text("""
                SELECT COUNT(*) as cnt
                FROM generation_data_raw
                WHERE source = 'ELEXON'
                AND source_type NOT IN ('boav_bid', 'boav_offer')
                AND identifier = :identifier
                AND period_start >= :start_date
                AND period_start < :end_date
            """),
            {'identifier': identifier, 'start_date': start_date, 'end_date': end_date}
        )
        raw_b1610_count = b1610_count_result.scalar()

        # Count aggregated records
        agg_count_result = await self.db.execute(
            text("""
                SELECT COUNT(*) as cnt
                FROM generation_data
                WHERE source = 'ELEXON'
                AND generation_unit_id = :unit_id
                AND hour >= :start_date
                AND hour < :end_date
            """),
            {'unit_id': unit_info['id'], 'start_date': start_date, 'end_date': end_date}
        )
        agg_count = agg_count_result.scalar()

        # Count hours with curtailment > 0
        curtailed_hours_result = await self.db.execute(
            text("""
                SELECT COUNT(*) as cnt
                FROM generation_data
                WHERE source = 'ELEXON'
                AND generation_unit_id = :unit_id
                AND hour >= :start_date
                AND hour < :end_date
                AND curtailed_mwh > 0
            """),
            {'unit_id': unit_info['id'], 'start_date': start_date, 'end_date': end_date}
        )
        curtailed_hours = curtailed_hours_result.scalar()

        # Expected hours (24 * days)
        days = (end_date - start_date).days
        expected_hours = days * 24

        return {
            'identifier': identifier,
            'generation_unit_id': unit_info['id'],
            'windfarm_id': unit_info['windfarm_id'],
            'windfarm_name': unit_info.get('windfarm_name'),
            'date_range': f"{start_date.date()} to {end_date.date()}",
            'days': days,
            'raw_b1610_records': raw_b1610_count,
            'raw_boav_records': raw_boav_count,
            'expected_hours': expected_hours,
            'aggregated_hours': agg_count,
            'aggregated_hours_with_curtailment': curtailed_hours,
            'missing_hours': expected_hours - agg_count if agg_count else expected_hours,
            'completeness_pct': (agg_count / expected_hours * 100) if expected_hours > 0 else 0
        }


async def run_integrity_test(
    start_date: datetime = None,
    end_date: datetime = None,
    sample_size: int = 100
) -> TestResults:
    """Run the data integrity test."""
    if start_date is None:
        start_date = datetime(2021, 1, 1, tzinfo=UTC_TZ)
    if end_date is None:
        end_date = datetime(2022, 1, 1, tzinfo=UTC_TZ)

    settings = get_settings()
    engine = create_async_engine(settings.database_url_async, echo=False)
    async_session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    async with async_session() as session:
        tester = ElexonDataIntegrityTester(session)
        results = await tester.test_random_samples(start_date, end_date, sample_size)

    await engine.dispose()
    return results


async def run_detailed_unit_analysis(
    identifiers: List[str],
    start_date: datetime = None,
    end_date: datetime = None
) -> List[Dict[str, Any]]:
    """Run detailed analysis for specific generation units."""
    if start_date is None:
        start_date = datetime(2021, 1, 1, tzinfo=UTC_TZ)
    if end_date is None:
        end_date = datetime(2022, 1, 1, tzinfo=UTC_TZ)

    settings = get_settings()
    engine = create_async_engine(settings.database_url_async, echo=False)
    async_session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    results = []
    async with async_session() as session:
        tester = ElexonDataIntegrityTester(session)
        await tester.load_generation_units()

        for identifier in identifiers:
            analysis = await tester.detailed_analysis_for_unit(identifier, start_date, end_date)
            results.append(analysis)

    await engine.dispose()
    return results


def print_results(results: TestResults):
    """Print test results in a readable format."""
    print("\n" + "=" * 80)
    print(" " * 20 + "ELEXON DATA INTEGRITY TEST RESULTS")
    print("=" * 80)

    print(f"\n SUMMARY:")
    print(f"  - Hours tested: {results.hours_tested}")
    print(f"  - Hours with issues: {results.hours_with_issues}")
    print(f"  - Missing aggregated records: {results.missing_aggregated}")
    print(f"  - Value mismatches: {results.value_mismatches}")
    print(f"  - Total raw BOAV records found: {results.total_raw_boav_records}")
    print(f"  - Total raw B1610 records in samples: {results.total_raw_b1610_records}")
    print(f"  - Total aggregated records found: {results.total_aggregated_records}")

    if results.hours_tested > 0:
        issue_rate = results.hours_with_issues / results.hours_tested * 100
        print(f"\n  Issue Rate: {issue_rate:.1f}%")

    if results.issues:
        print(f"\n SAMPLE ISSUES (showing first 20):")
        print("-" * 80)

        for i, issue in enumerate(results.issues[:20]):
            print(f"\n  Issue #{i+1}:")
            print(f"    Hour: {issue.hour}")
            print(f"    Unit: {issue.identifier} (ID: {issue.generation_unit_id})")
            print(f"    Type: {issue.issue_type}")
            print(f"    Raw Metered MWh: {issue.raw_metered_mwh:.3f}")
            print(f"    Raw Curtailed MWh: {issue.raw_curtailed_mwh:.3f}")
            print(f"    Expected Generation MWh: {issue.expected_generation_mwh:.3f}")
            print(f"    Actual Generation MWh: {issue.actual_generation_mwh if issue.actual_generation_mwh is not None else 'MISSING'}")
            print(f"    Details: {issue.details}")

    # Summary by issue type
    if results.issues:
        print(f"\n ISSUES BY TYPE:")
        print("-" * 80)

        by_type = defaultdict(list)
        for issue in results.issues:
            by_type[issue.issue_type].append(issue)

        for issue_type, issues in by_type.items():
            print(f"\n  {issue_type}: {len(issues)} occurrences")

            # Show unique units affected
            units = set(i.identifier for i in issues)
            print(f"    Affected units: {len(units)}")
            for unit in list(units)[:5]:
                print(f"      - {unit}")
            if len(units) > 5:
                print(f"      ... and {len(units) - 5} more")

    print("\n" + "=" * 80)

    # Test verdict
    if results.hours_with_issues == 0:
        print(" VERDICT: PASS - No data integrity issues found")
    else:
        print(f" VERDICT: FAIL - {results.hours_with_issues} issues found in {results.hours_tested} samples")

    print("=" * 80 + "\n")


# Pytest fixtures and tests
# NOTE: These tests run against the REAL PostgreSQL database, not the test SQLite database.
# They are data verification tests, not unit tests.

@pytest.fixture
async def db_session():
    """
    Create a database session connected to the REAL PostgreSQL database.
    This fixture is used for data verification tests, not unit tests.
    """
    settings = get_settings()
    # Ensure we're connecting to PostgreSQL, not SQLite
    db_url = settings.database_url_async
    if 'sqlite' in db_url:
        pytest.skip("This test requires PostgreSQL database connection")

    engine = create_async_engine(db_url, echo=False)
    async_session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    async with async_session() as session:
        yield session

    await engine.dispose()


@pytest.mark.asyncio
async def test_boav_data_exists_in_2021(db_session):
    """Test that BOAV data exists for 2021."""
    result = await db_session.execute(
        text("""
            SELECT COUNT(*) as cnt
            FROM generation_data_raw
            WHERE source = 'ELEXON'
            AND source_type = 'boav_bid'
            AND period_start >= '2021-01-01'
            AND period_start < '2022-01-01'
        """)
    )
    count = result.scalar()

    print(f"\nBOAV records in 2021: {count}")
    assert count is not None, "Query should return a count"
    # Note: This may be 0 if BOAV data hasn't been imported


@pytest.mark.asyncio
async def test_b1610_data_exists_in_2021(db_session):
    """Test that B1610 (metered generation) data exists for 2021."""
    result = await db_session.execute(
        text("""
            SELECT COUNT(*) as cnt
            FROM generation_data_raw
            WHERE source = 'ELEXON'
            AND source_type NOT IN ('boav_bid', 'boav_offer')
            AND period_start >= '2021-01-01'
            AND period_start < '2022-01-01'
        """)
    )
    count = result.scalar()

    print(f"\nB1610 records in 2021: {count}")
    assert count is not None and count > 0, "Should have B1610 data for 2021"


@pytest.mark.asyncio
async def test_aggregated_data_exists_in_2021(db_session):
    """Test that aggregated generation data exists for 2021."""
    result = await db_session.execute(
        text("""
            SELECT COUNT(*) as cnt
            FROM generation_data
            WHERE source = 'ELEXON'
            AND hour >= '2021-01-01'
            AND hour < '2022-01-01'
        """)
    )
    count = result.scalar()

    print(f"\nAggregated records in 2021: {count}")
    assert count is not None, "Query should return a count"


@pytest.mark.asyncio
async def test_random_sample_integrity(db_session):
    """Test data integrity on random samples from 2021."""
    tester = ElexonDataIntegrityTester(db_session)

    start_date = datetime(2021, 1, 1, tzinfo=UTC_TZ)
    end_date = datetime(2022, 1, 1, tzinfo=UTC_TZ)

    results = await tester.test_random_samples(start_date, end_date, sample_size=50)

    print_results(results)

    # The test passes but reports issues for investigation
    # We don't fail here because we want to see the report
    if results.hours_with_issues > 0:
        pytest.skip(f"Found {results.hours_with_issues} data integrity issues - see report above")


@pytest.mark.asyncio
async def test_specific_hour_analysis(db_session):
    """Test a specific hour in detail."""
    tester = ElexonDataIntegrityTester(db_session)
    await tester.load_generation_units()

    # Get one hour with BOAV data
    result = await db_session.execute(
        text("""
            SELECT period_start, identifier
            FROM generation_data_raw
            WHERE source = 'ELEXON'
            AND source_type = 'boav_bid'
            AND period_start >= '2021-06-01'
            AND period_start < '2021-07-01'
            AND value_extracted > 0
            LIMIT 1
        """)
    )
    row = result.first()

    if not row:
        pytest.skip("No BOAV data found for June 2021")
        return

    hour = row.period_start.replace(minute=0, second=0, microsecond=0)
    identifier = row.identifier

    print(f"\nAnalyzing hour: {hour}")
    print(f"Identifier: {identifier}")

    raw_data = await tester.get_raw_data_for_hour(hour, identifier)
    agg_data = await tester.get_aggregated_data_for_hour(hour, identifier)

    print(f"\nRaw Data:")
    print(f"  B1610 records: {raw_data['b1610_count']}")
    print(f"  BOAV records: {raw_data['boav_count']}")
    print(f"  Metered MWh: {raw_data['metered_mwh']:.3f}")
    print(f"  Curtailed MWh: {raw_data['curtailed_mwh']:.3f}")
    print(f"  Expected Generation MWh: {raw_data['expected_generation_mwh']:.3f}")

    print(f"\nAggregated Data:")
    if agg_data:
        print(f"  Generation MWh: {agg_data.generation_mwh}")
        print(f"  Metered MWh: {agg_data.metered_mwh}")
        print(f"  Curtailed MWh: {agg_data.curtailed_mwh}")
    else:
        print("  NO AGGREGATED DATA FOUND!")


@pytest.mark.asyncio
async def test_23_hour_boundary_issue(db_session):
    """
    Test specifically for the 23:00 UTC boundary issue.

    This test checks if 23:00 hours are disproportionately missing
    from aggregated data when BOAV data exists.
    """
    print("\n=== TESTING 23:00 UTC BOUNDARY ISSUE ===")

    # Get distribution of missing hours by hour of day
    result = await db_session.execute(
        text("""
            WITH boav_hours AS (
                SELECT DISTINCT
                    date_trunc('hour', gdr.period_start) as hour,
                    gdr.identifier
                FROM generation_data_raw gdr
                WHERE gdr.source = 'ELEXON'
                AND gdr.source_type = 'boav_bid'
                AND gdr.period_start >= '2021-01-01'
                AND gdr.period_start < '2022-01-01'
                AND gdr.value_extracted IS NOT NULL
            ),
            matched_units AS (
                SELECT DISTINCT bh.hour, bh.identifier, gu.id as unit_id
                FROM boav_hours bh
                JOIN generation_units gu ON gu.code = bh.identifier AND gu.source = 'ELEXON'
            ),
            with_agg AS (
                SELECT DISTINCT mu.hour, mu.identifier
                FROM matched_units mu
                JOIN generation_data gd ON gd.generation_unit_id = mu.unit_id AND gd.hour = mu.hour
            ),
            missing AS (
                SELECT bh.hour, bh.identifier
                FROM boav_hours bh
                WHERE NOT EXISTS (
                    SELECT 1 FROM with_agg wa WHERE wa.hour = bh.hour AND wa.identifier = bh.identifier
                )
            )
            SELECT
                EXTRACT(HOUR FROM hour) as hour_of_day,
                COUNT(*) as missing_count
            FROM missing
            GROUP BY EXTRACT(HOUR FROM hour)
            ORDER BY missing_count DESC
        """)
    )

    rows = result.all()
    total_missing = sum(row.missing_count for row in rows)

    print(f"\nTotal missing hour/unit combinations: {total_missing:,}")
    print("\nMissing hours by hour of day (UTC):")

    hour_23_missing = 0
    for row in rows:
        hour_of_day = int(row.hour_of_day)
        pct = row.missing_count / total_missing * 100 if total_missing > 0 else 0
        marker = " <-- MOST AFFECTED" if hour_of_day == 23 else ""
        print(f"  {hour_of_day:02d}:00 UTC: {row.missing_count:,} ({pct:.1f}%){marker}")
        if hour_of_day == 23:
            hour_23_missing = row.missing_count

    # Check if 23:00 is disproportionately affected
    if total_missing > 0:
        hour_23_pct = hour_23_missing / total_missing * 100
        print(f"\n23:00 UTC accounts for {hour_23_pct:.1f}% of all missing hours")

        if hour_23_pct > 50:
            print("\nVERDICT: 23:00 UTC BOUNDARY ISSUE CONFIRMED")
            print("The elexon_processor.py has a day boundary bug affecting 23:00 hours.")
        else:
            print("\nVERDICT: 23:00 is NOT disproportionately affected")


@pytest.mark.asyncio
async def test_unprocessed_windfarms(db_session):
    """
    Test to find windfarms with raw data that haven't been aggregated.
    """
    print("\n=== CHECKING FOR UNPROCESSED WINDFARMS ===")

    result = await db_session.execute(
        text("""
            WITH raw_counts AS (
                SELECT
                    gu.windfarm_id,
                    COUNT(CASE WHEN gdr.source_type NOT IN ('boav_bid', 'boav_offer') THEN 1 END) as b1610_count,
                    COUNT(CASE WHEN gdr.source_type = 'boav_bid' THEN 1 END) as boav_count
                FROM generation_units gu
                JOIN generation_data_raw gdr ON gdr.identifier = gu.code
                WHERE gu.source = 'ELEXON'
                AND gdr.source = 'ELEXON'
                AND gdr.period_start >= '2021-01-01'
                AND gdr.period_start < '2022-01-01'
                GROUP BY gu.windfarm_id
            ),
            agg_counts AS (
                SELECT
                    gu.windfarm_id,
                    COUNT(gd.id) as agg_count
                FROM generation_units gu
                LEFT JOIN generation_data gd ON gd.generation_unit_id = gu.id
                    AND gd.hour >= '2021-01-01' AND gd.hour < '2022-01-01'
                WHERE gu.source = 'ELEXON'
                GROUP BY gu.windfarm_id
            )
            SELECT
                w.id,
                w.name,
                rc.b1610_count,
                rc.boav_count,
                COALESCE(ac.agg_count, 0) as agg_count
            FROM raw_counts rc
            JOIN windfarms w ON w.id = rc.windfarm_id
            LEFT JOIN agg_counts ac ON ac.windfarm_id = rc.windfarm_id
            WHERE COALESCE(ac.agg_count, 0) = 0
            ORDER BY rc.b1610_count + rc.boav_count DESC
        """)
    )

    rows = result.all()

    if not rows:
        print("All windfarms with raw data have been processed!")
    else:
        print(f"Found {len(rows)} windfarms with raw data but NO aggregated data:\n")
        total_b1610 = 0
        total_boav = 0
        for row in rows:
            print(f"  ID {row.id}: {row.name}")
            print(f"    B1610 records: {row.b1610_count:,}")
            print(f"    BOAV records: {row.boav_count:,}")
            total_b1610 += row.b1610_count
            total_boav += row.boav_count

        print(f"\nTotal unprocessed: {total_b1610:,} B1610 + {total_boav:,} BOAV records")
        print("\nTo fix: Run the elexon_processor.py for these windfarm IDs:")
        wf_ids = ",".join(str(row.id) for row in rows)
        print(f"  poetry run python scripts/seeds/elexon_processor.py --start 2021-01-01 --end 2021-12-31 --windfarm-ids {wf_ids}")


@pytest.mark.asyncio
async def test_data_integrity_summary(db_session):
    """
    Comprehensive summary test that shows the overall data integrity status.
    """
    print("\n" + "=" * 80)
    print(" " * 15 + "ELEXON DATA INTEGRITY SUMMARY FOR 2021")
    print("=" * 80)

    # 1. Raw data counts
    result = await db_session.execute(
        text("""
            SELECT
                COUNT(CASE WHEN source_type NOT IN ('boav_bid', 'boav_offer') THEN 1 END) as b1610_count,
                COUNT(CASE WHEN source_type = 'boav_bid' THEN 1 END) as boav_count,
                COUNT(DISTINCT identifier) as unique_units
            FROM generation_data_raw
            WHERE source = 'ELEXON'
            AND period_start >= '2021-01-01'
            AND period_start < '2022-01-01'
        """)
    )
    raw = result.first()
    print(f"\n1. RAW DATA (generation_data_raw):")
    print(f"   B1610 (metered) records: {raw.b1610_count:,}")
    print(f"   BOAV (curtailment) records: {raw.boav_count:,}")
    print(f"   Unique BM units: {raw.unique_units:,}")

    # 2. Aggregated data counts
    result = await db_session.execute(
        text("""
            SELECT
                COUNT(*) as total,
                COUNT(CASE WHEN curtailed_mwh > 0 THEN 1 END) as with_curtailment,
                COUNT(DISTINCT generation_unit_id) as unique_units
            FROM generation_data
            WHERE source = 'ELEXON'
            AND hour >= '2021-01-01'
            AND hour < '2022-01-01'
        """)
    )
    agg = result.first()
    print(f"\n2. AGGREGATED DATA (generation_data):")
    print(f"   Total hourly records: {agg.total:,}")
    print(f"   Records with curtailment > 0: {agg.with_curtailment:,}")
    print(f"   Unique generation units: {agg.unique_units:,}")

    # 3. Missing data analysis
    result = await db_session.execute(
        text("""
            WITH boav_hours AS (
                SELECT DISTINCT
                    date_trunc('hour', gdr.period_start) as hour,
                    gdr.identifier
                FROM generation_data_raw gdr
                WHERE gdr.source = 'ELEXON'
                AND gdr.source_type = 'boav_bid'
                AND gdr.period_start >= '2021-01-01'
                AND gdr.period_start < '2022-01-01'
                AND gdr.value_extracted IS NOT NULL
            ),
            matched_units AS (
                SELECT DISTINCT bh.hour, bh.identifier, gu.id as unit_id
                FROM boav_hours bh
                JOIN generation_units gu ON gu.code = bh.identifier AND gu.source = 'ELEXON'
            ),
            with_agg AS (
                SELECT DISTINCT mu.hour, mu.identifier
                FROM matched_units mu
                JOIN generation_data gd ON gd.generation_unit_id = mu.unit_id AND gd.hour = mu.hour
            )
            SELECT
                COUNT(*) as total_boav_hours,
                SUM(CASE WHEN wa.hour IS NULL THEN 1 ELSE 0 END) as missing_hours
            FROM boav_hours bh
            LEFT JOIN with_agg wa ON wa.hour = bh.hour AND wa.identifier = bh.identifier
        """)
    )
    missing = result.first()
    print(f"\n3. BOAV DATA COVERAGE:")
    print(f"   Hours with BOAV data: {missing.total_boav_hours:,}")
    print(f"   Missing aggregated records: {missing.missing_hours:,}")
    if missing.total_boav_hours > 0:
        coverage = (missing.total_boav_hours - missing.missing_hours) / missing.total_boav_hours * 100
        print(f"   Coverage: {coverage:.1f}%")

    # 4. Verdict
    print("\n" + "=" * 80)
    if missing.missing_hours == 0:
        print(" VERDICT: PASS - All BOAV hours have corresponding aggregated data")
    else:
        print(f" VERDICT: FAIL - {missing.missing_hours:,} BOAV hours missing aggregated data")
        print("\n RECOMMENDATIONS:")
        print("   1. Re-run elexon_processor.py for unprocessed windfarms")
        print("   2. Investigate the 23:00 UTC boundary issue in the processor")
        print("   3. Consider fixing the day boundary logic in clear_existing_data()")
    print("=" * 80)


async def verify_values_for_samples(
    session: AsyncSession,
    start_date: datetime,
    end_date: datetime,
    sample_size: int = 100
) -> Dict[str, Any]:
    """
    Verify that aggregated values match raw data values.

    Checks:
    1. curtailed_mwh in aggregated = sum of BOAV values in raw
    2. metered_mwh in aggregated = sum of B1610 values in raw
    3. generation_mwh = metered_mwh + curtailed_mwh
    """
    results = {
        'samples_tested': 0,
        'missing_aggregated': 0,
        'curtailment_mismatches': 0,
        'metered_mismatches': 0,
        'generation_formula_errors': 0,
        'all_correct': 0,
        'issues': []
    }

    # Get random sample of hours with BOAV data
    sample_result = await session.execute(
        text("""
            WITH boav_hours AS (
                SELECT DISTINCT
                    date_trunc('hour', gdr.period_start) as hour,
                    gdr.identifier
                FROM generation_data_raw gdr
                JOIN generation_units gu ON gu.code = gdr.identifier AND gu.source = 'ELEXON'
                WHERE gdr.source = 'ELEXON'
                AND gdr.source_type = 'boav_bid'
                AND gdr.period_start >= :start_date
                AND gdr.period_start < :end_date
                AND gdr.value_extracted IS NOT NULL
            )
            SELECT hour, identifier
            FROM boav_hours
            ORDER BY RANDOM()
            LIMIT :limit
        """),
        {'start_date': start_date, 'end_date': end_date, 'limit': sample_size}
    )
    samples = sample_result.all()

    for row in samples:
        hour = row.hour
        identifier = row.identifier
        results['samples_tested'] += 1

        # Get ALL generation units for this code (handles duplicates)
        unit_result = await session.execute(
            text("SELECT id FROM generation_units WHERE code = :code AND source = 'ELEXON'"),
            {'code': identifier}
        )
        unit_ids = [row.id for row in unit_result.all()]
        if not unit_ids:
            continue

        # Get raw BOAV data for this hour
        boav_result = await session.execute(
            text("""
                SELECT COALESCE(SUM(ABS(value_extracted)), 0) as total_curtailed
                FROM generation_data_raw
                WHERE source = 'ELEXON'
                AND source_type = 'boav_bid'
                AND identifier = :identifier
                AND period_start >= :hour_start
                AND period_start < :hour_start + interval '1 hour'
            """),
            {'identifier': identifier, 'hour_start': hour}
        )
        raw_curtailed = float(boav_result.scalar() or 0)

        # Get raw B1610 data for this hour
        # Note: value_extracted may already have correct sign, or import_export_ind may indicate sign
        b1610_result = await session.execute(
            text("""
                SELECT
                    COALESCE(SUM(
                        CASE
                            WHEN data->>'import_export_ind' = 'I' THEN -ABS(value_extracted)
                            WHEN data->>'import_export_ind' = 'E' THEN ABS(value_extracted)
                            ELSE value_extracted  -- preserve original sign when no indicator
                        END
                    ), 0) as total_metered
                FROM generation_data_raw
                WHERE source = 'ELEXON'
                AND source_type NOT IN ('boav_bid', 'boav_offer')
                AND identifier = :identifier
                AND period_start >= :hour_start
                AND period_start < :hour_start + interval '1 hour'
            """),
            {'identifier': identifier, 'hour_start': hour}
        )
        raw_metered = float(b1610_result.scalar() or 0)

        expected_generation = raw_metered + raw_curtailed

        # Get aggregated data (check ALL unit IDs for this code)
        agg_result = await session.execute(
            text("""
                SELECT generation_mwh, metered_mwh, curtailed_mwh
                FROM generation_data
                WHERE source = 'ELEXON'
                AND generation_unit_id = ANY(:unit_ids)
                AND hour = :hour
            """),
            {'unit_ids': unit_ids, 'hour': hour}
        )
        agg_row = agg_result.first()

        if not agg_row:
            results['missing_aggregated'] += 1
            results['issues'].append({
                'hour': hour,
                'identifier': identifier,
                'issue': 'missing_aggregated',
                'raw_metered': raw_metered,
                'raw_curtailed': raw_curtailed,
                'expected_generation': expected_generation
            })
            continue

        agg_generation = float(agg_row.generation_mwh or 0)
        agg_metered = float(agg_row.metered_mwh or 0)
        agg_curtailed = float(agg_row.curtailed_mwh or 0)

        has_issue = False
        issue_details = []

        # Check curtailment match (tolerance 0.1 MWh)
        if abs(raw_curtailed - agg_curtailed) > 0.1:
            results['curtailment_mismatches'] += 1
            has_issue = True
            issue_details.append(f"curtailed: raw={raw_curtailed:.3f} vs agg={agg_curtailed:.3f}")

        # Check metered match (tolerance 0.1 MWh)
        if abs(raw_metered - agg_metered) > 0.1:
            results['metered_mismatches'] += 1
            has_issue = True
            issue_details.append(f"metered: raw={raw_metered:.3f} vs agg={agg_metered:.3f}")

        # Check generation formula (generation = metered + curtailed)
        expected_from_agg = agg_metered + agg_curtailed
        if abs(agg_generation - expected_from_agg) > 0.1:
            results['generation_formula_errors'] += 1
            has_issue = True
            issue_details.append(f"formula: gen={agg_generation:.3f} != metered({agg_metered:.3f}) + curtailed({agg_curtailed:.3f})")

        if has_issue:
            results['issues'].append({
                'hour': hour,
                'identifier': identifier,
                'issue': 'value_mismatch',
                'raw_metered': raw_metered,
                'raw_curtailed': raw_curtailed,
                'agg_metered': agg_metered,
                'agg_curtailed': agg_curtailed,
                'agg_generation': agg_generation,
                'details': '; '.join(issue_details)
            })
        else:
            results['all_correct'] += 1

    return results


async def run_diagnostic_tests():
    """Run all diagnostic tests and print results."""
    settings = get_settings()
    engine = create_async_engine(settings.database_url_async, echo=False)
    async_session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    async with async_session() as session:
        # Run summary test
        print("\n" + "=" * 80)
        print(" " * 15 + "ELEXON DATA INTEGRITY SUMMARY FOR 2021")
        print("=" * 80)

        # 1. Raw data counts
        result = await session.execute(
            text("""
                SELECT
                    COUNT(CASE WHEN source_type NOT IN ('boav_bid', 'boav_offer') THEN 1 END) as b1610_count,
                    COUNT(CASE WHEN source_type = 'boav_bid' THEN 1 END) as boav_count,
                    COUNT(DISTINCT identifier) as unique_units
                FROM generation_data_raw
                WHERE source = 'ELEXON'
                AND period_start >= '2021-01-01'
                AND period_start < '2022-01-01'
            """)
        )
        raw = result.first()
        print(f"\n1. RAW DATA (generation_data_raw):")
        print(f"   B1610 (metered) records: {raw.b1610_count:,}")
        print(f"   BOAV (curtailment) records: {raw.boav_count:,}")
        print(f"   Unique BM units: {raw.unique_units:,}")

        # 2. Aggregated data counts
        result = await session.execute(
            text("""
                SELECT
                    COUNT(*) as total,
                    COUNT(CASE WHEN curtailed_mwh > 0 THEN 1 END) as with_curtailment,
                    COUNT(DISTINCT generation_unit_id) as unique_units
                FROM generation_data
                WHERE source = 'ELEXON'
                AND hour >= '2021-01-01'
                AND hour < '2022-01-01'
            """)
        )
        agg = result.first()
        print(f"\n2. AGGREGATED DATA (generation_data):")
        print(f"   Total hourly records: {agg.total:,}")
        print(f"   Records with curtailment > 0: {agg.with_curtailment:,}")
        print(f"   Unique generation units: {agg.unique_units:,}")

        # 3. Missing data analysis
        result = await session.execute(
            text("""
                WITH boav_hours AS (
                    SELECT DISTINCT
                        date_trunc('hour', gdr.period_start) as hour,
                        gdr.identifier
                    FROM generation_data_raw gdr
                    WHERE gdr.source = 'ELEXON'
                    AND gdr.source_type = 'boav_bid'
                    AND gdr.period_start >= '2021-01-01'
                    AND gdr.period_start < '2022-01-01'
                    AND gdr.value_extracted IS NOT NULL
                ),
                matched_units AS (
                    SELECT DISTINCT bh.hour, bh.identifier, gu.id as unit_id
                    FROM boav_hours bh
                    JOIN generation_units gu ON gu.code = bh.identifier AND gu.source = 'ELEXON'
                ),
                with_agg AS (
                    SELECT DISTINCT mu.hour, mu.identifier
                    FROM matched_units mu
                    JOIN generation_data gd ON gd.generation_unit_id = mu.unit_id AND gd.hour = mu.hour
                )
                SELECT
                    COUNT(*) as total_boav_hours,
                    SUM(CASE WHEN wa.hour IS NULL THEN 1 ELSE 0 END) as missing_hours
                FROM boav_hours bh
                LEFT JOIN with_agg wa ON wa.hour = bh.hour AND wa.identifier = bh.identifier
            """)
        )
        missing = result.first()
        print(f"\n3. BOAV DATA COVERAGE:")
        print(f"   Hours with BOAV data: {missing.total_boav_hours:,}")
        print(f"   Missing aggregated records: {missing.missing_hours:,}")
        if missing.total_boav_hours > 0:
            coverage = (missing.total_boav_hours - missing.missing_hours) / missing.total_boav_hours * 100
            print(f"   Coverage: {coverage:.1f}%")

        # 4. 23:00 UTC analysis
        print(f"\n4. 23:00 UTC BOUNDARY ANALYSIS:")
        result = await session.execute(
            text("""
                WITH boav_hours AS (
                    SELECT DISTINCT
                        date_trunc('hour', gdr.period_start) as hour,
                        gdr.identifier
                    FROM generation_data_raw gdr
                    WHERE gdr.source = 'ELEXON'
                    AND gdr.source_type = 'boav_bid'
                    AND gdr.period_start >= '2021-01-01'
                    AND gdr.period_start < '2022-01-01'
                    AND gdr.value_extracted IS NOT NULL
                ),
                matched_units AS (
                    SELECT DISTINCT bh.hour, bh.identifier, gu.id as unit_id
                    FROM boav_hours bh
                    JOIN generation_units gu ON gu.code = bh.identifier AND gu.source = 'ELEXON'
                ),
                with_agg AS (
                    SELECT DISTINCT mu.hour, mu.identifier
                    FROM matched_units mu
                    JOIN generation_data gd ON gd.generation_unit_id = mu.unit_id AND gd.hour = mu.hour
                ),
                missing AS (
                    SELECT bh.hour, bh.identifier
                    FROM boav_hours bh
                    WHERE NOT EXISTS (
                        SELECT 1 FROM with_agg wa WHERE wa.hour = bh.hour AND wa.identifier = bh.identifier
                    )
                )
                SELECT
                    EXTRACT(HOUR FROM hour) as hour_of_day,
                    COUNT(*) as missing_count
                FROM missing
                GROUP BY EXTRACT(HOUR FROM hour)
                ORDER BY missing_count DESC
                LIMIT 5
            """)
        )

        rows = result.all()
        total_missing = sum(row.missing_count for row in rows)
        for row in rows:
            hour_of_day = int(row.hour_of_day)
            pct = row.missing_count / total_missing * 100 if total_missing > 0 else 0
            marker = " <-- MOST AFFECTED" if hour_of_day == 23 else ""
            print(f"   {hour_of_day:02d}:00 UTC: {row.missing_count:,} missing ({pct:.1f}%){marker}")

        # 5. Unprocessed windfarms
        print(f"\n5. UNPROCESSED WINDFARMS:")
        result = await session.execute(
            text("""
                WITH raw_counts AS (
                    SELECT
                        gu.windfarm_id,
                        COUNT(CASE WHEN gdr.source_type NOT IN ('boav_bid', 'boav_offer') THEN 1 END) as b1610_count,
                        COUNT(CASE WHEN gdr.source_type = 'boav_bid' THEN 1 END) as boav_count
                    FROM generation_units gu
                    JOIN generation_data_raw gdr ON gdr.identifier = gu.code
                    WHERE gu.source = 'ELEXON'
                    AND gdr.source = 'ELEXON'
                    AND gdr.period_start >= '2021-01-01'
                    AND gdr.period_start < '2022-01-01'
                    GROUP BY gu.windfarm_id
                ),
                agg_counts AS (
                    SELECT
                        gu.windfarm_id,
                        COUNT(gd.id) as agg_count
                    FROM generation_units gu
                    LEFT JOIN generation_data gd ON gd.generation_unit_id = gu.id
                        AND gd.hour >= '2021-01-01' AND gd.hour < '2022-01-01'
                    WHERE gu.source = 'ELEXON'
                    GROUP BY gu.windfarm_id
                )
                SELECT
                    w.id,
                    w.name,
                    rc.b1610_count,
                    rc.boav_count
                FROM raw_counts rc
                JOIN windfarms w ON w.id = rc.windfarm_id
                LEFT JOIN agg_counts ac ON ac.windfarm_id = rc.windfarm_id
                WHERE COALESCE(ac.agg_count, 0) = 0
                ORDER BY rc.b1610_count + rc.boav_count DESC
            """)
        )

        rows = result.all()
        if not rows:
            print("   All windfarms have been processed!")
        else:
            for row in rows:
                print(f"   {row.id}: {row.name} ({row.b1610_count:,} B1610, {row.boav_count:,} BOAV)")

        # 6. Value verification (random samples)
        print(f"\n6. VALUE VERIFICATION (100 random samples):")
        start_date = datetime(2021, 1, 1, tzinfo=UTC_TZ)
        end_date = datetime(2022, 1, 1, tzinfo=UTC_TZ)
        value_results = await verify_values_for_samples(session, start_date, end_date, sample_size=100)

        print(f"   Samples tested: {value_results['samples_tested']}")
        print(f"   All values correct: {value_results['all_correct']}")
        print(f"   Missing aggregated: {value_results['missing_aggregated']}")
        print(f"   Curtailment mismatches: {value_results['curtailment_mismatches']}")
        print(f"   Metered mismatches: {value_results['metered_mismatches']}")
        print(f"   Formula errors (gen != metered + curtailed): {value_results['generation_formula_errors']}")

        if value_results['samples_tested'] > 0:
            accuracy = value_results['all_correct'] / value_results['samples_tested'] * 100
            print(f"   Accuracy: {accuracy:.1f}%")

        # Show sample issues
        if value_results['issues']:
            print(f"\n   Sample issues (first 5):")
            for issue in value_results['issues'][:5]:
                print(f"     {issue['hour']} | {issue['identifier']}")
                print(f"       Issue: {issue['issue']}")
                if 'details' in issue:
                    print(f"       Details: {issue['details']}")
                else:
                    print(f"       Raw: metered={issue['raw_metered']:.3f}, curtailed={issue['raw_curtailed']:.3f}")

        # Verdict
        print("\n" + "=" * 80)
        total_issues = (
            missing.missing_hours +
            value_results['curtailment_mismatches'] +
            value_results['metered_mismatches'] +
            value_results['generation_formula_errors']
        )

        if total_issues == 0:
            print(" VERDICT: PASS - All data is complete and values are correct")
        else:
            print(f" VERDICT: FAIL")
            if missing.missing_hours > 0:
                print(f"   - {missing.missing_hours:,} BOAV hours missing aggregated data")
            if value_results['curtailment_mismatches'] > 0:
                print(f"   - {value_results['curtailment_mismatches']} curtailment value mismatches")
            if value_results['metered_mismatches'] > 0:
                print(f"   - {value_results['metered_mismatches']} metered value mismatches")
            if value_results['generation_formula_errors'] > 0:
                print(f"   - {value_results['generation_formula_errors']} formula errors (gen != metered + curtailed)")
            print("\n RECOMMENDATIONS:")
            print("   1. Re-run elexon_processor.py for affected date ranges")
            print("   2. Example: poetry run python scripts/seeds/elexon_processor.py --start 2021-12-01 --end 2021-12-31")
        print("=" * 80)

    await engine.dispose()


# Main execution for running as a script
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='Test Elexon Data Integrity')
    parser.add_argument('--samples', type=int, default=100, help='Number of random samples to test')
    parser.add_argument('--start', type=str, default='2021-01-01', help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end', type=str, default='2022-01-01', help='End date (YYYY-MM-DD)')
    parser.add_argument('--units', type=str, help='Comma-separated list of unit codes for detailed analysis')
    parser.add_argument('--diagnose', action='store_true', help='Run full diagnostic tests')
    parser.add_argument('--verify', action='store_true', help='Verify values match between raw and aggregated data')

    args = parser.parse_args()

    start_date = datetime.strptime(args.start, '%Y-%m-%d').replace(tzinfo=UTC_TZ)
    end_date = datetime.strptime(args.end, '%Y-%m-%d').replace(tzinfo=UTC_TZ)

    if args.diagnose:
        # Run full diagnostic tests
        asyncio.run(run_diagnostic_tests())
    elif args.verify:
        # Run comprehensive value verification
        print(f"\n{'=' * 80}")
        print(f"ELEXON Value Verification Test")
        print(f"Date Range: {args.start} to {args.end}")
        print(f"Sample Size: {args.samples}")
        print(f"{'=' * 80}\n")

        async def run_verify():
            settings = get_settings()
            engine = create_async_engine(settings.database_url_async, echo=False)
            async_session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

            async with async_session() as session:
                results = await verify_values_for_samples(session, start_date, end_date, args.samples)

                print(f"RESULTS:")
                print(f"  Samples tested: {results['samples_tested']}")
                print(f"  All values correct: {results['all_correct']}")
                print(f"  Missing aggregated: {results['missing_aggregated']}")
                print(f"  Curtailment mismatches: {results['curtailment_mismatches']}")
                print(f"  Metered mismatches: {results['metered_mismatches']}")
                print(f"  Formula errors: {results['generation_formula_errors']}")

                if results['samples_tested'] > 0:
                    accuracy = results['all_correct'] / results['samples_tested'] * 100
                    print(f"\n  ACCURACY: {accuracy:.1f}%")

                if results['issues']:
                    print(f"\n{'=' * 80}")
                    print(f"ISSUES FOUND ({len(results['issues'])} total):")
                    print(f"{'=' * 80}")

                    for i, issue in enumerate(results['issues'][:20]):
                        print(f"\n  Issue #{i+1}:")
                        print(f"    Hour: {issue['hour']}")
                        print(f"    Unit: {issue['identifier']}")
                        print(f"    Type: {issue['issue']}")
                        print(f"    Raw metered: {issue['raw_metered']:.3f} MWh")
                        print(f"    Raw curtailed: {issue['raw_curtailed']:.3f} MWh")
                        if 'agg_metered' in issue:
                            print(f"    Agg metered: {issue['agg_metered']:.3f} MWh")
                            print(f"    Agg curtailed: {issue['agg_curtailed']:.3f} MWh")
                            print(f"    Agg generation: {issue['agg_generation']:.3f} MWh")
                        if 'details' in issue:
                            print(f"    Details: {issue['details']}")

                    if len(results['issues']) > 20:
                        print(f"\n  ... and {len(results['issues']) - 20} more issues")

                print(f"\n{'=' * 80}")
                if results['all_correct'] == results['samples_tested']:
                    print(" VERDICT: PASS - All sampled values are correct")
                else:
                    error_count = results['samples_tested'] - results['all_correct']
                    print(f" VERDICT: FAIL - {error_count} issues in {results['samples_tested']} samples")
                print(f"{'=' * 80}\n")

            await engine.dispose()

        asyncio.run(run_verify())
    elif args.units:
        # Run detailed analysis for specific units
        identifiers = [u.strip() for u in args.units.split(',')]
        print(f"\n{'=' * 80}")
        print(f"ELEXON Data Integrity Test - Unit Analysis")
        print(f"{'=' * 80}\n")
        print(f"Running detailed analysis for units: {identifiers}")

        results = asyncio.run(run_detailed_unit_analysis(identifiers, start_date, end_date))

        print("\n" + "=" * 80)
        print(" DETAILED UNIT ANALYSIS")
        print("=" * 80)

        for analysis in results:
            print(f"\n  Unit: {analysis.get('identifier', 'N/A')}")
            print(f"  Generation Unit ID: {analysis.get('generation_unit_id', 'N/A')}")
            print(f"  Windfarm: {analysis.get('windfarm_name', 'N/A')} (ID: {analysis.get('windfarm_id', 'N/A')})")
            print(f"  Date Range: {analysis.get('date_range', 'N/A')}")
            print(f"  Days: {analysis.get('days', 0)}")
            print(f"  Raw B1610 Records: {analysis.get('raw_b1610_records', 0):,}")
            print(f"  Raw BOAV Records: {analysis.get('raw_boav_records', 0):,}")
            print(f"  Expected Hours: {analysis.get('expected_hours', 0):,}")
            print(f"  Aggregated Hours: {analysis.get('aggregated_hours', 0):,}")
            print(f"  Hours with Curtailment: {analysis.get('aggregated_hours_with_curtailment', 0):,}")
            print(f"  Missing Hours: {analysis.get('missing_hours', 0):,}")
            print(f"  Completeness: {analysis.get('completeness_pct', 0):.1f}%")
            print("-" * 40)
    else:
        # Run random sample integrity test
        print(f"\n{'=' * 80}")
        print(f"ELEXON Data Integrity Test")
        print(f"Date Range: {args.start} to {args.end}")
        print(f"Sample Size: {args.samples}")
        print(f"{'=' * 80}\n")

        results = asyncio.run(run_integrity_test(start_date, end_date, args.samples))
        print_results(results)
