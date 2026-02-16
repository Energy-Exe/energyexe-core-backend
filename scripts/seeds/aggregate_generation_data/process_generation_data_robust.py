"""
Robust Generation Data Processor

Processes generation data day-by-day or month-by-month for any date range with comprehensive
error handling, progress tracking, and detailed logging to JSON files.

Features:
- Processes each day/month independently (failure of one doesn't stop others)
- Monthly mode: Much faster for large datasets (NVE: ~30x faster)
- Saves detailed logs to JSON file
- Resume capability from last successful day/month
- Memory efficient (processes one day/month at a time)
- Progress tracking with ETA

Usage:
    # Process any date range (daily mode - default)
    poetry run python scripts/process_generation_data_robust.py --start 2020-01-01 --end 2024-12-31

    # Process month-by-month (much faster for NVE and large datasets)
    poetry run python scripts/process_generation_data_robust.py --start 2002-01-01 --end 2024-12-31 --source NVE --monthly

    # Resume from last checkpoint
    poetry run python scripts/process_generation_data_robust.py --start 2020-01-01 --end 2024-12-31 --resume

    # Process specific source
    poetry run python scripts/process_generation_data_robust.py --start 2020-01-01 --end 2024-12-31 --source ENTSOE

    # Dry run
    poetry run python scripts/process_generation_data_robust.py --start 2020-01-01 --end 2024-12-31 --dry-run
"""

import asyncio
import json
import logging
import traceback
from datetime import datetime, timedelta, timezone, date
from pathlib import Path
from typing import List, Dict, Optional, Any
import argparse
import sys
import time
from dataclasses import dataclass, asdict

from sqlalchemy.ext.asyncio import AsyncSession

# Add parent directories to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from app.core.database import get_session_factory

# Import the daily processor
from scripts.seeds.aggregate_generation_data.process_generation_data_daily import DailyGenerationProcessor

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


@dataclass
class DayProcessingResult:
    """Result of processing a single day."""
    date: str
    source: Optional[str]
    status: str  # 'success', 'failed', 'skipped'
    raw_records: int = 0
    hourly_records: int = 0
    processing_time_seconds: float = 0
    error: Optional[str] = None
    error_traceback: Optional[str] = None
    timestamp: str = ""

    def to_dict(self):
        return asdict(self)


class RobustGenerationProcessor:
    """Robust processor that handles any date range with day-by-day processing."""

    def __init__(
        self,
        source: Optional[str] = None,
        dry_run: bool = False,
        log_dir: str = "generation_processing_logs",
        monthly: bool = False
    ):
        self.source = source
        self.dry_run = dry_run
        self.log_dir = Path(log_dir)
        try:
            self.log_dir.mkdir(parents=True, exist_ok=True)
        except OSError:
            # Fallback to /tmp if configured path isn't writable (e.g. Docker)
            self.log_dir = Path("/tmp") / log_dir
            self.log_dir.mkdir(parents=True, exist_ok=True)
        self.monthly = monthly

        # Initialize results tracking
        self.results: List[DayProcessingResult] = []
        self.checkpoint_file = None
        self.log_file = None

        # Statistics
        self.total_days = 0
        self.total_months = 0
        self.processed_days = 0
        self.processed_months = 0
        self.failed_days = 0
        self.failed_months = 0
        self.total_raw_records = 0
        self.total_hourly_records = 0
        self.start_time = None

    def initialize_logging(self, start_date: date, end_date: date):
        """Initialize log files for this processing run."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        # Create log filename
        source_suffix = f"_{self.source}" if self.source else "_all"
        date_range = f"{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}"

        self.log_file = self.log_dir / f"process_{date_range}{source_suffix}_{timestamp}.json"
        self.checkpoint_file = self.log_dir / f"checkpoint_{date_range}{source_suffix}.json"

        logger.info(f"Log file: {self.log_file}")
        logger.info(f"Checkpoint file: {self.checkpoint_file}")

    def load_checkpoint(self) -> Optional[date]:
        """Load last successful date from checkpoint file."""
        if self.checkpoint_file and self.checkpoint_file.exists():
            try:
                with open(self.checkpoint_file, 'r') as f:
                    checkpoint = json.load(f)
                    last_date_str = checkpoint.get('last_successful_date')
                    if last_date_str:
                        last_date = datetime.strptime(last_date_str, '%Y-%m-%d').date()
                        logger.info(f"Resuming from checkpoint: {last_date}")
                        return last_date
            except Exception as e:
                logger.warning(f"Could not load checkpoint: {e}")
        return None

    def save_checkpoint(self, last_successful_date: date):
        """Save checkpoint with last successful date."""
        if self.checkpoint_file:
            checkpoint_data = {
                'last_successful_date': last_successful_date.strftime('%Y-%m-%d'),
                'timestamp': datetime.now().isoformat(),
                'source': self.source,
                'processed_days': self.processed_days,
                'failed_days': self.failed_days
            }

            with open(self.checkpoint_file, 'w') as f:
                json.dump(checkpoint_data, f, indent=2)

    def save_results(self):
        """Save all results to JSON log file."""
        if self.log_file:
            log_data = {
                'summary': {
                    'start_time': self.start_time.isoformat() if self.start_time else None,
                    'end_time': datetime.now().isoformat(),
                    'total_days': self.total_days,
                    'processed_days': self.processed_days,
                    'failed_days': self.failed_days,
                    'skipped_days': self.total_days - self.processed_days - self.failed_days,
                    'total_raw_records': self.total_raw_records,
                    'total_hourly_records': self.total_hourly_records,
                    'source': self.source or 'ALL',
                    'dry_run': self.dry_run
                },
                'daily_results': [r.to_dict() for r in self.results]
            }

            with open(self.log_file, 'w') as f:
                json.dump(log_data, f, indent=2)

            logger.info(f"Results saved to {self.log_file}")

    async def process_date_range(
        self,
        start_date: date,
        end_date: date,
        resume: bool = False
    ) -> Dict[str, Any]:
        """Process a date range day by day or month by month."""

        self.start_time = datetime.now()

        # Initialize logging
        self.initialize_logging(start_date, end_date)

        # Check for resume
        if resume:
            last_successful = self.load_checkpoint()
            if last_successful and last_successful >= start_date:
                start_date = last_successful + timedelta(days=1)
                logger.info(f"Resuming from {start_date}")

        # Calculate total periods
        self.total_days = (end_date - start_date).days + 1

        if self.monthly:
            # Calculate total months
            self.total_months = (end_date.year - start_date.year) * 12 + end_date.month - start_date.month + 1
            logger.info(f"Processing {self.total_months} months from {start_date} to {end_date} (MONTHLY MODE)")
        else:
            logger.info(f"Processing {self.total_days} days from {start_date} to {end_date}")

        # Get database session factory
        session_factory = get_session_factory()

        # Process by month or by day
        if self.monthly:
            await self.process_monthly(session_factory, start_date, end_date)
        else:
            await self.process_daily(session_factory, start_date, end_date)

        # Final save
        self.save_results()

        # Print final summary
        self.print_summary()

        return self.get_summary()

    async def process_daily(self, session_factory, start_date: date, end_date: date):
        """Process date range day by day."""
        current_date = start_date

        while current_date <= end_date:
            day_result = await self.process_single_day(session_factory, current_date)
            self.results.append(day_result)

            # Update statistics
            if day_result.status == 'success':
                self.processed_days += 1
                self.total_raw_records += day_result.raw_records
                self.total_hourly_records += day_result.hourly_records

                # Save checkpoint after each successful day
                self.save_checkpoint(current_date)
            elif day_result.status == 'failed':
                self.failed_days += 1

            # Progress report
            days_done = self.processed_days + self.failed_days
            if days_done % 10 == 0 or days_done == self.total_days:
                self.print_progress(days_done)

            # Save results periodically (every 30 days)
            if len(self.results) % 30 == 0:
                self.save_results()

            # Move to next day
            current_date += timedelta(days=1)

    async def process_monthly(self, session_factory, start_date: date, end_date: date):
        """Process date range month by month."""
        # Start from the first day of the start month
        current_month_start = start_date.replace(day=1)

        while current_month_start <= end_date:
            # Calculate month end date
            if current_month_start.month == 12:
                next_month = current_month_start.replace(year=current_month_start.year + 1, month=1)
            else:
                next_month = current_month_start.replace(month=current_month_start.month + 1)

            month_end = next_month - timedelta(days=1)

            # Don't process beyond end_date
            if month_end > end_date:
                month_end = end_date

            # Also don't start before start_date
            month_start_actual = max(current_month_start, start_date)

            # Process this month
            month_result = await self.process_single_month(session_factory, month_start_actual, month_end)
            self.results.append(month_result)

            # Update statistics
            if month_result.status == 'success':
                self.processed_months += 1
                self.total_raw_records += month_result.raw_records
                self.total_hourly_records += month_result.hourly_records

                # Save checkpoint after each successful month
                self.save_checkpoint(month_end)
            elif month_result.status == 'failed':
                self.failed_months += 1

            # Progress report
            months_done = self.processed_months + self.failed_months
            if months_done % 1 == 0 or months_done == self.total_months:
                self.print_monthly_progress(months_done)

            # Save results periodically (every 6 months)
            if len(self.results) % 6 == 0:
                self.save_results()

            # Move to next month
            current_month_start = next_month

    async def process_single_month(
        self,
        session_factory,
        month_start: date,
        month_end: date
    ) -> DayProcessingResult:
        """Process an entire month at once."""

        start_time = time.time()

        month_label = f"{month_start.strftime('%Y-%m')} ({month_start} to {month_end})"
        logger.info(f"Processing month {month_label}...")

        result = DayProcessingResult(
            date=month_start.strftime('%Y-%m'),  # Use YYYY-MM format for months
            source=self.source,
            status='failed',
            timestamp=datetime.now().isoformat()
        )

        try:
            # Create a new session for this month
            async with session_factory() as db:
                processor = DailyGenerationProcessor(db, self.dry_run)

                # Load generation units ONCE for the entire month
                await processor.load_generation_units()
                logger.info(f"Loaded generation units cache for month processing")

                # Process each day in the month
                total_raw = 0
                total_hourly = 0
                days_processed = 0

                current_date = month_start
                while current_date <= month_end:
                    # Process the day (skip loading units and committing - batch mode)
                    day_start = datetime.combine(current_date, datetime.min.time(), tzinfo=timezone.utc)
                    sources = [self.source] if self.source else None

                    # Batch mode: skip unit loading, skip per-day commits
                    day_result = await processor.process_day(
                        day_start,
                        sources,
                        skip_load_units=True,
                        skip_commit=True
                    )

                    # Extract statistics
                    for source_key, source_result in day_result.get('sources', {}).items():
                        if 'error' not in source_result:
                            total_raw += source_result.get('raw_records', 0)
                            total_hourly += source_result.get('saved', 0)

                    days_processed += 1
                    current_date += timedelta(days=1)

                # Commit once for the entire month
                if not self.dry_run:
                    await db.commit()
                    logger.info(f"✓ {month_label}: {total_raw} raw → {total_hourly} hourly records ({days_processed} days) - committed")
                else:
                    await db.rollback()
                    logger.info(f"✓ {month_label}: {total_raw} raw → {total_hourly} hourly records ({days_processed} days) - dry run")

                result.raw_records = total_raw
                result.hourly_records = total_hourly
                result.status = 'success'

        except Exception as e:
            # Capture error details
            result.status = 'failed'
            result.error = str(e)
            result.error_traceback = traceback.format_exc()

            logger.error(f"✗ {month_label}: {e}")
            logger.debug(f"Traceback: {result.error_traceback}")

        finally:
            # Record processing time
            result.processing_time_seconds = round(time.time() - start_time, 2)

        return result

    async def process_single_day(
        self,
        session_factory,
        process_date: date
    ) -> DayProcessingResult:
        """Process a single day with error handling."""

        start_time = time.time()

        logger.info(f"Processing {process_date}...")

        result = DayProcessingResult(
            date=process_date.strftime('%Y-%m-%d'),
            source=self.source,
            status='failed',
            timestamp=datetime.now().isoformat()
        )

        try:
            # Create a new session for this day
            async with session_factory() as db:
                processor = DailyGenerationProcessor(db, self.dry_run)

                # Process the day
                day_start = datetime.combine(process_date, datetime.min.time(), tzinfo=timezone.utc)
                sources = [self.source] if self.source else None

                day_result = await processor.process_day(day_start, sources)

                # Extract statistics
                total_raw = 0
                total_hourly = 0

                for source_key, source_result in day_result.get('sources', {}).items():
                    if 'error' not in source_result:
                        total_raw += source_result.get('raw_records', 0)
                        total_hourly += source_result.get('saved', 0)

                result.raw_records = total_raw
                result.hourly_records = total_hourly
                result.status = 'success'

                logger.info(f"✓ {process_date}: {total_raw} raw → {total_hourly} hourly records")

        except Exception as e:
            # Capture error details
            result.status = 'failed'
            result.error = str(e)
            result.error_traceback = traceback.format_exc()

            logger.error(f"✗ {process_date}: {e}")
            logger.debug(f"Traceback: {result.error_traceback}")

        finally:
            # Record processing time
            result.processing_time_seconds = round(time.time() - start_time, 2)

        return result

    def print_monthly_progress(self, months_done: int):
        """Print progress report for monthly processing with ETA."""

        if self.start_time:
            elapsed = (datetime.now() - self.start_time).total_seconds()
            months_per_second = months_done / elapsed if elapsed > 0 else 0

            remaining_months = self.total_months - months_done
            eta_seconds = remaining_months / months_per_second if months_per_second > 0 else 0
            eta_minutes = eta_seconds / 60

            progress_pct = (months_done / self.total_months * 100) if self.total_months > 0 else 0

            logger.info(
                f"Progress: {months_done}/{self.total_months} months ({progress_pct:.1f}%) | "
                f"ETA: {eta_minutes:.1f} minutes | "
                f"Records: {self.total_hourly_records:,}"
            )

    def print_progress(self, days_done: int):
        """Print progress report with ETA."""

        if self.start_time:
            elapsed = (datetime.now() - self.start_time).total_seconds()
            days_per_second = days_done / elapsed if elapsed > 0 else 0
            remaining_days = self.total_days - days_done
            eta_seconds = remaining_days / days_per_second if days_per_second > 0 else 0

            eta_str = str(timedelta(seconds=int(eta_seconds)))

            logger.info(
                f"Progress: {days_done}/{self.total_days} days "
                f"({days_done*100/self.total_days:.1f}%) - "
                f"Success: {self.processed_days}, Failed: {self.failed_days} - "
                f"ETA: {eta_str}"
            )

    def print_summary(self):
        """Print final processing summary."""

        duration = datetime.now() - self.start_time if self.start_time else timedelta(0)

        print("\n" + "=" * 60)
        print("PROCESSING COMPLETE")
        print("=" * 60)
        print(f"Date range:          {self.results[0].date if self.results else 'N/A'} to "
              f"{self.results[-1].date if self.results else 'N/A'}")
        print(f"Source:              {self.source or 'ALL'}")
        print(f"Mode:                {'MONTHLY' if self.monthly else 'DAILY'}")

        if self.monthly:
            print(f"Total months:        {self.total_months}")
            print(f"Successful months:   {self.processed_months}")
            print(f"Failed months:       {self.failed_months}")
        else:
            print(f"Total days:          {self.total_days}")
            print(f"Successful days:     {self.processed_days}")
            print(f"Failed days:         {self.failed_days}")
            print(f"Skipped days:        {self.total_days - self.processed_days - self.failed_days}")

        print("-" * 60)
        print(f"Total raw records:   {self.total_raw_records:,}")
        print(f"Total hourly records: {self.total_hourly_records:,}")
        print(f"Processing time:     {duration}")

        if self.monthly and self.total_months > 0:
            print(f"Average per month:   {duration.total_seconds()/self.total_months:.1f} seconds")
        elif not self.monthly and self.total_days > 0:
            print(f"Average per day:     {duration.total_seconds()/self.total_days:.1f} seconds")

        failed_count = self.failed_months if self.monthly else self.failed_days
        if failed_count > 0:
            period_label = "months" if self.monthly else "days"
            print(f"\nFailed {period_label}:")
            for result in self.results:
                if result.status == 'failed':
                    print(f"  - {result.date}: {result.error}")

        print(f"\nDetailed log: {self.log_file}")

        if self.dry_run:
            print("\nDRY RUN - No changes made to database")

        print("=" * 60)

    def get_summary(self) -> Dict[str, Any]:
        """Get summary statistics."""

        return {
            'total_days': self.total_days,
            'processed_days': self.processed_days,
            'failed_days': self.failed_days,
            'total_raw_records': self.total_raw_records,
            'total_hourly_records': self.total_hourly_records,
            'log_file': str(self.log_file),
            'failed_dates': [r.date for r in self.results if r.status == 'failed']
        }


async def analyze_logs(log_file: str):
    """Analyze a log file to show statistics."""

    with open(log_file, 'r') as f:
        data = json.load(f)

    summary = data['summary']
    daily_results = data['daily_results']

    print("\n" + "=" * 60)
    print(f"LOG ANALYSIS: {Path(log_file).name}")
    print("=" * 60)

    # Summary stats
    for key, value in summary.items():
        if key not in ['start_time', 'end_time']:
            print(f"{key:20}: {value}")

    # Failed days details
    failed_days = [r for r in daily_results if r['status'] == 'failed']
    if failed_days:
        print(f"\nFailed days ({len(failed_days)}):")
        for day in failed_days[:10]:
            print(f"  {day['date']}: {day.get('error', 'Unknown error')}")
        if len(failed_days) > 10:
            print(f"  ... and {len(failed_days) - 10} more")

    # Processing time analysis
    successful_days = [r for r in daily_results if r['status'] == 'success']
    if successful_days:
        processing_times = [r['processing_time_seconds'] for r in successful_days]
        avg_time = sum(processing_times) / len(processing_times)
        max_time = max(processing_times)
        min_time = min(processing_times)

        print(f"\nProcessing time per day:")
        print(f"  Average: {avg_time:.1f} seconds")
        print(f"  Min:     {min_time:.1f} seconds")
        print(f"  Max:     {max_time:.1f} seconds")

    # Data volume analysis
    if successful_days:
        total_raw = sum(r['raw_records'] for r in successful_days)
        total_hourly = sum(r['hourly_records'] for r in successful_days)

        print(f"\nData processed:")
        print(f"  Total raw records:    {total_raw:,}")
        print(f"  Total hourly records: {total_hourly:,}")
        print(f"  Compression ratio:    {total_raw/total_hourly:.1f}:1")


async def retry_failed_days(log_file: str, dry_run: bool = False):
    """Retry processing for failed days from a previous run."""

    with open(log_file, 'r') as f:
        data = json.load(f)

    failed_days = [
        datetime.strptime(r['date'], '%Y-%m-%d').date()
        for r in data['daily_results']
        if r['status'] == 'failed'
    ]

    if not failed_days:
        print("No failed days to retry")
        return

    print(f"Found {len(failed_days)} failed days to retry")

    source = data['summary'].get('source')
    if source == 'ALL':
        source = None

    # Process failed days
    processor = RobustGenerationProcessor(source=source, dry_run=dry_run)

    # Process each failed day
    for failed_date in sorted(failed_days):
        await processor.process_date_range(failed_date, failed_date)


async def main():
    """Main entry point."""

    parser = argparse.ArgumentParser(
        description='Robust generation data processor for any date range'
    )

    # Main options
    parser.add_argument(
        '--start',
        type=str,
        help='Start date (YYYY-MM-DD)'
    )
    parser.add_argument(
        '--end',
        type=str,
        help='End date (YYYY-MM-DD)'
    )
    parser.add_argument(
        '--source',
        type=str,
        choices=['ENTSOE', 'ELEXON', 'TAIPOWER', 'NVE', 'ENERGISTYRELSEN'],
        help='Process only specific source'
    )

    # Processing options
    parser.add_argument(
        '--resume',
        action='store_true',
        help='Resume from last checkpoint'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Run without making database changes'
    )
    parser.add_argument(
        '--monthly',
        action='store_true',
        help='Process month-by-month instead of day-by-day (much faster for large datasets)'
    )

    # Utility options
    parser.add_argument(
        '--analyze',
        type=str,
        metavar='LOG_FILE',
        help='Analyze a previous log file'
    )
    parser.add_argument(
        '--retry',
        type=str,
        metavar='LOG_FILE',
        help='Retry failed days from a log file'
    )

    args = parser.parse_args()

    # Handle utility commands
    if args.analyze:
        await analyze_logs(args.analyze)
        return

    if args.retry:
        await retry_failed_days(args.retry, args.dry_run)
        return

    # Validate main processing arguments
    if not args.start or not args.end:
        print("Error: --start and --end dates are required")
        parser.print_help()
        sys.exit(1)

    # Parse dates
    try:
        start_date = datetime.strptime(args.start, '%Y-%m-%d').date()
        end_date = datetime.strptime(args.end, '%Y-%m-%d').date()
    except ValueError as e:
        print(f"Invalid date format: {e}")
        sys.exit(1)

    if start_date > end_date:
        print("Error: Start date must be before or equal to end date")
        sys.exit(1)

    # Process the date range
    processor = RobustGenerationProcessor(
        source=args.source,
        dry_run=args.dry_run,
        monthly=args.monthly
    )

    try:
        await processor.process_date_range(
            start_date,
            end_date,
            resume=args.resume
        )

        # Exit with non-zero code if any days/months failed
        # This ensures the import job status reflects aggregation failures
        if processor.failed_days > 0 or processor.failed_months > 0:
            logger.warning(
                f"Aggregation completed with failures: "
                f"{processor.failed_days} failed days, {processor.failed_months} failed months"
            )
            sys.exit(1)

    except KeyboardInterrupt:
        logger.info("Processing interrupted by user")
        processor.save_results()
        sys.exit(1)
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        processor.save_results()
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())