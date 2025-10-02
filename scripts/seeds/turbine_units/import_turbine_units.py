#!/usr/bin/env python3
"""
Import turbine units from CSV file with optimized performance.

This script:
1. Reads turbine_units.csv
2. Matches windfarms by name
3. Matches turbine models by model name
4. Generates code as {windfarm_code}-{serial_number}
5. Bulk inserts turbine units with start/end dates
"""

import argparse
import sys
import time
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Dict, List, Optional, Set

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session, sessionmaker

# Add parent directory to path
sys.path.append(str(Path(__file__).parent.parent.parent.parent))

from app.core.config import get_settings
from app.models.turbine_model import TurbineModel
from app.models.turbine_unit import TurbineUnit
from app.models.windfarm import Windfarm


def get_sync_session():
    """Get synchronous database session."""
    settings = get_settings()
    engine = create_engine(settings.database_url_sync)
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    return SessionLocal(), engine


def parse_date(date_str):
    """Parse date from various formats."""
    if pd.isna(date_str) or not date_str or str(date_str).strip() == '':
        return None
    if isinstance(date_str, datetime):
        return date_str.date()
    try:
        # Try various date formats
        for fmt in ['%m/%d/%Y', '%Y-%m-%d', '%d/%m/%Y', '%Y/%m/%d', '%d.%m.%Y']:
            try:
                return datetime.strptime(str(date_str).strip(), fmt).date()
            except ValueError:
                continue
    except:
        pass
    return None


def normalize_string(s):
    """Normalize string for comparison."""
    if pd.isna(s) or not s:
        return ''
    # Remove spaces, hyphens, and convert to lowercase for fuzzy matching
    normalized = str(s).strip().lower()
    normalized = normalized.replace(' ', '').replace('-', '').replace('/', '')
    return normalized


def normalize_model_name(s):
    """Normalize turbine model name for case-insensitive matching."""
    if pd.isna(s) or not s:
        return ''
    # Convert to lowercase and remove spaces for flexible matching
    # But keep slashes and hyphens as they're part of model numbers
    normalized = str(s).strip().lower()
    normalized = normalized.replace(' ', '')
    return normalized


def generate_windfarm_code(windfarm_name):
    """Generate windfarm code from name."""
    if pd.isna(windfarm_name) or not windfarm_name:
        return None
    # Remove special characters, replace spaces with underscores, uppercase
    code = str(windfarm_name).strip()
    code = code.replace(' ', '_').replace('-', '_').replace('ø', 'o').replace('å', 'a').replace('æ', 'ae')
    code = code.replace('Ø', 'O').replace('Å', 'A').replace('Æ', 'AE')
    code = ''.join(c for c in code if c.isalnum() or c == '_')
    return code.upper()


class TurbineUnitImporter:
    """Handles turbine unit import with optimized performance."""

    def __init__(self, db: Session, engine):
        self.db = db
        self.engine = engine

        # Lookups
        self.windfarm_by_name = {}  # normalized name -> windfarm object
        self.windfarm_by_code = {}  # code -> windfarm object
        self.turbine_model_by_name = {}  # normalized model name -> turbine_model object
        self.existing_turbine_codes = set()

        # Serial counter per windfarm
        self.windfarm_serial_counter = {}

        # Statistics
        self.stats = {
            'rows_processed': 0,
            'turbine_units_created': 0,
            'turbine_units_skipped': 0,
            'errors': [],
            'windfarms_not_found': set(),
            'models_not_found': set()
        }

    def load_lookups(self):
        """Load all lookup data into memory."""
        print("\nLoading lookup data...")

        # Load all windfarms
        windfarms = self.db.query(Windfarm).all()
        for wf in windfarms:
            normalized_name = normalize_string(wf.name)
            self.windfarm_by_name[normalized_name] = wf
            self.windfarm_by_code[wf.code] = wf
        print(f"  Loaded {len(windfarms)} windfarms")

        # Load all turbine models with case-insensitive normalization
        turbine_models = self.db.query(TurbineModel).all()
        for tm in turbine_models:
            normalized_model = normalize_model_name(tm.model)
            self.turbine_model_by_name[normalized_model] = tm
        print(f"  Loaded {len(turbine_models)} turbine models")

        # Load existing turbine unit codes
        result = self.db.execute(text("SELECT code FROM turbine_units"))
        self.existing_turbine_codes = {row[0] for row in result}
        print(f"  Found {len(self.existing_turbine_codes)} existing turbine units")

    def get_windfarm(self, windfarm_name):
        """
        Get windfarm by name.

        First tries exact match, then falls back to partial match if windfarm name
        appears to be missing a phase/version suffix.
        """
        normalized_name = normalize_string(windfarm_name)

        # Try exact match first
        if normalized_name in self.windfarm_by_name:
            return self.windfarm_by_name[normalized_name]

        # Try partial match - find windfarms that start with this name
        # This handles cases like "Horns Rev" matching "Horns Rev 1", "Horns Rev 2", etc.
        matches = [
            wf for name, wf in self.windfarm_by_name.items()
            if name.startswith(normalized_name)
        ]

        if len(matches) == 1:
            # Exactly one match - use it
            return matches[0]
        elif len(matches) > 1:
            # Multiple matches - prefer the first one (could be improved with better heuristics)
            # For now, return the first alphabetically
            return sorted(matches, key=lambda wf: wf.name)[0]

        return None

    def get_turbine_model(self, model_name):
        """
        Get turbine model by name with fuzzy matching.

        Tries exact match first, then fuzzy matches for common variations.
        """
        normalized_model = normalize_model_name(model_name)

        # Try exact match first
        if normalized_model in self.turbine_model_by_name:
            return self.turbine_model_by_name[normalized_model]

        # Try fuzzy matching for common variations
        # Handle cases like "NTK 150" -> "NTK150/25", "72c/1500" -> "72C/1500"
        for db_model_key, db_model in self.turbine_model_by_name.items():
            # Check if the CSV model is a substring of the DB model (case-insensitive)
            if normalized_model in db_model_key or db_model_key.startswith(normalized_model):
                return db_model

        # Try partial match - check if any stored model contains the input
        # This handles "V1172" -> "V117" typo
        input_digits = ''.join(c for c in normalized_model if c.isdigit())
        if input_digits:
            for db_model_key, db_model in self.turbine_model_by_name.items():
                db_digits = ''.join(c for c in db_model_key if c.isdigit())
                # Check if first significant digits match
                if input_digits[:3] == db_digits[:3]:
                    # Also check that non-digit parts are similar
                    input_letters = ''.join(c for c in normalized_model if c.isalpha())
                    db_letters = ''.join(c for c in db_model_key if c.isalpha())
                    if input_letters and db_letters and input_letters[:2] == db_letters[:2]:
                        return db_model

        return None

    def generate_turbine_code(self, windfarm_code, turbine_id):
        """
        Generate turbine unit code.

        Format: {WINDFARM_CODE}-{SERIAL}
        where SERIAL is a zero-padded 3-digit number per windfarm
        """
        # Get or initialize serial counter for this windfarm
        if windfarm_code not in self.windfarm_serial_counter:
            # Find highest existing serial for this windfarm
            prefix = f"{windfarm_code}-"
            existing_serials = [
                int(code.split('-')[-1])
                for code in self.existing_turbine_codes
                if code.startswith(prefix) and code.split('-')[-1].isdigit()
            ]
            max_serial = max(existing_serials) if existing_serials else 0
            self.windfarm_serial_counter[windfarm_code] = max_serial

        # Increment counter
        self.windfarm_serial_counter[windfarm_code] += 1
        serial = self.windfarm_serial_counter[windfarm_code]

        # Format: WINDFARM_CODE-001, WINDFARM_CODE-002, etc.
        code = f"{windfarm_code}-{serial:03d}"

        # Add to tracking
        self.existing_turbine_codes.add(code)

        return code

    def process_csv(self, csv_path: Path, limit: Optional[int] = None):
        """Process CSV file and bulk import turbine units."""
        print(f"\nReading CSV file: {csv_path}")

        # Read CSV with proper encoding (try different encodings for Danish characters)
        encodings = ['utf-8', 'latin1', 'iso-8859-1', 'cp1252']
        df = None
        for encoding in encodings:
            try:
                df = pd.read_csv(csv_path, encoding=encoding)
                print(f"  Successfully read with encoding: {encoding}")
                break
            except UnicodeDecodeError:
                continue

        if df is None:
            raise ValueError(f"Could not read CSV with any of these encodings: {encodings}")

        # Apply limit for testing
        if limit:
            df = df.head(limit)
            print(f"  Processing first {limit} rows (limited)")

        print(f"  Total rows to process: {len(df)}")

        # Required columns
        required_cols = ['turbine_unit_id', 'windfarm_name', 'turbine_model', 'start_date']
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            raise ValueError(f"Missing required columns: {missing_cols}")

        # Prepare turbine units for bulk insert
        turbine_units_to_insert = []

        for idx, row in df.iterrows():
            self.stats['rows_processed'] += 1

            # Get windfarm
            windfarm_name = str(row['windfarm_name']).strip()
            windfarm = self.get_windfarm(windfarm_name)
            if not windfarm:
                self.stats['windfarms_not_found'].add(windfarm_name)
                self.stats['turbine_units_skipped'] += 1
                continue

            # Get turbine model
            turbine_model = self.get_turbine_model(row['turbine_model'])
            if not turbine_model:
                self.stats['models_not_found'].add(str(row['turbine_model']))
                self.stats['turbine_units_skipped'] += 1
                continue

            # Use GSRN (turbine_unit_id) as the code for ENERGISTYRELSEN turbines
            # This allows direct matching with generation data
            turbine_code = str(row['turbine_unit_id']).strip()

            # Parse dates
            start_date = parse_date(row['start_date'])
            end_date = parse_date(row.get('end_date'))

            # Parse status
            status = normalize_string(row.get('turbine_status', 'operational'))

            # Parse hub height
            hub_height = None
            if 'hub_height' in row and not pd.isna(row['hub_height']):
                try:
                    hub_height = Decimal(str(row['hub_height']))
                except (ValueError, TypeError):
                    pass

            # Default lat/lng to windfarm location (will be updated later with actual turbine positions)
            # For now, we use windfarm location as placeholder
            lat = 0.0  # Placeholder - should be updated with actual turbine location
            lng = 0.0  # Placeholder

            # Create turbine unit dict
            turbine_unit = {
                'code': turbine_code,
                'windfarm_id': windfarm.id,
                'turbine_model_id': turbine_model.id,
                'lat': lat,
                'lng': lng,
                'status': status,
                'hub_height_m': hub_height,
                'start_date': start_date,
                'end_date': end_date,
                'created_at': datetime.utcnow(),
                'updated_at': datetime.utcnow()
            }

            turbine_units_to_insert.append(turbine_unit)

        print(f"\n  Prepared {len(turbine_units_to_insert)} turbine units for insertion")

        # Bulk insert/update turbine units
        if turbine_units_to_insert:
            print(f"\nBulk inserting/updating {len(turbine_units_to_insert)} turbine units...")

            try:
                # Use PostgreSQL INSERT ... ON CONFLICT DO UPDATE to update existing records
                stmt = insert(TurbineUnit).values(turbine_units_to_insert)
                stmt = stmt.on_conflict_do_update(
                    index_elements=['code'],
                    set_={
                        'hub_height_m': stmt.excluded.hub_height_m,
                        'status': stmt.excluded.status,
                        'start_date': stmt.excluded.start_date,
                        'end_date': stmt.excluded.end_date,
                        'updated_at': stmt.excluded.updated_at
                    }
                )
                result = self.db.execute(stmt.returning(TurbineUnit.id))
                affected_count = len(result.all())

                self.stats['turbine_units_created'] = affected_count
                print(f"  ✓ Inserted/Updated {affected_count} turbine units")
            except Exception as e:
                print(f"  ❌ Error during bulk insert/update: {e}")
                import traceback
                traceback.print_exc()
                raise
        else:
            print("\n  ⚠ No turbine units to insert!")

        self.db.commit()

    def print_summary(self, elapsed_time):
        """Print import summary."""
        print("\n" + "="*60)
        print("TURBINE UNIT IMPORT SUMMARY")
        print("="*60)
        print(f"✓ Rows processed: {self.stats['rows_processed']}")
        print(f"✓ Turbine units created: {self.stats['turbine_units_created']}")
        print(f"⚠ Turbine units skipped: {self.stats['turbine_units_skipped']}")

        if self.stats['windfarms_not_found']:
            print(f"\n⚠ Windfarms not found ({len(self.stats['windfarms_not_found'])}):")
            for name in sorted(self.stats['windfarms_not_found'])[:10]:
                print(f"  - {name}")
            if len(self.stats['windfarms_not_found']) > 10:
                print(f"  ... and {len(self.stats['windfarms_not_found']) - 10} more")

        if self.stats['models_not_found']:
            print(f"\n⚠ Turbine models not found ({len(self.stats['models_not_found'])}):")
            for name in sorted(self.stats['models_not_found'])[:10]:
                print(f"  - {name}")
            if len(self.stats['models_not_found']) > 10:
                print(f"  ... and {len(self.stats['models_not_found']) - 10} more")

        if self.stats['errors']:
            print(f"\n❌ Errors ({len(self.stats['errors'])}):")
            for error in self.stats['errors'][:10]:
                print(f"  - {error}")

        print(f"\n⏱ Time elapsed: {elapsed_time:.2f} seconds")
        if self.stats['rows_processed'] > 0:
            rate = self.stats['rows_processed'] / elapsed_time
            print(f"📊 Processing rate: {rate:.1f} rows/second")
        print("="*60)


def main():
    parser = argparse.ArgumentParser(description='Import turbine units from CSV')
    parser.add_argument(
        '--csv',
        type=str,
        default='scripts/seeds/turbine_units/turbine_units.csv',
        help='Path to turbine units CSV file'
    )
    parser.add_argument(
        '--limit',
        type=int,
        help='Limit number of rows to process (for testing)'
    )

    args = parser.parse_args()

    # Validate CSV file exists
    csv_path = Path(args.csv)
    if not csv_path.exists():
        print(f"❌ CSV file not found: {csv_path}")
        sys.exit(1)

    print("="*60)
    print("TURBINE UNIT IMPORT")
    print("="*60)

    start_time = time.time()

    # Get database session
    db, engine = get_sync_session()

    try:
        # Create importer
        importer = TurbineUnitImporter(db, engine)

        # Load lookups
        importer.load_lookups()

        # Process CSV
        importer.process_csv(csv_path, limit=args.limit)

        # Print summary
        elapsed_time = time.time() - start_time
        importer.print_summary(elapsed_time)

    except Exception as e:
        print(f"\n❌ Fatal error: {e}")
        import traceback
        traceback.print_exc()
        db.rollback()
        sys.exit(1)
    finally:
        db.close()
        engine.dispose()


if __name__ == '__main__':
    main()
