"""Unified service for generation data management."""

import json
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Dict, List, Optional, Any
from uuid import UUID
import pandas as pd
from io import StringIO

from sqlalchemy import select, update, and_, or_
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.generation_data import GenerationDataRaw, GenerationData, GenerationUnitMapping
from app.models.generation_unit import GenerationUnit
from app.models.user import User


class UnifiedGenerationService:
    """Service for managing all generation data operations."""
    
    def __init__(self, db: AsyncSession):
        self.db = db
    
    # ============== RAW DATA OPERATIONS ==============
    
    async def import_elexon_csv(
        self,
        file_path: str,
        limit: Optional[int] = None
    ) -> Dict[str, Any]:
        """Import Elexon CSV data to raw storage."""
        
        # Read CSV file
        df = pd.read_csv(
            file_path,
            nrows=limit,
            parse_dates=['settlement_date'],
            dtype={
                'bmu_id': str,
                'settlement_period': int,
                'metered_volume': float,
                'import_export_ind': str
            }
        )
        
        # Clean column names (remove spaces)
        df.columns = df.columns.str.strip()
        
        records_to_insert = []
        
        for _, row in df.iterrows():
            # Calculate period start and end for 30-minute settlement period
            settlement_date = pd.to_datetime(row['settlement_date'])
            period_number = int(row['settlement_period'])
            
            # Each period is 30 minutes: Period 1 = 00:00-00:30, Period 2 = 00:30-01:00, etc.
            minutes_start = (period_number - 1) * 30
            period_start = settlement_date + timedelta(minutes=minutes_start)
            period_end = period_start + timedelta(minutes=30)
            
            # Convert import/export indicator to sign
            value = float(row['metered_volume'])
            if row['import_export_ind'] == 'I':
                value = -value  # Imports are negative
            
            record = {
                'source': 'ELEXON',
                'source_type': 'csv',
                'period_start': period_start.replace(tzinfo=timezone.utc),
                'period_end': period_end.replace(tzinfo=timezone.utc),
                'period_type': '30min',
                'data': {
                    'bmu_id': row['bmu_id'].strip(),
                    'settlement_date': row['settlement_date'].isoformat(),
                    'settlement_run_type': row['settlement_run_type'],
                    'cdca_run_number': int(row['cdca_run_number']),
                    'settlement_period': period_number,
                    'estimate_ind': row['estimate_ind'],
                    'metered_volume': float(row['metered_volume']),
                    'import_export_ind': row['import_export_ind']
                },
                'identifier': row['bmu_id'].strip(),
                'value_extracted': value,
                'unit': 'MW'
            }
            
            records_to_insert.append(record)
        
        # Bulk insert
        if records_to_insert:
            stmt = insert(GenerationDataRaw).values(records_to_insert)
            await self.db.execute(stmt)
            await self.db.commit()
        
        return {
            'success': True,
            'records_imported': len(records_to_insert),
            'source': 'ELEXON',
            'period_range': {
                'start': records_to_insert[0]['period_start'] if records_to_insert else None,
                'end': records_to_insert[-1]['period_end'] if records_to_insert else None
            }
        }
    
    async def import_elexon_csv_chunk(
        self,
        df_chunk: pd.DataFrame,
        batch_size: int = 1000,
        skip_duplicates: bool = True
    ) -> Dict[str, Any]:
        """Import a chunk of Elexon CSV data to raw storage.
        
        Args:
            df_chunk: DataFrame chunk to import
            batch_size: Number of records to insert at once (default 1000)
            skip_duplicates: If True, skip records that already exist
        """
        
        # Clean column names (remove spaces)
        df_chunk.columns = df_chunk.columns.str.strip()
        
        records_to_insert = []
        total_imported = 0
        total_skipped = 0
        first_period = None
        last_period = None
        
        for _, row in df_chunk.iterrows():
            # Calculate period start and end for 30-minute settlement period
            settlement_date = pd.to_datetime(row['settlement_date'])
            period_number = int(row['settlement_period'])
            
            # Each period is 30 minutes: Period 1 = 00:00-00:30, Period 2 = 00:30-01:00, etc.
            minutes_start = (period_number - 1) * 30
            period_start = settlement_date + timedelta(minutes=minutes_start)
            period_end = period_start + timedelta(minutes=30)
            
            # Track first and last periods
            if first_period is None:
                first_period = period_start
            last_period = period_end
            
            # Convert import/export indicator to sign
            value = float(row['metered_volume'])
            if row['import_export_ind'] == 'I':
                value = -value  # Imports are negative
            
            record = {
                'source': 'ELEXON',
                'source_type': 'csv',
                'period_start': period_start.replace(tzinfo=timezone.utc),
                'period_end': period_end.replace(tzinfo=timezone.utc),
                'period_type': '30min',
                'data': {
                    'bmu_id': row['bmu_id'].strip(),
                    'settlement_date': row['settlement_date'].isoformat(),
                    'settlement_run_type': row['settlement_run_type'],
                    'cdca_run_number': int(row['cdca_run_number']),
                    'settlement_period': period_number,
                    'estimate_ind': row['estimate_ind'],
                    'metered_volume': float(row['metered_volume']),
                    'import_export_ind': row['import_export_ind']
                },
                'identifier': row['bmu_id'].strip(),
                'value_extracted': value,
                'unit': 'MW'
            }
            
            records_to_insert.append(record)
            
            # Insert in batches to avoid parameter limit
            if len(records_to_insert) >= batch_size:
                try:
                    await self.db.commit()  # Commit any pending transaction
                except Exception:
                    await self.db.rollback()  # Rollback if there's an issue
                
                if skip_duplicates:
                    # Check for existing records
                    from sqlalchemy import select, and_, or_
                    
                    # Build conditions for checking existing records
                    conditions = []
                    for rec in records_to_insert:
                        conditions.append(
                            and_(
                                GenerationDataRaw.source == rec['source'],
                                GenerationDataRaw.identifier == rec['identifier'],
                                GenerationDataRaw.period_start == rec['period_start'],
                                GenerationDataRaw.period_end == rec['period_end']
                            )
                        )
                    
                    # Find existing records
                    if conditions:
                        existing_query = select(GenerationDataRaw).where(or_(*conditions))
                        existing_result = await self.db.execute(existing_query)
                        existing_records = existing_result.scalars().all()
                        
                        # Create set of existing record keys
                        existing_keys = {
                            (r.source, r.identifier, r.period_start, r.period_end) 
                            for r in existing_records
                        }
                        
                        # Filter out duplicates
                        filtered_records = [
                            rec for rec in records_to_insert
                            if (rec['source'], rec['identifier'], rec['period_start'], rec['period_end']) 
                            not in existing_keys
                        ]
                        
                        skipped = len(records_to_insert) - len(filtered_records)
                        total_skipped += skipped
                        records_to_insert = filtered_records
                
                # Insert non-duplicate records
                if records_to_insert:
                    try:
                        stmt = insert(GenerationDataRaw).values(records_to_insert)
                        await self.db.execute(stmt)
                        await self.db.commit()
                        total_imported += len(records_to_insert)
                    except Exception as e:
                        await self.db.rollback()
                        # Log error but continue processing
                        print(f"Warning: Failed to insert batch: {str(e)[:100]}")
                        # Could retry here if needed
                    
                records_to_insert = []
        
        # Insert any remaining records
        if records_to_insert:
            if skip_duplicates:
                # Check for existing records
                from sqlalchemy import select, and_, or_
                
                # Build conditions for checking existing records
                conditions = []
                for rec in records_to_insert:
                    conditions.append(
                        and_(
                            GenerationDataRaw.source == rec['source'],
                            GenerationDataRaw.identifier == rec['identifier'],
                            GenerationDataRaw.period_start == rec['period_start'],
                            GenerationDataRaw.period_end == rec['period_end']
                        )
                    )
                
                # Find existing records
                if conditions:
                    existing_query = select(GenerationDataRaw).where(or_(*conditions))
                    existing_result = await self.db.execute(existing_query)
                    existing_records = existing_result.scalars().all()
                    
                    # Create set of existing record keys
                    existing_keys = {
                        (r.source, r.identifier, r.period_start, r.period_end) 
                        for r in existing_records
                    }
                    
                    # Filter out duplicates
                    filtered_records = [
                        rec for rec in records_to_insert
                        if (rec['source'], rec['identifier'], rec['period_start'], rec['period_end']) 
                        not in existing_keys
                    ]
                    
                    skipped = len(records_to_insert) - len(filtered_records)
                    total_skipped += skipped
                    records_to_insert = filtered_records
            
            # Insert non-duplicate records
            if records_to_insert:
                try:
                    stmt = insert(GenerationDataRaw).values(records_to_insert)
                    await self.db.execute(stmt)
                    await self.db.commit()
                    total_imported += len(records_to_insert)
                except Exception as e:
                    await self.db.rollback()
                    print(f"Warning: Failed to insert final batch: {str(e)[:100]}")
                    # Could retry here if needed
        
        return {
            'success': True,
            'records_imported': total_imported,
            'records_skipped': total_skipped,
            'source': 'ELEXON',
            'period_range': {
                'start': first_period.replace(tzinfo=timezone.utc) if first_period else None,
                'end': last_period.replace(tzinfo=timezone.utc) if last_period else None
            }
        }
    
    async def store_raw_data(
        self,
        source: str,
        data: List[Dict],
        source_type: str = 'api'
    ) -> Dict[str, Any]:
        """Store raw data from any source."""
        
        records_to_insert = []
        
        for item in data:
            # Extract period information based on source
            period_info = self._extract_period_info(item, source)
            
            record = {
                'source': source,
                'source_type': source_type,
                'period_start': period_info.get('start'),
                'period_end': period_info.get('end'),
                'period_type': period_info.get('type'),
                'data': json.dumps(item, default=str) if not isinstance(item, dict) else item,
                'identifier': period_info.get('identifier'),
                'value_extracted': period_info.get('value'),
                'unit': period_info.get('unit')
            }
            
            records_to_insert.append(record)
        
        # Bulk insert
        if records_to_insert:
            stmt = insert(GenerationDataRaw).values(records_to_insert)
            await self.db.execute(stmt)
            await self.db.commit()
        
        return {
            'success': True,
            'records_stored': len(records_to_insert),
            'source': source
        }
    
    def _extract_period_info(self, item: Dict, source: str) -> Dict:
        """Extract period information based on source."""
        
        if source == 'ENTSOE':
            timestamp = pd.to_datetime(item.get('timestamp'))
            return {
                'start': timestamp,
                'end': timestamp + timedelta(hours=1),
                'type': 'hourly',
                'identifier': f"{item.get('area_code')}_{item.get('production_type')}",
                'value': item.get('value'),
                'unit': 'MW'
            }
        
        elif source == 'ELEXON':
            # Handle both API and CSV formats
            if 'settlement_date' in item:
                date = pd.to_datetime(item['settlement_date'])
                period = int(item['settlement_period'])
            else:
                date = pd.to_datetime(item.get('timestamp'))
                period = item.get('period', 1)
            
            minutes_start = (period - 1) * 30
            period_start = date + timedelta(minutes=minutes_start)
            
            return {
                'start': period_start,
                'end': period_start + timedelta(minutes=30),
                'type': '30min',
                'identifier': item.get('bmu_id', item.get('bm_unit')),
                'value': item.get('metered_volume', item.get('value')),
                'unit': 'MW'
            }
        
        elif source == 'EIA':
            period = item.get('period')  # Format: "2024-01"
            year, month = map(int, period.split('-'))
            period_start = datetime(year, month, 1, tzinfo=timezone.utc)
            
            if month == 12:
                period_end = datetime(year + 1, 1, 1, tzinfo=timezone.utc)
            else:
                period_end = datetime(year, month + 1, 1, tzinfo=timezone.utc)
            
            return {
                'start': period_start,
                'end': period_end,
                'type': 'monthly',
                'identifier': item.get('plant_code'),
                'value': item.get('generation'),
                'unit': 'MWh'
            }
        
        elif source == 'TAIPOWER':
            timestamp = pd.to_datetime(item.get('update_time'))
            return {
                'start': timestamp - timedelta(minutes=15),
                'end': timestamp,
                'type': '15min',
                'identifier': item.get('unit_name'),
                'value': item.get('net_generation'),
                'unit': 'MW'
            }
        
        return {}
    
    # ============== PROCESSING OPERATIONS ==============
    
    async def process_to_hourly(
        self,
        source: str,
        identifier: Optional[str] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        generation_unit_id: Optional[int] = None,
        windfarm_id: Optional[int] = None
    ) -> Dict[str, Any]:
        """Process raw data to hourly resolution."""
        
        # Build query for raw data
        query = select(GenerationDataRaw).where(
            GenerationDataRaw.source == source
        )
        
        if identifier:
            query = query.where(GenerationDataRaw.identifier == identifier)
        if start_date:
            query = query.where(GenerationDataRaw.period_start >= start_date)
        if end_date:
            query = query.where(GenerationDataRaw.period_end <= end_date)
        
        result = await self.db.execute(query.order_by(GenerationDataRaw.period_start))
        raw_records = result.scalars().all()
        
        if not raw_records:
            return {
                'success': False,
                'message': 'No raw data found for processing',
                'processed_count': 0
            }
        
        # Get or create mapping if generation_unit_id provided
        if generation_unit_id and identifier:
            await self._ensure_mapping(source, identifier, generation_unit_id, windfarm_id)
        
        # Group by hour
        hourly_groups = self._group_by_hour(raw_records)
        
        # Process each hour
        processed_records = []
        for (hour, group_identifier), group_records in hourly_groups.items():
            hourly_record = await self._create_hourly_record(
                hour, 
                group_identifier, 
                group_records,
                source,
                generation_unit_id,
                windfarm_id
            )
            if hourly_record:
                processed_records.append(hourly_record)
        
        # Bulk upsert
        if processed_records:
            stmt = insert(GenerationData).values(processed_records)
            stmt = stmt.on_conflict_do_update(
                index_elements=['hour', 'generation_unit_id', 'source'],
                set_={
                    'generation_mwh': stmt.excluded.generation_mwh,
                    'quality_score': stmt.excluded.quality_score,
                    'completeness': stmt.excluded.completeness,
                    'raw_data_ids': stmt.excluded.raw_data_ids,
                    'updated_at': datetime.utcnow()
                }
            )
            await self.db.execute(stmt)
            await self.db.commit()
        
        return {
            'success': True,
            'raw_records_processed': len(raw_records),
            'hourly_records_created': len(processed_records),
            'period_range': {
                'start': raw_records[0].period_start if raw_records else None,
                'end': raw_records[-1].period_end if raw_records else None
            }
        }
    
    def _group_by_hour(self, raw_records: List[GenerationDataRaw]) -> Dict:
        """Group raw records by hour and identifier."""
        
        from collections import defaultdict
        hourly_groups = defaultdict(list)
        
        for record in raw_records:
            if record.period_type == 'hourly':
                hour = record.period_start.replace(minute=0, second=0, microsecond=0)
                hourly_groups[(hour, record.identifier)].append(record)
            
            elif record.period_type in ['15min', '30min']:
                hour = record.period_start.replace(minute=0, second=0, microsecond=0)
                hourly_groups[(hour, record.identifier)].append(record)
            
            elif record.period_type == 'monthly':
                # Distribute monthly data across all hours
                current = record.period_start.replace(minute=0, second=0, microsecond=0)
                while current < record.period_end:
                    hourly_groups[(current, record.identifier)].append(record)
                    current += timedelta(hours=1)
        
        return hourly_groups
    
    async def _create_hourly_record(
        self,
        hour: datetime,
        identifier: str,
        raw_records: List[GenerationDataRaw],
        source: str,
        generation_unit_id: Optional[int] = None,
        windfarm_id: Optional[int] = None
    ) -> Optional[Dict]:
        """Create hourly record from raw data."""
        
        # If not provided, try to get mapping
        if not generation_unit_id:
            mapping = await self._get_mapping(source, identifier)
            if mapping:
                generation_unit_id = mapping.generation_unit_id
                windfarm_id = mapping.windfarm_id or windfarm_id
        
        # Calculate hourly value based on resolution
        first_record = raw_records[0]
        
        if first_record.period_type == 'hourly':
            generation_mwh = float(first_record.value_extracted)
            quality_flag = 'measured'
            quality_score = 1.0
            completeness = 1.0
        
        elif first_record.period_type == '30min':
            # Two 30-minute periods per hour
            if len(raw_records) == 2:
                generation_mwh = sum(float(r.value_extracted) * 0.5 for r in raw_records)
                quality_flag = 'aggregated'
                quality_score = 0.95
                completeness = 1.0
            else:
                # Only one period - estimate
                generation_mwh = float(raw_records[0].value_extracted)
                quality_flag = 'aggregated'
                quality_score = 0.7
                completeness = 0.5
        
        elif first_record.period_type == '15min':
            # Four 15-minute periods per hour
            if len(raw_records) == 4:
                generation_mwh = sum(float(r.value_extracted) * 0.25 for r in raw_records)
                quality_flag = 'aggregated'
                quality_score = 0.95
                completeness = 1.0
            else:
                # Partial data - scale up
                total = sum(float(r.value_extracted) for r in raw_records)
                generation_mwh = total * (4 / len(raw_records)) * 0.25
                quality_flag = 'aggregated'
                quality_score = 0.6 + (0.1 * len(raw_records))
                completeness = len(raw_records) / 4
        
        elif first_record.period_type == 'monthly':
            # Distribute monthly total
            if first_record.unit == 'MWh':
                duration = first_record.period_end - first_record.period_start
                hours_in_period = int(duration.total_seconds() / 3600)
                generation_mwh = float(first_record.value_extracted) / hours_in_period
            else:
                generation_mwh = float(first_record.value_extracted)
            
            quality_flag = 'interpolated'
            quality_score = 0.5
            completeness = 1.0
        
        else:
            return None
        
        return {
            'hour': hour,
            'generation_unit_id': generation_unit_id,
            'windfarm_id': windfarm_id,
            'generation_mwh': generation_mwh,
            'source': source,
            'source_resolution': first_record.period_type,
            'raw_data_ids': [r.id for r in raw_records],
            'quality_flag': quality_flag,
            'quality_score': quality_score,
            'completeness': completeness
        }
    
    async def _ensure_mapping(
        self,
        source: str,
        identifier: str,
        generation_unit_id: int,
        windfarm_id: Optional[int]
    ):
        """Ensure mapping exists between source identifier and generation unit."""
        
        # Check if mapping exists
        result = await self.db.execute(
            select(GenerationUnitMapping).where(
                GenerationUnitMapping.source == source,
                GenerationUnitMapping.source_identifier == identifier
            )
        )
        mapping = result.scalar_one_or_none()
        
        if not mapping:
            # Create new mapping
            mapping = GenerationUnitMapping(
                source=source,
                source_identifier=identifier,
                generation_unit_id=generation_unit_id,
                windfarm_id=windfarm_id,
                is_active=True
            )
            self.db.add(mapping)
            await self.db.commit()
    
    async def _get_mapping(
        self,
        source: str,
        identifier: str
    ) -> Optional[GenerationUnitMapping]:
        """Get mapping for source identifier."""
        
        result = await self.db.execute(
            select(GenerationUnitMapping).where(
                GenerationUnitMapping.source == source,
                GenerationUnitMapping.source_identifier == identifier,
                GenerationUnitMapping.is_active == True
            )
        )
        return result.scalar_one_or_none()
    
    # ============== QUERY OPERATIONS ==============
    
    async def get_hourly_data(
        self,
        generation_unit_id: Optional[int] = None,
        windfarm_id: Optional[int] = None,
        source: Optional[str] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        min_quality_score: float = 0.0
    ) -> List[GenerationData]:
        """Query hourly generation data."""
        
        query = select(GenerationData).where(
            GenerationData.quality_score >= min_quality_score
        )
        
        if generation_unit_id:
            query = query.where(GenerationData.generation_unit_id == generation_unit_id)
        if windfarm_id:
            query = query.where(GenerationData.windfarm_id == windfarm_id)
        if source:
            query = query.where(GenerationData.source == source)
        if start_date:
            query = query.where(GenerationData.hour >= start_date)
        if end_date:
            query = query.where(GenerationData.hour <= end_date)
        
        result = await self.db.execute(query.order_by(GenerationData.hour))
        return result.scalars().all()
    
    async def manual_override(
        self,
        hour: datetime,
        generation_unit_id: int,
        source: str,
        new_value: float,
        reason: str,
        user: User
    ) -> Dict[str, Any]:
        """Manually override a generation value."""
        
        # Get existing record
        result = await self.db.execute(
            select(GenerationData).where(
                GenerationData.hour == hour,
                GenerationData.generation_unit_id == generation_unit_id,
                GenerationData.source == source
            )
        )
        record = result.scalar_one_or_none()
        
        if not record:
            return {
                'success': False,
                'message': 'Record not found'
            }
        
        # Store original value and apply override
        original_value = record.generation_mwh
        record.original_value = original_value
        record.generation_mwh = new_value
        record.is_manual_override = True
        record.override_reason = reason
        record.override_by_id = user.id
        record.override_at = datetime.utcnow()
        record.updated_at = datetime.utcnow()
        
        await self.db.commit()
        
        return {
            'success': True,
            'original_value': float(original_value),
            'new_value': new_value,
            'hour': hour.isoformat()
        }