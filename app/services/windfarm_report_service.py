"""Service for generating comprehensive windfarm performance reports."""

from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple
from sqlalchemy import select, and_, func, extract
from sqlalchemy.ext.asyncio import AsyncSession
from dateutil.relativedelta import relativedelta

from app.models.generation_data import GenerationData
from app.models.generation_unit import GenerationUnit
from app.models.windfarm import Windfarm
from app.schemas.windfarm_report import (
    WindfarmReportData,
    PerformanceSummary,
    WindfarmRankings,
    RankingRow,
    PeerComparisonData,
    PeerComparisonTimeseries,
    TimeseriesDataPoint,
    BoxPlotData
)
from app.services.peer_analysis_service import PeerAnalysisService
from app.services.statistical_analysis import StatisticalAnalysis


class WindfarmReportService:
    """Service for generating comprehensive performance reports."""

    def __init__(self, db: AsyncSession):
        self.db = db
        self.peer_service = PeerAnalysisService(db)
        self.stats = StatisticalAnalysis()
        self._peer_data_cache = {}  # Cache for peer group monthly data

    async def generate_report_data(
        self,
        windfarm_id: int,
        start_date: datetime,
        end_date: datetime,
        include_peer_groups: Optional[List[str]] = None
    ) -> WindfarmReportData:
        """
        Generate complete report data for a windfarm.

        Args:
            windfarm_id: ID of target windfarm
            start_date: Start of analysis period
            end_date: End of analysis period
            include_peer_groups: List of peer groups to include, or None for all

        Returns:
            Complete report data with all sections
        """
        # Get windfarm with relationships
        windfarm = await self.peer_service.get_windfarm_with_relations(windfarm_id)
        if not windfarm:
            raise ValueError(f"Windfarm {windfarm_id} not found")

        # Get all peer groups
        peer_groups = await self.peer_service.get_all_peer_groups(windfarm_id)

        # Filter peer groups if specified
        if include_peer_groups:
            peer_groups = {
                k: v for k, v in peer_groups.items()
                if k in include_peer_groups
            }

        # Calculate summary metrics for target windfarm
        summary = await self._calculate_performance_summary(
            windfarm_id,
            start_date,
            end_date,
            peer_groups
        )

        # Calculate rankings
        rankings = await self._calculate_rankings(
            windfarm_id,
            start_date,
            end_date,
            peer_groups
        )

        # Generate peer comparison data for each group
        peer_comparisons = {}
        for group_type, group_info in peer_groups.items():
            peer_comparisons[group_type] = await self._generate_peer_comparison(
                windfarm_id,
                windfarm.name,
                group_type,
                group_info,
                start_date,
                end_date
            )

        # Generate highlights
        target_monthly_cfs = await self._get_monthly_capacity_factors(
            windfarm_id,
            start_date,
            end_date
        )
        target_stats = self.stats.calculate_performance_metrics(target_monthly_cfs)

        # Use bidzone or country peers for comparison stats
        peer_stats = {}
        if 'bidzone' in peer_comparisons:
            peer_monthly_data = await self._get_peer_group_monthly_data(
                'bidzone',
                peer_groups['bidzone'].group_id,
                start_date,
                end_date
            )
            all_peer_values = [v for month_data in peer_monthly_data.values() for v in month_data.values()]
            peer_stats = self.stats.calculate_performance_metrics(all_peer_values)
        elif 'country' in peer_comparisons:
            peer_monthly_data = await self._get_peer_group_monthly_data(
                'country',
                peer_groups['country'].group_id,
                start_date,
                end_date
            )
            all_peer_values = [v for month_data in peer_monthly_data.values() for v in month_data.values()]
            peer_stats = self.stats.calculate_performance_metrics(all_peer_values)

        highlights = self.stats.generate_highlights(
            windfarm.name,
            target_stats,
            peer_stats if peer_stats else target_stats,
            {
                'country_rank': rankings.country_rank,
                'bidzone_rank': rankings.bidzone_rank
            } if rankings.bidzone_rank else {'country_rank': rankings.country_rank},
            {
                'country_total': rankings.total_in_country,
                'bidzone_total': rankings.total_in_bidzone
            } if rankings.total_in_bidzone else {'country_total': rankings.total_in_country}
        )

        return WindfarmReportData(
            windfarm_id=windfarm.id,
            windfarm_name=windfarm.name,
            windfarm_code=windfarm.code,
            date_range_start=start_date,
            date_range_end=end_date,
            country={
                'id': windfarm.country.id,
                'name': windfarm.country.name,
                'code': windfarm.country.code
            } if windfarm.country else {'id': windfarm.country_id, 'name': 'Unknown', 'code': 'UNK'},
            bidzone={
                'id': windfarm.bidzone.id,
                'name': windfarm.bidzone.name,
                'code': windfarm.bidzone.code
            } if windfarm.bidzone else None,
            summary=summary,
            rankings=rankings,
            peer_comparisons=peer_comparisons,
            highlights=highlights
        )

    async def _calculate_performance_summary(
        self,
        windfarm_id: int,
        start_date: datetime,
        end_date: datetime,
        peer_groups: Dict
    ) -> PerformanceSummary:
        """Calculate summary performance metrics."""
        monthly_cfs = await self._get_monthly_capacity_factors(windfarm_id, start_date, end_date)
        monthly_generation = await self._get_monthly_generation(windfarm_id, start_date, end_date)

        if not monthly_cfs:
            return PerformanceSummary(
                avg_capacity_factor=0.0,
                avg_monthly_generation_gwh=0.0,
                total_generation_gwh=0.0,
                max_monthly_cf=0.0,
                min_monthly_cf=0.0,
                months_above_peer_average=0,
                total_months=0
            )

        # Calculate peer average for comparison
        months_above_peer = 0
        if peer_groups:
            # Use first available peer group
            peer_type = list(peer_groups.keys())[0]
            peer_info = peer_groups[peer_type]
            peer_monthly_data = await self._get_peer_group_monthly_data(
                peer_type,
                peer_info.group_id,
                start_date,
                end_date
            )
            peer_averages = self.stats.calculate_peer_average(peer_monthly_data)

            # Count months above peer average
            target_monthly_dict = await self._get_monthly_capacity_factors_dict(
                windfarm_id,
                start_date,
                end_date
            )
            for month, target_cf in target_monthly_dict.items():
                if month in peer_averages and target_cf > peer_averages[month]:
                    months_above_peer += 1

        return PerformanceSummary(
            avg_capacity_factor=float(sum(monthly_cfs) / len(monthly_cfs)),
            avg_monthly_generation_gwh=float(sum(monthly_generation) / len(monthly_generation)) if monthly_generation else 0.0,
            total_generation_gwh=float(sum(monthly_generation)),
            max_monthly_cf=float(max(monthly_cfs)),
            min_monthly_cf=float(min(monthly_cfs)),
            months_above_peer_average=months_above_peer,
            total_months=len(monthly_cfs)
        )

    async def _calculate_rankings(
        self,
        windfarm_id: int,
        start_date: datetime,
        end_date: datetime,
        peer_groups: Dict
    ) -> WindfarmRankings:
        """Calculate rankings within all peer groups."""
        rankings = WindfarmRankings(
            country_rank=0,
            total_in_country=0,
            bidzone_table=[],
            country_table=[],
            owner_table=[],
            turbine_table=[]
        )

        # Country ranking (required)
        if 'country' in peer_groups:
            country_table, country_rank = await self._generate_ranking_table(
                'country',
                peer_groups['country'].group_id,
                windfarm_id,
                start_date,
                end_date
            )
            rankings.country_table = country_table
            rankings.country_rank = country_rank
            rankings.total_in_country = len(country_table)

        # Bidzone ranking (optional)
        if 'bidzone' in peer_groups:
            bidzone_table, bidzone_rank = await self._generate_ranking_table(
                'bidzone',
                peer_groups['bidzone'].group_id,
                windfarm_id,
                start_date,
                end_date
            )
            rankings.bidzone_table = bidzone_table
            rankings.bidzone_rank = bidzone_rank
            rankings.total_in_bidzone = len(bidzone_table)

        # Owner ranking (optional)
        if 'owner' in peer_groups:
            owner_table, owner_rank = await self._generate_ranking_table(
                'owner',
                peer_groups['owner'].group_id,
                windfarm_id,
                start_date,
                end_date
            )
            rankings.owner_table = owner_table
            rankings.owner_rank = owner_rank
            rankings.total_in_owner = len(owner_table)

        # Turbine ranking (optional)
        if 'turbine' in peer_groups:
            turbine_table, turbine_rank = await self._generate_ranking_table(
                'turbine',
                peer_groups['turbine'].group_id,
                windfarm_id,
                start_date,
                end_date
            )
            rankings.turbine_table = turbine_table
            rankings.turbine_rank = turbine_rank
            rankings.total_in_turbine = len(turbine_table)

        return rankings

    async def _generate_ranking_table(
        self,
        peer_type: str,
        group_id: int,
        target_windfarm_id: int,
        start_date: datetime,
        end_date: datetime
    ) -> Tuple[List[RankingRow], int]:
        """
        Generate ranking table for a peer group.

        OPTIMIZED: Uses single bulk query instead of N queries.

        Returns (table_rows, target_rank)
        """
        # Get all windfarms in peer group
        windfarm_summaries = await self.peer_service.get_peer_windfarms_summary(
            peer_type,
            group_id
        )

        if not windfarm_summaries:
            return [], 0

        windfarm_ids = [wf['id'] for wf in windfarm_summaries]

        # OPTIMIZATION: Single bulk query for ALL windfarms at once
        # Instead of N separate queries, fetch everything in one go
        stmt = (
            select(
                GenerationUnit.windfarm_id,
                extract('year', GenerationData.hour).label('year'),
                extract('month', GenerationData.hour).label('month'),
                func.avg(GenerationData.capacity_factor).label('avg_cf'),
                func.sum(GenerationData.generation_mwh).label('total_gen_mwh')
            )
            .join(GenerationUnit, GenerationData.generation_unit_id == GenerationUnit.id)
            .where(
                and_(
                    GenerationUnit.windfarm_id.in_(windfarm_ids),
                    GenerationData.hour >= start_date,
                    GenerationData.hour < end_date,
                    GenerationData.capacity_factor.isnot(None)
                )
            )
            .group_by(GenerationUnit.windfarm_id, 'year', 'month')
        )

        result = await self.db.execute(stmt)

        # Organize data by windfarm
        windfarm_data = {}
        for row in result.all():
            wf_id = row.windfarm_id
            month_key = f"{int(row.year)}-{int(row.month):02d}"

            if wf_id not in windfarm_data:
                windfarm_data[wf_id] = {
                    'monthly_cfs': [],
                    'monthly_gen_gwh': [],
                    'monthly_dict': {}
                }

            windfarm_data[wf_id]['monthly_dict'][month_key] = float(row.avg_cf)
            windfarm_data[wf_id]['monthly_cfs'].append(float(row.avg_cf))
            windfarm_data[wf_id]['monthly_gen_gwh'].append(float(row.total_gen_mwh) / 1000.0 if row.total_gen_mwh else 0.0)

        # Calculate averages and prepare for ranking
        windfarm_cfs = []
        for wf_summary in windfarm_summaries:
            wf_id = wf_summary['id']

            if wf_id in windfarm_data and windfarm_data[wf_id]['monthly_cfs']:
                data = windfarm_data[wf_id]
                avg_cf = sum(data['monthly_cfs']) / len(data['monthly_cfs'])
                total_gen = sum(data['monthly_gen_gwh'])
                monthly_cfs = data['monthly_cfs']

                windfarm_cfs.append((wf_id, avg_cf, monthly_cfs, total_gen, wf_summary))

        # Sort by avg CF descending
        windfarm_cfs.sort(key=lambda x: x[1], reverse=True)

        # Generate table rows
        table_rows = []
        target_rank = 0
        for rank, (wf_id, avg_cf, monthly_cfs, total_gen, wf_summary) in enumerate(windfarm_cfs, start=1):
            if wf_id == target_windfarm_id:
                target_rank = rank

            table_rows.append(RankingRow(
                rank=rank,
                windfarm_id=wf_id,
                windfarm_name=wf_summary['name'],
                windfarm_code=wf_summary['code'],
                avg_capacity_factor=avg_cf,
                bidzone_code=wf_summary['bidzone_code'],
                country_code=wf_summary['country_code'],
                monthly_trend=monthly_cfs,
                total_generation_gwh=total_gen
            ))

        return table_rows, target_rank

    async def _generate_peer_comparison(
        self,
        windfarm_id: int,
        windfarm_name: str,
        peer_type: str,
        peer_info,
        start_date: datetime,
        end_date: datetime
    ) -> PeerComparisonData:
        """Generate complete peer comparison data for one peer group."""
        # Get target windfarm monthly data
        target_monthly_dict = await self._get_monthly_capacity_factors_dict(
            windfarm_id,
            start_date,
            end_date
        )

        # Get peer group monthly data
        peer_monthly_data = await self._get_peer_group_monthly_data(
            peer_type,
            peer_info.group_id,
            start_date,
            end_date
        )

        # Calculate peer statistics
        peer_averages = self.stats.calculate_peer_average(peer_monthly_data)
        peer_min, peer_max = self.stats.calculate_peer_band(peer_monthly_data)

        # Build timeseries
        months = sorted(target_monthly_dict.keys())
        target_timeseries = [
            TimeseriesDataPoint(date=month, value=target_monthly_dict[month])
            for month in months
        ]
        peer_avg_timeseries = [
            TimeseriesDataPoint(date=month, value=peer_averages.get(month, 0.0))
            for month in months
        ]
        peer_min_timeseries = [
            TimeseriesDataPoint(date=month, value=peer_min.get(month, 0.0))
            for month in months
        ]
        peer_max_timeseries = [
            TimeseriesDataPoint(date=month, value=peer_max.get(month, 0.0))
            for month in months
        ]

        timeseries = PeerComparisonTimeseries(
            target_name=windfarm_name,
            target_data=target_timeseries,
            peer_group_name=peer_info.group_name,
            peer_average_data=peer_avg_timeseries,
            peer_min_data=peer_min_timeseries,
            peer_max_data=peer_max_timeseries
        )

        # Calculate distribution (box plots)
        target_values = list(target_monthly_dict.values())
        all_peer_values = [v for month_data in peer_monthly_data.values() for v in month_data.values()]

        distribution = [
            self.stats.calculate_box_plot_data(target_values, windfarm_name),
            self.stats.calculate_box_plot_data(all_peer_values, f"{peer_info.group_name} Average")
        ]

        # Build heatmap matrix
        heatmap_data = await self._build_heatmap_data(
            windfarm_id,
            peer_type,
            peer_info.group_id,
            start_date,
            end_date
        )

        return PeerComparisonData(
            peer_group_info=peer_info,
            timeseries=timeseries,
            distribution=distribution,
            heatmap_matrix=heatmap_data['matrix'],
            heatmap_windfarm_names=heatmap_data['windfarm_names'],
            heatmap_month_labels=heatmap_data['month_labels'],
            target_heatmap_index=heatmap_data['target_index']
        )

    async def _build_heatmap_data(
        self,
        target_windfarm_id: int,
        peer_type: str,
        group_id: int,
        start_date: datetime,
        end_date: datetime
    ) -> Dict:
        """
        Build heatmap matrix data for peer group.

        OPTIMIZED: Uses single bulk query instead of N queries.
        """
        # Get all windfarms in peer group
        windfarm_summaries = await self.peer_service.get_peer_windfarms_summary(peer_type, group_id)

        if not windfarm_summaries:
            return {
                'matrix': [],
                'windfarm_names': [],
                'month_labels': [],
                'target_index': 0
            }

        # Sort by name for consistent ordering
        windfarm_summaries.sort(key=lambda x: x['name'])

        # Find target index
        target_index = next(
            (i for i, wf in enumerate(windfarm_summaries) if wf['id'] == target_windfarm_id),
            0
        )

        # Build month labels
        months = []
        current = start_date.replace(day=1)
        while current <= end_date:
            months.append(current.strftime('%Y-%m'))
            current += relativedelta(months=1)

        windfarm_ids = [wf['id'] for wf in windfarm_summaries]

        # OPTIMIZATION: Single bulk query for ALL windfarms
        stmt = (
            select(
                GenerationUnit.windfarm_id,
                extract('year', GenerationData.hour).label('year'),
                extract('month', GenerationData.hour).label('month'),
                func.avg(GenerationData.capacity_factor).label('avg_cf')
            )
            .join(GenerationUnit, GenerationData.generation_unit_id == GenerationUnit.id)
            .where(
                and_(
                    GenerationUnit.windfarm_id.in_(windfarm_ids),
                    GenerationData.hour >= start_date,
                    GenerationData.hour < end_date,
                    GenerationData.capacity_factor.isnot(None)
                )
            )
            .group_by(GenerationUnit.windfarm_id, 'year', 'month')
        )

        result = await self.db.execute(stmt)

        # Organize by windfarm
        windfarm_monthly_data = {wf['id']: {} for wf in windfarm_summaries}
        for row in result.all():
            month_key = f"{int(row.year)}-{int(row.month):02d}"
            windfarm_monthly_data[row.windfarm_id][month_key] = float(row.avg_cf)

        # Build matrix: rows = windfarms, cols = months
        matrix = []
        for wf_summary in windfarm_summaries:
            wf_id = wf_summary['id']
            monthly_dict = windfarm_monthly_data.get(wf_id, {})
            row = [monthly_dict.get(month, 0.0) for month in months]
            matrix.append(row)

        return {
            'matrix': matrix,
            'windfarm_names': [wf['name'] for wf in windfarm_summaries],
            'month_labels': months,
            'target_index': target_index
        }

    async def _get_monthly_capacity_factors(
        self,
        windfarm_id: int,
        start_date: datetime,
        end_date: datetime
    ) -> List[float]:
        """Get list of monthly average capacity factors."""
        stmt = (
            select(func.avg(GenerationData.capacity_factor))
            .join(GenerationUnit, GenerationData.generation_unit_id == GenerationUnit.id)
            .where(
                and_(
                    GenerationUnit.windfarm_id == windfarm_id,
                    GenerationData.hour >= start_date,
                    GenerationData.hour < end_date,
                    GenerationData.capacity_factor.isnot(None)
                )
            )
            .group_by(
                extract('year', GenerationData.hour),
                extract('month', GenerationData.hour)
            )
            .order_by(
                extract('year', GenerationData.hour),
                extract('month', GenerationData.hour)
            )
        )

        result = await self.db.execute(stmt)
        return [float(row[0]) for row in result.all() if row[0] is not None]

    async def _get_monthly_capacity_factors_dict(
        self,
        windfarm_id: int,
        start_date: datetime,
        end_date: datetime
    ) -> Dict[str, float]:
        """Get dict of monthly average capacity factors keyed by YYYY-MM."""
        stmt = (
            select(
                extract('year', GenerationData.hour).label('year'),
                extract('month', GenerationData.hour).label('month'),
                func.avg(GenerationData.capacity_factor).label('avg_cf')
            )
            .join(GenerationUnit, GenerationData.generation_unit_id == GenerationUnit.id)
            .where(
                and_(
                    GenerationUnit.windfarm_id == windfarm_id,
                    GenerationData.hour >= start_date,
                    GenerationData.hour < end_date,
                    GenerationData.capacity_factor.isnot(None)
                )
            )
            .group_by('year', 'month')
            .order_by('year', 'month')
        )

        result = await self.db.execute(stmt)
        return {
            f"{int(row.year)}-{int(row.month):02d}": float(row.avg_cf)
            for row in result.all()
            if row.avg_cf is not None
        }

    async def _get_monthly_generation(
        self,
        windfarm_id: int,
        start_date: datetime,
        end_date: datetime
    ) -> List[float]:
        """Get list of monthly total generation in GWh."""
        stmt = (
            select(func.sum(GenerationData.generation_mwh) / 1000.0)
            .join(GenerationUnit, GenerationData.generation_unit_id == GenerationUnit.id)
            .where(
                and_(
                    GenerationUnit.windfarm_id == windfarm_id,
                    GenerationData.hour >= start_date,
                    GenerationData.hour < end_date,
                    GenerationData.generation_mwh.isnot(None)
                )
            )
            .group_by(
                extract('year', GenerationData.hour),
                extract('month', GenerationData.hour)
            )
            .order_by(
                extract('year', GenerationData.hour),
                extract('month', GenerationData.hour)
            )
        )

        result = await self.db.execute(stmt)
        return [float(row[0]) for row in result.all() if row[0] is not None]

    async def _get_peer_group_monthly_data(
        self,
        peer_type: str,
        group_id: int,
        start_date: datetime,
        end_date: datetime
    ) -> Dict[str, Dict[int, float]]:
        """
        Get monthly capacity factors for all windfarms in peer group.

        OPTIMIZED: Caches results to avoid duplicate queries.

        Returns: {
            'YYYY-MM': {
                windfarm_id: capacity_factor,
                ...
            },
            ...
        }
        """
        # Check cache first
        cache_key = f"{peer_type}_{group_id}_{start_date.date()}_{end_date.date()}"
        if cache_key in self._peer_data_cache:
            return self._peer_data_cache[cache_key]

        # Get peer windfarm IDs
        if peer_type == 'bidzone':
            peer_ids = await self.peer_service.get_bidzone_peers(group_id)
        elif peer_type == 'country':
            peer_ids = await self.peer_service.get_country_peers(group_id)
        elif peer_type == 'owner':
            peer_ids = await self.peer_service.get_owner_peers(group_id)
        elif peer_type == 'turbine':
            peer_ids = await self.peer_service.get_turbine_model_peers(group_id)
        else:
            return {}

        if not peer_ids:
            return {}

        # Query monthly data for all peers
        stmt = (
            select(
                GenerationUnit.windfarm_id,
                extract('year', GenerationData.hour).label('year'),
                extract('month', GenerationData.hour).label('month'),
                func.avg(GenerationData.capacity_factor).label('avg_cf')
            )
            .join(GenerationUnit, GenerationData.generation_unit_id == GenerationUnit.id)
            .where(
                and_(
                    GenerationUnit.windfarm_id.in_(peer_ids),
                    GenerationData.hour >= start_date,
                    GenerationData.hour < end_date,
                    GenerationData.capacity_factor.isnot(None)
                )
            )
            .group_by(GenerationUnit.windfarm_id, 'year', 'month')
        )

        result = await self.db.execute(stmt)

        # Organize by month, then by windfarm
        monthly_data = {}
        for row in result.all():
            month_key = f"{int(row.year)}-{int(row.month):02d}"
            if month_key not in monthly_data:
                monthly_data[month_key] = {}
            monthly_data[month_key][row.windfarm_id] = float(row.avg_cf)

        # Cache the result
        self._peer_data_cache[cache_key] = monthly_data

        return monthly_data
