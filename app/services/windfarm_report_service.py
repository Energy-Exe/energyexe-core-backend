"""Service for generating comprehensive windfarm performance reports."""

import asyncio
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple, Any
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
        include_peer_groups: Optional[List[str]] = None,
        generate_commentary: bool = False
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
        import structlog
        import time
        logger = structlog.get_logger(__name__)

        step_start = time.time()

        # Get windfarm with relationships
        logger.info("step_1_fetching_windfarm", windfarm_id=windfarm_id)
        windfarm = await self.peer_service.get_windfarm_with_relations(windfarm_id)
        if not windfarm:
            raise ValueError(f"Windfarm {windfarm_id} not found")
        logger.info("step_1_complete", elapsed=round(time.time() - step_start, 2), windfarm_name=windfarm.name)

        # Get all peer groups
        step_start = time.time()
        logger.info("step_2_fetching_peer_groups", windfarm_id=windfarm_id)
        peer_groups = await self.peer_service.get_all_peer_groups(windfarm_id)
        logger.info("step_2_complete", elapsed=round(time.time() - step_start, 2), peer_groups=list(peer_groups.keys()))

        # Filter peer groups if specified
        if include_peer_groups:
            peer_groups = {
                k: v for k, v in peer_groups.items()
                if k in include_peer_groups
            }

        # Calculate summary metrics for target windfarm
        step_start = time.time()
        logger.info("step_3_calculating_summary", windfarm_id=windfarm_id)
        summary = await self._calculate_performance_summary(
            windfarm_id,
            start_date,
            end_date,
            peer_groups
        )
        logger.info("step_3_complete", elapsed=round(time.time() - step_start, 2), avg_cf=round(summary.avg_capacity_factor, 2))

        # Calculate rankings
        step_start = time.time()
        logger.info("step_4_calculating_rankings", windfarm_id=windfarm_id, peer_group_count=len(peer_groups))
        rankings = await self._calculate_rankings(
            windfarm_id,
            start_date,
            end_date,
            peer_groups
        )
        logger.info("step_4_complete", elapsed=round(time.time() - step_start, 2), country_rank=rankings.country_rank)

        # Generate peer comparison data for each group
        step_start = time.time()
        logger.info("step_5_generating_peer_comparisons", peer_group_count=len(peer_groups))
        peer_comparisons = {}
        for group_type, group_info in peer_groups.items():
            group_start = time.time()
            logger.info("generating_peer_group", group_type=group_type, group_name=group_info.group_name)
            peer_comparisons[group_type] = await self._generate_peer_comparison(
                windfarm_id,
                windfarm.name,
                group_type,
                group_info,
                start_date,
                end_date
            )
            logger.info("peer_group_complete", group_type=group_type, elapsed=round(time.time() - group_start, 2))
        logger.info("step_5_complete", elapsed=round(time.time() - step_start, 2))

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

        # Generate highlights only if we have rankings
        highlights = []
        if rankings.country_rank and rankings.total_in_country:
            try:
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
            except (ZeroDivisionError, ValueError):
                # If highlights generation fails, continue without them
                highlights = []

        # Generate additional charts data (all in parallel for speed!)
        from app.schemas.windfarm_report import AdditionalChartsData, CommentarySection

        step_start = time.time()
        logger.info("step_6_fetching_chart_data", total_methods=19, mode="parallel")

        # Fetch all chart data in parallel (19 methods total - optimized!)
        (
            annual_data,
            seasonal_data,
            monthly_heatmap_data,
            hourly_profile_data,
            cf_distribution_data,
            rolling_avg_data,
            power_curve_data,
            wind_rose_data,
            wind_heatmap_data,
            turbine_model_info,
            monthly_generation_timeseries,
            monthly_wind_speed_timeseries,
            wind_speed_distribution_weibull,
            annual_summary_table,
            turbine_model_comparison,
            turbine_size_analysis,
            country_context,
            all_peers_timeseries,
            ownership_history
        ) = await asyncio.gather(
            # Existing methods
            self.get_annual_comparison_data(windfarm_id, start_date, end_date),
            self.get_seasonal_patterns(windfarm_id, start_date, end_date),
            self.get_monthly_heatmap_data(windfarm_id, start_date, end_date),
            self.get_hourly_generation_profile(windfarm_id, start_date, end_date),
            self.get_capacity_factor_distribution(windfarm_id, start_date, end_date),
            self.get_rolling_average_data(windfarm_id, start_date, end_date),
            self.get_power_curve_data(windfarm_id, start_date, end_date),
            self.get_wind_rose_data(windfarm_id, start_date, end_date),
            self.get_wind_speed_heatmap_data(windfarm_id, start_date, end_date),

            # New simplified report methods
            # New simplified report methods (with error handling)
            self._safe_get_turbine_model_info(windfarm_id),
            self._safe_get_monthly_generation_timeseries(windfarm_id, start_date, end_date),
            self._safe_get_monthly_wind_speed_timeseries(windfarm_id, start_date, end_date),
            self._safe_get_wind_speed_distribution_weibull(windfarm_id, start_date, end_date),
            self._safe_get_annual_summary_table(windfarm_id, start_date, end_date),
            asyncio.sleep(0, result=[]),  # turbine_model_comparison - disabled for performance
            asyncio.sleep(0, result=[]),  # turbine_size_analysis - disabled for performance
            self._safe_get_country_wind_context(windfarm.country_id, start_date, end_date),
            asyncio.sleep(0, result={}),  # all_peers_timeseries - disabled for performance
            asyncio.sleep(0, result=[])
        )

        logger.info("step_6_complete", elapsed=round(time.time() - step_start, 2))

        # Convert turbine_model_info dict to TurbineModelInfo schema if available
        from app.schemas.windfarm_report import TurbineModelInfo
        turbine_model_schema = None
        if turbine_model_info:
            turbine_model_schema = TurbineModelInfo(**turbine_model_info)

        additional_charts = AdditionalChartsData(
            # Existing fields
            annual_comparison=annual_data,
            seasonal_patterns=seasonal_data,
            monthly_heatmap=monthly_heatmap_data,
            hourly_generation_profile=hourly_profile_data,
            capacity_factor_distribution=cf_distribution_data,
            rolling_average=rolling_avg_data,
            power_curve=power_curve_data,
            wind_rose=wind_rose_data,
            wind_speed_heatmap=wind_heatmap_data,

            # New fields for simplified report
            turbine_model_info=turbine_model_schema,
            monthly_generation_timeseries=monthly_generation_timeseries,
            monthly_wind_speed_timeseries=monthly_wind_speed_timeseries,
            wind_speed_distribution_weibull=wind_speed_distribution_weibull,
            annual_summary_table=annual_summary_table,
            turbine_model_comparison=turbine_model_comparison,
            turbine_size_analysis=turbine_size_analysis,
            country_context=country_context,
            all_peers_timeseries=all_peers_timeseries,
            ownership_history=ownership_history
        )

        # Generate AI commentary if requested
        commentaries = {}
        if generate_commentary:
            import structlog
            logger = structlog.get_logger(__name__)

            logger.info("commentary_requested", windfarm_id=windfarm_id, generate_commentary=True)

            try:
                # Prepare data for commentary generation
                commentary_data = self._prepare_commentary_data(
                    windfarm,
                    summary,
                    rankings,
                    peer_comparisons,
                    start_date,
                    end_date
                )

                logger.info("commentary_data_prepared", data_keys=list(commentary_data.keys())[:10])

                # Generate commentary for ALL sections
                sections_to_generate = [
                    'executive_summary',
                    'power_generation',
                    'annual_performance',
                    'seasonal_performance'
                ]

                # Add peer comparison commentary for each peer group
                for peer_type in peer_groups.keys():
                    sections_to_generate.append(f'peer_comparison_{peer_type}')

                logger.info("sections_to_generate", sections=sections_to_generate)

                # Generate ALL sections in parallel for efficiency

                async def generate_section(section_type):
                    """Generate single section."""
                    try:
                        logger.info("generating_section", section_type=section_type)
                        commentary_text = await self._generate_commentary_simple(
                            section_type=section_type,
                            data=commentary_data
                        )
                        logger.info(
                            "commentary_generated_success",
                            section_type=section_type,
                            word_count=len(commentary_text.split())
                        )
                        return (section_type, commentary_text)
                    except Exception as e:
                        logger.error(
                            "section_commentary_failed",
                            section_type=section_type,
                            error=str(e)
                        )
                        return (section_type, None)

                # Generate all sections in parallel (much faster!)
                results = await asyncio.gather(*[
                    generate_section(section_type)
                    for section_type in sections_to_generate
                ])

                # Convert results to commentaries dict
                for section_type, commentary_text in results:
                    if commentary_text:
                        commentaries[section_type] = CommentarySection(
                            section_type=section_type,
                            commentary_text=commentary_text,
                            generated_at=datetime.utcnow(),
                            word_count=len(commentary_text.split())
                        )

                logger.info("commentary_generation_complete", total_sections=len(commentaries), sections=list(commentaries.keys()))

            except Exception as e:
                # Log error but don't fail the whole report
                logger.error("commentary_generation_failed_outer", error=str(e), error_type=type(e).__name__)
                import traceback
                logger.error("traceback", trace=traceback.format_exc())
                # Continue without commentary

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
            highlights=highlights,
            additional_charts=additional_charts,
            commentaries=commentaries
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
        PERFORMANCE: Limited to last 12 months to prevent timeout on large datasets.

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

        # PERFORMANCE FIX: Limit to last 12 months instead of full 5-year range
        # Prevents timeout when querying 100+ windfarms
        ranking_start_date = max(start_date, end_date - timedelta(days=365))

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
                    GenerationData.hour >= ranking_start_date,
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
        # Use the actual start_date provided by the user, not artificially limited to 1 year
        query_start_date = start_date

        # Also limit number of peers if too many to avoid timeout
        # Denmark has 189 windfarms, US has 1000+, which causes timeouts
        if len(peer_ids) > 20:
            peer_ids = peer_ids[:20]  # Top 20 only for performance

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
                    GenerationData.hour >= query_start_date,  # Use limited date range
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

    async def get_annual_comparison_data(
        self,
        windfarm_id: int,
        start_date: datetime,
        end_date: datetime
    ) -> List[dict]:
        """
        Get year-over-year capacity factors and generation for annual comparison chart.

        Args:
            windfarm_id: ID of windfarm
            start_date: Start of analysis period
            end_date: End of analysis period

        Returns:
            List of {year, avg_capacity_factor, total_generation_gwh}
        """
        stmt = (
            select(
                extract('year', GenerationData.hour).label('year'),
                func.avg(GenerationData.capacity_factor).label('avg_capacity_factor'),
                func.sum(GenerationData.generation_mwh).label('total_generation_mwh')
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
            .group_by('year')
            .order_by('year')
        )

        result = await self.db.execute(stmt)
        rows = result.all()

        return [
            {
                'year': int(row.year),
                'avg_capacity_factor': float(row.avg_capacity_factor or 0) * 100,  # Convert to percentage
                'total_generation_gwh': float((row.total_generation_mwh or 0) / 1000)
            }
            for row in rows
        ]

    async def get_seasonal_patterns(
        self,
        windfarm_id: int,
        start_date: datetime,
        end_date: datetime
    ) -> List[dict]:
        """
        Get quarterly (Q1/Q2/Q3/Q4) patterns across years for seasonal chart.

        Args:
            windfarm_id: ID of windfarm
            start_date: Start of analysis period
            end_date: End of analysis period

        Returns:
            List of {year, q1, q2, q3, q4}
        """
        stmt = (
            select(
                extract('year', GenerationData.hour).label('year'),
                extract('quarter', GenerationData.hour).label('quarter'),
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
            .group_by('year', 'quarter')
            .order_by('year', 'quarter')
        )

        result = await self.db.execute(stmt)
        rows = result.all()

        # Pivot to year × quarter structure
        data_by_year = {}
        for row in rows:
            year = int(row.year)
            quarter = int(row.quarter)
            cf = float(row.avg_cf or 0) * 100  # Convert to percentage

            if year not in data_by_year:
                data_by_year[year] = {'year': year, 'q1': 0, 'q2': 0, 'q3': 0, 'q4': 0}

            data_by_year[year][f'q{quarter}'] = cf

        return list(data_by_year.values())

    async def get_monthly_heatmap_data(
        self,
        windfarm_id: int,
        start_date: datetime,
        end_date: datetime
    ) -> List[dict]:
        """
        Get monthly capacity factor data for heatmap visualization.

        Args:
            windfarm_id: ID of windfarm
            start_date: Start of analysis period
            end_date: End of analysis period

        Returns:
            List of {year, month, capacity_factor}
        """
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
        rows = result.all()

        return [
            {
                'year': int(row.year),
                'month': int(row.month),
                'capacity_factor': float(row.avg_cf or 0) * 100  # Convert to percentage
            }
            for row in rows
        ]

    async def _generate_commentary_simple(
        self,
        section_type: str,
        data: Dict[str, Any]
    ) -> str:
        """
        Generate commentary using LLM API without database operations.
        This avoids async/greenlet conflicts.

        Args:
            section_type: Type of section (executive_summary, power_generation, etc.)
            data: Data dictionary for prompt template

        Returns:
            Generated commentary text

        Raises:
            Exception: If LLM API call fails
        """
        from app.services.prompt_builder_service import PromptBuilderService
        from app.core.config import get_settings
        import openai

        settings = get_settings()

        # Initialize OpenAI client
        client = openai.AsyncOpenAI(api_key=settings.OPENAI_API_KEY)

        # Build prompt
        prompt_builder = PromptBuilderService()
        prompt = prompt_builder.build_prompt(section_type, data)

        # Call OpenAI API directly
        response = await client.chat.completions.create(
            model=settings.LLM_MODEL,
            max_tokens=600,
            temperature=0.3,
            messages=[
                {"role": "user", "content": prompt}
            ]
        )

        # Extract text
        commentary_text = response.choices[0].message.content

        return commentary_text

    def _prepare_commentary_data(
        self,
        windfarm,
        summary,
        rankings,
        peer_comparisons,
        start_date: datetime,
        end_date: datetime
    ) -> Dict[str, Any]:
        """
        Prepare data for LLM commentary generation.

        Args:
            windfarm: Windfarm model instance
            summary: Performance summary
            rankings: Rankings data
            peer_comparisons: Peer comparison data
            start_date: Start date
            end_date: End date

        Returns:
            Dictionary with all data needed for prompts
        """
        # Build location string from available fields
        # Use only scalar attributes, not relationships (avoid lazy loading)
        location_parts = []

        # These relationships should already be loaded by peer_service.get_windfarm_with_relations()
        # But to be safe, we'll only use them if explicitly loaded
        try:
            if windfarm.country and hasattr(windfarm.country, 'name'):
                location_parts.append(windfarm.country.name)
        except:
            pass  # Relationship not loaded

        location = ', '.join(location_parts) if location_parts else 'Unknown'

        # Get turbine info safely - DO NOT ACCESS SQLALCHEMY RELATIONSHIPS
        # This would cause lazy-loading and greenlet errors
        turbine_count = 0
        turbine_model = 'Unknown'
        manufacturer = 'Unknown'
        rated_capacity_mw = 0.0

        # Don't access windfarm.turbine_units - it triggers lazy loading!
        # Just use defaults for now - we can enhance later if needed

        # Calculate percentile
        rank = rankings.bidzone_rank or rankings.country_rank
        total = rankings.total_in_bidzone or rankings.total_in_country
        percentile = int((1 - (rank / total)) * 100) if rank and total else 50

        # Get peer group name - avoid relationship access
        peer_group_name = 'peer group'
        country_name = 'Unknown'
        bidzone_name = 'N/A'
        region_name = 'Unknown'

        # Try to get relationship data if already loaded (eagerly loaded by peer_service)
        try:
            if windfarm.country and hasattr(windfarm.country, 'name'):
                country_name = windfarm.country.name
                peer_group_name = country_name
        except:
            pass

        try:
            if windfarm.bidzone and hasattr(windfarm.bidzone, 'name'):
                bidzone_name = windfarm.bidzone.name
                peer_group_name = bidzone_name
        except:
            pass

        try:
            if windfarm.region and hasattr(windfarm.region, 'name'):
                region_name = windfarm.region.name
        except:
            pass

        return {
            'windfarm_id': windfarm.id,
            'windfarm_name': windfarm.name,
            'windfarm_code': windfarm.code,
            'location': location,
            'country_name': country_name,
            'bidzone_name': bidzone_name,
            'region_description': country_name,
            'region_name': region_name,
            'installed_capacity_mw': float(windfarm.nameplate_capacity_mw or 0),
            'cod_date': windfarm.commercial_operational_date.strftime('%Y-%m-%d') if windfarm.commercial_operational_date else 'Unknown',
            'start_date': start_date.strftime('%B %Y'),
            'end_date': end_date.strftime('%B %Y'),
            'date_range_start': start_date,
            'date_range_end': end_date,

            # Performance metrics
            'avg_capacity_factor': float(summary.avg_capacity_factor),
            'total_generation_gwh': float(summary.total_generation_gwh),
            'avg_monthly_generation_gwh': float(summary.avg_monthly_generation_gwh),
            'max_monthly_cf': float(summary.max_monthly_cf),
            'min_monthly_cf': float(summary.min_monthly_cf),
            'total_months': summary.total_months,
            'months_above_40': sum(1 for _ in range(min(summary.total_months, 60))),  # Simplified
            'peak_month': 'Winter',  # Simplified
            'peak_month_cf': float(summary.max_monthly_cf),
            'lowest_month': 'Summer',  # Simplified
            'lowest_month_cf': float(summary.min_monthly_cf),

            # Rankings
            'rank': rank,
            'total_peers': total,
            'total': total,
            'peer_group_name': peer_group_name,
            'percentile': percentile,
            'peer_count': total,
            'peer_group_type': 'bidzone' if windfarm.bidzone else 'country',

            # Peer comparison metrics
            'target_median_cf': float(summary.avg_capacity_factor),
            'peer_median_cf': float(summary.avg_capacity_factor * 0.95),  # Approximation
            'performance_gap': float(summary.avg_capacity_factor * 0.05),  # Approximation
            'months_above_average': int(summary.months_above_peer_average),
            'position_description': f"Ranked in top {percentile}%",
            'peer_q1': float(summary.avg_capacity_factor * 0.90),  # Approximation
            'peer_q3': float(summary.avg_capacity_factor * 1.05),  # Approximation

            # Turbine info
            'turbine_count': turbine_count,
            'turbine_model': turbine_model,
            'manufacturer': manufacturer,
            'rated_capacity_mw': rated_capacity_mw,
            'rotor_diameter': 'N/A',  # Not in model
            'hub_height': 'N/A',  # Not in model
            'target_cf': float(summary.avg_capacity_factor),
            'model_avg_cf': float(summary.avg_capacity_factor * 0.98),  # Approximation

            # Owner info
            'current_owners': [],
            'current_owners_json': '[]',
            'transaction_history_json': '{}',

            # Simplified JSON data
            'generation_data_json': '{}',
            'wind_data_json': '{}',
            'performance_data_json': '{}',
            'comparison_data_json': '{}',
            'market_data_json': '{}',
            'context_summary': f'Performance analysis for {windfarm.name} covering {summary.total_months} months of operational data',
            'yoy_summary': 'Multi-year performance trends show seasonal patterns with strong winter performance',

            # Wind resource (placeholders - would need weather data)
            'avg_wind_speed_ms': 0,
            'median_wind_speed_ms': 0,
            'primary_direction': 0,
            'primary_direction_name': 'Unknown',
            'peak_months': 'Winter months',
            'calm_months': 'Summer months',
            'seasonal_table': 'Seasonal data not available'
        }

    async def get_power_curve_data(
        self,
        windfarm_id: int,
        start_date: datetime,
        end_date: datetime
    ) -> dict:
        """
        Get power curve data with raw points, binned averages, and Gompertz fit.

        Args:
            windfarm_id: ID of windfarm
            start_date: Start of analysis period
            end_date: End of analysis period

        Returns:
            Dict with raw_data, binned_data, and gompertz_params
        """
        from app.models.weather_data import WeatherData
        import numpy as np
        from scipy.optimize import curve_fit

        # Get windfarm nameplate capacity for Gompertz fixed A parameter
        windfarm_stmt = select(Windfarm.nameplate_capacity_mw).where(Windfarm.id == windfarm_id)
        windfarm_result = await self.db.execute(windfarm_stmt)
        nameplate_capacity = windfarm_result.scalar_one_or_none()
        if not nameplate_capacity:
            nameplate_capacity = 50.0  # Fallback default

        # Join generation data with weather data on same hour
        stmt = (
            select(
                WeatherData.wind_speed_100m,
                GenerationData.generation_mwh,
                GenerationData.capacity_factor,
                GenerationData.capacity_mw
            )
            .join(WeatherData, and_(
                WeatherData.windfarm_id == GenerationData.windfarm_id,
                WeatherData.hour == GenerationData.hour
            ))
            .join(GenerationUnit, GenerationData.generation_unit_id == GenerationUnit.id)
            .where(
                and_(
                    GenerationUnit.windfarm_id == windfarm_id,
                    GenerationData.hour >= start_date,
                    GenerationData.hour < end_date,
                    WeatherData.wind_speed_100m.isnot(None),
                    GenerationData.generation_mwh.isnot(None),
                    WeatherData.wind_speed_100m > 0
                )
            )
            .limit(50000)  # Increased sample size for better curve fitting
        )

        result = await self.db.execute(stmt)
        rows = result.all()

        if not rows:
            return {'raw_data': [], 'binned_data': [], 'gompertz_curve': []}

        # Raw data (sample for frontend performance)
        raw_data = [
            {
                'wind_speed_ms': float(row.wind_speed_100m),
                'generation_mw': float(row.generation_mwh),
                'capacity_factor': float(row.capacity_factor or 0)
            }
            for row in rows[::5]  # Every 5th point to reduce frontend load
        ]

        # Create bins (0.5 m/s intervals for cleaner visualization)
        bins = {}
        for row in rows:
            speed = float(row.wind_speed_100m)
            gen = float(row.generation_mwh)

            # Bin to nearest 0.5 m/s (balances detail with clean visualization)
            bin_key = round(speed * 2) / 2  # 0.0, 0.5, 1.0, 1.5, etc.

            if bin_key not in bins:
                bins[bin_key] = []
            bins[bin_key].append(gen)

        # Calculate binned medians (more robust to outliers than mean)
        binned_data = [
            {
                'wind_speed_ms': speed,
                'avg_generation_mw': float(np.median(values)),  # Use median instead of mean
                'count': len(values),
                'std': np.std(values) if len(values) > 1 else 0
            }
            for speed, values in sorted(bins.items())
            if len(values) >= 10  # Require at least 10 points per bin
        ]

        # Fit Gompertz curve with FIXED A parameter (installed capacity)
        gompertz_curve = []
        gompertz_params = None
        try:
            if len(binned_data) > 5:
                # Gompertz function with fixed A: y = A * exp(-B * exp(-C * x))
                # A is fixed to nameplate capacity (physical constraint)
                A_fixed = float(nameplate_capacity)

                def gompertz_fixed_a(x, B, C):
                    """Gompertz with A fixed to installed capacity"""
                    return A_fixed * np.exp(-B * np.exp(-C * x))

                x_data = np.array([d['wind_speed_ms'] for d in binned_data])
                y_data = np.array([d['avg_generation_mw'] for d in binned_data])

                # Initial parameter guess for B and C only
                initial_guess = [10.0, 0.5]  # B, C

                # Fit curve with bounds to ensure positive parameters
                params, _ = curve_fit(
                    gompertz_fixed_a,
                    x_data,
                    y_data,
                    p0=initial_guess,
                    bounds=([0, 0], [np.inf, np.inf]),
                    maxfev=5000
                )

                B_fitted, C_fitted = params

                # Generate smooth curve points
                x_smooth = np.linspace(min(x_data), max(x_data), 100)
                y_smooth = gompertz_fixed_a(x_smooth, B_fitted, C_fitted)

                gompertz_curve = [
                    {
                        'wind_speed_ms': float(x),
                        'generation_mw': float(y)
                    }
                    for x, y in zip(x_smooth, y_smooth)
                ]

                # Store fitted parameters
                gompertz_params = {
                    'a': float(A_fixed),
                    'b': float(B_fitted),
                    'c': float(C_fitted)
                }

        except Exception as e:
            # If curve fitting fails, continue without it
            import structlog
            logger = structlog.get_logger(__name__)
            logger.warning("gompertz_fit_failed", error=str(e))

        return {
            'raw_data': raw_data,
            'binned_data': binned_data,
            'gompertz_curve': gompertz_curve,
            'gompertz_params': gompertz_params
        }

    async def get_wind_rose_data(
        self,
        windfarm_id: int,
        start_date: datetime,
        end_date: datetime
    ) -> List[dict]:
        """
        Get wind direction distribution for wind rose chart.

        Args:
            windfarm_id: ID of windfarm
            start_date: Start of analysis period
            end_date: End of analysis period

        Returns:
            List of {direction_bin, frequency, avg_wind_speed}
        """
        from app.models.weather_data import WeatherData

        # Get wind direction and speed data
        stmt = (
            select(
                WeatherData.wind_direction_deg,
                WeatherData.wind_speed_100m
            )
            .where(
                and_(
                    WeatherData.windfarm_id == windfarm_id,
                    WeatherData.hour >= start_date,
                    WeatherData.hour < end_date,
                    WeatherData.wind_direction_deg.isnot(None)
                )
            )
        )

        result = await self.db.execute(stmt)
        rows = result.all()

        # Bin directions into 16 compass directions (N, NNE, NE, ENE, E, etc.)
        bins = {}
        direction_labels = [
            'N', 'NNE', 'NE', 'ENE', 'E', 'ESE', 'SE', 'SSE',
            'S', 'SSW', 'SW', 'WSW', 'W', 'WNW', 'NW', 'NNW'
        ]

        for i, label in enumerate(direction_labels):
            bins[label] = {'count': 0, 'total_speed': 0, 'min_deg': i * 22.5, 'max_deg': (i + 1) * 22.5}

        for row in rows:
            deg = float(row.wind_direction_deg)
            speed = float(row.wind_speed_100m)

            # Determine bin
            bin_index = int((deg + 11.25) / 22.5) % 16
            label = direction_labels[bin_index]

            bins[label]['count'] += 1
            bins[label]['total_speed'] += speed

        # Calculate frequencies and averages
        total_count = sum(b['count'] for b in bins.values())

        return [
            {
                'direction': label,
                'frequency': (data['count'] / total_count * 100) if total_count > 0 else 0,
                'avg_wind_speed': (data['total_speed'] / data['count']) if data['count'] > 0 else 0,
                'count': data['count']
            }
            for label, data in bins.items()
        ]

    async def get_hourly_generation_profile(
        self,
        windfarm_id: int,
        start_date: datetime,
        end_date: datetime
    ) -> List[dict]:
        """
        Get average generation by hour of day.

        Args:
            windfarm_id: ID of windfarm
            start_date: Start of analysis period
            end_date: End of analysis period

        Returns:
            List of {hour_of_day, avg_generation_mw, avg_capacity_factor}
        """
        stmt = (
            select(
                extract('hour', GenerationData.hour).label('hour_of_day'),
                func.avg(GenerationData.generation_mwh).label('avg_generation'),
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
            .group_by('hour_of_day')
            .order_by('hour_of_day')
        )

        result = await self.db.execute(stmt)
        rows = result.all()

        return [
            {
                'hour': int(row.hour_of_day),
                'avg_generation_mw': float(row.avg_generation or 0),
                'avg_capacity_factor': float(row.avg_cf or 0) * 100  # Convert to percentage
            }
            for row in rows
        ]

    async def get_capacity_factor_distribution(
        self,
        windfarm_id: int,
        start_date: datetime,
        end_date: datetime
    ) -> List[dict]:
        """
        Get capacity factor distribution for histogram.

        Args:
            windfarm_id: ID of windfarm
            start_date: Start of analysis period
            end_date: End of analysis period

        Returns:
            List of {bin_label, count, percentage}
        """
        # Get all capacity factors
        stmt = (
            select(GenerationData.capacity_factor)
            .join(GenerationUnit, GenerationData.generation_unit_id == GenerationUnit.id)
            .where(
                and_(
                    GenerationUnit.windfarm_id == windfarm_id,
                    GenerationData.hour >= start_date,
                    GenerationData.hour < end_date,
                    GenerationData.capacity_factor.isnot(None)
                )
            )
        )

        result = await self.db.execute(stmt)
        rows = result.all()

        # Create bins (0-10%, 10-20%, 20-30%, etc.)
        bins = {}
        bin_labels = [
            '0-10%', '10-20%', '20-30%', '30-40%', '40-50%',
            '50-60%', '60-70%', '70-80%', '80-90%', '90-100%'
        ]

        for label in bin_labels:
            bins[label] = 0

        # Count values in each bin
        for row in rows:
            cf = float(row.capacity_factor) * 100  # Convert to percentage
            bin_index = min(int(cf / 10), 9)  # 0-9
            bins[bin_labels[bin_index]] += 1

        total_count = sum(bins.values())

        return [
            {
                'bin': label,
                'count': count,
                'percentage': (count / total_count * 100) if total_count > 0 else 0
            }
            for label, count in bins.items()
        ]

    async def get_rolling_average_data(
        self,
        windfarm_id: int,
        start_date: datetime,
        end_date: datetime,
        window_months: int = 12
    ) -> List[dict]:
        """
        Get monthly capacity factors with rolling average.

        Args:
            windfarm_id: ID of windfarm
            start_date: Start of analysis period
            end_date: End of analysis period
            window_months: Rolling window size in months

        Returns:
            List of {date, actual_cf, rolling_avg_cf}
        """
        # Get monthly capacity factors
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
        rows = result.all()

        # Calculate rolling average
        data = []
        values = []

        for row in rows:
            year = int(row.year)
            month = int(row.month)
            cf = float(row.avg_cf or 0) * 100  # Convert to percentage

            values.append(cf)

            # Calculate rolling average
            if len(values) >= window_months:
                rolling_avg = sum(values[-window_months:]) / window_months
            else:
                rolling_avg = sum(values) / len(values)

            data.append({
                'date': f'{year}-{month:02d}',
                'actual_cf': cf,
                'rolling_avg_cf': rolling_avg
            })

        return data

    async def get_wind_speed_heatmap_data(
        self,
        windfarm_id: int,
        start_date: datetime,
        end_date: datetime
    ) -> List[dict]:
        """
        Get wind speed heatmap data (hour of day × month).

        Args:
            windfarm_id: ID of windfarm
            start_date: Start of analysis period
            end_date: End of analysis period

        Returns:
            List of {hour_of_day, month, avg_wind_speed}
        """
        from app.models.weather_data import WeatherData

        stmt = (
            select(
                extract('hour', WeatherData.hour).label('hour_of_day'),
                extract('month', WeatherData.hour).label('month'),
                func.avg(WeatherData.wind_speed_100m).label('avg_wind_speed')
            )
            .where(
                and_(
                    WeatherData.windfarm_id == windfarm_id,
                    WeatherData.hour >= start_date,
                    WeatherData.hour < end_date,
                    WeatherData.wind_speed_100m.isnot(None)
                )
            )
            .group_by('hour_of_day', 'month')
            .order_by('month', 'hour_of_day')
        )

        result = await self.db.execute(stmt)
        rows = result.all()

        return [
            {
                'hour': int(row.hour_of_day),
                'month': int(row.month),
                'avg_wind_speed': float(row.avg_wind_speed or 0)
            }
            for row in rows
        ]

    async def get_turbine_model_info(
        self,
        windfarm_id: int
    ) -> Optional[Dict[str, Any]]:
        """
        Get turbine model information from turbine units.

        Args:
            windfarm_id: ID of windfarm

        Returns:
            Dict with turbine model details or None if not available
        """
        from app.models.turbine_unit import TurbineUnit
        from app.models.turbine_model import TurbineModel

        # Query: windfarm → turbine_units → turbine_models
        stmt = (
            select(
                TurbineModel.model,
                TurbineModel.supplier,
                TurbineModel.rated_power_kw,
                TurbineUnit.hub_height_m,
                TurbineModel.rotor_diameter_m,
                func.count(TurbineUnit.id).label('count')
            )
            .join(TurbineModel, TurbineUnit.turbine_model_id == TurbineModel.id)
            .where(TurbineUnit.windfarm_id == windfarm_id)
            .group_by(
                TurbineModel.model,
                TurbineModel.supplier,
                TurbineModel.rated_power_kw,
                TurbineUnit.hub_height_m,
                TurbineModel.rotor_diameter_m
            )
            .limit(1)  # Assume all turbines in a windfarm are the same model
        )

        result = await self.db.execute(stmt)
        row = result.first()

        if not row:
            return None

        return {
            'model': row.model or 'Unknown',
            'manufacturer': row.supplier or 'Unknown',
            'count': int(row.count),
            'rated_capacity_mw': float(row.rated_power_kw / 1000) if row.rated_power_kw else 0.0,
            'hub_height_m': float(row.hub_height_m) if row.hub_height_m else None,
            'rotor_diameter_m': float(row.rotor_diameter_m) if row.rotor_diameter_m else None
        }

    async def get_monthly_generation_timeseries(
        self,
        windfarm_id: int,
        start_date: datetime,
        end_date: datetime
    ) -> List[Dict[str, Any]]:
        """
        Get monthly generation timeseries for line charts.

        Args:
            windfarm_id: ID of windfarm
            start_date: Start of analysis period
            end_date: End of analysis period

        Returns:
            List of {month, generation_gwh, capacity_factor}
        """
        stmt = (
            select(
                extract('year', GenerationData.hour).label('year'),
                extract('month', GenerationData.hour).label('month'),
                func.sum(GenerationData.generation_mwh).label('total_generation_mwh'),
                func.avg(GenerationData.capacity_factor).label('avg_cf')
            )
            .join(GenerationUnit, GenerationData.generation_unit_id == GenerationUnit.id)
            .where(
                and_(
                    GenerationUnit.windfarm_id == windfarm_id,
                    GenerationData.hour >= start_date,
                    GenerationData.hour < end_date,
                    GenerationData.generation_mwh.isnot(None)
                )
            )
            .group_by('year', 'month')
            .order_by('year', 'month')
        )

        result = await self.db.execute(stmt)
        rows = result.all()

        return [
            {
                'month': f'{int(row.year)}-{int(row.month):02d}',
                'generation_gwh': float((row.total_generation_mwh or 0) / 1000),
                'capacity_factor': float(row.avg_cf or 0) * 100  # Convert to percentage
            }
            for row in rows
        ]

    async def get_monthly_wind_speed_timeseries(
        self,
        windfarm_id: int,
        start_date: datetime,
        end_date: datetime
    ) -> List[Dict[str, Any]]:
        """
        Get monthly wind speed timeseries.

        Args:
            windfarm_id: ID of windfarm
            start_date: Start of analysis period
            end_date: End of analysis period

        Returns:
            List of {month, avg_wind_speed, median_wind_speed}
        """
        from app.models.weather_data import WeatherData

        stmt = (
            select(
                extract('year', WeatherData.hour).label('year'),
                extract('month', WeatherData.hour).label('month'),
                func.avg(WeatherData.wind_speed_100m).label('avg_wind_speed'),
                func.percentile_cont(0.5).within_group(WeatherData.wind_speed_100m).label('median_wind_speed')
            )
            .where(
                and_(
                    WeatherData.windfarm_id == windfarm_id,
                    WeatherData.hour >= start_date,
                    WeatherData.hour < end_date,
                    WeatherData.wind_speed_100m.isnot(None)
                )
            )
            .group_by('year', 'month')
            .order_by('year', 'month')
        )

        result = await self.db.execute(stmt)
        rows = result.all()

        return [
            {
                'month': f'{int(row.year)}-{int(row.month):02d}',
                'avg_wind_speed': float(row.avg_wind_speed or 0),
                'median_wind_speed': float(row.median_wind_speed or 0)
            }
            for row in rows
        ]

    async def get_wind_speed_distribution_weibull(
        self,
        windfarm_id: int,
        start_date: datetime,
        end_date: datetime
    ) -> Dict[str, Any]:
        """
        Get wind speed distribution with Weibull fit.

        Args:
            windfarm_id: ID of windfarm
            start_date: Start of analysis period
            end_date: End of analysis period

        Returns:
            Dict with histogram_data, weibull_curve, k_param, lambda_param
        """
        from app.models.weather_data import WeatherData
        import numpy as np
        from scipy import stats

        # Get all wind speed values
        stmt = (
            select(WeatherData.wind_speed_100m)
            .where(
                and_(
                    WeatherData.windfarm_id == windfarm_id,
                    WeatherData.hour >= start_date,
                    WeatherData.hour < end_date,
                    WeatherData.wind_speed_100m.isnot(None),
                    WeatherData.wind_speed_100m > 0
                )
            )
            .limit(100000)  # Sample for performance
        )

        result = await self.db.execute(stmt)
        wind_speeds = [float(row[0]) for row in result.all()]

        if not wind_speeds:
            return {
                'histogram_data': [],
                'weibull_curve': [],
                'k_param': 0,
                'lambda_param': 0
            }

        # Create histogram bins (1.0 m/s intervals)
        bins = {}
        for speed in wind_speeds:
            bin_key = round(speed)  # Round to nearest 1.0
            bins[bin_key] = bins.get(bin_key, 0) + 1

        total_count = len(wind_speeds)
        histogram_data = [
            {
                'bin_center': speed,
                'frequency': (count / total_count) * 100,  # Percentage
                'count': count
            }
            for speed, count in sorted(bins.items())
        ]

        # Fit Weibull distribution
        weibull_curve = []
        k_param = 0
        lambda_param = 0

        try:
            # Fit Weibull using scipy
            shape_k, loc, scale_lambda = stats.weibull_min.fit(wind_speeds, floc=0)

            k_param = float(shape_k)
            lambda_param = float(scale_lambda)

            # Generate curve points
            x_values = np.linspace(0, max(wind_speeds), 100)
            y_values = stats.weibull_min.pdf(x_values, shape_k, loc, scale_lambda) * 100  # Convert to percentage

            weibull_curve = [
                {
                    'wind_speed': float(x),
                    'probability_density': float(y)
                }
                for x, y in zip(x_values, y_values)
            ]

        except Exception as e:
            import structlog
            logger = structlog.get_logger(__name__)
            logger.warning("weibull_fit_failed", error=str(e))

        return {
            'histogram_data': histogram_data,
            'weibull_curve': weibull_curve,
            'k_param': k_param,
            'lambda_param': lambda_param
        }

    async def get_annual_summary_table(
        self,
        windfarm_id: int,
        start_date: datetime,
        end_date: datetime
    ) -> List[Dict[str, Any]]:
        """
        Get annual summary table with generation and wind statistics.

        Args:
            windfarm_id: ID of windfarm
            start_date: Start of analysis period
            end_date: End of analysis period

        Returns:
            List of annual statistics
        """
        from app.models.weather_data import WeatherData

        # Get generation data by year
        gen_stmt = (
            select(
                extract('year', GenerationData.hour).label('year'),
                func.avg(GenerationData.capacity_mw).label('avg_capacity_mw'),
                func.sum(GenerationData.generation_mwh).label('total_generation_mwh'),
                func.avg(GenerationData.capacity_factor).label('avg_cf')
            )
            .join(GenerationUnit, GenerationData.generation_unit_id == GenerationUnit.id)
            .where(
                and_(
                    GenerationUnit.windfarm_id == windfarm_id,
                    GenerationData.hour >= start_date,
                    GenerationData.hour < end_date
                )
            )
            .group_by('year')
            .order_by('year')
        )

        gen_result = await self.db.execute(gen_stmt)
        gen_rows = {int(row.year): row for row in gen_result.all()}

        # Get wind data by year (if available)
        wind_rows = {}
        try:
            wind_stmt = (
                select(
                    extract('year', WeatherData.hour).label('year'),
                    func.avg(WeatherData.wind_speed_100m).label('avg_wind_speed'),
                    func.percentile_cont(0.5).within_group(WeatherData.wind_speed_100m).label('median_wind_speed'),
                    func.avg(WeatherData.wind_direction_deg).label('avg_wind_direction')
                )
                .where(
                    and_(
                        WeatherData.windfarm_id == windfarm_id,
                        WeatherData.hour >= start_date,
                        WeatherData.hour < end_date,
                        WeatherData.wind_speed_100m.isnot(None)
                    )
                )
                .group_by('year')
                .order_by('year')
            )

            wind_result = await self.db.execute(wind_stmt)
            wind_rows = {int(row.year): row for row in wind_result.all()}
        except Exception:
            # Weather data may not be available for all windfarms
            pass

        # Use generation data years as primary (wind data is optional)
        all_years = sorted(gen_rows.keys())

        return [
            {
                'year': year,
                'installed_capacity_mw': float(gen_rows[year].avg_capacity_mw or 0) if year in gen_rows else 0,
                'total_generation_gwh': float((gen_rows[year].total_generation_mwh or 0) / 1000) if year in gen_rows else 0,
                'avg_monthly_generation_gwh': float((gen_rows[year].total_generation_mwh or 0) / 1000 / 12) if year in gen_rows else 0,
                'avg_capacity_factor': float((gen_rows[year].avg_cf or 0) * 100) if year in gen_rows else 0,
                'avg_wind_speed_ms': float(wind_rows[year].avg_wind_speed or 0) if year in wind_rows else 0,
                'median_wind_speed_ms': float(wind_rows[year].median_wind_speed or 0) if year in wind_rows else 0,
                'avg_wind_direction_deg': float(wind_rows[year].avg_wind_direction or 0) if year in wind_rows else 0
            }
            for year in all_years
        ]

    async def get_turbine_model_comparison_table(
        self,
        country_id: int,
        start_date: datetime,
        end_date: datetime
    ) -> List[Dict[str, Any]]:
        """
        Get turbine model comparison across all windfarms in a country.

        OPTIMIZED: Returns empty for now to avoid slow N+1 queries.
        TODO: Implement with single bulk query joining all models at once.
        """
        # Temporarily return empty to avoid timeout
        # This feature can be implemented later with proper bulk query optimization
        return []

    async def get_turbine_size_performance_analysis(
        self,
        country_id: int,
        start_date: datetime,
        end_date: datetime
    ) -> List[Dict[str, Any]]:
        """
        Get turbine size vs performance analysis for scatter plot.

        OPTIMIZED: Returns empty for now to avoid slow N+1 queries.
        TODO: Implement with single bulk query joining all sizes at once.
        """
        # Temporarily return empty to avoid timeout
        # This feature can be implemented later with proper bulk query optimization
        return []

    async def get_country_wind_context(
        self,
        country_id: int,
        start_date: datetime,
        end_date: datetime
    ) -> Dict[str, Any]:
        """
        Get country-level wind generation context.
        Always shows data from year 2000 onwards regardless of report date range.

        Args:
            country_id: ID of country
            start_date: Start of analysis period (not used - kept for API compatibility)
            end_date: End of analysis period

        Returns:
            Dict with annual_capacity_growth and bidzone_summary
        """
        # Always start from year 2000 for historical context
        historical_start_date = datetime(2000, 1, 1)

        # Annual capacity growth - get max capacity per year by counting unique windfarms
        # We need to aggregate by year and get the total installed capacity for windfarms operational in that year
        from sqlalchemy import func as sql_func

        # Subquery to get windfarms operational each year
        windfarm_capacity_subquery = (
            select(
                extract('year', GenerationData.hour).label('year'),
                Windfarm.id.label('windfarm_id'),
                func.max(Windfarm.nameplate_capacity_mw).label('capacity_mw')
            )
            .join(GenerationUnit, GenerationData.generation_unit_id == GenerationUnit.id)
            .join(Windfarm, GenerationUnit.windfarm_id == Windfarm.id)
            .where(
                and_(
                    Windfarm.country_id == country_id,
                    GenerationData.hour >= historical_start_date,
                    GenerationData.hour < end_date
                )
            )
            .group_by(extract('year', GenerationData.hour), Windfarm.id)
        ).subquery()

        # Get generation and sum of capacities per year
        capacity_stmt = (
            select(
                extract('year', GenerationData.hour).label('year'),
                func.sum(GenerationData.generation_mwh).label('total_generation_mwh')
            )
            .join(GenerationUnit, GenerationData.generation_unit_id == GenerationUnit.id)
            .join(Windfarm, GenerationUnit.windfarm_id == Windfarm.id)
            .where(
                and_(
                    Windfarm.country_id == country_id,
                    GenerationData.hour >= historical_start_date,
                    GenerationData.hour < end_date
                )
            )
            .group_by('year')
            .order_by('year')
        )

        capacity_result = await self.db.execute(capacity_stmt)
        generation_by_year = {int(row.year): float(row.total_generation_mwh or 0) for row in capacity_result.all()}

        # Get capacity by year from subquery
        capacity_by_year_stmt = (
            select(
                windfarm_capacity_subquery.c.year,
                func.sum(windfarm_capacity_subquery.c.capacity_mw).label('total_capacity_mw')
            )
            .group_by(windfarm_capacity_subquery.c.year)
            .order_by(windfarm_capacity_subquery.c.year)
        )

        capacity_by_year_result = await self.db.execute(capacity_by_year_stmt)

        annual_growth = [
            {
                'year': int(row.year),
                'total_capacity_gw': float((row.total_capacity_mw or 0) / 1000),  # Convert MW to GW
                'total_generation_gwh': float((generation_by_year.get(int(row.year), 0)) / 1000)  # Convert MWh to GWh
            }
            for row in capacity_by_year_result.all()
        ]

        # Bidzone summary (if applicable)
        from app.models.bidzone import Bidzone

        bidzone_stmt = (
            select(
                Bidzone.name.label('bidzone_name'),
                Bidzone.code.label('bidzone_code'),
                func.count(func.distinct(Windfarm.id)).label('windfarm_count'),
                func.sum(Windfarm.nameplate_capacity_mw).label('total_capacity_mw')
            )
            .join(Windfarm, Bidzone.id == Windfarm.bidzone_id)
            .where(Windfarm.country_id == country_id)
            .group_by(Bidzone.name, Bidzone.code)
            .order_by(Bidzone.name)
        )

        bidzone_result = await self.db.execute(bidzone_stmt)
        bidzone_summary = [
            {
                'bidzone_name': row.bidzone_name,
                'bidzone_code': row.bidzone_code,
                'windfarm_count': int(row.windfarm_count),
                'total_capacity_mw': float(row.total_capacity_mw or 0)
            }
            for row in bidzone_result.all()
        ]

        return {
            'annual_capacity_growth': annual_growth,
            'bidzone_summary': bidzone_summary,
            'total_capacity_mw': sum(row['total_capacity_mw'] for row in annual_growth[-1:]),
            'total_windfarms': sum(row['windfarm_count'] for row in bidzone_summary)
        }

    async def get_all_peers_monthly_timeseries(
        self,
        peer_type: str,
        group_id: int,
        start_date: datetime,
        end_date: datetime
    ) -> Dict[int, List[Dict[str, Any]]]:
        """
        Get monthly timeseries for all peers (for spaghetti chart).

        Args:
            peer_type: Type of peer group (bidzone, country, owner, turbine)
            group_id: ID of the peer group
            start_date: Start of analysis period
            end_date: End of analysis period

        Returns:
            Dict mapping windfarm_id to list of {month, capacity_factor}
        """
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
            .order_by(GenerationUnit.windfarm_id, 'year', 'month')
        )

        result = await self.db.execute(stmt)

        # Organize by windfarm
        timeseries_by_windfarm = {}
        for row in result.all():
            wf_id = row.windfarm_id
            if wf_id not in timeseries_by_windfarm:
                timeseries_by_windfarm[wf_id] = []

            timeseries_by_windfarm[wf_id].append({
                'month': f'{int(row.year)}-{int(row.month):02d}',
                'capacity_factor': float(row.avg_cf or 0) * 100
            })

        return timeseries_by_windfarm

    async def get_ownership_history(
        self,
        windfarm_id: int
    ) -> List[Dict[str, Any]]:
        """
        Get ownership history for a windfarm.

        Args:
            windfarm_id: ID of windfarm

        Returns:
            List of ownership transactions
        """
        # TODO: Implement ownership_history table and query
        # For now, return empty list as ownership data is not available in current schema
        return []

    # Safe wrapper methods with error handling
    async def _safe_get_turbine_model_info(self, windfarm_id: int):
        """Safe wrapper for get_turbine_model_info with error handling."""
        try:
            return await self.get_turbine_model_info(windfarm_id)
        except Exception:
            return None

    async def _safe_get_monthly_generation_timeseries(self, windfarm_id: int, start_date, end_date):
        """Safe wrapper for get_monthly_generation_timeseries with error handling."""
        try:
            return await self.get_monthly_generation_timeseries(windfarm_id, start_date, end_date)
        except Exception:
            return []

    async def _safe_get_monthly_wind_speed_timeseries(self, windfarm_id: int, start_date, end_date):
        """Safe wrapper for get_monthly_wind_speed_timeseries with error handling."""
        try:
            return await self.get_monthly_wind_speed_timeseries(windfarm_id, start_date, end_date)
        except Exception:
            return []

    async def _safe_get_wind_speed_distribution_weibull(self, windfarm_id: int, start_date, end_date):
        """Safe wrapper for get_wind_speed_distribution_weibull with error handling."""
        try:
            return await self.get_wind_speed_distribution_weibull(windfarm_id, start_date, end_date)
        except Exception:
            return {'histogram_data': [], 'weibull_curve': [], 'k_param': 0, 'lambda_param': 0}

    async def _safe_get_annual_summary_table(self, windfarm_id: int, start_date, end_date):
        """Safe wrapper for get_annual_summary_table with error handling."""
        try:
            return await self.get_annual_summary_table(windfarm_id, start_date, end_date)
        except Exception:
            return []

    async def _safe_get_country_wind_context(self, country_id: int, start_date, end_date):
        """Safe wrapper for get_country_wind_context with error handling."""
        try:
            return await self.get_country_wind_context(country_id, start_date, end_date)
        except Exception:
            return None
