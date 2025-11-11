"""Statistical analysis utilities for report generation."""

from typing import List, Dict, Tuple
import numpy as np
from app.schemas.windfarm_report import BoxPlotData


class StatisticalAnalysis:
    """Reusable statistical functions for performance analysis."""

    @staticmethod
    def calculate_box_plot_data(
        values: List[float],
        group_name: str
    ) -> BoxPlotData:
        """
        Calculate box plot statistics from a list of values.

        Args:
            values: List of numeric values
            group_name: Name/label for this data group

        Returns:
            BoxPlotData with quartiles, whiskers, and outliers
        """
        if not values:
            return BoxPlotData(
                group=group_name,
                min=0,
                q1=0,
                median=0,
                q3=0,
                max=0,
                outliers=[],
                mean=0,
                std_dev=0
            )

        arr = np.array(values)

        # Calculate quartiles
        q1 = float(np.percentile(arr, 25))
        median = float(np.percentile(arr, 50))
        q3 = float(np.percentile(arr, 75))
        mean = float(np.mean(arr))
        std_dev = float(np.std(arr))

        # Calculate IQR and whiskers
        iqr = q3 - q1
        lower_whisker = q1 - 1.5 * iqr
        upper_whisker = q3 + 1.5 * iqr

        # Find outliers
        outliers = [float(v) for v in arr if v < lower_whisker or v > upper_whisker]

        # Whisker limits (don't extend beyond actual data)
        min_val = float(max(lower_whisker, arr.min()))
        max_val = float(min(upper_whisker, arr.max()))

        return BoxPlotData(
            group=group_name,
            min=min_val,
            q1=q1,
            median=median,
            q3=q3,
            max=max_val,
            outliers=outliers,
            mean=mean,
            std_dev=std_dev
        )

    @staticmethod
    def calculate_peer_band(
        monthly_data: Dict[str, Dict[int, float]]
    ) -> Tuple[Dict[str, float], Dict[str, float]]:
        """
        Calculate min/max envelope across peer group for each time point.

        Args:
            monthly_data: {
                'month_label': {
                    windfarm_id: capacity_factor,
                    ...
                }
            }

        Returns:
            Tuple of (min_values, max_values) dicts keyed by month_label
        """
        min_values = {}
        max_values = {}

        for month_label, windfarm_values in monthly_data.items():
            if windfarm_values:
                values = list(windfarm_values.values())
                min_values[month_label] = float(min(values))
                max_values[month_label] = float(max(values))
            else:
                min_values[month_label] = 0.0
                max_values[month_label] = 0.0

        return min_values, max_values

    @staticmethod
    def calculate_peer_average(
        monthly_data: Dict[str, Dict[int, float]]
    ) -> Dict[str, float]:
        """
        Calculate average across peer group for each time point.

        Args:
            monthly_data: {
                'month_label': {
                    windfarm_id: capacity_factor,
                    ...
                }
            }

        Returns:
            Dict of average values keyed by month_label
        """
        averages = {}

        for month_label, windfarm_values in monthly_data.items():
            if windfarm_values:
                values = list(windfarm_values.values())
                averages[month_label] = float(np.mean(values))
            else:
                averages[month_label] = 0.0

        return averages

    @staticmethod
    def rank_values(values: List[Tuple[int, float]]) -> Dict[int, int]:
        """
        Rank items by value (descending - higher is better).

        Args:
            values: List of (id, value) tuples

        Returns:
            Dict mapping id to rank (1 = highest value)
        """
        # Sort by value descending
        sorted_values = sorted(values, key=lambda x: x[1], reverse=True)

        # Assign ranks
        rankings = {}
        for rank, (item_id, _) in enumerate(sorted_values, start=1):
            rankings[item_id] = rank

        return rankings

    @staticmethod
    def calculate_performance_metrics(values: List[float]) -> Dict[str, float]:
        """
        Calculate comprehensive performance statistics.

        Args:
            values: List of numeric values (e.g., monthly capacity factors)

        Returns:
            Dict with various statistical metrics
        """
        if not values:
            return {
                'mean': 0.0,
                'median': 0.0,
                'std_dev': 0.0,
                'min': 0.0,
                'max': 0.0,
                'range': 0.0,
                'coefficient_of_variation': 0.0
            }

        arr = np.array(values)

        mean = float(np.mean(arr))
        std_dev = float(np.std(arr))
        cv = (std_dev / mean * 100) if mean != 0 else 0.0

        return {
            'mean': mean,
            'median': float(np.median(arr)),
            'std_dev': std_dev,
            'min': float(arr.min()),
            'max': float(arr.max()),
            'range': float(arr.max() - arr.min()),
            'coefficient_of_variation': cv
        }

    @staticmethod
    def generate_highlights(
        windfarm_name: str,
        target_stats: Dict[str, float],
        peer_stats: Dict[str, float],
        rankings: Dict[str, int],
        total_peers: Dict[str, int]
    ) -> List[str]:
        """
        Generate text highlights summarizing performance.

        Args:
            windfarm_name: Name of target windfarm
            target_stats: Statistics for target windfarm
            peer_stats: Statistics for peer group
            rankings: Ranks within different peer groups
            total_peers: Total number of peers in each group

        Returns:
            List of highlight strings
        """
        highlights = []

        # Capacity factor comparison
        cf_diff = target_stats['mean'] - peer_stats['mean']
        if cf_diff > 0:
            highlights.append(
                f"Average monthly capacity factor of {target_stats['mean']:.1f}% "
                f"outperforms peer group average by {cf_diff:.1f} percentage points"
            )
        else:
            highlights.append(
                f"Average monthly capacity factor of {target_stats['mean']:.1f}% "
                f"is {abs(cf_diff):.1f} percentage points below peer group average"
            )

        # Ranking highlights
        if 'country_rank' in rankings and 'country_total' in total_peers:
            country_rank = rankings['country_rank']
            country_total = total_peers['country_total']
            percentile = (1 - (country_rank - 1) / country_total) * 100

            if percentile >= 75:
                highlights.append(
                    f"Ranks {country_rank} out of {country_total} nationally, "
                    f"placing in the top quartile of wind farms"
                )
            elif percentile >= 50:
                highlights.append(
                    f"Ranks {country_rank} out of {country_total} nationally, "
                    f"performing above median"
                )

        # Variability comparison
        if target_stats['coefficient_of_variation'] < peer_stats['coefficient_of_variation']:
            highlights.append(
                f"Shows more consistent performance (CV: {target_stats['coefficient_of_variation']:.1f}%) "
                f"compared to peer group average (CV: {peer_stats['coefficient_of_variation']:.1f}%)"
            )

        # Performance range
        highlights.append(
            f"Monthly capacity factor ranges from {target_stats['min']:.1f}% to {target_stats['max']:.1f}%, "
            f"with median of {target_stats['median']:.1f}%"
        )

        return highlights
