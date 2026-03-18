"""
processing/dask_aggregator.py
==============================
Aggregates logs into time windows for anomaly detection.

Usage:
    from processing.dask_aggregator import LogAggregator
    
    aggregator = LogAggregator()
    metrics = aggregator.aggregate_by_window(df, window_minutes=5)
"""

import dask.dataframe as dd
import pandas as pd
from datetime import datetime, timedelta


class LogAggregator:
    """
    Aggregates log data into time windows for anomaly detection.
    """
    
    def __init__(self, window_minutes: int = 5):
        """
        Initialize the aggregator.
        
        Args:
            window_minutes: Size of aggregation window in minutes (default: 5)
        """
        self.window_minutes = window_minutes
    
    def aggregate_by_window(self, df: dd.DataFrame) -> pd.DataFrame:
        """
        Aggregate logs into time windows with various metrics.
        
        This creates the metrics that anomaly detection rules check against.
        
        Args:
            df: Dask DataFrame with logs
        
        Returns:
            Pandas DataFrame with aggregated metrics per service per window
        """
        print(f"⚙️  Aggregating logs into {self.window_minutes}-minute windows...")
        
        # Add time window column (floor timestamp to nearest N minutes)
        df['window'] = df['timestamp'].dt.floor(f'{self.window_minutes}min')
        
        # Count total logs per service per window
        total_counts = (
            df.groupby(['service', 'window'])
            .size()
            .to_frame('total_logs')
            .reset_index()
        )
        
        # Count ERROR-level logs per service per window
        error_counts = (
            df[df['level'] == 'ERROR']
            .groupby(['service', 'window'])
            .size()
            .to_frame('error_count')
            .reset_index()
        )
        
        # Count WARNING-level logs per service per window
        warning_counts = (
            df[df['level'] == 'WARNING']
            .groupby(['service', 'window'])
            .size()
            .to_frame('warning_count')
            .reset_index()
        )
        
        # Count CRITICAL-level logs per service per window
        critical_counts = (
            df[df['level'] == 'CRITICAL']
            .groupby(['service', 'window'])
            .size()
            .to_frame('critical_count')
            .reset_index()
        )
        
        # Calculate average response time per service per window
        avg_duration = (
            df[df['duration_ms'].notnull()]
            .groupby(['service', 'window'])['duration_ms']
            .mean()
            .to_frame('avg_duration_ms')
            .reset_index()
        )
        
        # Calculate max response time per service per window
        max_duration = (
            df[df['duration_ms'].notnull()]
            .groupby(['service', 'window'])['duration_ms']
            .max()
            .to_frame('max_duration_ms')
            .reset_index()
        )
        
        # Compute all aggregations (convert from Dask to Pandas)
        print("   Computing aggregations...")
        total_counts = total_counts.compute()
        error_counts = error_counts.compute()
        warning_counts = warning_counts.compute()
        critical_counts = critical_counts.compute()
        avg_duration = avg_duration.compute()
        max_duration = max_duration.compute()
        
        # Merge all metrics together
        print("   Merging metrics...")
        metrics = total_counts
        metrics = metrics.merge(error_counts, on=['service', 'window'], how='left')
        metrics = metrics.merge(warning_counts, on=['service', 'window'], how='left')
        metrics = metrics.merge(critical_counts, on=['service', 'window'], how='left')
        metrics = metrics.merge(avg_duration, on=['service', 'window'], how='left')
        metrics = metrics.merge(max_duration, on=['service', 'window'], how='left')
        
        # Fill NaN values with 0 (no errors/warnings in that window)
        metrics = metrics.fillna({
            'error_count': 0,
            'warning_count': 0,
            'critical_count': 0,
            'avg_duration_ms': 0,
            'max_duration_ms': 0,
        })
        
        # Convert counts to integers
        metrics['error_count'] = metrics['error_count'].astype(int)
        metrics['warning_count'] = metrics['warning_count'].astype(int)
        metrics['critical_count'] = metrics['critical_count'].astype(int)
        
        # Sort by service and window
        metrics = metrics.sort_values(['service', 'window']).reset_index(drop=True)
        
        print(f"✅ Created {len(metrics)} metric snapshots")
        
        return metrics
    
    def detect_service_silence(self, df: dd.DataFrame, 
                              all_services: list,
                              silence_threshold_minutes: int = 5) -> pd.DataFrame:
        """
        Detect services that have stopped logging (service_silence anomaly).
        
        Args:
            df: Dask DataFrame with logs
            all_services: List of all expected services
            silence_threshold_minutes: Minutes of no logs = silence
        
        Returns:
            DataFrame with silent services
        """
        print(f"🔍 Checking for silent services (>{silence_threshold_minutes} min)...")
        
        # Get the latest timestamp in the dataset
        latest_time = df['timestamp'].max().compute()
        
        # For each service, get their last log time
        last_seen = (
            df.groupby('service')['timestamp']
            .max()
            .compute()
        )
        
        silent_services = []
        
        for service in all_services:
            if service not in last_seen.index:
                # Service has NO logs at all
                silent_services.append({
                    'service': service,
                    'last_seen': None,
                    'silence_duration_minutes': float('inf'),
                })
            else:
                last_log = last_seen[service]
                silence_duration = (latest_time - last_log).total_seconds() / 60
                
                if silence_duration > silence_threshold_minutes:
                    silent_services.append({
                        'service': service,
                        'last_seen': last_log,
                        'silence_duration_minutes': silence_duration,
                    })
        
        if silent_services:
            print(f"⚠️  Found {len(silent_services)} silent service(s)")
        else:
            print(f"✅ All services are active")
        
        return pd.DataFrame(silent_services)
    
    def calculate_error_rate(self, metrics: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate error rate (errors / total logs) for each window.
        
        Args:
            metrics: Aggregated metrics DataFrame
        
        Returns:
            Metrics with added error_rate column
        """
        metrics['error_rate'] = (
            metrics['error_count'] / metrics['total_logs']
        ).fillna(0)
        
        return metrics


# =============================================================================
# CLI Interface (for testing)
# =============================================================================

def main():
    """Command-line interface for testing aggregation."""
    import argparse
    import sys
    from pathlib import Path
    
    # Add project root to path
    sys.path.insert(0, str(Path(__file__).parent.parent))
    
    from ingestion.dask_reader import LogIngestion
    
    parser = argparse.ArgumentParser(description="Aggregate logs with Dask")
    parser.add_argument(
        '--log-dir',
        type=str,
        default='data/raw_logs',
        help='Directory containing log files'
    )
    parser.add_argument(
        '--window-minutes',
        type=int,
        default=5,
        help='Aggregation window size in minutes (default: 5)'
    )
    parser.add_argument(
        '--output',
        type=str,
        help='Save metrics to CSV file'
    )
    
    args = parser.parse_args()
    
    print(f"\n{'='*60}")
    print(f"  Dask Log Aggregation — Milestone 2")
    print(f"{'='*60}\n")
    
    try:
        # Read logs
        ingestion = LogIngestion()
        df = ingestion.read_logs(args.log_dir)
        
        # Aggregate
        aggregator = LogAggregator(window_minutes=args.window_minutes)
        metrics = aggregator.aggregate_by_window(df)
        
        # Calculate error rate
        metrics = aggregator.calculate_error_rate(metrics)
        
        # Show results
        print("\n📊 Aggregated Metrics (first 10):")
        print(metrics.head(10).to_string(index=False))
        
        print(f"\n📈 Summary:")
        print(f"   Total windows: {len(metrics)}")
        print(f"   Services: {metrics['service'].nunique()}")
        print(f"   Max errors in a window: {metrics['error_count'].max()}")
        print(f"   Avg errors per window: {metrics['error_count'].mean():.1f}")
        
        # Save if requested
        if args.output:
            metrics.to_csv(args.output, index=False)
            print(f"\n💾 Saved metrics to {args.output}")
    
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()