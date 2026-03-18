"""
ingestion/dask_reader.py
=========================
Reads log files using Dask for parallel processing.

Usage:
    from ingestion.dask_reader import LogIngestion
    
    ingestion = LogIngestion()
    df = ingestion.read_logs('data/raw_logs/')
    print(f"Loaded {len(df)} logs")
"""

import dask.dataframe as dd
import pandas as pd
from pathlib import Path
from typing import Optional
import yaml


class LogIngestion:
    """
    Handles reading and validating log files using Dask.
    """
    
    def __init__(self, schema_path: str = 'schemas/log_schema.yaml'):
        """
        Initialize the ingestion module.
        
        Args:
            schema_path: Path to log_schema.yaml for validation
        """
        self.schema_path = schema_path
        self.required_columns = [
            'timestamp', 'level', 'service', 'message',
            'trace_id', 'host', 'user_id', 'duration_ms', 'metadata'
        ]
    
    def read_logs(self, log_dir: str, pattern: str = '*.csv') -> dd.DataFrame:
        """
        Read all log files from a directory into a Dask DataFrame.
        
        Args:
            log_dir: Directory containing log CSV files
            pattern: File pattern to match (default: '*.csv')
        
        Returns:
            Dask DataFrame with all logs combined
        """
        log_path = Path(log_dir)
        
        if not log_path.exists():
            raise FileNotFoundError(f"Log directory not found: {log_dir}")
        
        # Find all matching files
        files = list(log_path.glob(pattern))
        if not files:
            raise FileNotFoundError(f"No log files found matching {pattern} in {log_dir}")
        
        print(f"📂 Found {len(files)} log file(s)")
        
        # Read all CSV files in parallel
        file_pattern = str(log_path / pattern)
        df = dd.read_csv(
            file_pattern,
            dtype={
                'timestamp': 'object',  # Will convert to datetime later
                'level': 'object',
                'service': 'object',
                'message': 'object',
                'trace_id': 'object',
                'host': 'object',
                'user_id': 'object',
                'duration_ms': 'float64',
                'metadata': 'object',
            },
            assume_missing=True  # Handle missing values gracefully
        )
        
        # Validate schema
        self._validate_schema(df)
        
        # Convert timestamp to datetime
        df['timestamp'] = dd.to_datetime(df['timestamp'], format='%Y-%m-%d %H:%M:%S')
        
        # Add parsed_at column (when the log was ingested)
        from datetime import datetime
        df['parsed_at'] = datetime.now()
        
        total_rows = len(df)
        print(f"✅ Loaded {total_rows} logs from {len(files)} file(s)")
        
        return df
    
    def _validate_schema(self, df: dd.DataFrame):
        """
        Validate that DataFrame has all required columns.
        
        Args:
            df: Dask DataFrame to validate
        
        Raises:
            ValueError: If required columns are missing
        """
        missing_columns = set(self.required_columns) - set(df.columns)
        
        if missing_columns:
            raise ValueError(
                f"Missing required columns: {missing_columns}\n"
                f"Expected: {self.required_columns}\n"
                f"Got: {list(df.columns)}"
            )
    
    def filter_by_service(self, df: dd.DataFrame, service: str) -> dd.DataFrame:
        """
        Filter logs for a specific service.
        
        Args:
            df: Dask DataFrame with logs
            service: Service name to filter
        
        Returns:
            Filtered DataFrame
        """
        return df[df['service'] == service]
    
    def filter_by_level(self, df: dd.DataFrame, level: str) -> dd.DataFrame:
        """
        Filter logs by severity level.
        
        Args:
            df: Dask DataFrame with logs
            level: Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        
        Returns:
            Filtered DataFrame
        """
        return df[df['level'] == level]
    
    def filter_by_time_range(self, df: dd.DataFrame, 
                            start_time: str, end_time: str) -> dd.DataFrame:
        """
        Filter logs within a time range.
        
        Args:
            df: Dask DataFrame with logs
            start_time: Start timestamp (YYYY-MM-DD HH:MM:SS)
            end_time: End timestamp (YYYY-MM-DD HH:MM:SS)
        
        Returns:
            Filtered DataFrame
        """
        start = pd.to_datetime(start_time)
        end = pd.to_datetime(end_time)
        
        return df[(df['timestamp'] >= start) & (df['timestamp'] <= end)]
    
    def get_summary_stats(self, df: dd.DataFrame) -> dict:
        """
        Get summary statistics about the loaded logs.
        
        Args:
            df: Dask DataFrame with logs
        
        Returns:
            Dictionary with statistics
        """
        stats = {
            'total_logs': len(df),
            'services': df['service'].nunique().compute(),
            'time_range': {
                'start': df['timestamp'].min().compute(),
                'end': df['timestamp'].max().compute(),
            },
            'level_distribution': df['level'].value_counts().compute().to_dict(),
            'logs_per_service': df.groupby('service').size().compute().to_dict(),
        }
        
        return stats


# =============================================================================
# CLI Interface (for testing)
# =============================================================================

def main():
    """Command-line interface for testing ingestion."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Ingest log files with Dask")
    parser.add_argument(
        '--log-dir',
        type=str,
        default='data/raw_logs',
        help='Directory containing log files'
    )
    parser.add_argument(
        '--service',
        type=str,
        help='Filter by specific service'
    )
    parser.add_argument(
        '--level',
        type=str,
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
        help='Filter by log level'
    )
    parser.add_argument(
        '--stats',
        action='store_true',
        help='Show summary statistics'
    )
    
    args = parser.parse_args()
    
    print(f"\n{'='*60}")
    print(f"  Dask Log Ingestion — Milestone 2")
    print(f"{'='*60}\n")
    
    # Initialize ingestion
    ingestion = LogIngestion()
    
    # Read logs
    df = ingestion.read_logs(args.log_dir)
    
    # Apply filters
    if args.service:
        df = ingestion.filter_by_service(df, args.service)
        print(f"🔍 Filtered to service: {args.service}")
    
    if args.level:
        df = ingestion.filter_by_level(df, args.level)
        print(f"🔍 Filtered to level: {args.level}")
    
    # Show statistics
    if args.stats:
        print("\n📊 Summary Statistics:")
        print("-" * 60)
        stats = ingestion.get_summary_stats(df)
        
        print(f"Total logs: {stats['total_logs']}")
        print(f"Unique services: {stats['services']}")
        print(f"Time range: {stats['time_range']['start']} to {stats['time_range']['end']}")
        
        print("\nLevel distribution:")
        for level, count in stats['level_distribution'].items():
            print(f"  {level}: {count}")
        
        print("\nLogs per service:")
        for service, count in stats['logs_per_service'].items():
            print(f"  {service}: {count}")
    else:
        # Show sample
        print("\n📝 Sample logs (first 5):")
        print(df.head())


if __name__ == "__main__":
    main()