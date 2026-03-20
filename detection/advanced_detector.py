"""
detection/advanced_detector.py
===============================
Advanced anomaly detection using statistical and ML models.

Usage:
    from detection.advanced_detector import AdvancedAnomalyDetector
    
    detector = AdvancedAnomalyDetector()
    anomalies = detector.detect(metrics)
"""

import pandas as pd
import numpy as np
from typing import List, Dict, Optional
from datetime import datetime
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler
import warnings
warnings.filterwarnings('ignore')


class AdvancedAnomalyDetector:
    """
    Advanced anomaly detection using statistical and ML methods.
    """
    
    def __init__(self):
        """Initialize the advanced detector."""
        self.scaler = StandardScaler()
        self.isolation_forest = None
        
        # Statistical thresholds
        self.z_score_threshold = 3.0  # 3 standard deviations
        self.moving_avg_window = 3     # 3 time windows for moving average
    
    def detect_with_statistics(self, metrics: pd.DataFrame) -> List[Dict]:
        """
        Detect anomalies using statistical methods (Z-score, moving averages).
        
        Args:
            metrics: Aggregated metrics DataFrame
        
        Returns:
            List of detected anomalies
        """
        print("\n📊 Running statistical anomaly detection...")
        anomalies = []
        
        # Group by service
        for service in metrics['service'].unique():
            service_data = metrics[metrics['service'] == service].copy()
            service_data = service_data.sort_values('window').reset_index(drop=True)
            
            # Method 1: Z-Score Detection
            z_score_anomalies = self._detect_zscore(service_data, service)
            anomalies.extend(z_score_anomalies)
            
            # Method 2: Moving Average Detection
            moving_avg_anomalies = self._detect_moving_average(service_data, service)
            anomalies.extend(moving_avg_anomalies)
            
            # Method 3: Rate of Change Detection
            rate_change_anomalies = self._detect_rate_of_change(service_data, service)
            anomalies.extend(rate_change_anomalies)
        
        print(f"✅ Statistical detection found {len(anomalies)} anomalie(s)")
        return anomalies
    
    def _detect_zscore(self, service_data: pd.DataFrame, service: str) -> List[Dict]:
        """Detect anomalies using Z-score method."""
        anomalies = []
        
        if len(service_data) < 3:
            return anomalies  # Need at least 3 data points
        
        # Calculate Z-scores for error_count
        mean = service_data['error_count'].mean()
        std = service_data['error_count'].std()
        
        if std == 0:
            return anomalies  # No variation
        
        service_data['z_score'] = (service_data['error_count'] - mean) / std
        
        # Flag points with |z_score| > threshold
        outliers = service_data[abs(service_data['z_score']) > self.z_score_threshold]
        
        for _, row in outliers.iterrows():
            anomalies.append({
                'anomaly_id': f"stat_zscore_{service}_{row['window']}_{datetime.now().timestamp()}",
                'name': 'statistical_error_spike',
                'method': 'z_score',
                'description': f'Error count is {abs(row["z_score"]):.1f} standard deviations from mean',
                'severity': 'HIGH' if abs(row['z_score']) > 4 else 'MEDIUM',
                'service': service,
                'window': str(row['window']),
                'detected_at': datetime.now().isoformat(),
                'trigger_value': int(row['error_count']),
                'threshold_value': f"{self.z_score_threshold} std devs",
                'z_score': float(row['z_score']),
                'resolved': False,
            })
        
        return anomalies
    
    def _detect_moving_average(self, service_data: pd.DataFrame, service: str) -> List[Dict]:
        """Detect anomalies using moving average method."""
        anomalies = []
        
        if len(service_data) < self.moving_avg_window:
            return anomalies
        
        # Calculate moving average
        service_data['moving_avg'] = service_data['error_count'].rolling(
            window=self.moving_avg_window
        ).mean()
        
        # Calculate deviation from moving average
        service_data['deviation'] = abs(
            service_data['error_count'] - service_data['moving_avg']
        )
        
        # Flag if current value is 2x the moving average
        threshold_multiplier = 2.0
        service_data['is_anomaly'] = (
            service_data['error_count'] > 
            threshold_multiplier * service_data['moving_avg']
        )
        
        outliers = service_data[service_data['is_anomaly'] == True]
        
        for _, row in outliers.iterrows():
            if pd.notna(row['moving_avg']):
                anomalies.append({
                    'anomaly_id': f"stat_ma_{service}_{row['window']}_{datetime.now().timestamp()}",
                    'name': 'moving_average_spike',
                    'method': 'moving_average',
                    'description': f'Error count is {threshold_multiplier}x the moving average',
                    'severity': 'MEDIUM',
                    'service': service,
                    'window': str(row['window']),
                    'detected_at': datetime.now().isoformat(),
                    'trigger_value': int(row['error_count']),
                    'threshold_value': f"{threshold_multiplier}x moving avg ({row['moving_avg']:.0f})",
                    'resolved': False,
                })
        
        return anomalies
    
    def _detect_rate_of_change(self, service_data: pd.DataFrame, service: str) -> List[Dict]:
        """Detect anomalies based on sudden rate of change."""
        anomalies = []
        
        if len(service_data) < 2:
            return anomalies
        
        # Calculate rate of change (percentage change from previous window)
        service_data['pct_change'] = service_data['error_count'].pct_change() * 100
        
        # Flag sudden increases > 200%
        threshold_pct = 200.0
        sudden_spikes = service_data[service_data['pct_change'] > threshold_pct]
        
        for _, row in sudden_spikes.iterrows():
            if pd.notna(row['pct_change']):
                anomalies.append({
                    'anomaly_id': f"stat_roc_{service}_{row['window']}_{datetime.now().timestamp()}",
                    'name': 'sudden_error_increase',
                    'method': 'rate_of_change',
                    'description': f'Error count increased by {row["pct_change"]:.0f}% from previous window',
                    'severity': 'HIGH',
                    'service': service,
                    'window': str(row['window']),
                    'detected_at': datetime.now().isoformat(),
                    'trigger_value': int(row['error_count']),
                    'threshold_value': f'{threshold_pct}% increase',
                    'percent_change': float(row['pct_change']),
                    'resolved': False,
                })
        
        return anomalies
    
    def detect_with_ml(self, metrics: pd.DataFrame) -> List[Dict]:
        """
        Detect anomalies using machine learning (Isolation Forest).
        
        Args:
            metrics: Aggregated metrics DataFrame
        
        Returns:
            List of detected anomalies
        """
        print("\n🤖 Running ML-based anomaly detection (Isolation Forest)...")
        anomalies = []
        
        # Prepare features for ML
        feature_cols = ['error_count', 'warning_count', 'critical_count', 
                       'avg_duration_ms', 'max_duration_ms']
        
        # Check if we have enough data
        if len(metrics) < 10:
            print("   ⚠️  Not enough data for ML detection (need at least 10 samples)")
            return anomalies
        
        X = metrics[feature_cols].fillna(0)
        
        # Fit Isolation Forest
        iso_forest = IsolationForest(
            contamination=0.1,  # Expect 10% of data to be anomalies
            random_state=42,
            n_estimators=100
        )
        
        predictions = iso_forest.fit_predict(X)
        anomaly_scores = iso_forest.score_samples(X)
        
        # -1 = anomaly, 1 = normal
        anomaly_mask = predictions == -1
        
        anomaly_data = metrics[anomaly_mask].copy()
        anomaly_data['anomaly_score'] = anomaly_scores[anomaly_mask]
        
        for _, row in anomaly_data.iterrows():
            anomalies.append({
                'anomaly_id': f"ml_iso_{row['service']}_{row['window']}_{datetime.now().timestamp()}",
                'name': 'ml_detected_anomaly',
                'method': 'isolation_forest',
                'description': 'ML model detected unusual pattern in metrics',
                'severity': 'MEDIUM',
                'service': row['service'],
                'window': str(row['window']),
                'detected_at': datetime.now().isoformat(),
                'trigger_value': {
                    'error_count': int(row['error_count']),
                    'warning_count': int(row['warning_count']),
                    'avg_duration_ms': float(row['avg_duration_ms']),
                },
                'anomaly_score': float(row['anomaly_score']),
                'resolved': False,
            })
        
        print(f"✅ ML detection found {len(anomalies)} anomalie(s)")
        return anomalies
    
    def detect_all(self, metrics: pd.DataFrame, use_ml: bool = True) -> Dict[str, List[Dict]]:
        """
        Run all detection methods and return results grouped by method.
        
        Args:
            metrics: Aggregated metrics DataFrame
            use_ml: Whether to use ML-based detection
        
        Returns:
            Dictionary with anomalies grouped by detection method
        """
        results = {
            'statistical': self.detect_with_statistics(metrics),
            'ml': self.detect_with_ml(metrics) if use_ml else [],
        }
        
        total = len(results['statistical']) + len(results['ml'])
        
        print(f"\n📋 Total anomalies detected: {total}")
        print(f"   Statistical methods: {len(results['statistical'])}")
        print(f"   ML methods: {len(results['ml'])}")
        
        return results
    
    def deduplicate_anomalies(self, anomaly_results: Dict[str, List[Dict]]) -> List[Dict]:
        """
        Remove duplicate anomalies detected by multiple methods.
        Keep the one with highest severity.
        
        Args:
            anomaly_results: Results from detect_all()
        
        Returns:
            Deduplicated list of anomalies
        """
        all_anomalies = []
        for method_anomalies in anomaly_results.values():
            all_anomalies.extend(method_anomalies)
        
        # Group by service + window
        groups = {}
        for anom in all_anomalies:
            key = (anom['service'], anom['window'])
            if key not in groups:
                groups[key] = []
            groups[key].append(anom)
        
        # Keep highest severity from each group
        severity_order = {'CRITICAL': 0, 'HIGH': 1, 'MEDIUM': 2, 'LOW': 3}
        
        deduplicated = []
        for group in groups.values():
            best = min(group, key=lambda x: severity_order.get(x['severity'], 99))
            deduplicated.append(best)
        
        return deduplicated


# =============================================================================
# CLI Interface
# =============================================================================

def main():
    """Command-line interface for testing advanced detection."""
    import argparse
    import sys
    from pathlib import Path
    
    sys.path.insert(0, str(Path(__file__).parent.parent))
    
    from ingestion.dask_reader import LogIngestion
    from processing.dask_aggregator import LogAggregator
    import json
    
    parser = argparse.ArgumentParser(description="Advanced anomaly detection")
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
        help='Aggregation window size in minutes'
    )
    parser.add_argument(
        '--output',
        type=str,
        default='data/advanced_anomalies.json',
        help='Output file for detected anomalies'
    )
    parser.add_argument(
        '--no-ml',
        action='store_true',
        help='Skip ML-based detection'
    )
    
    args = parser.parse_args()
    
    print(f"\n{'='*70}")
    print(f"  Advanced Anomaly Detection — Milestone 3")
    print(f"{'='*70}\n")
    
    try:
        # Load and aggregate data
        ingestion = LogIngestion()
        df = ingestion.read_logs(args.log_dir)
        
        aggregator = LogAggregator(window_minutes=args.window_minutes)
        metrics = aggregator.aggregate_by_window(df)
        
        # Run advanced detection
        detector = AdvancedAnomalyDetector()
        results = detector.detect_all(metrics, use_ml=not args.no_ml)
        
        # Deduplicate
        anomalies = detector.deduplicate_anomalies(results)
        
        # Show summary
        print(f"\n{'='*70}")
        print(f"  DETECTION SUMMARY")
        print(f"{'='*70}\n")
        
        print(f"Total unique anomalies: {len(anomalies)}")
        
        # Group by severity
        by_severity = {}
        for anom in anomalies:
            sev = anom['severity']
            by_severity[sev] = by_severity.get(sev, 0) + 1
        
        print("\nBy Severity:")
        for sev in ['CRITICAL', 'HIGH', 'MEDIUM', 'LOW']:
            count = by_severity.get(sev, 0)
            if count > 0:
                print(f"   {sev}: {count}")
        
        # Group by method
        by_method = {}
        for anom in anomalies:
            method = anom.get('method', 'unknown')
            by_method[method] = by_method.get(method, 0) + 1
        
        print("\nBy Detection Method:")
        for method, count in by_method.items():
            print(f"   {method}: {count}")
        
        # Save results
        with open(args.output, 'w') as f:
            json.dump(anomalies, f, indent=2, default=str)
        
        print(f"\n💾 Saved {len(anomalies)} anomalies to {args.output}")
    
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()