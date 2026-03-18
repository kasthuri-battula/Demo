"""
processing/ray_detector.py
===========================
Detects anomalies using Ray for parallel rule evaluation.

Usage:
    from processing.ray_detector import AnomalyDetector
    
    detector = AnomalyDetector()
    anomalies = detector.detect(metrics)
"""

import ray
import pandas as pd
from datetime import datetime
from typing import List, Dict, Optional
import json


class AnomalyDetector:
    """
    Detects anomalies in aggregated log metrics using Ray.
    """
    
    def __init__(self, schema_path: str = 'schemas/anomaly_schema.yaml'):
        """
        Initialize the anomaly detector.
        
        Args:
            schema_path: Path to anomaly_schema.yaml (for reference)
        """
        self.schema_path = schema_path
        
        # Anomaly detection rules (from anomaly_schema.yaml)
        self.rules = {
            'error_spike': {
                'threshold': 100,
                'window_minutes': 5,
                'severity': 'HIGH',
                'description': 'Too many errors in a short time window',
            },
            'slow_response': {
                'threshold': 2000,  # milliseconds
                'window_minutes': 10,
                'severity': 'MEDIUM',
                'description': 'Service response times exceed acceptable latency',
            },
            'warning_flood': {
                'threshold': 200,
                'window_minutes': 15,
                'severity': 'MEDIUM',
                'description': 'Sustained stream of WARNING logs',
            },
            'critical_spike': {
                'threshold': 5,
                'window_minutes': 5,
                'severity': 'CRITICAL',
                'description': 'Multiple CRITICAL logs detected',
            },
        }
    
    def detect(self, metrics: pd.DataFrame, use_ray: bool = True) -> List[Dict]:
        """
        Run all anomaly detection rules on the metrics.
        
        Args:
            metrics: Aggregated metrics from Dask
            use_ray: Whether to use Ray for parallel processing (default: True)
        
        Returns:
            List of detected anomalies
        """
        print(f"\n🔍 Running anomaly detection on {len(metrics)} metric snapshots...")
        
        if use_ray:
            return self._detect_with_ray(metrics)
        else:
            return self._detect_sequential(metrics)
    
    def _detect_with_ray(self, metrics: pd.DataFrame) -> List[Dict]:
        """
        Detect anomalies using Ray for parallel processing.
        """
        # Initialize Ray
        if not ray.is_initialized():
            ray.init(ignore_reinit_error=True)
            print("   Ray initialized")
        
        # Convert DataFrame rows to list of dicts for Ray
        metric_records = metrics.to_dict('records')
        
        # Create remote tasks for each metric snapshot
        futures = []
        for record in metric_records:
            future = check_all_rules.remote(record, self.rules)
            futures.append(future)
        
        # Collect results
        print(f"   Processing {len(futures)} snapshots in parallel...")
        results = ray.get(futures)
        
        # Flatten results (each result is a list of anomalies)
        anomalies = []
        for result in results:
            if result:
                anomalies.extend(result)
        
        print(f"✅ Detected {len(anomalies)} anomalie(s)")
        
        return anomalies
    
    def _detect_sequential(self, metrics: pd.DataFrame) -> List[Dict]:
        """
        Detect anomalies without Ray (for comparison/testing).
        """
        anomalies = []
        
        for _, record in metrics.iterrows():
            record_dict = record.to_dict()
            detected = check_all_rules_local(record_dict, self.rules)
            if detected:
                anomalies.extend(detected)
        
        print(f"✅ Detected {len(anomalies)} anomalie(s)")
        
        return anomalies
    
    def save_anomalies(self, anomalies: List[Dict], output_path: str):
        """
        Save detected anomalies to a JSON file.
        
        Args:
            anomalies: List of anomaly dictionaries
            output_path: Where to save the JSON file
        """
        from pathlib import Path
        
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        
        with open(output_path, 'w') as f:
            json.dump(anomalies, f, indent=2, default=str)
        
        print(f"💾 Saved {len(anomalies)} anomalies to {output_path}")
    
    def print_summary(self, anomalies: List[Dict]):
        """
        Print a summary of detected anomalies.
        
        Args:
            anomalies: List of detected anomalies
        """
        if not anomalies:
            print("\n✅ No anomalies detected - all services healthy!")
            return
        
        print(f"\n⚠️  Anomaly Summary ({len(anomalies)} total)")
        print("=" * 80)
        
        # Group by severity
        by_severity = {}
        for anom in anomalies:
            severity = anom['severity']
            by_severity[severity] = by_severity.get(severity, 0) + 1
        
        print(f"\nBy Severity:")
        for severity in ['CRITICAL', 'HIGH', 'MEDIUM', 'LOW']:
            count = by_severity.get(severity, 0)
            if count > 0:
                print(f"   {severity}: {count}")
        
        # Group by anomaly type
        by_type = {}
        for anom in anomalies:
            anom_type = anom['name']
            by_type[anom_type] = by_type.get(anom_type, 0) + 1
        
        print(f"\nBy Type:")
        for anom_type, count in by_type.items():
            print(f"   {anom_type}: {count}")
        
        # Show top 5 anomalies
        print(f"\n📋 Top Anomalies:")
        print("-" * 80)
        
        # Sort by severity (CRITICAL > HIGH > MEDIUM > LOW)
        severity_order = {'CRITICAL': 0, 'HIGH': 1, 'MEDIUM': 2, 'LOW': 3}
        sorted_anomalies = sorted(
            anomalies,
            key=lambda x: severity_order.get(x['severity'], 99)
        )
        
        for i, anom in enumerate(sorted_anomalies[:5], 1):
            print(f"\n{i}. [{anom['severity']}] {anom['name']}")
            print(f"   Service: {anom['service']}")
            print(f"   Window: {anom['window']}")
            print(f"   Trigger: {anom['trigger_value']} (threshold: {anom['threshold_value']})")
            print(f"   Description: {anom['description']}")


# =============================================================================
# Ray Remote Functions (run in parallel)
# =============================================================================

@ray.remote
def check_all_rules(metric_record: Dict, rules: Dict) -> List[Dict]:
    """
    Check all anomaly detection rules for a single metric snapshot.
    This function runs in parallel on Ray workers.
    
    Args:
        metric_record: Single row from metrics DataFrame (as dict)
        rules: Dictionary of anomaly detection rules
    
    Returns:
        List of detected anomalies (empty if none detected)
    """
    anomalies = []
    
    service = metric_record['service']
    window = metric_record['window']
    
    # Rule 1: Error Spike
    if metric_record['error_count'] > rules['error_spike']['threshold']:
        anomalies.append({
            'anomaly_id': f"anom_{service}_{window}_{datetime.now().timestamp()}",
            'name': 'error_spike',
            'description': rules['error_spike']['description'],
            'severity': rules['error_spike']['severity'],
            'service': service,
            'window': str(window),
            'detected_at': datetime.now().isoformat(),
            'trigger_value': int(metric_record['error_count']),
            'threshold_value': rules['error_spike']['threshold'],
            'resolved': False,
        })
    
    # Rule 2: Slow Response
    if metric_record['avg_duration_ms'] > rules['slow_response']['threshold']:
        anomalies.append({
            'anomaly_id': f"anom_{service}_{window}_{datetime.now().timestamp()}",
            'name': 'slow_response',
            'description': rules['slow_response']['description'],
            'severity': rules['slow_response']['severity'],
            'service': service,
            'window': str(window),
            'detected_at': datetime.now().isoformat(),
            'trigger_value': float(metric_record['avg_duration_ms']),
            'threshold_value': rules['slow_response']['threshold'],
            'resolved': False,
        })
    
    # Rule 3: Warning Flood
    if metric_record['warning_count'] > rules['warning_flood']['threshold']:
        anomalies.append({
            'anomaly_id': f"anom_{service}_{window}_{datetime.now().timestamp()}",
            'name': 'warning_flood',
            'description': rules['warning_flood']['description'],
            'severity': rules['warning_flood']['severity'],
            'service': service,
            'window': str(window),
            'detected_at': datetime.now().isoformat(),
            'trigger_value': int(metric_record['warning_count']),
            'threshold_value': rules['warning_flood']['threshold'],
            'resolved': False,
        })
    
    # Rule 4: Critical Spike
    if metric_record['critical_count'] > rules['critical_spike']['threshold']:
        anomalies.append({
            'anomaly_id': f"anom_{service}_{window}_{datetime.now().timestamp()}",
            'name': 'critical_spike',
            'description': rules['critical_spike']['description'],
            'severity': rules['critical_spike']['severity'],
            'service': service,
            'window': str(window),
            'detected_at': datetime.now().isoformat(),
            'trigger_value': int(metric_record['critical_count']),
            'threshold_value': rules['critical_spike']['threshold'],
            'resolved': False,
        })
    
    return anomalies


# Non-Ray version (for testing without Ray)
def check_all_rules_local(metric_record: Dict, rules: Dict) -> List[Dict]:
    """Same as check_all_rules but without @ray.remote decorator."""
    return ray.get(check_all_rules.remote(metric_record, rules))


# =============================================================================
# CLI Interface
# =============================================================================

def main():
    """Command-line interface for testing anomaly detection."""
    import argparse
    import sys
    from pathlib import Path
    
    sys.path.insert(0, str(Path(__file__).parent.parent))
    
    from ingestion.dask_reader import LogIngestion
    from processing.dask_aggregator import LogAggregator
    
    parser = argparse.ArgumentParser(description="Detect anomalies with Ray")
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
        default='data/anomalies.json',
        help='Save anomalies to JSON file'
    )
    parser.add_argument(
        '--no-ray',
        action='store_true',
        help='Run without Ray (sequential processing)'
    )
    
    args = parser.parse_args()
    
    print(f"\n{'='*60}")
    print(f"  Ray Anomaly Detection — Milestone 2")
    print(f"{'='*60}\n")
    
    try:
        # Step 1: Ingest logs
        ingestion = LogIngestion()
        df = ingestion.read_logs(args.log_dir)
        
        # Step 2: Aggregate metrics
        aggregator = LogAggregator(window_minutes=args.window_minutes)
        metrics = aggregator.aggregate_by_window(df)
        
        # Step 3: Detect anomalies
        detector = AnomalyDetector()
        anomalies = detector.detect(metrics, use_ray=not args.no_ray)
        
        # Step 4: Show summary
        detector.print_summary(anomalies)
        
        # Step 5: Save results
        if anomalies:
            detector.save_anomalies(anomalies, args.output)
        
        # Shutdown Ray
        if ray.is_initialized():
            ray.shutdown()
    
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        
        if ray.is_initialized():
            ray.shutdown()
        
        sys.exit(1)


if __name__ == "__main__":
    main()