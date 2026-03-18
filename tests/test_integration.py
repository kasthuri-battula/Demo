"""
tests/test_integration.py
==========================
Integration tests for the complete pipeline.

Run with:
    pytest tests/test_integration.py -v
"""

import pytest
import time
from pathlib import Path
import sys
from datetime import datetime

sys.path.insert(0, str(Path(__file__).parent.parent))

from generators.log_generator import generate_service_logs, write_logs_csv
from ingestion.dask_reader import LogIngestion
from processing.dask_aggregator import LogAggregator
from processing.ray_detector import AnomalyDetector
import ray


@pytest.fixture
def test_logs(tmp_path):
    """Generate test logs."""
    start_time = datetime.now()
    logs = generate_service_logs('test-service', 1000, start_time, include_anomalies=True)
    csv_path = tmp_path / "test-service.csv"
    write_logs_csv(logs, csv_path)
    return tmp_path


def test_full_pipeline(test_logs):
    """Test complete pipeline end-to-end."""
    print("\n🚀 Testing full pipeline...")
    
    # Ingest
    ingestion = LogIngestion()
    df = ingestion.read_logs(str(test_logs))
    assert len(df) == 1000
    
    # Aggregate
    aggregator = LogAggregator(window_minutes=5)
    metrics = aggregator.aggregate_by_window(df)
    assert len(metrics) > 0
    
    # Detect
    detector = AnomalyDetector()
    anomalies = detector.detect(metrics, use_ray=True)
    
    # Should detect at least some anomalies
    assert len(anomalies) > 0, f"Should detect anomalies, found {len(anomalies)}"

# Check what types were detected
    anomaly_types = set(a['name'] for a in anomalies)
    print(f"   Detected types: {anomaly_types}")
    
    if ray.is_initialized():
        ray.shutdown()
    
    print(f"✅ Pipeline test passed - detected {len(anomalies)} anomalies")


def test_throughput(test_logs):
    """Measure pipeline throughput."""
    print("\n📊 Measuring throughput...")
    
    start = time.time()
    
    ingestion = LogIngestion()
    df = ingestion.read_logs(str(test_logs))
    
    aggregator = LogAggregator(window_minutes=5)
    metrics = aggregator.aggregate_by_window(df)
    
    detector = AnomalyDetector()
    anomalies = detector.detect(metrics, use_ray=True)
    
    elapsed = time.time() - start
    throughput = 1000 / elapsed
    
    print(f"   Throughput: {throughput:.0f} logs/second")
    
    assert throughput > 100, f"Too slow: {throughput:.0f} logs/sec"
    
    if ray.is_initialized():
        ray.shutdown()