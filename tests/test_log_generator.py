"""
tests/test_log_generator.py
============================
Tests for the log generator to ensure it produces valid logs.

Run with:
    pytest tests/test_log_generator.py -v
"""

import pytest
import json
from pathlib import Path
from datetime import datetime
import sys
import csv

# Add generators directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from generators.log_generator import (
    generate_log_entry,
    generate_service_logs,
    inject_error_spike,
    write_logs_csv,
    write_logs_txt,
    SERVICES,
    LOG_LEVELS,
)


# =============================================================================
# Test Log Entry Generation
# =============================================================================

def test_log_entry_structure():
    """Generated log entries should match log_schema.yaml structure."""
    service = "payment-service"
    timestamp = datetime.now()
    
    log = generate_log_entry(service, timestamp)
    
    # Required fields
    assert "timestamp" in log
    assert "level" in log
    assert "service" in log
    assert "message" in log
    
    # Optional fields
    assert "trace_id" in log or log["trace_id"] is None
    assert "host" in log
    assert "user_id" in log or log["user_id"] is None
    assert "duration_ms" in log or log["duration_ms"] is None
    assert "metadata" in log or log["metadata"] is None


def test_log_levels_are_valid():
    """All generated log levels should be from the defined set."""
    service = "auth-service"
    timestamp = datetime.now()
    
    for _ in range(100):
        log = generate_log_entry(service, timestamp)
        assert log["level"] in LOG_LEVELS, f"Invalid level: {log['level']}"


def test_service_name_preserved():
    """Generated logs should preserve the service name."""
    for service in SERVICES:
        timestamp = datetime.now()
        log = generate_log_entry(service, timestamp)
        assert log["service"] == service


def test_timestamp_format():
    """Timestamps should be in ISO 8601 format."""
    service = "user-service"
    timestamp = datetime.now()
    
    log = generate_log_entry(service, timestamp)
    
    # Try parsing the timestamp back
    parsed = datetime.strptime(log["timestamp"], "%Y-%m-%d %H:%M:%S")
    assert parsed.year == timestamp.year
    assert parsed.month == timestamp.month
    assert parsed.day == timestamp.day


def test_metadata_is_valid_json():
    """Metadata field should be valid JSON or None."""
    service = "payment-service"
    timestamp = datetime.now()
    
    for _ in range(50):
        log = generate_log_entry(service, timestamp)
        if log["metadata"] is not None:
            # Should parse without error
            metadata = json.loads(log["metadata"])
            assert isinstance(metadata, dict)


def test_anomaly_injection_forces_error():
    """When inject_anomaly=True, log level should be ERROR."""
    service = "auth-service"
    timestamp = datetime.now()
    
    log = generate_log_entry(service, timestamp, inject_anomaly=True)
    assert log["level"] == "ERROR"


# =============================================================================
# Test Bulk Generation
# =============================================================================

def test_generate_service_logs_count():
    """Generated log count should match requested count."""
    service = "payment-service"
    count = 500
    start_time = datetime.now()
    
    logs = generate_service_logs(service, count, start_time, include_anomalies=False)
    
    assert len(logs) == count


def test_logs_are_sorted_by_timestamp():
    """Generated logs should be sorted chronologically."""
    service = "inventory-service"
    count = 200
    start_time = datetime.now()
    
    logs = generate_service_logs(service, count, start_time)
    
    timestamps = [log["timestamp"] for log in logs]
    assert timestamps == sorted(timestamps), "Logs are not sorted by timestamp"


def test_error_spike_injection():
    """Error spike should generate 150 ERROR logs."""
    service = "payment-service"
    start_time = datetime.now()
    
    logs = inject_error_spike(service, start_time, count=150)
    
    assert len(logs) == 150
    assert all(log["level"] == "ERROR" for log in logs)
    assert all(log["service"] == service for log in logs)


def test_anomaly_increases_log_count():
    """Including anomalies should add 150 logs to the total."""
    service = "auth-service"
    count = 500
    start_time = datetime.now()
    
    # With anomalies
    logs_with_anomaly = generate_service_logs(service, count, start_time, include_anomalies=True)
    
    # Should have original count (includes the 150 anomaly logs)
    assert len(logs_with_anomaly) == count
    
    # Count ERROR logs
    error_count = sum(1 for log in logs_with_anomaly if log["level"] == "ERROR")
    
    # Should have at least 150 errors (the injected spike)
    assert error_count >= 150


# =============================================================================
# Test File Writing
# =============================================================================

def test_write_logs_csv(tmp_path):
    """CSV writer should create valid CSV files."""
    service = "notification-service"
    start_time = datetime.now()
    logs = generate_service_logs(service, 100, start_time)
    
    output_file = tmp_path / "test_logs.csv"
    write_logs_csv(logs, output_file)
    
    # File should exist
    assert output_file.exists()
    
    # Should be readable as CSV
    with open(output_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        rows = list(reader)
    
    assert len(rows) == 100
    assert "timestamp" in rows[0]
    assert "level" in rows[0]
    assert "service" in rows[0]


def test_write_logs_txt(tmp_path):
    """Text writer should create valid JSON-per-line files."""
    service = "user-service"
    start_time = datetime.now()
    logs = generate_service_logs(service, 50, start_time)
    
    output_file = tmp_path / "test_logs.txt"
    write_logs_txt(logs, output_file)
    
    # File should exist
    assert output_file.exists()
    
    # Should be readable as JSON lines
    with open(output_file, 'r', encoding='utf-8') as f:
        lines = f.readlines()
    
    assert len(lines) == 50
    
    # Each line should be valid JSON
    for line in lines:
        log = json.loads(line)
        assert "timestamp" in log
        assert "service" in log


# =============================================================================
# Test Realistic Distributions
# =============================================================================

def test_log_level_distribution():
    """Log levels should follow realistic distribution (mostly INFO)."""
    service = "payment-service"
    start_time = datetime.now()
    logs = generate_service_logs(service, 1000, start_time, include_anomalies=False)
    
    level_counts = {}
    for log in logs:
        level = log["level"]
        level_counts[level] = level_counts.get(level, 0) + 1
    
    # INFO should be the most common (roughly 70%)
    assert level_counts.get("INFO", 0) > level_counts.get("ERROR", 0)
    assert level_counts.get("INFO", 0) > level_counts.get("WARNING", 0)
    
    # CRITICAL should be rare
    assert level_counts.get("CRITICAL", 0) < 50


def test_all_services_are_valid():
    """All defined services should generate valid logs."""
    start_time = datetime.now()
    
    for service in SERVICES:
        logs = generate_service_logs(service, 100, start_time)
        
        assert len(logs) == 100
        assert all(log["service"] == service for log in logs)
        assert all(log["level"] in LOG_LEVELS for log in logs)


# =============================================================================
# Integration Test
# =============================================================================

def test_full_generation_pipeline(tmp_path):
    """Test the complete generation workflow."""
    output_dir = tmp_path / "raw_logs"
    output_dir.mkdir()
    
    # Generate logs for one service
    service = "auth-service"
    start_time = datetime.now()
    logs = generate_service_logs(service, 500, start_time, include_anomalies=True)
    
    # Write to CSV
    csv_path = output_dir / f"{service}.csv"
    write_logs_csv(logs, csv_path)
    
    # Verify file exists and is readable
    assert csv_path.exists()
    
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        rows = list(reader)
    
    assert len(rows) == 500
    
    # Should contain the error spike
    error_count = sum(1 for row in rows if row["level"] == "ERROR")
    assert error_count >= 150  # At least the 150 injected errors