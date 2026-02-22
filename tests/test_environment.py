"""
tests/test_environment.py
=========================
Verifies that Dask and Ray are correctly installed and can perform
basic operations. Run with:  pytest tests/test_environment.py -v
"""

import pytest


# ============================================================
# Dask Tests
# ============================================================

def test_dask_importable():
    """Dask can be imported without errors."""
    import dask
    assert dask.__version__, "Dask version string should not be empty"


def test_dask_dataframe_basic():
    """Dask can create and compute a simple dataframe."""
    import pandas as pd
    import dask.dataframe as dd

    # Build a small pandas dataframe and convert to Dask
    pdf = pd.DataFrame({
        "service": ["auth-service", "payment-service", "inventory-service"],
        "level":   ["INFO", "ERROR", "WARNING"],
        "duration_ms": [100, 5000, 200],
    })
    ddf = dd.from_pandas(pdf, npartitions=2)

    # Compute a simple aggregation — count rows
    result = ddf.shape[0].compute()
    assert result == 3, f"Expected 3 rows, got {result}"


def test_dask_error_count_aggregation():
    """Dask can count ERROR-level logs per service (core use case)."""
    import pandas as pd
    import dask.dataframe as dd

    pdf = pd.DataFrame({
        "service": ["auth", "auth", "payment", "payment", "auth"],
        "level":   ["ERROR", "INFO", "ERROR", "ERROR", "ERROR"],
    })
    ddf = dd.from_pandas(pdf, npartitions=1)

    error_counts = (
        ddf[ddf["level"] == "ERROR"]
        .groupby("service")["level"]
        .count()
        .compute()
    )

    assert error_counts["auth"] == 2, "auth-service should have 2 errors"
    assert error_counts["payment"] == 2, "payment-service should have 2 errors"


def test_dask_scheduler_available():
    """Dask synchronous scheduler works (no cluster needed)."""
    import dask
    import dask.array as da

    x = da.ones((100,), chunks=25)
    result = x.sum().compute(scheduler="synchronous")
    assert result == 100.0


# ============================================================
# Ray Tests
# ============================================================

def test_ray_importable():
    """Ray can be imported without errors."""
    import ray
    assert ray.__version__, "Ray version string should not be empty"


@pytest.fixture(scope="module")
def ray_initialized():
    """Start a local Ray cluster once for all Ray tests, shut it down after."""
    import ray
    if not ray.is_initialized():
        ray.init(num_cpus=2, ignore_reinit_error=True)
    yield
    ray.shutdown()


def test_ray_remote_function(ray_initialized):
    """Ray can execute a simple remote function."""
    import ray

    @ray.remote
    def add(a, b):
        return a + b

    result = ray.get(add.remote(3, 7))
    assert result == 10, f"Expected 10, got {result}"


def test_ray_parallel_anomaly_detection(ray_initialized):
    """Ray can run anomaly detection rules in parallel across services."""
    import ray

    @ray.remote
    def check_error_spike(service_name, error_count, threshold=100):
        """Returns anomaly dict if threshold exceeded, else None."""
        if error_count > threshold:
            return {
                "name": "error_spike",
                "service": service_name,
                "trigger_value": error_count,
                "threshold_value": threshold,
                "severity": "HIGH",
            }
        return None

    # Simulate metric snapshots for 3 services
    snapshots = [
        ("auth-service", 30),       # normal
        ("payment-service", 147),   # anomaly!
        ("inventory-service", 5),   # normal
    ]

    futures = [check_error_spike.remote(svc, count) for svc, count in snapshots]
    results = ray.get(futures)

    anomalies = [r for r in results if r is not None]
    assert len(anomalies) == 1, f"Expected 1 anomaly, got {len(anomalies)}"
    assert anomalies[0]["service"] == "payment-service"
    assert anomalies[0]["severity"] == "HIGH"


# ============================================================
# YAML Schema Tests
# ============================================================

def test_yaml_importable():
    """PyYAML can be imported (needed to load schema files)."""
    import yaml
    assert yaml.__version__


def test_log_schema_loads():
    """log_schema.yaml exists and parses correctly."""
    import yaml
    import os

    schema_path = os.path.join(
        os.path.dirname(__file__), "..", "schemas", "log_schema.yaml"
    )
    assert os.path.exists(schema_path), f"Schema file not found at {schema_path}"

    with open(schema_path) as f:
        schema = yaml.safe_load(f)

    assert "log_entry" in schema, "Schema must define 'log_entry'"
    assert "fields" in schema["log_entry"], "log_entry must contain 'fields'"


def test_anomaly_schema_loads():
    """anomaly_schema.yaml exists and parses correctly."""
    import yaml
    import os

    schema_path = os.path.join(
        os.path.dirname(__file__), "..", "schemas", "anomaly_schema.yaml"
    )
    assert os.path.exists(schema_path), f"Schema file not found at {schema_path}"

    with open(schema_path) as f:
        schema = yaml.safe_load(f)

    assert "anomalies" in schema, "Schema must define 'anomalies'"
    assert len(schema["anomalies"]) >= 5, "Must define at least 5 anomaly types"


# ============================================================
# Integration Test: Dask + Ray Together
# ============================================================

def test_dask_feeds_ray(ray_initialized):
    """
    Integration test: Dask aggregates log data, Ray detects anomalies.
    This mirrors the actual production data flow.
    """
    import ray
    import pandas as pd
    import dask.dataframe as dd

    # Step 1: Dask aggregates raw log counts
    raw_logs = pd.DataFrame({
        "service": ["payment"] * 120 + ["auth"] * 40,
        "level":   ["ERROR"] * 120 + ["ERROR"] * 40,
    })
    ddf = dd.from_pandas(raw_logs, npartitions=2)
    error_counts = (
        ddf[ddf["level"] == "ERROR"]
        .groupby("service")["level"]
        .count()
        .compute()
        .to_dict()
    )

    # Step 2: Ray checks each service for anomalies
    @ray.remote
    def detect(service, count):
        return {"service": service, "anomaly": count > 100}

    futures = [detect.remote(svc, cnt) for svc, cnt in error_counts.items()]
    results = ray.get(futures)

    payment_result = next(r for r in results if r["service"] == "payment")
    auth_result    = next(r for r in results if r["service"] == "auth")

    assert payment_result["anomaly"] is True,  "payment-service should trigger anomaly"
    assert auth_result["anomaly"]    is False, "auth-service should not trigger anomaly"