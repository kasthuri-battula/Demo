# Distributed Log Analytics Platform — Milestone 1

A system that collects logs from applications and automatically detects anomalies
using Dask for batch processing and Ray for parallel rule evaluation.

---

## What This Does

- **Collects** structured log entries from microservices
- **Processes** them in parallel using Dask (handles millions of log lines)
- **Detects** anomalies using Ray workers running rules in parallel
- **Alerts** when something like an error spike or silent service is detected

---

## Milestone 1 Deliverables

| Task | File(s) | Points |
|------|---------|--------|
| Task 1: Log schema | `schemas/log_schema.yaml` | 45 |
| Task 2: Anomaly schema | `schemas/anomaly_schema.yaml` | 45 |
| Task 3: Architecture | `docs/architecture.md` + diagrams | 30 |
| Task 4: Environment setup | `environment/` + `tests/` | 25 |

---

## Quick Start

```bash
chmod +x environment/setup.sh
./environment/setup.sh
source .venv/bin/activate
pytest tests/test_environment.py -v
```

See `README_SETUP.md` for full setup instructions and troubleshooting.

---

## Key Design Decisions

**Why structured logs?** Plain text logs are hard to parse automatically. Structured
entries (timestamp, level, service, message) let the system count errors per service
per time window without fragile regex patterns.

**Why Dask?** Log files can be gigabytes large. Dask lets you process them in parallel
partitions with a pandas-compatible API — no rewriting needed when data grows.

**Why Ray?** Anomaly detection rules need to run across many services simultaneously.
Ray's remote functions make this trivial and scale to a full cluster if needed.

**Why YAML schemas?** YAML is human-readable and version-controlled. Both developers
and non-developers can understand and edit the anomaly rules without touching code.