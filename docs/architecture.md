# System Architecture — Distributed Log Analytics Platform

## Overview

This system collects log entries from multiple microservices, processes them in
parallel using Dask and Ray, and automatically detects anomalies in near real-time.

---

## Components

### 1. Log Producers
Individual services (auth-service, payment-service, inventory-service, etc.)
emit structured log entries matching `log_schema.yaml`. Each entry contains at
minimum a timestamp, severity level, service name, and message.

### 2. Log Ingestion Layer
A lightweight collector receives logs from all producers. It validates incoming
entries against the schema and writes them to a shared storage layer (file system
or object store). This is the single entry point for all log data.

### 3. Dask Processing Cluster
Dask reads raw log files in parallel partitions. It is responsible for:
- Parsing and normalizing log entries
- Aggregating metrics per service per time window (e.g., error counts per 5 min)
- Filtering and grouping large volumes of historical log data

Dask is ideal here because it handles large-scale batch processing using a
pandas-compatible API, making it easy to write aggregations.

### 4. Ray Anomaly Detection Workers
Ray receives pre-aggregated metric snapshots from Dask and runs anomaly detection
rules in parallel across services. Each Ray task evaluates one service's metrics
against all rules defined in `anomaly_schema.yaml`. Ray is used here because its
actor model allows stateful, low-latency rule evaluation at scale.

### 5. Anomaly Store
Detected anomalies are written to a structured output (JSON / database). Each
record matches the anomaly schema and is stamped with a detection timestamp,
severity, and the triggering metric values.

### 6. Dashboard / Alert Layer
The Dask dashboard exposes cluster health metrics. The anomaly store feeds a
simple alerting layer that can notify on-call engineers when HIGH or CRITICAL
anomalies are detected.

---

## Data Flow

```
[Services]  →  [Log Ingestion]  →  [Raw Log Storage]
                                          │
                                     [Dask Cluster]
                                    (parse + aggregate)
                                          │
                                  [Metric Snapshots]
                                          │
                                   [Ray Workers]
                                 (anomaly detection)
                                          │
                                  [Anomaly Store]
                                          │
                               [Dashboard / Alerts]
```

### Step-by-step flow

1. A microservice emits a log entry (e.g., ERROR from payment-service).
2. The ingestion layer validates and appends it to the log store.
3. On a scheduled trigger (every 1 minute), Dask reads all new log entries.
4. Dask aggregates counts by service, level, and time window, then writes metric
   snapshots.
5. Ray picks up the metric snapshots and runs each anomaly rule in a parallel task.
6. If a threshold is crossed (e.g., errors > 100 in 5 min), Ray writes an anomaly
   record to the anomaly store.
7. The alerting layer polls the anomaly store and sends notifications for unresolved
   HIGH/CRITICAL anomalies.

---

## Technology Choices

| Component         | Technology | Why                                              |
|-------------------|------------|--------------------------------------------------|
| Batch processing  | Dask       | Pandas-compatible, scales to large log files     |
| Parallel rules    | Ray        | Low-latency parallel tasks, actor model          |
| Schema definition | YAML       | Human-readable, easy to version in Git           |
| Log format        | Structured | Enables automated parsing without regex hacks    |

---

## Diagrams

See `diagrams/system_architecture.png` for the component diagram and
`diagrams/data_flow.png` for the end-to-end data flow visualization.

---
