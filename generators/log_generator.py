"""
generators/log_generator.py
============================
Generates realistic sample logs for testing the log analytics platform.

Usage:
    python generators/log_generator.py --service payment-service --count 1000
    python generators/log_generator.py --all-services --count 5000
"""

import random
import csv
import json
from datetime import datetime, timedelta
from pathlib import Path
import argparse


# =============================================================================
# Configuration: Service Names and Log Patterns
# =============================================================================

SERVICES = [
    "auth-service",
    "payment-service",
    "inventory-service",
    "notification-service",
    "user-service",
]

LOG_LEVELS = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]

# Weighted probabilities (realistic distribution)
LEVEL_WEIGHTS = {
    "DEBUG": 0.05,      # 5% debug logs
    "INFO": 0.70,       # 70% info logs
    "WARNING": 0.15,    # 15% warnings
    "ERROR": 0.08,      # 8% errors
    "CRITICAL": 0.02,   # 2% critical
}

# Message templates per service
MESSAGES = {
    "auth-service": {
        "INFO": [
            "User login successful",
            "Password reset requested",
            "Two-factor authentication enabled",
            "Session created",
            "User logged out",
        ],
        "WARNING": [
            "Failed login attempt",
            "Suspicious IP detected",
            "Account locked due to multiple failures",
        ],
        "ERROR": [
            "Database connection timeout",
            "Invalid authentication token",
            "LDAP server unreachable",
        ],
        "CRITICAL": [
            "Authentication service down",
            "All database connections exhausted",
        ],
    },
    "payment-service": {
        "INFO": [
            "Payment processed successfully",
            "Refund initiated",
            "Payment gateway connected",
        ],
        "WARNING": [
            "Payment processing slow",
            "Retry attempt",
        ],
        "ERROR": [
            "Payment gateway timeout",
            "Transaction failed",
            "Insufficient funds",
            "Credit card declined",
        ],
        "CRITICAL": [
            "Payment gateway unreachable",
            "All payment processors down",
        ],
    },
    "inventory-service": {
        "INFO": [
            "Stock level updated",
            "Item added to inventory",
            "Warehouse sync completed",
        ],
        "WARNING": [
            "Low stock alert",
            "Reorder threshold reached",
        ],
        "ERROR": [
            "Item not found in inventory",
            "Database write failed",
        ],
        "CRITICAL": [
            "Inventory database offline",
        ],
    },
    "notification-service": {
        "INFO": [
            "Email sent successfully",
            "SMS delivered",
            "Push notification sent",
        ],
        "WARNING": [
            "Email delivery delayed",
            "SMS rate limit approaching",
        ],
        "ERROR": [
            "SMTP server connection failed",
            "Push notification token invalid",
        ],
        "CRITICAL": [
            "Notification queue overflowed",
        ],
    },
    "user-service": {
        "INFO": [
            "User profile updated",
            "Account created",
            "Preferences saved",
        ],
        "WARNING": [
            "Slow database query",
        ],
        "ERROR": [
            "User not found",
            "Profile update failed",
        ],
        "CRITICAL": [
            "User database unresponsive",
        ],
    },
}


# =============================================================================
# Log Entry Generator
# =============================================================================

def generate_log_entry(service: str, timestamp: datetime, inject_anomaly: bool = False) -> dict:
    """
    Generate a single log entry matching log_schema.yaml.
    
    Args:
        service: Name of the service (e.g., "payment-service")
        timestamp: When this log occurred
        inject_anomaly: If True, force ERROR level (for testing anomaly detection)
    
    Returns:
        Dictionary matching log_schema.yaml structure
    """
    # Choose log level
    if inject_anomaly:
        level = "ERROR"
    else:
        level = random.choices(
            LOG_LEVELS,
            weights=[LEVEL_WEIGHTS[lvl] for lvl in LOG_LEVELS]
        )[0]
    
    # Get message template for this service and level
    service_messages = MESSAGES.get(service, MESSAGES["auth-service"])
    level_messages = service_messages.get(level, service_messages.get("INFO", ["Generic log message"]))
    message = random.choice(level_messages)
    
    # Generate trace ID (simulate distributed tracing)
    trace_id = f"tr_{random.randint(100000, 999999)}" if random.random() > 0.3 else None
    
    # Generate user ID (some logs have users, some don't)
    user_id = f"user_{random.randint(1000, 9999)}" if random.random() > 0.4 else None
    
    # Generate host
    host = f"prod-server-{random.randint(1, 10):02d}"
    
    # Generate duration (ms) for operations
    if level in ["INFO", "WARNING"]:
        duration_ms = random.randint(50, 500)
    elif level == "ERROR":
        duration_ms = random.randint(1000, 8000)  # Errors are often slow
    else:
        duration_ms = None
    
    # Generate metadata (extra context)
    metadata = {}
    if service == "payment-service":
        metadata = {
            "order_id": str(random.randint(1000, 9999)),
            "amount": round(random.uniform(10, 500), 2),
            "currency": "USD",
        }
        if level == "ERROR":
            metadata["error_code"] = random.choice(["GATEWAY_TIMEOUT", "DECLINED", "INSUFFICIENT_FUNDS"])
    elif service == "auth-service" and level in ["WARNING", "ERROR"]:
        metadata = {
            "ip_address": f"192.168.{random.randint(1, 255)}.{random.randint(1, 255)}",
            "login_method": random.choice(["password", "oauth2", "sso"]),
        }
    
    return {
        "timestamp": timestamp.strftime("%Y-%m-%d %H:%M:%S"),
        "level": level,
        "service": service,
        "message": message,
        "trace_id": trace_id,
        "host": host,
        "user_id": user_id,
        "duration_ms": duration_ms,
        "metadata": json.dumps(metadata) if metadata else None,
    }


# =============================================================================
# Anomaly Injection (for testing detection)
# =============================================================================

def inject_error_spike(service: str, start_time: datetime, count: int = 150) -> list:
    """
    Generate an error spike anomaly (150 errors in 5 minutes).
    This should trigger the 'error_spike' anomaly detection rule.
    """
    logs = []
    for i in range(count):
        # Spread errors over 5 minutes
        offset_seconds = random.randint(0, 300)
        timestamp = start_time + timedelta(seconds=offset_seconds)
        logs.append(generate_log_entry(service, timestamp, inject_anomaly=True))
    return logs


def inject_service_silence(service: str, start_time: datetime, silence_duration_minutes: int = 10) -> list:
    """
    Simulate a service crash by generating NO logs for a period.
    This should trigger the 'service_silence' anomaly.
    """
    # Return empty list — the anomaly detector will notice the absence
    return []


# =============================================================================
# File Writers
# =============================================================================

def write_logs_csv(logs: list, output_path: Path):
    """Write logs to CSV format."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    fieldnames = ["timestamp", "level", "service", "message", "trace_id", 
                  "host", "user_id", "duration_ms", "metadata"]
    
    with open(output_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(logs)
    
    print(f"✅ Wrote {len(logs)} logs to {output_path}")


def write_logs_txt(logs: list, output_path: Path):
    """Write logs to plain text format (one JSON per line)."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    with open(output_path, 'w', encoding='utf-8') as f:
        for log in logs:
            f.write(json.dumps(log) + '\n')
    
    print(f"✅ Wrote {len(logs)} logs to {output_path}")


# =============================================================================
# Main Generator Functions
# =============================================================================

def generate_service_logs(service: str, count: int, start_time: datetime, 
                          include_anomalies: bool = False) -> list:
    """
    Generate logs for a single service over a time period.
    
    Args:
        service: Service name
        count: Number of log entries to generate
        start_time: Starting timestamp
        include_anomalies: Whether to inject anomalies for testing
    
    Returns:
        List of log entry dictionaries
    """
    logs = []
    
    # Generate normal logs
    normal_count = count - (150 if include_anomalies else 0)
    for i in range(normal_count):
        # Spread logs over 1 hour
        offset_seconds = random.randint(0, 3600)
        timestamp = start_time + timedelta(seconds=offset_seconds)
        logs.append(generate_log_entry(service, timestamp))
    
    # Inject anomaly if requested
    if include_anomalies:
        anomaly_time = start_time + timedelta(minutes=30)
        anomaly_logs = inject_error_spike(service, anomaly_time, count=150)
        logs.extend(anomaly_logs)
    
    # Sort by timestamp
    logs.sort(key=lambda x: x['timestamp'])
    
    return logs


def generate_all_services(count_per_service: int, output_dir: Path, 
                          format: str = 'csv', include_anomalies: bool = False):
    """
    Generate logs for all services.
    
    Args:
        count_per_service: Number of logs per service
        output_dir: Directory to write output files
        format: 'csv' or 'txt'
        include_anomalies: Whether to inject test anomalies
    """
    start_time = datetime.now() - timedelta(hours=1)
    
    for service in SERVICES:
        logs = generate_service_logs(service, count_per_service, start_time, include_anomalies)
        
        if format == 'csv':
            output_path = output_dir / f"{service}.csv"
            write_logs_csv(logs, output_path)
        else:
            output_path = output_dir / f"{service}.txt"
            write_logs_txt(logs, output_path)
    
    print(f"\n🎉 Generated logs for {len(SERVICES)} services")
    print(f"   Total logs: {len(SERVICES) * count_per_service}")
    print(f"   Time range: {start_time.strftime('%Y-%m-%d %H:%M')} to {datetime.now().strftime('%Y-%m-%d %H:%M')}")


# =============================================================================
# CLI Interface
# =============================================================================

def main():
    parser = argparse.ArgumentParser(description="Generate sample logs for testing")
    
    parser.add_argument(
        '--service',
        type=str,
        choices=SERVICES + ['all'],
        default='all',
        help='Service to generate logs for (default: all)'
    )
    
    parser.add_argument(
        '--count',
        type=int,
        default=1000,
        help='Number of logs to generate (default: 1000)'
    )
    
    parser.add_argument(
        '--output-dir',
        type=str,
        default='data/raw_logs',
        help='Output directory (default: data/raw_logs)'
    )
    
    parser.add_argument(
        '--format',
        type=str,
        choices=['csv', 'txt'],
        default='csv',
        help='Output format (default: csv)'
    )
    
    parser.add_argument(
        '--inject-anomalies',
        action='store_true',
        help='Inject test anomalies (error spikes) into logs'
    )
    
    args = parser.parse_args()
    
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    print(f"\n{'='*60}")
    print(f"  Log Generator — Milestone 2")
    print(f"{'='*60}\n")
    
    if args.service == 'all':
        generate_all_services(
            count_per_service=args.count,
            output_dir=output_dir,
            format=args.format,
            include_anomalies=args.inject_anomalies
        )
    else:
        start_time = datetime.now() - timedelta(hours=1)
        logs = generate_service_logs(
            service=args.service,
            count=args.count,
            start_time=start_time,
            include_anomalies=args.inject_anomalies
        )
        
        if args.format == 'csv':
            output_path = output_dir / f"{args.service}.csv"
            write_logs_csv(logs, output_path)
        else:
            output_path = output_dir / f"{args.service}.txt"
            write_logs_txt(logs, output_path)


if __name__ == "__main__":
    main()