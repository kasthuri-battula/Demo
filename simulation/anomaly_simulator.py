"""
simulation/anomaly_simulator.py
================================
Simulates different anomaly patterns for testing detection accuracy.

Usage:
    python simulation/anomaly_simulator.py --pattern error_spike --count 5000
"""

import random
import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict
import sys

sys.path.insert(0, str(Path(__file__).parent.parent))

from generators.log_generator import generate_log_entry, write_logs_csv


class AnomalySimulator:
    """
    Simulates various anomaly patterns for testing detection systems.
    """
    
    def __init__(self, service: str = 'test-service'):
        """
        Initialize the simulator.
        
        Args:
            service: Name of the service to simulate
        """
        self.service = service
        self.anomaly_patterns = {
            'error_spike': self.simulate_error_spike,
            'slow_response': self.simulate_slow_response,
            'gradual_degradation': self.simulate_gradual_degradation,
            'intermittent_failures': self.simulate_intermittent_failures,
            'service_crash': self.simulate_service_crash,
            'memory_leak': self.simulate_memory_leak,
            'cascading_failure': self.simulate_cascading_failure,
        }
    
    def simulate_error_spike(self, count: int, start_time: datetime) -> List[Dict]:
        """
        Simulate a sudden error spike (150 errors in 5 minutes).
        Ground truth: Should trigger 'error_spike' anomaly.
        """
        print(f"📊 Simulating ERROR SPIKE pattern...")
        logs = []
        
        # Normal logs for first 30 minutes
        for i in range(count - 150):
            offset = random.randint(0, 1800)  # First 30 minutes
            timestamp = start_time + timedelta(seconds=offset)
            logs.append(generate_log_entry(self.service, timestamp, inject_anomaly=False))
        
        # Error spike in 5-minute window (at 30-35 minutes)
        spike_start = start_time + timedelta(minutes=30)
        for i in range(150):
            offset = random.randint(0, 300)  # 5-minute window
            timestamp = spike_start + timedelta(seconds=offset)
            logs.append(generate_log_entry(self.service, timestamp, inject_anomaly=True))
        
        # Normal logs for remaining time
        for i in range(count - len(logs)):
            offset = random.randint(2100, 3600)  # Last 25 minutes
            timestamp = start_time + timedelta(seconds=offset)
            logs.append(generate_log_entry(self.service, timestamp, inject_anomaly=False))
        
        logs.sort(key=lambda x: x['timestamp'])
        
        print(f"✅ Generated {len(logs)} logs with ERROR SPIKE at 30-35 min mark")
        print(f"   Expected detection: error_spike (150 errors in 5 minutes)")
        return logs
    
    def simulate_slow_response(self, count: int, start_time: datetime) -> List[Dict]:
        """
        Simulate gradually increasing response times.
        Ground truth: Should trigger 'slow_response' anomaly.
        """
        print(f"📊 Simulating SLOW RESPONSE pattern...")
        logs = []
        
        for i in range(count):
            offset = random.randint(0, 3600)
            timestamp = start_time + timedelta(seconds=offset)
            
            # Create log entry
            log = generate_log_entry(self.service, timestamp, inject_anomaly=False)
            
            # Artificially increase duration_ms over time
            progress = offset / 3600  # 0 to 1
            
            if progress < 0.5:
                # First half: normal response times (100-500ms)
                log['duration_ms'] = random.randint(100, 500)
            else:
                # Second half: degraded response times (2000-5000ms)
                log['duration_ms'] = random.randint(2000, 5000)
            
            logs.append(log)
        
        logs.sort(key=lambda x: x['timestamp'])
        
        print(f"✅ Generated {len(logs)} logs with SLOW RESPONSE in second half")
        print(f"   Expected detection: slow_response (avg duration > 2000ms)")
        return logs
    
    def simulate_gradual_degradation(self, count: int, start_time: datetime) -> List[Dict]:
        """
        Simulate gradual increase in error rate over time.
        Ground truth: Should trigger multiple statistical anomalies.
        """
        print(f"📊 Simulating GRADUAL DEGRADATION pattern...")
        logs = []
        
        for i in range(count):
            offset = random.randint(0, 3600)
            timestamp = start_time + timedelta(seconds=offset)
            
            # Calculate error probability based on time progression
            progress = offset / 3600  # 0 to 1
            error_probability = 0.05 + (progress * 0.15)  # 5% → 20%
            
            inject_error = random.random() < error_probability
            
            log = generate_log_entry(self.service, timestamp, inject_anomaly=inject_error)
            logs.append(log)
        
        logs.sort(key=lambda x: x['timestamp'])
        
        print(f"✅ Generated {len(logs)} logs with GRADUAL DEGRADATION")
        print(f"   Expected detection: rate_of_change, moving_average_spike")
        return logs
    
    def simulate_intermittent_failures(self, count: int, start_time: datetime) -> List[Dict]:
        """
        Simulate periodic error bursts (every 10 minutes).
        Ground truth: Multiple small spikes.
        """
        print(f"📊 Simulating INTERMITTENT FAILURES pattern...")
        logs = []
        
        burst_every_seconds = 600  # Every 10 minutes
        burst_duration = 60        # 1-minute bursts
        
        for i in range(count):
            offset = random.randint(0, 3600)
            timestamp = start_time + timedelta(seconds=offset)
            
            # Check if we're in a burst window
            time_in_cycle = offset % burst_every_seconds
            in_burst = time_in_cycle < burst_duration
            
            inject_error = in_burst and random.random() < 0.5
            
            log = generate_log_entry(self.service, timestamp, inject_anomaly=inject_error)
            logs.append(log)
        
        logs.sort(key=lambda x: x['timestamp'])
        
        print(f"✅ Generated {len(logs)} logs with INTERMITTENT FAILURES")
        print(f"   Expected detection: Multiple small error spikes")
        return logs
    
    def simulate_service_crash(self, count: int, start_time: datetime) -> List[Dict]:
        """
        Simulate service crash (no logs for 10+ minutes).
        Ground truth: Should trigger 'service_silence' anomaly.
        """
        print(f"📊 Simulating SERVICE CRASH pattern...")
        logs = []
        
        crash_start = 30 * 60  # Crash at 30 minutes
        crash_duration = 15 * 60  # 15-minute outage
        
        for i in range(count):
            offset = random.randint(0, 3600)
            
            # Skip logs during crash window
            if crash_start <= offset < crash_start + crash_duration:
                continue
            
            timestamp = start_time + timedelta(seconds=offset)
            log = generate_log_entry(self.service, timestamp, inject_anomaly=False)
            logs.append(log)
        
        logs.sort(key=lambda x: x['timestamp'])
        
        print(f"✅ Generated {len(logs)} logs with SERVICE CRASH (15-min silence)")
        print(f"   Expected detection: service_silence")
        return logs
    
    def simulate_memory_leak(self, count: int, start_time: datetime) -> List[Dict]:
        """
        Simulate memory leak (increasing WARNING logs over time).
        Ground truth: Should trigger 'warning_flood' anomaly.
        """
        print(f"📊 Simulating MEMORY LEAK pattern...")
        logs = []
        
        for i in range(count):
            offset = random.randint(0, 3600)
            timestamp = start_time + timedelta(seconds=offset)
            
            # Create log
            log = generate_log_entry(self.service, timestamp, inject_anomaly=False)
            
            # Increase WARNING probability over time
            progress = offset / 3600
            if progress > 0.5 and random.random() < progress:
                log['level'] = 'WARNING'
                log['message'] = 'High memory usage detected'
            
            logs.append(log)
        
        logs.sort(key=lambda x: x['timestamp'])
        
        print(f"✅ Generated {len(logs)} logs with MEMORY LEAK pattern")
        print(f"   Expected detection: warning_flood")
        return logs
    
    def simulate_cascading_failure(self, count: int, start_time: datetime) -> List[Dict]:
        """
        Simulate cascading failure (errors increase exponentially).
        Ground truth: Multiple detection methods should trigger.
        """
        print(f"📊 Simulating CASCADING FAILURE pattern...")
        logs = []
        
        for i in range(count):
            offset = random.randint(0, 3600)
            timestamp = start_time + timedelta(seconds=offset)
            
            # Exponential error increase
            progress = offset / 3600
            error_probability = min(0.05 * (2 ** (progress * 3)), 0.5)
            
            inject_error = random.random() < error_probability
            
            log = generate_log_entry(self.service, timestamp, inject_anomaly=inject_error)
            
            # Also increase CRITICAL logs
            if inject_error and random.random() < 0.3:
                log['level'] = 'CRITICAL'
            
            logs.append(log)
        
        logs.sort(key=lambda x: x['timestamp'])
        
        print(f"✅ Generated {len(logs)} logs with CASCADING FAILURE")
        print(f"   Expected detection: error_spike, critical_spike, rate_of_change")
        return logs
    
    def generate_pattern(self, pattern: str, count: int, output_dir: str) -> str:
        """
        Generate a specific anomaly pattern and save to file.
        
        Args:
            pattern: Name of anomaly pattern to simulate
            count: Number of logs to generate
            output_dir: Directory to save output
        
        Returns:
            Path to generated CSV file
        """
        if pattern not in self.anomaly_patterns:
            raise ValueError(f"Unknown pattern: {pattern}. Available: {list(self.anomaly_patterns.keys())}")
        
        start_time = datetime.now() - timedelta(hours=1)
        
        # Generate logs with pattern
        simulate_func = self.anomaly_patterns[pattern]
        logs = simulate_func(count, start_time)
        
        # Save to CSV
        output_path = Path(output_dir) / f"simulation_{pattern}_{self.service}.csv"
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        write_logs_csv(logs, output_path)
        
        return str(output_path)
    
    def generate_all_patterns(self, count_per_pattern: int, output_dir: str) -> Dict[str, str]:
        """
        Generate all anomaly patterns for comprehensive testing.
        
        Args:
            count_per_pattern: Number of logs per pattern
            output_dir: Directory to save outputs
        
        Returns:
            Dictionary mapping pattern names to file paths
        """
        print(f"\n{'='*70}")
        print(f"  Generating All Anomaly Patterns")
        print(f"{'='*70}\n")
        
        results = {}
        
        for pattern in self.anomaly_patterns.keys():
            print(f"\n{pattern}:")
            print("-" * 70)
            
            output_path = self.generate_pattern(pattern, count_per_pattern, output_dir)
            results[pattern] = output_path
        
        print(f"\n{'='*70}")
        print(f"  Generated {len(results)} patterns")
        print(f"{'='*70}")
        
        return results


# =============================================================================
# CLI Interface
# =============================================================================

def main():
    """Command-line interface for anomaly simulation."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Simulate anomaly patterns")
    parser.add_argument(
        '--pattern',
        type=str,
        choices=[
            'error_spike', 'slow_response', 'gradual_degradation',
            'intermittent_failures', 'service_crash', 'memory_leak',
            'cascading_failure', 'all'
        ],
        default='error_spike',
        help='Anomaly pattern to simulate'
    )
    parser.add_argument(
        '--count',
        type=int,
        default=2000,
        help='Number of logs to generate per pattern'
    )
    parser.add_argument(
        '--output-dir',
        type=str,
        default='data/simulations',
        help='Output directory'
    )
    parser.add_argument(
        '--service',
        type=str,
        default='test-service',
        help='Service name'
    )
    
    args = parser.parse_args()
    
    print(f"\n{'='*70}")
    print(f"  Anomaly Pattern Simulator — Milestone 3 Task 3")
    print(f"{'='*70}\n")
    
    simulator = AnomalySimulator(service=args.service)
    
    if args.pattern == 'all':
        results = simulator.generate_all_patterns(args.count, args.output_dir)
        
        print(f"\n📂 Generated files:")
        for pattern, path in results.items():
            print(f"   {pattern}: {path}")
    else:
        output_path = simulator.generate_pattern(args.pattern, args.count, args.output_dir)
        print(f"\n📂 Generated: {output_path}")
    
    print(f"\n💡 Next steps:")
    print(f"   1. Run detection: python detection/advanced_detector.py --log-dir {args.output_dir}")
    print(f"   2. Check if expected anomalies were detected")
    print(f"   3. Measure false positive/negative rates")


if __name__ == "__main__":
    main()