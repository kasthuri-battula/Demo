"""
alerts/alert_system.py
=======================
Alert system for sending notifications via Email, Slack, and Console.

Usage:
    from alerts.alert_system import AlertSystem
    
    alert_system = AlertSystem()
    alert_system.send_alerts(anomalies)
"""

import smtplib
import requests
import json
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from typing import List, Dict, Optional
from datetime import datetime
from pathlib import Path


class AlertSystem:
    """
    Sends alerts via multiple channels (Email, Slack, Console).
    """
    
    def __init__(self, config_path: str = 'config/alert_config.json'):
        """
        Initialize the alert system.
        
        Args:
            config_path: Path to alert configuration file
        """
        self.config = self._load_config(config_path)
        self.enabled_channels = self.config.get('enabled_channels', ['console'])
    
    def _load_config(self, config_path: str) -> Dict:
        """Load alert configuration from JSON file."""
        config_file = Path(config_path)
        
        if config_file.exists():
            with open(config_file, 'r') as f:
                return json.load(f)
        else:
            # Return default config
            print(f"⚠️  Config file not found: {config_path}")
            print(f"   Using default configuration (console only)")
            return {
                'enabled_channels': ['console'],
                'email': {
                    'smtp_server': 'smtp.gmail.com',
                    'smtp_port': 587,
                    'sender_email': 'your-email@gmail.com',
                    'sender_password': 'your-app-password',
                    'recipients': ['admin@example.com'],
                },
                'slack': {
                    'webhook_url': 'https://hooks.slack.com/services/YOUR/WEBHOOK/URL',
                },
                'severity_filters': {
                    'console': ['CRITICAL', 'HIGH', 'MEDIUM', 'LOW'],
                    'email': ['CRITICAL', 'HIGH'],
                    'slack': ['CRITICAL', 'HIGH'],
                }
            }
    
    def send_alerts(self, anomalies: List[Dict]) -> Dict[str, int]:
        """
        Send alerts for detected anomalies via all enabled channels.
        
        Args:
            anomalies: List of anomaly dictionaries
        
        Returns:
            Dictionary with count of alerts sent per channel
        """
        if not anomalies:
            print("\n✅ No anomalies to alert on")
            return {'console': 0, 'email': 0, 'slack': 0}
        
        print(f"\n🔔 Sending alerts for {len(anomalies)} anomalie(s)...")
        
        results = {
            'console': 0,
            'email': 0,
            'slack': 0,
        }
        
        # Group anomalies by severity for filtering
        for channel in self.enabled_channels:
            filtered = self._filter_by_severity(anomalies, channel)
            
            if not filtered:
                continue
            
            if channel == 'console':
                self._send_console_alerts(filtered)
                results['console'] = len(filtered)
            
            elif channel == 'email':
                success = self._send_email_alerts(filtered)
                if success:
                    results['email'] = len(filtered)
            
            elif channel == 'slack':
                success = self._send_slack_alerts(filtered)
                if success:
                    results['slack'] = len(filtered)
        
        return results
    
    def _filter_by_severity(self, anomalies: List[Dict], channel: str) -> List[Dict]:
        """Filter anomalies based on channel's severity threshold."""
        severity_filters = self.config.get('severity_filters', {})
        allowed_severities = severity_filters.get(channel, ['CRITICAL', 'HIGH', 'MEDIUM', 'LOW'])
        
        return [a for a in anomalies if a.get('severity') in allowed_severities]
    
    # =========================================================================
    # Console Alerts
    # =========================================================================
    
    def _send_console_alerts(self, anomalies: List[Dict]):
        """Print alerts to console."""
        print(f"\n{'='*70}")
        print(f"  🚨 ANOMALY ALERTS ({len(anomalies)} total)")
        print(f"{'='*70}\n")
        
        # Group by severity
        by_severity = {}
        for anom in anomalies:
            sev = anom['severity']
            if sev not in by_severity:
                by_severity[sev] = []
            by_severity[sev].append(anom)
        
        # Print in order of severity
        for severity in ['CRITICAL', 'HIGH', 'MEDIUM', 'LOW']:
            if severity not in by_severity:
                continue
            
            severity_anomalies = by_severity[severity]
            icon = self._get_severity_icon(severity)
            
            print(f"{icon} {severity} ALERTS ({len(severity_anomalies)})")
            print("-" * 70)
            
            for anom in severity_anomalies:
                print(f"\n  Service: {anom['service']}")
                print(f"  Type: {anom['name']}")
                print(f"  Window: {anom.get('window', 'N/A')}")
                print(f"  Description: {anom['description']}")
                
                # Show trigger value if available
                if 'trigger_value' in anom:
                    trigger = anom['trigger_value']
                    threshold = anom.get('threshold_value', 'N/A')
                    print(f"  Trigger: {trigger} (threshold: {threshold})")
                
                # Show detection method if available
                if 'method' in anom:
                    print(f"  Detection Method: {anom['method']}")
            
            print()
    
    def _get_severity_icon(self, severity: str) -> str:
        """Get emoji icon for severity level."""
        icons = {
            'CRITICAL': '🔴',
            'HIGH': '🟠',
            'MEDIUM': '🟡',
            'LOW': '🟢',
        }
        return icons.get(severity, '⚪')
    
    # =========================================================================
    # Email Alerts
    # =========================================================================
    
    def _send_email_alerts(self, anomalies: List[Dict]) -> bool:
        """Send alerts via email."""
        email_config = self.config.get('email', {})
        
        try:
            # Create message
            msg = MIMEMultipart('alternative')
            msg['Subject'] = f"🚨 Log Analytics Alert: {len(anomalies)} Anomalies Detected"
            msg['From'] = email_config.get('sender_email')
            msg['To'] = ', '.join(email_config.get('recipients', []))
            
            # Create email body
            html_body = self._create_email_html(anomalies)
            text_body = self._create_email_text(anomalies)
            
            msg.attach(MIMEText(text_body, 'plain'))
            msg.attach(MIMEText(html_body, 'html'))
            
            # Send email
            with smtplib.SMTP(email_config.get('smtp_server'), email_config.get('smtp_port')) as server:
                server.starttls()
                server.login(
                    email_config.get('sender_email'),
                    email_config.get('sender_password')
                )
                server.send_message(msg)
            
            print(f"✅ Email alert sent to {len(email_config.get('recipients', []))} recipient(s)")
            return True
        
        except Exception as e:
            print(f"❌ Failed to send email alert: {e}")
            return False
    
    def _create_email_html(self, anomalies: List[Dict]) -> str:
        """Create HTML email body."""
        html = f"""
        <html>
        <head>
            <style>
                body {{ font-family: Arial, sans-serif; }}
                .header {{ background-color: #d32f2f; color: white; padding: 20px; }}
                .anomaly {{ border: 1px solid #ddd; margin: 10px 0; padding: 15px; }}
                .critical {{ border-left: 5px solid #d32f2f; }}
                .high {{ border-left: 5px solid #ff9800; }}
                .medium {{ border-left: 5px solid #ffc107; }}
                .service {{ font-weight: bold; font-size: 16px; }}
                .description {{ color: #555; margin: 5px 0; }}
            </style>
        </head>
        <body>
            <div class="header">
                <h2>🚨 Anomaly Detection Alert</h2>
                <p>Detected {len(anomalies)} anomalies at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
            </div>
        """
        
        for anom in anomalies[:10]:  # Limit to 10 in email
            severity_class = anom['severity'].lower()
            html += f"""
            <div class="anomaly {severity_class}">
                <div class="service">{anom['service']} - {anom['name']}</div>
                <div class="description">{anom['description']}</div>
                <p><strong>Severity:</strong> {anom['severity']}</p>
                <p><strong>Window:</strong> {anom.get('window', 'N/A')}</p>
            </div>
            """
        
        if len(anomalies) > 10:
            html += f"<p><em>... and {len(anomalies) - 10} more anomalies</em></p>"
        
        html += """
        </body>
        </html>
        """
        
        return html
    
    def _create_email_text(self, anomalies: List[Dict]) -> str:
        """Create plain text email body."""
        text = f"ANOMALY DETECTION ALERT\n"
        text += f"Detected {len(anomalies)} anomalies at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
        
        for i, anom in enumerate(anomalies[:10], 1):
            text += f"{i}. [{anom['severity']}] {anom['service']} - {anom['name']}\n"
            text += f"   {anom['description']}\n\n"
        
        if len(anomalies) > 10:
            text += f"... and {len(anomalies) - 10} more anomalies\n"
        
        return text
    
    # =========================================================================
    # Slack Alerts
    # =========================================================================
    
    def _send_slack_alerts(self, anomalies: List[Dict]) -> bool:
        """Send alerts via Slack webhook."""
        slack_config = self.config.get('slack', {})
        webhook_url = slack_config.get('webhook_url')
        
        if not webhook_url or 'YOUR/WEBHOOK/URL' in webhook_url:
            print("⚠️  Slack webhook not configured, skipping Slack alerts")
            return False
        
        try:
            # Create Slack message
            message = self._create_slack_message(anomalies)
            
            # Send to Slack
            response = requests.post(
                webhook_url,
                json=message,
                headers={'Content-Type': 'application/json'}
            )
            
            if response.status_code == 200:
                print(f"✅ Slack alert sent successfully")
                return True
            else:
                print(f"❌ Slack alert failed: {response.status_code}")
                return False
        
        except Exception as e:
            print(f"❌ Failed to send Slack alert: {e}")
            return False
    
    def _create_slack_message(self, anomalies: List[Dict]) -> Dict:
        """Create Slack message payload."""
        # Count by severity
        severity_counts = {}
        for anom in anomalies:
            sev = anom['severity']
            severity_counts[sev] = severity_counts.get(sev, 0) + 1
        
        # Create summary
        summary = []
        for sev in ['CRITICAL', 'HIGH', 'MEDIUM', 'LOW']:
            count = severity_counts.get(sev, 0)
            if count > 0:
                icon = self._get_severity_icon(sev)
                summary.append(f"{icon} {sev}: {count}")
        
        # Build message
        message = {
            "text": f"🚨 *Anomaly Detection Alert*",
            "blocks": [
                {
                    "type": "header",
                    "text": {
                        "type": "plain_text",
                        "text": "🚨 Anomaly Detection Alert"
                    }
                },
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"*{len(anomalies)} anomalies detected* at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n" + " | ".join(summary)
                    }
                },
                {
                    "type": "divider"
                }
            ]
        }
        
        # Add top 5 anomalies as sections
        for anom in anomalies[:5]:
            icon = self._get_severity_icon(anom['severity'])
            message["blocks"].append({
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"{icon} *{anom['service']}* - {anom['name']}\n_{anom['description']}_"
                }
            })
        
        if len(anomalies) > 5:
            message["blocks"].append({
                "type": "context",
                "elements": [
                    {
                        "type": "mrkdwn",
                        "text": f"_... and {len(anomalies) - 5} more anomalies_"
                    }
                ]
            })
        
        return message


# =============================================================================
# CLI Interface
# =============================================================================

def main():
    """Command-line interface for testing alerts."""
    import argparse
    import sys
    from pathlib import Path
    
    sys.path.insert(0, str(Path(__file__).parent.parent))
    
    parser = argparse.ArgumentParser(description="Test alert system")
    parser.add_argument(
        '--anomalies',
        type=str,
        default='data/anomalies.json',
        help='Path to anomalies JSON file'
    )
    parser.add_argument(
        '--config',
        type=str,
        default='config/alert_config.json',
        help='Path to alert configuration'
    )
    parser.add_argument(
        '--channels',
        nargs='+',
        choices=['console', 'email', 'slack'],
        help='Override enabled channels'
    )
    
    args = parser.parse_args()
    
    print(f"\n{'='*70}")
    print(f"  Alert System Test — Milestone 3 Task 2")
    print(f"{'='*70}\n")
    
    try:
        # Load anomalies
        with open(args.anomalies, 'r') as f:
            anomalies = json.load(f)
        
        print(f"📂 Loaded {len(anomalies)} anomalies from {args.anomalies}")
        
        # Initialize alert system
        alert_system = AlertSystem(config_path=args.config)
        
        # Override channels if specified
        if args.channels:
            alert_system.enabled_channels = args.channels
            print(f"📢 Using channels: {', '.join(args.channels)}")
        else:
            print(f"📢 Enabled channels: {', '.join(alert_system.enabled_channels)}")
        
        # Send alerts
        results = alert_system.send_alerts(anomalies)
        
        # Summary
        print(f"\n{'='*70}")
        print(f"  ALERT SUMMARY")
        print(f"{'='*70}\n")
        
        for channel, count in results.items():
            if count > 0:
                print(f"   {channel.title()}: {count} alert(s) sent")
    
    except FileNotFoundError:
        print(f"❌ Anomalies file not found: {args.anomalies}")
        print(f"   Run detection first to generate anomalies")
        sys.exit(1)
    
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()