"""
Automated Data Collection Scheduler
Daily Bloomberg data collection with email/slack alerts
"""

import os
import sys
import schedule
import time
import smtplib
import json
import pandas as pd
from datetime import datetime, timedelta
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
import subprocess
import logging

# Add project root to path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

# Configure logging
import os

# Create logs directory if it doesn't exist
os.makedirs('logs', exist_ok=True)
os.makedirs('reports', exist_ok=True)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/scheduler.log'),
        logging.StreamHandler()
    ]
)

class DataCollectionScheduler:
    """Automated Bloomberg data collection with notifications"""
    
    def __init__(self):
        self.setup_directories()
        self.config = self.load_config()

    
    def load_config(self):
        """Load configuration for email/slack notifications"""
        config = {
            # Email settings
            'email': {
                'smtp_server': 'smtp.gmail.com',
                'smtp_port': 587,
                'sender_email': 'your_email@gmail.com',  # UPDATE THIS
                'sender_password': 'your_app_password',   # UPDATE THIS
                'recipient_emails': ['team@yourcompany.com']  # UPDATE THIS
            },
            # Slack settings (optional)
            'slack': {
                'webhook_url': 'https://hooks.slack.com/your/webhook/url',  # UPDATE THIS
                'channel': '#bloomberg-data'
            },
            # Data collection settings
            'collection': {
                'max_retries': 3,
                'retry_delay_minutes': 30,
                'data_quality_threshold': 0.8  # 80% data completeness required
            }
        }
        return config
    
    def setup_directories(self):
        """Create necessary directories"""
        directories = ['logs', 'reports', 'backups']
        for directory in directories:
            os.makedirs(directory, exist_ok=True)

    def run_script(self, script_path, script_name):
        """Run a data collection script with error handling"""
        try:
            logging.info(f"Starting {script_name}...")

            # Run the script
            result = subprocess.run(
                [sys.executable, script_path],
                capture_output=True,
                text=True,
                timeout=3600  # 1 hour timeout
            )

            if result.returncode == 0:
                logging.info(f"SUCCESS: {script_name} completed successfully")
                return True, result.stdout, None
            else:
                logging.error(f"FAILED: {script_name} failed with return code {result.returncode}")
                logging.error(f"STDERR: {result.stderr}")  # Show actual error
                return False, result.stdout, result.stderr

        except subprocess.TimeoutExpired:
            logging.error(f"TIMEOUT: {script_name} timed out after 1 hour")
            return False, None, "Script timed out"
        except Exception as e:
            logging.error(f"EXCEPTION: {script_name} failed with exception: {e}")
            return False, None, str(e)

    def collect_daily_data(self):
        """Run daily data collection with retries"""
        logging.info("=" * 60)
        logging.info("DAILY BLOOMBERG DATA COLLECTION STARTED")
        logging.info("=" * 60)

        collection_results = {}  # FIX: Initialize this variable

        # Scripts to run
        scripts = [
            ('scripts/fetch_labeled_volatility_data.py', 'Current Volatility Data'),  # Use emoji-free version
            ('scripts/fetch_spy_weights.py', 'SPY Weights Data'),  # Use emoji-free version
            ('scripts/fetch_historical_volatility.py', 'Historical Volatility Update')  # Use emoji-free version
        ]

        for script_path, script_name in scripts:
            success = False
            retries = 0

            while not success and retries < self.config['collection']['max_retries']:
                if retries > 0:
                    logging.info(f"Retrying {script_name} (attempt {retries + 1})")
                    time.sleep(self.config['collection']['retry_delay_minutes'] * 60)

                success, stdout, stderr = self.run_script(script_path, script_name)
                retries += 1

                collection_results[script_name] = {
                    'success': success,
                    'attempts': retries,
                    'stdout': stdout,
                    'stderr': stderr,
                    'timestamp': datetime.now().isoformat()
                }

                if success:
                    break

        # Generate report
        report = self.generate_daily_report(collection_results)

        # Send notifications
        self.send_notifications(report, collection_results)

        logging.info("=" * 60)
        logging.info("DAILY DATA COLLECTION COMPLETED")
        logging.info("=" * 60)

        return collection_results
    
    def generate_daily_report(self, collection_results):
        """Generate daily data collection report"""
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        # Calculate success rate
        total_scripts = len(collection_results)
        successful_scripts = sum(1 for result in collection_results.values() if result['success'])
        success_rate = (successful_scripts / total_scripts) * 100 if total_scripts > 0 else 0
        
        # Get data quality metrics
        data_quality = self.assess_data_quality()
        
        report = {
            'timestamp': timestamp,
            'summary': {
                'total_scripts': total_scripts,
                'successful_scripts': successful_scripts,
                'success_rate': success_rate,
                'overall_status': 'SUCCESS' if success_rate >= 80 else 'PARTIAL' if success_rate >= 50 else 'FAILED'
            },
            'script_results': collection_results,
            'data_quality': data_quality,
            'next_collection': (datetime.now() + timedelta(days=1)).strftime('%Y-%m-%d 09:00:00')
        }
        
        # Save report
        report_file = f"reports/daily_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(report_file, 'w') as f:
            json.dump(report, f, indent=2)
        
        logging.info(f"📊 Daily report saved to: {report_file}")
        
        return report
    
    def assess_data_quality(self):
        """Assess quality of collected data"""
        try:
            # Check latest volatility data
            vol_files = []
            vol_dir = 'data/processed/volatility'
            if os.path.exists(vol_dir):
                vol_files = [f for f in os.listdir(vol_dir) if f.startswith('labeled_volatility_data_')]
            
            # Check latest SPY weights
            weights_files = []
            weights_dir = 'data/processed/spy_weights'
            if os.path.exists(weights_dir):
                weights_files = [f for f in os.listdir(weights_dir) if f.startswith('spy_weights_')]
            
            quality_metrics = {
                'volatility_data': {
                    'files_available': len(vol_files),
                    'latest_file': max(vol_files) if vol_files else None,
                    'file_size_mb': None,
                    'row_count': None
                },
                'spy_weights': {
                    'files_available': len(weights_files),
                    'latest_file': max(weights_files) if weights_files else None,
                    'file_size_mb': None,
                    'row_count': None
                }
            }
            
            # Get detailed metrics for latest files
            if vol_files:
                latest_vol_file = os.path.join(vol_dir, max(vol_files))
                if os.path.exists(latest_vol_file):
                    file_size = os.path.getsize(latest_vol_file) / (1024 * 1024)  # MB
                    quality_metrics['volatility_data']['file_size_mb'] = round(file_size, 2)
                    
                    try:
                        df = pd.read_csv(latest_vol_file)
                        quality_metrics['volatility_data']['row_count'] = len(df)
                    except:
                        pass
            
            return quality_metrics
            
        except Exception as e:
            logging.error(f"Error assessing data quality: {e}")
            return {'error': str(e)}
    
    def send_email_notification(self, report):
        """Send email notification with daily report"""
        try:
            msg = MIMEMultipart()
            msg['From'] = self.config['email']['sender_email']
            msg['To'] = ', '.join(self.config['email']['recipient_emails'])
            msg['Subject'] = f"Bloomberg Data Collection Report - {datetime.now().strftime('%Y-%m-%d')}"
            
            # Create email body
            status_emoji = "✅" if report['summary']['overall_status'] == 'SUCCESS' else "⚠️" if report['summary']['overall_status'] == 'PARTIAL' else "❌"
            
            body = f"""
{status_emoji} Bloomberg Data Collection Daily Report
{'='*50}

Summary:
• Overall Status: {report['summary']['overall_status']}
• Success Rate: {report['summary']['success_rate']:.1f}%
• Successful Scripts: {report['summary']['successful_scripts']}/{report['summary']['total_scripts']}
• Collection Time: {report['timestamp']}

Script Results:
"""
            
            for script_name, result in report['script_results'].items():
                status = "✅" if result['success'] else "❌"
                body += f"• {status} {script_name} (attempts: {result['attempts']})\n"
            
            body += f"""
Data Quality:
• Volatility files: {report['data_quality']['volatility_data']['files_available']}
• SPY weights files: {report['data_quality']['spy_weights']['files_available']}

Next collection scheduled: {report['next_collection']}

---
Automated Bloomberg Data Collection System
"""
            
            msg.attach(MIMEText(body, 'plain'))
            
            # Send email
            server = smtplib.SMTP(self.config['email']['smtp_server'], self.config['email']['smtp_port'])
            server.starttls()
            server.login(self.config['email']['sender_email'], self.config['email']['sender_password'])
            text = msg.as_string()
            server.sendmail(self.config['email']['sender_email'], self.config['email']['recipient_emails'], text)
            server.quit()
            
            logging.info("✅ Email notification sent successfully")
            
        except Exception as e:
            logging.error(f"❌ Failed to send email notification: {e}")
    
    def send_slack_notification(self, report):
        """Send Slack notification (optional)"""
        try:
            import requests
            
            status_emoji = "✅" if report['summary']['overall_status'] == 'SUCCESS' else "⚠️" if report['summary']['overall_status'] == 'PARTIAL' else "❌"
            
            message = {
                "channel": self.config['slack']['channel'],
                "username": "Bloomberg Data Bot",
                "text": f"{status_emoji} Daily Bloomberg Data Collection Complete",
                "attachments": [
                    {
                        "color": "good" if report['summary']['overall_status'] == 'SUCCESS' else "warning" if report['summary']['overall_status'] == 'PARTIAL' else "danger",
                        "fields": [
                            {
                                "title": "Success Rate",
                                "value": f"{report['summary']['success_rate']:.1f}%",
                                "short": True
                            },
                            {
                                "title": "Scripts Completed",
                                "value": f"{report['summary']['successful_scripts']}/{report['summary']['total_scripts']}",
                                "short": True
                            }
                        ]
                    }
                ]
            }
            
            response = requests.post(self.config['slack']['webhook_url'], json=message)
            if response.status_code == 200:
                logging.info("✅ Slack notification sent successfully")
            else:
                logging.error(f"❌ Slack notification failed: {response.status_code}")
                
        except Exception as e:
            logging.error(f"❌ Failed to send Slack notification: {e}")
    
    def send_notifications(self, report, collection_results):
        """Send all configured notifications"""
        # Always send email
        self.send_email_notification(report)
        
        # Send Slack if configured
        if self.config['slack']['webhook_url'] != 'https://hooks.slack.com/your/webhook/url':
            self.send_slack_notification(report)
    
    def start_scheduler(self):
        """Start the daily scheduler"""
        logging.info("🚀 Starting Bloomberg Data Collection Scheduler")
        
        # Schedule daily collection at 9:00 AM
        schedule.every().day.at("09:00").do(self.collect_daily_data)
        
        # Optional: Schedule additional collections
        # schedule.every().day.at("17:00").do(self.collect_daily_data)  # End of day
        
        logging.info("📅 Scheduled daily data collection at 09:00 AM")
        logging.info("⏰ Scheduler running... Press Ctrl+C to stop")
        
        try:
            while True:
                schedule.run_pending()
                time.sleep(60)  # Check every minute
        except KeyboardInterrupt:
            logging.info("🛑 Scheduler stopped by user")

def main():
    """Main execution function"""
    scheduler = DataCollectionScheduler()
    
    # Test run (uncomment to test immediately)
    # scheduler.collect_daily_data()
    
    # Start daily scheduler
    scheduler.start_scheduler()

if __name__ == "__main__":
    main()