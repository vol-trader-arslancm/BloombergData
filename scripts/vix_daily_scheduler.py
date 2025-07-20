"""
VIX Daily Data Scheduler with Email/Slack Alerts
Windows-compatible scheduler for daily VIX data collection and monitoring
"""

import schedule
import time
import subprocess
import sys
import os
import json
import logging
import smtplib
import requests
from datetime import datetime, timedelta
from pathlib import Path
from email.mime.text import MimeText
from email.mime.multipart import MimeMultipart

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('vix_scheduler.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class VIXScheduler:
    """
    Daily scheduler for VIX data collection with comprehensive monitoring
    """
    
    def __init__(self):
        self.project_root = Path(__file__).parent.parent.absolute()
        self.config_dir = self.project_root / 'config'
        self.data_dir = self.project_root / 'data' / 'vix_data'
        self.scripts_dir = self.project_root / 'scripts'
        
        # Create directories
        self.config_dir.mkdir(parents=True, exist_ok=True)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.scripts_dir.mkdir(parents=True, exist_ok=True)
        
        self.schedule_config = self.load_schedule_config()
        self.email_config = self.load_email_config()
        self.slack_config = self.load_slack_config()
        
        logger.info("VIX Scheduler initialized")
    
    def load_schedule_config(self):
        """Load or create scheduling configuration"""
        config_file = self.config_dir / 'schedule_config.json'
        
        default_config = {
            "daily_collection_time": "18:30",  # 6:30 PM (after market close)
            "weekend_collection": False,
            "retry_attempts": 3,
            "retry_delay_minutes": 15,
            "data_validation": True,
            "alert_on_failure": True,
            "alert_on_success": True,
            "timezone": "US/Eastern"
        }
        
        if config_file.exists():
            with open(config_file) as f:
                config = json.load(f)
            logger.info(f"Loaded schedule config from {config_file}")
        else:
            config = default_config
            with open(config_file, 'w') as f:
                json.dump(config, f, indent=2)
            logger.info(f"Created default schedule config at {config_file}")
        
        return config
    
    def load_email_config(self):
        """Load email configuration"""
        config_file = self.config_dir / 'email_config.json'
        
        if config_file.exists():
            with open(config_file) as f:
                config = json.load(f)
            logger.info("Email configuration loaded")
            return config
        else:
            logger.warning(f"Email config not found at {config_file}")
            logger.info("Create email_config.json with your SMTP settings")
            return None
    
    def load_slack_config(self):
        """Load Slack configuration"""
        config_file = self.config_dir / 'slack_config.json'
        
        if config_file.exists():
            with open(config_file) as f:
                config = json.load(f)
            logger.info("Slack configuration loaded")
            return config
        else:
            logger.warning(f"Slack config not found at {config_file}")
            return None
    
    def send_email_notification(self, subject, body):
        """Send email notification"""
        if not self.email_config:
            logger.warning("Email config not available, skipping email notification")
            return False
        
        try:
            msg = MimeMultipart()
            msg['From'] = self.email_config['sender_email']
            msg['To'] = ', '.join(self.email_config['recipient_emails'])
            msg['Subject'] = f"[VIX Data] {subject}"
            
            msg.attach(MimeText(body, 'plain'))
            
            server = smtplib.SMTP(self.email_config['smtp_server'], self.email_config['smtp_port'])
            server.starttls()
            server.login(self.email_config['sender_email'], self.email_config['sender_password'])
            
            text = msg.as_string()
            server.sendmail(self.email_config['sender_email'], self.email_config['recipient_emails'], text)
            server.quit()
            
            logger.info("Email notification sent successfully")
            return True
            
        except Exception as e:
            logger.error(f"Failed to send email notification: {e}")
            return False
    
    def send_slack_notification(self, message):
        """Send Slack notification"""
        if not self.slack_config:
            logger.warning("Slack config not available, skipping Slack notification")
            return False
        
        try:
            payload = {
                "channel": self.slack_config['channel'],
                "text": f"🔔 VIX Data Alert\n{message}",
                "username": "VIX Data Bot",
                "icon_emoji": ":chart_with_upwards_trend:"
            }
            
            response = requests.post(self.slack_config['webhook_url'], json=payload)
            
            if response.status_code == 200:
                logger.info("Slack notification sent successfully")
                return True
            else:
                logger.error(f"Slack notification failed: {response.status_code}")
                return False
                
        except Exception as e:
            logger.error(f"Failed to send Slack notification: {e}")
            return False
    
    def run_vix_collection(self):
        """Execute VIX data collection script"""
        try:
            logger.info("Starting daily VIX data collection...")
            
            # Path to the VIX data fetcher script
            vix_script = self.scripts_dir / 'vix_data_fetcher.py'
            
            if not vix_script.exists():
                raise FileNotFoundError(f"VIX data fetcher script not found: {vix_script}")
            
            # Run the VIX data collection script
            result = subprocess.run(
                [sys.executable, str(vix_script)],
                capture_output=True,
                text=True,
                timeout=3600  # 1 hour timeout
            )
            
            if result.returncode == 0:
                logger.info("VIX data collection completed successfully")
                
                if self.schedule_config.get('alert_on_success', True):
                    success_message = f"""
✅ Daily VIX Data Collection - SUCCESS

📅 Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
📊 Collection completed successfully
🗂️ Data saved to: {self.data_dir}

Output:
{result.stdout[-500:] if result.stdout else 'No output'}
                    """
                    
                    self.send_email_notification("Daily Collection Success", success_message)
                    self.send_slack_notification(success_message)
                
                return True
            else:
                raise subprocess.CalledProcessError(result.returncode, vix_script, result.stderr)
                
        except Exception as e:
            logger.error(f"VIX data collection failed: {e}")
            
            if self.schedule_config.get('alert_on_failure', True):
                error_message = f"""
❌ Daily VIX Data Collection - FAILED

📅 Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
💥 Error: {str(e)}

Please check:
1. Bloomberg Terminal is running and logged in
2. Network connectivity is stable
3. VIX data fetcher script is accessible
4. Sufficient disk space for data storage

Log file: vix_scheduler.log
                """
                
                self.send_email_notification("Daily Collection FAILED", error_message)
                self.send_slack_notification(error_message)
            
            return False
    
    def validate_recent_data(self):
        """Validate that recent data collection was successful"""
        try:
            # Check for recent data files
            today = datetime.now().strftime('%Y%m%d')
            yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')
            
            recent_files = []
            for date_pattern in [today, yesterday]:
                pattern = f"vix_*{date_pattern}*.csv"
                recent_files.extend(list(self.data_dir.glob(pattern)))
            
            if recent_files:
                logger.info(f"Found {len(recent_files)} recent VIX data files")
                return True
            else:
                logger.warning("No recent VIX data files found")
                
                alert_message = f"""
⚠️ VIX Data Validation Warning

📅 Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
📁 No recent VIX data files found in {self.data_dir}

This could indicate:
- Collection process is not running
- Files are being saved to different location
- Data collection is failing silently

Please investigate immediately.
                """
                
                self.send_email_notification("Data Validation Warning", alert_message)
                self.send_slack_notification(alert_message)
                return False
                
        except Exception as e:
            logger.error(f"Data validation failed: {e}")
            return False
    
    def daily_collection_job(self):
        """Main daily collection job with retry logic"""
        logger.info("="*60)
        logger.info("DAILY VIX DATA COLLECTION JOB STARTED")
        logger.info("="*60)
        
        max_attempts = self.schedule_config.get('retry_attempts', 3)
        retry_delay = self.schedule_config.get('retry_delay_minutes', 15)
        
        for attempt in range(1, max_attempts + 1):
            logger.info(f"Collection attempt {attempt}/{max_attempts}")
            
            success = self.run_vix_collection()
            
            if success:
                logger.info("Daily VIX collection completed successfully")
                
                # Validate data if configured
                if self.schedule_config.get('data_validation', True):
                    self.validate_recent_data()
                
                break
            else:
                if attempt < max_attempts:
                    logger.warning(f"Attempt {attempt} failed, retrying in {retry_delay} minutes...")
                    time.sleep(retry_delay * 60)
                else:
                    logger.error("All collection attempts failed")
        
        logger.info("Daily VIX collection job completed")
    
    def weekend_maintenance_job(self):
        """Weekend maintenance and system checks"""
        logger.info("Running weekend maintenance...")
        
        # Check data directory size
        total_size = sum(f.stat().st_size for f in self.data_dir.rglob('*') if f.is_file())
        size_mb = total_size / (1024 * 1024)
        
        # Check for old log files
        log_files = list(Path('.').glob('*.log'))
        
        maintenance_report = f"""
🔧 Weekend VIX Data System Maintenance

📅 Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
📊 System Status: Healthy
💾 Data Directory Size: {size_mb:.2f} MB
📁 Total Data Files: {len(list(self.data_dir.rglob('*.csv')))}
📝 Log Files: {len(log_files)}

Next scheduled collection: {schedule.next_run()}
        """
        
        self.send_email_notification("Weekend System Maintenance", maintenance_report)
        self.send_slack_notification(maintenance_report)
        
        logger.info("Weekend maintenance completed")
    
    def setup_schedule(self):
        """Setup the daily schedule"""
        collection_time = self.schedule_config.get('daily_collection_time', '18:30')
        weekend_collection = self.schedule_config.get('weekend_collection', False)
        
        if weekend_collection:
            # Schedule daily collection including weekends
            schedule.every().day.at(collection_time).do(self.daily_collection_job)
            logger.info(f"Scheduled daily VIX collection at {collection_time} (including weekends)")
        else:
            # Schedule only on weekdays
            schedule.every().monday.at(collection_time).do(self.daily_collection_job)
            schedule.every().tuesday.at(collection_time).do(self.daily_collection_job)
            schedule.every().wednesday.at(collection_time).do(self.daily_collection_job)
            schedule.every().thursday.at(collection_time).do(self.daily_collection_job)
            schedule.every().friday.at(collection_time).do(self.daily_collection_job)
            logger.info(f"Scheduled weekday VIX collection at {collection_time}")
        
        # Schedule weekend maintenance
        schedule.every().saturday.at("10:00").do(self.weekend_maintenance_job)
        
        # Send startup notification
        startup_message = f"""
🚀 VIX Data Scheduler Started

📅 Startup Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
⏰ Daily Collection: {collection_time}
📊 Weekend Collection: {'Enabled' if weekend_collection else 'Disabled'}
🔄 Retry Attempts: {self.schedule_config.get('retry_attempts', 3)}

Next scheduled run: {schedule.next_run()}

System is now monitoring VIX data collection.
        """
        
        self.send_email_notification("VIX Scheduler Started", startup_message)
        self.send_slack_notification(startup_message)
    
    def run(self):
        """Run the scheduler"""
        logger.info("VIX Data Scheduler starting...")
        
        self.setup_schedule()
        
        logger.info("Scheduler is now running. Press Ctrl+C to stop.")
        
        try:
            while True:
                schedule.run_pending()
                time.sleep(60)  # Check every minute
                
        except KeyboardInterrupt:
            logger.info("Scheduler stopped by user")
            
            shutdown_message = f"""
⏹️ VIX Data Scheduler Stopped

📅 Shutdown Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
👤 Stopped by user (Ctrl+C)

To restart the scheduler, run the script again.
            """
            
            self.send_email_notification("VIX Scheduler Stopped", shutdown_message)
            self.send_slack_notification(shutdown_message)

def create_config_templates():
    """Create configuration file templates"""
    project_root = Path(__file__).parent.parent.absolute()
    config_dir = project_root / 'config'
    config_dir.mkdir(parents=True, exist_ok=True)
    
    # Email config template
    email_template = {
        "smtp_server": "smtp.gmail.com",
        "smtp_port": 587,
        "sender_email": "your.email@gmail.com",
        "sender_password": "your_app_password_here",
        "recipient_emails": [
            "recipient1@company.com",
            "recipient2@company.com"
        ]
    }
    
    email_config_file = config_dir / 'email_config_template.json'
    with open(email_config_file, 'w') as f:
        json.dump(email_template, f, indent=2)
    
    # Slack config template
    slack_template = {
        "webhook_url": "https://hooks.slack.com/services/YOUR/WEBHOOK/URL",
        "channel": "#bloomberg-data"
    }
    
    slack_config_file = config_dir / 'slack_config_template.json'
    with open(slack_config_file, 'w') as f:
        json.dump(slack_template, f, indent=2)
    
    print("📋 Configuration templates created:")
    print(f"   Email: {email_config_file}")
    print(f"   Slack: {slack_config_file}")
    print("\n📝 Next steps:")
    print("1. Copy templates to remove '_template' from filename")
    print("2. Update with your actual credentials")
    print("3. Run the scheduler")

def main():
    """Main function"""
    if len(sys.argv) > 1 and sys.argv[1] == '--create-configs':
        create_config_templates()
        return
    
    # Create and run scheduler
    scheduler = VIXScheduler()
    scheduler.run()

if __name__ == "__main__":
    main()
