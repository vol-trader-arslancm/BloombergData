"""
VIX Data Collection - Configuration Helper
Interactive setup for email and Slack notifications
"""

import json
import os
import getpass
from pathlib import Path

def setup_email_config():
    """Interactive email configuration setup"""
    print("📧 EMAIL NOTIFICATION SETUP")
    print("="*40)
    
    config = {}
    
    # Email settings
    print("\n🔧 SMTP Settings:")
    config['smtp_server'] = input("SMTP Server (default: smtp.gmail.com): ").strip() or 'smtp.gmail.com'
    
    port_input = input("SMTP Port (default: 587): ").strip()
    config['smtp_port'] = int(port_input) if port_input else 587
    
    config['sender_email'] = input("Your email address: ").strip()
    
    print("\n🔐 For Gmail, you need an 'App Password':")
    print("   1. Go to your Google Account settings")
    print("   2. Security → 2-Step Verification")
    print("   3. App passwords → Generate app password")
    print("   4. Use that password here (not your regular password)")
    print("   5. More info: https://support.google.com/accounts/answer/185833")
    
    config['sender_password'] = getpass.getpass("App password (hidden): ")
    
    # Recipients
    recipients = []
    print("\n👥 Add recipient email addresses:")
    print("   Enter one email per line, press Enter twice when done")
    
    while True:
        email = input("Recipient email (or press Enter to finish): ").strip()
        if not email:
            break
        recipients.append(email)
    
    if not recipients:
        recipients = [config['sender_email']]  # Default to sender if no recipients
    
    config['recipient_emails'] = recipients
    
    return config

def setup_slack_config():
    """Interactive Slack configuration setup"""
    print("\n💬 SLACK NOTIFICATION SETUP (OPTIONAL)")
    print("="*40)
    
    use_slack = input("Do you want to configure Slack notifications? (y/n): ").lower().startswith('y')
    
    if not use_slack:
        return None
    
    print("\n🔗 To get a Slack webhook URL:")
    print("   1. Go to https://api.slack.com/apps")
    print("   2. Create a new app → From scratch")
    print("   3. Choose your workspace")
    print("   4. Incoming Webhooks → Activate")
    print("   5. Add New Webhook to Workspace")
    print("   6. Choose channel and copy the webhook URL")
    print("   7. More info: https://api.slack.com/messaging/webhooks")
    
    config = {}
    config['webhook_url'] = input("\nSlack webhook URL: ").strip()
    config['channel'] = input("Channel name (default: #bloomberg-data): ").strip() or '#bloomberg-data'
    
    return config

def setup_schedule_config():
    """Interactive schedule configuration setup"""
    print("\n⏰ SCHEDULE CONFIGURATION")
    print("="*40)
    
    config = {}
    
    # Collection time
    time_input = input("Daily collection time (HH:MM, default: 18:30): ").strip()
    config['daily_collection_time'] = time_input if time_input else "18:30"
    
    # Weekend collection
    weekend_input = input("Collect data on weekends? (y/n, default: n): ").lower()
    config['weekend_collection'] = weekend_input.startswith('y')
    
    # Retry settings
    retry_input = input("Number of retry attempts on failure (default: 3): ").strip()
    config['retry_attempts'] = int(retry_input) if retry_input.isdigit() else 3
    
    delay_input = input("Delay between retries in minutes (default: 15): ").strip()
    config['retry_delay_minutes'] = int(delay_input) if delay_input.isdigit() else 15
    
    # Alert settings
    success_alert = input("Send alerts on successful collection? (y/n, default: y): ").lower()
    config['alert_on_success'] = not success_alert.startswith('n')
    
    failure_alert = input("Send alerts on collection failure? (y/n, default: y): ").lower()
    config['alert_on_failure'] = not failure_alert.startswith('n')
    
    # Data validation
    validation = input("Enable data validation checks? (y/n, default: y): ").lower()
    config['data_validation'] = not validation.startswith('n')
    
    config['timezone'] = "US/Eastern"
    
    return config

def save_config_file(config, filename, config_dir):
    """Save configuration to JSON file"""
    config_path = config_dir / filename
    
    try:
        with open(config_path, 'w') as f:
            json.dump(config, f, indent=2)
        print(f"✅ Configuration saved: {config_path}")
        return True
    except Exception as e:
        print(f"❌ Failed to save configuration: {e}")
        return False

def test_email_config(config):
    """Test email configuration"""
    print("\n🧪 Testing email configuration...")
    
    try:
        import smtplib
        from email.mime.text import MimeText
        
        # Create test message
        msg = MimeText("This is a test message from VIX Data Collection system.")
        msg['Subject'] = "[VIX Data] Configuration Test"
        msg['From'] = config['sender_email']
        msg['To'] = ', '.join(config['recipient_emails'])
        
        # Connect and send
        server = smtplib.SMTP(config['smtp_server'], config['smtp_port'])
        server.starttls()
        server.login(config['sender_email'], config['sender_password'])
        
        text = msg.as_string()
        server.sendmail(config['sender_email'], config['recipient_emails'], text)
        server.quit()
        
        print("✅ Email test successful! Check your inbox.")
        return True
        
    except Exception as e:
        print(f"❌ Email test failed: {e}")
        print("💡 Common issues:")
        print("   - Check your email/password")
        print("   - Ensure 2-factor auth is enabled for Gmail")
        print("   - Use App Password, not regular password")
        print("   - Check SMTP server and port settings")
        return False

def test_slack_config(config):
    """Test Slack configuration"""
    if not config:
        return True
    
    print("\n🧪 Testing Slack configuration...")
    
    try:
        import requests
        
        payload = {
            "channel": config['channel'],
            "text": "🧪 This is a test message from VIX Data Collection system.",
            "username": "VIX Data Bot"
        }
        
        response = requests.post(config['webhook_url'], json=payload, timeout=10)
        
        if response.status_code == 200:
            print("✅ Slack test successful! Check your channel.")
            return True
        else:
            print(f"❌ Slack test failed: HTTP {response.status_code}")
            return False
            
    except Exception as e:
        print(f"❌ Slack test failed: {e}")
        print("💡 Common issues:")
        print("   - Check your webhook URL")
        print("   - Ensure the webhook is active")
        print("   - Check channel name (include #)")
        return False

def main():
    """Main configuration setup"""
    print("🚀 VIX DATA COLLECTION - CONFIGURATION SETUP")
    print("=" * 50)
    
    # Determine project root (assume this script is in project root or scripts/)
    script_path = Path(__file__).parent.absolute()
    
    # Check if we're in scripts directory, if so go up one level
    if script_path.name == 'scripts':
        project_root = script_path.parent
    else:
        project_root = script_path
    
    config_dir = project_root / 'config'
    config_dir.mkdir(exist_ok=True)
    
    print(f"📁 Project root: {project_root}")
    print(f"⚙️ Config directory: {config_dir}")
    
    # Setup configurations
    try:
        # Email configuration
        email_config = setup_email_config()
        if save_config_file(email_config, 'email_config.json', config_dir):
            test_email = input("\n🧪 Test email configuration now? (y/n): ").lower().startswith('y')
            if test_email:
                test_email_config(email_config)
        
        # Slack configuration
        slack_config = setup_slack_config()
        if slack_config:
            if save_config_file(slack_config, 'slack_config.json', config_dir):
                test_slack = input("\n🧪 Test Slack configuration now? (y/n): ").lower().startswith('y')
                if test_slack:
                    test_slack_config(slack_config)
        
        # Schedule configuration
        schedule_config = setup_schedule_config()
        save_config_file(schedule_config, 'schedule_config.json', config_dir)
        
        print("\n🎉 Configuration setup completed!")
        print("=" * 50)
        print("📁 Configuration files created in:", config_dir)
        print("📧 Email config:", config_dir / 'email_config.json')
        if slack_config:
            print("💬 Slack config:", config_dir / 'slack_config.json')
        print("⏰ Schedule config:", config_dir / 'schedule_config.json')
        
        print("\n📝 Next steps:")
        print("1. Run your VIX data collection system")
        print("2. Monitor the first few collections")
        print("3. Check logs for any issues")
        
        # Show schedule summary
        print(f"\n📅 Your schedule:")
        print(f"   Collection time: {schedule_config['daily_collection_time']}")
        print(f"   Weekend collection: {'Yes' if schedule_config['weekend_collection'] else 'No'}")
        print(f"   Retry attempts: {schedule_config['retry_attempts']}")
        print(f"   Success alerts: {'Yes' if schedule_config['alert_on_success'] else 'No'}")
        print(f"   Failure alerts: {'Yes' if schedule_config['alert_on_failure'] else 'No'}")
        
    except KeyboardInterrupt:
        print("\n\n⏹️ Configuration setup cancelled by user.")
    except Exception as e:
        print(f"\n❌ Setup failed: {e}")

if __name__ == "__main__":
    main()
