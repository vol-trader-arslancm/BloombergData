"""
Notification Configuration Setup
Configure email and Slack notifications for automated data collection
"""

import json
import os
from getpass import getpass

def setup_email_config():
    """Setup email configuration interactively"""
    print("📧 EMAIL NOTIFICATION SETUP")
    print("="*40)
    
    config = {}
    
    # Email settings
    config['smtp_server'] = input("SMTP Server (default: smtp.gmail.com): ") or 'smtp.gmail.com'
    config['smtp_port'] = int(input("SMTP Port (default: 587): ") or '587')
    config['sender_email'] = input("Your email address: ")
    
    print("\n🔐 For Gmail, you need an 'App Password':")
    print("   1. Go to your Google Account settings")
    print("   2. Security → 2-Step Verification")
    print("   3. App passwords → Generate app password")
    print("   4. Use that password here (not your regular password)")
    
    config['sender_password'] = getpass("App password (hidden): ")
    
    # Recipients
    recipients = []
    print("\n👥 Add recipient email addresses (press Enter when done):")
    while True:
        email = input("Recipient email (or Enter to finish): ")
        if not email:
            break
        recipients.append(email)
    
    config['recipient_emails'] = recipients
    
    return config

def setup_slack_config():
    """Setup Slack configuration"""
    print("\n💬 SLACK NOTIFICATION SETUP (OPTIONAL)")
    print("="*40)
    
    use_slack = input("Do you want to configure Slack notifications? (y/n): ").lower() == 'y'
    
    if not use_slack:
        return {
            'webhook_url': 'https://hooks.slack.com/your/webhook/url',
            'channel': '#bloomberg-data'
        }
    
    print("\n🔗 To get a Slack webhook URL:")
    print("   1. Go to https://api.slack.com/apps")
    print("   2. Create a new app → From scratch")
    print("   3. Incoming Webhooks → Activate")
    print("   4. Add New Webhook to Workspace")
    print("   5. Copy the webhook URL")
    
    config = {}
    config['webhook_url'] = input("Slack webhook URL: ")
    config['channel'] = input("Slack channel (e.g., #bloomberg-data): ") or '#bloomberg-data'
    
    return config

def save_config(email_config, slack_config):
    """Save configuration to file"""
    full_config = {
        'email': email_config,
        'slack': slack_config,
        'collection': {
            'max_retries': 3,
            'retry_delay_minutes': 30,
            'data_quality_threshold': 0.8
        }
    }
    
    # Create config directory
    os.makedirs('config', exist_ok=True)
    
    # Save configuration
    config_file = 'config/notifications.json'
    with open(config_file, 'w') as f:
        json.dump(full_config, f, indent=2)
    
    print(f"\n✅ Configuration saved to: {config_file}")
    return config_file

def test_email_config(config_file):
    """Test email configuration"""
    print("\n🧪 TESTING EMAIL CONFIGURATION")
    print("="*40)
    
    try:
        with open(config_file, 'r') as f:
            config = json.load(f)
        
        import smtplib
        from email.mime.text import MIMEText
        
        # Test email connection
        server = smtplib.SMTP(config['email']['smtp_server'], config['email']['smtp_port'])
        server.starttls()
        server.login(config['email']['sender_email'], config['email']['sender_password'])
        
        # Send test email
        msg = MIMEText("✅ Bloomberg data collection notifications are configured correctly!")
        msg['Subject'] = "Bloomberg Data Collection - Test Email"
        msg['From'] = config['email']['sender_email']
        msg['To'] = ', '.join(config['email']['recipient_emails'])
        
        server.send_message(msg)
        server.quit()
        
        print("✅ Test email sent successfully!")
        return True
        
    except Exception as e:
        print(f"❌ Email test failed: {e}")
        return False

def main():
    """Main configuration setup"""
    print("🚀 BLOOMBERG DATA COLLECTION - NOTIFICATION SETUP")
    print("="*60)
    
    # Setup email
    email_config = setup_email_config()
    
    # Setup Slack
    slack_config = setup_slack_config()
    
    # Save configuration
    config_file = save_config(email_config, slack_config)
    
    # Test email
    if input("\nTest email configuration now? (y/n): ").lower() == 'y':
        test_email_config(config_file)
    
    print("\n🎯 NEXT STEPS:")
    print("1. Run the scheduler: python scripts/scheduler.py")
    print("2. Or test data collection: python scripts/scheduler.py --test")
    print("3. The system will run daily at 9:00 AM")
    print("4. You'll receive email/Slack notifications with results")

if __name__ == "__main__":
    main()