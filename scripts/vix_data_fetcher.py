"""
VIX Futures and Options Data Fetcher
Collects 10 years of VIX 1-month futures and options data from Bloomberg API
Supports daily scheduling and automated alerts/notifications
"""
EMAIL_LEGACY = False  # Disable email temporarily
import sys
import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, date
import json
import time
import smtplib
import requests
try:
    from email.mime.text import MimeText
    from email.mime.multipart import MimeMultipart
    EMAIL_LEGACY = True
except ImportError:
    from email.message import EmailMessage
    EMAIL_LEGACY = False
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent.absolute()
sys.path.insert(0, str(project_root))

try:
    import blpapi
    print("✅ Bloomberg API imported successfully")
except ImportError as e:
    print(f"❌ Bloomberg API import error: {e}")
    sys.exit(1)

class VIXDataFetcher:
    """
    Comprehensive VIX Futures and Options Data Collection System
    Handles 1-month VIX futures and 10/50 delta call options with historical data
    """
    
    def __init__(self):
        self.session = None
        self.refDataService = None
        self.project_root = project_root
        self.data_dir = self.project_root / 'data' / 'vix_data'
        self.config_dir = self.project_root / 'config'
        self.log_file = self.data_dir / 'vix_collection_log.json'
        
        # Create directories
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.config_dir.mkdir(parents=True, exist_ok=True)
        
        # VIX contract mappings
        self.vix_futures_fields = {
            'last_price': 'PX_LAST',
            'open_price': 'PX_OPEN', 
            'high_price': 'PX_HIGH',
            'low_price': 'PX_LOW',
            'settle_price': 'PX_SETTLE',
            'volume': 'PX_VOLUME',
            'open_interest': 'OPEN_INT',
            'days_to_expiry': 'DAYS_TO_EXP',
            'contract_value': 'CONTRACT_VALUE'
        }
        
        self.vix_options_fields = {
            'last_price': 'PX_LAST',
            'bid_price': 'PX_BID',
            'ask_price': 'PX_ASK',
            'mid_price': 'PX_MID',
            'volume': 'PX_VOLUME', 
            'open_interest': 'OPEN_INT',
            'implied_vol': 'IVOL_MID',
            'delta': 'DELTA_MID',
            'gamma': 'GAMMA_MID',
            'theta': 'THETA_MID',
            'vega': 'VEGA_MID',
            'underlying_price': 'UNDL_PX'
        }
        
        # Date range for 10-year historical data
        self.end_date = datetime.now()
        self.start_date = self.end_date - timedelta(days=10*365 + 3)
        
        print(f"🔥 VIX Data Collection System Initialized")
        print(f"📅 Collection Period: {self.start_date.strftime('%Y-%m-%d')} to {self.end_date.strftime('%Y-%m-%d')}")
        print(f"💾 Data Directory: {self.data_dir}")
    
    def connect(self):
        """Connect to Bloomberg Terminal"""
        try:
            sessionOptions = blpapi.SessionOptions()
            self.session = blpapi.Session(sessionOptions)
            
            if not self.session.start():
                print("❌ Failed to start Bloomberg session")
                return False
            
            if not self.session.openService("//blp/refdata"):
                print("❌ Failed to open Bloomberg reference data service")
                return False
            
            self.refDataService = self.session.getService("//blp/refdata")
            print("✅ Bloomberg connection established")
            return True
            
        except Exception as e:
            print(f"❌ Bloomberg connection failed: {e}")
            return False
    
    def generate_vix_future_tickers(self, start_date, end_date):
        """
        Generate VIX 1-month futures tickers for the specified date range
        VIX futures trade with monthly expiries on 3rd Wed of each month
        """
        tickers = []
        current_date = start_date
        
        while current_date <= end_date:
            # VIX futures expire 3rd Wednesday of each month
            year = current_date.year
            month = current_date.month
            
            # Find 3rd Wednesday
            first_day = date(year, month, 1)
            first_weekday = first_day.weekday()
            days_to_first_wed = (2 - first_weekday) % 7  # Wednesday is day 2
            first_wed = first_day + timedelta(days=days_to_first_wed)
            third_wed = first_wed + timedelta(days=14)
            
            # VIX futures ticker format: VIX{Month}{Year} Curncy
            month_codes = ['F', 'G', 'H', 'J', 'K', 'M', 'N', 'Q', 'U', 'V', 'X', 'Z']
            month_code = month_codes[month - 1]
            year_code = str(year)[-2:]  # Last 2 digits of year
            
            ticker = f"VIX{month_code}{year_code} Curncy"
            
            tickers.append({
                'ticker': ticker,
                'expiry_date': third_wed,
                'year': year,
                'month': month,
                'contract_type': '1M_Future'
            })
            
            # Move to next month
            if month == 12:
                current_date = date(year + 1, 1, 1)
            else:
                current_date = date(year, month + 1, 1)
        
        print(f"📋 Generated {len(tickers)} VIX futures tickers")
        return tickers
    
    def generate_vix_option_tickers(self, futures_info):
        """
        Generate VIX options tickers for 10 delta and 50 delta calls
        Based on the futures expiry schedule
        """
        option_tickers = []
        
        for future_info in futures_info:
            expiry_date = future_info['expiry_date']
            year = future_info['year'] 
            month = future_info['month']
            
            # VIX options ticker format: VIX {MM/DD/YY} C{Strike} Index
            expiry_str = expiry_date.strftime('%m/%d/%y')
            
            # We'll need to determine strikes dynamically based on current VIX level
            # For now, generate standard strikes around typical VIX levels (15-40)
            strikes = [15, 16, 17, 18, 19, 20, 22, 25, 30, 35, 40, 45, 50]
            
            for strike in strikes:
                call_ticker = f"VIX {expiry_str} C{strike} Index"
                
                option_tickers.append({
                    'ticker': call_ticker,
                    'expiry_date': expiry_date,
                    'strike': strike,
                    'option_type': 'Call',
                    'underlying_future': future_info['ticker'],
                    'target_delta': 'TBD'  # Will be determined from actual data
                })
        
        print(f"📋 Generated {len(option_tickers)} VIX option tickers")
        return option_tickers
    
    def get_historical_futures_data(self, tickers, batch_size=10):
        """
        Fetch historical data for VIX futures contracts
        """
        print(f"📊 Fetching historical VIX futures data...")
        all_data = []
        
        bloomberg_fields = list(self.vix_futures_fields.values())
        
        for i in range(0, len(tickers), batch_size):
            batch = tickers[i:i + batch_size]
            print(f"   Processing futures batch {i//batch_size + 1}/{(len(tickers)-1)//batch_size + 1}")
            
            for ticker_info in batch:
                ticker = ticker_info['ticker']
                print(f"     Fetching {ticker}...")
                
                # Create historical data request
                request = self.refDataService.createRequest("HistoricalDataRequest")
                request.getElement("securities").appendValue(ticker)
                
                for field in bloomberg_fields:
                    request.getElement("fields").appendValue(field)
                
                request.set("startDate", self.start_date.strftime('%Y%m%d'))
                request.set("endDate", self.end_date.strftime('%Y%m%d'))
                request.set("periodicitySelection", "DAILY")
                
                self.session.sendRequest(request)
                
                # Process response
                ticker_data = []
                while True:
                    event = self.session.nextEvent(30000)  # 30 second timeout
                    
                    if event.eventType() == blpapi.Event.RESPONSE:
                        for msg in event:
                            securityData = msg.getElement("securityData")
                            
                            if securityData.hasElement("securityError"):
                                print(f"         WARNING: Error for {ticker}")
                                break
                            
                            fieldDataArray = securityData.getElement("fieldData")
                            
                            for j in range(fieldDataArray.numValues()):
                                fieldData = fieldDataArray.getValue(j)
                                data_date = fieldData.getElement("date").getValue()
                                
                                row_data = {
                                    'date': data_date.strftime('%Y-%m-%d'),
                                    'ticker': ticker,
                                    'contract_type': 'VIX_1M_Future',
                                    'expiry_date': ticker_info['expiry_date'].strftime('%Y-%m-%d')
                                }
                                
                                # Map Bloomberg fields to clean names
                                for clean_name, bloomberg_field in self.vix_futures_fields.items():
                                    if fieldData.hasElement(bloomberg_field):
                                        value = fieldData.getElement(bloomberg_field).getValue()
                                        row_data[clean_name] = value if value is not None else np.nan
                                    else:
                                        row_data[clean_name] = np.nan
                                
                                ticker_data.append(row_data)
                        break
                    
                    if event.eventType() == blpapi.Event.TIMEOUT:
                        print(f"         TIMEOUT: {ticker}")
                        break
                
                all_data.extend(ticker_data)
                time.sleep(0.1)  # Rate limiting
        
        df = pd.DataFrame(all_data)
        print(f"✅ Collected {len(df)} VIX futures data points")
        return df
    
    def get_historical_options_data(self, option_tickers, batch_size=5):
        """
        Fetch historical data for VIX options with delta targeting
        """
        print(f"📊 Fetching historical VIX options data...")
        all_data = []
        
        bloomberg_fields = list(self.vix_options_fields.values())
        
        for i in range(0, len(option_tickers), batch_size):
            batch = option_tickers[i:i + batch_size]
            print(f"   Processing options batch {i//batch_size + 1}/{(len(option_tickers)-1)//batch_size + 1}")
            
            for ticker_info in batch:
                ticker = ticker_info['ticker']
                print(f"     Fetching {ticker}...")
                
                # Create historical data request
                request = self.refDataService.createRequest("HistoricalDataRequest")
                request.getElement("securities").appendValue(ticker)
                
                for field in bloomberg_fields:
                    request.getElement("fields").appendValue(field)
                
                request.set("startDate", self.start_date.strftime('%Y%m%d'))
                request.set("endDate", self.end_date.strftime('%Y%m%d'))
                request.set("periodicitySelection", "DAILY")
                
                self.session.sendRequest(request)
                
                # Process response
                ticker_data = []
                while True:
                    event = self.session.nextEvent(30000)  # 30 second timeout
                    
                    if event.eventType() == blpapi.Event.RESPONSE:
                        for msg in event:
                            securityData = msg.getElement("securityData")
                            
                            if securityData.hasElement("securityError"):
                                print(f"         WARNING: Error for {ticker}")
                                break
                            
                            fieldDataArray = securityData.getElement("fieldData")
                            
                            for j in range(fieldDataArray.numValues()):
                                fieldData = fieldDataArray.getValue(j)
                                data_date = fieldData.getElement("date").getValue()
                                
                                row_data = {
                                    'date': data_date.strftime('%Y-%m-%d'),
                                    'ticker': ticker,
                                    'contract_type': 'VIX_Call_Option',
                                    'strike': ticker_info['strike'],
                                    'expiry_date': ticker_info['expiry_date'].strftime('%Y-%m-%d'),
                                    'underlying_future': ticker_info['underlying_future']
                                }
                                
                                # Map Bloomberg fields to clean names
                                for clean_name, bloomberg_field in self.vix_options_fields.items():
                                    if fieldData.hasElement(bloomberg_field):
                                        value = fieldData.getElement(bloomberg_field).getValue()
                                        row_data[clean_name] = value if value is not None else np.nan
                                    else:
                                        row_data[clean_name] = np.nan
                                
                                ticker_data.append(row_data)
                        break
                    
                    if event.eventType() == blpapi.Event.TIMEOUT:
                        print(f"         TIMEOUT: {ticker}")
                        break
                
                all_data.extend(ticker_data)
                time.sleep(0.2)  # More conservative rate limiting for options
        
        df = pd.DataFrame(all_data)
        print(f"✅ Collected {len(df)} VIX options data points")
        return df
    
    def filter_target_delta_options(self, options_df):
        """
        Filter options to get closest to 10 delta and 50 delta calls for each date/expiry
        """
        print("🎯 Filtering for target delta options (10Δ and 50Δ calls)...")
        
        target_deltas = [0.10, 0.50]
        filtered_data = []
        
        # Group by date and expiry
        for (date, expiry), group in options_df.groupby(['date', 'expiry_date']):
            # Filter valid options with delta data
            valid_options = group.dropna(subset=['delta'])
            
            if len(valid_options) == 0:
                continue
            
            for target_delta in target_deltas:
                # Find option closest to target delta
                valid_options['delta_diff'] = abs(valid_options['delta'] - target_delta)
                closest_option = valid_options.loc[valid_options['delta_diff'].idxmin()].copy()
                
                closest_option['target_delta'] = target_delta
                closest_option['delta_label'] = f"{int(target_delta*100)}Δ"
                
                filtered_data.append(closest_option.to_dict())
        
        filtered_df = pd.DataFrame(filtered_data)
        print(f"✅ Filtered to {len(filtered_df)} target delta options")
        return filtered_df
    
    def save_data(self, futures_df, options_df, target_delta_df):
        """
        Save collected VIX data to files with timestamp
        """
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        try:
            # Save futures data
            futures_file = self.data_dir / f'vix_futures_10yr_{timestamp}.csv'
            futures_df.to_csv(futures_file, index=False)
            print(f"✅ VIX futures data saved: {futures_file}")
            
            # Save all options data
            options_file = self.data_dir / f'vix_options_10yr_{timestamp}.csv'
            options_df.to_csv(options_file, index=False)
            print(f"✅ VIX options data saved: {options_file}")
            
            # Save target delta options
            delta_file = self.data_dir / f'vix_target_delta_options_10yr_{timestamp}.csv'
            target_delta_df.to_csv(delta_file, index=False)
            print(f"✅ VIX target delta options saved: {delta_file}")
            
            # Create summary
            summary = {
                'collection_timestamp': timestamp,
                'data_period': {
                    'start_date': self.start_date.strftime('%Y-%m-%d'),
                    'end_date': self.end_date.strftime('%Y-%m-%d'),
                    'total_days': (self.end_date - self.start_date).days
                },
                'data_summary': {
                    'futures_records': len(futures_df),
                    'options_records': len(options_df),
                    'target_delta_records': len(target_delta_df),
                    'unique_futures_contracts': len(futures_df['ticker'].unique()) if len(futures_df) > 0 else 0,
                    'unique_options_contracts': len(options_df['ticker'].unique()) if len(options_df) > 0 else 0
                },
                'files_created': {
                    'futures_file': str(futures_file),
                    'options_file': str(options_file),
                    'target_delta_file': str(delta_file)
                }
            }
            
            summary_file = self.data_dir / f'vix_collection_summary_{timestamp}.json'
            with open(summary_file, 'w') as f:
                json.dump(summary, f, indent=2)
            print(f"✅ Collection summary saved: {summary_file}")
            
            return summary
            
        except Exception as e:
            print(f"❌ Failed to save VIX data: {e}")
            return None
    
    def send_email_alert(self, subject, body, config_file='email_config.json'):
        """
        Send email notification
        """
        try:
            config_path = self.config_dir / config_file
            if not config_path.exists():
                print(f"⚠️ Email config not found: {config_path}")
                return False
            
            with open(config_path) as f:
                config = json.load(f)
            
            msg = MimeMultipart()
            msg['From'] = config['sender_email']
            msg['To'] = ', '.join(config['recipient_emails'])
            msg['Subject'] = subject
            
            msg.attach(MimeText(body, 'plain'))
            
            server = smtplib.SMTP(config['smtp_server'], config['smtp_port'])
            server.starttls()
            server.login(config['sender_email'], config['sender_password'])
            
            text = msg.as_string()
            server.sendmail(config['sender_email'], config['recipient_emails'], text)
            server.quit()
            
            print("✅ Email alert sent successfully")
            return True
            
        except Exception as e:
            print(f"❌ Failed to send email: {e}")
            return False
    
    def send_slack_webhook(self, message, config_file='slack_config.json'):
        """
        Send Slack webhook notification
        """
        try:
            config_path = self.config_dir / config_file
            if not config_path.exists():
                print(f"⚠️ Slack config not found: {config_path}")
                return False
            
            with open(config_path) as f:
                config = json.load(f)
            
            payload = {
                "channel": config['channel'],
                "text": message,
                "username": "VIX Data Bot"
            }
            
            response = requests.post(config['webhook_url'], json=payload)
            
            if response.status_code == 200:
                print("✅ Slack notification sent successfully")
                return True
            else:
                print(f"❌ Slack notification failed: {response.status_code}")
                return False
                
        except Exception as e:
            print(f"❌ Failed to send Slack notification: {e}")
            return False
    
    def run_full_collection(self):
        """
        Run complete VIX data collection process
        """
        print("🚀 Starting comprehensive VIX data collection...")
        
        try:
            # Connect to Bloomberg
            if not self.connect():
                return False
            
            # Generate contract tickers
            print("\n📋 Generating VIX contract tickers...")
            futures_info = self.generate_vix_future_tickers(self.start_date.date(), self.end_date.date())
            options_info = self.generate_vix_option_tickers(futures_info)
            
            # Collect futures data
            print("\n📊 Collecting VIX futures data...")
            futures_df = self.get_historical_futures_data(futures_info)
            
            # Collect options data
            print("\n📊 Collecting VIX options data...")
            options_df = self.get_historical_options_data(options_info)
            
            # Filter for target delta options
            if len(options_df) > 0:
                target_delta_df = self.filter_target_delta_options(options_df)
            else:
                target_delta_df = pd.DataFrame()
            
            # Save all data
            print("\n💾 Saving collected data...")
            summary = self.save_data(futures_df, options_df, target_delta_df)
            
            if summary:
                # Send notifications
                success_message = f"""
✅ VIX Data Collection Completed Successfully

📊 Collection Summary:
• Period: {summary['data_period']['start_date']} to {summary['data_period']['end_date']}
• Futures Records: {summary['data_summary']['futures_records']:,}
• Options Records: {summary['data_summary']['options_records']:,}
• Target Delta Records: {summary['data_summary']['target_delta_records']:,}

📁 Files Created:
• {summary['files_created']['futures_file']}
• {summary['files_created']['target_delta_file']}

Timestamp: {summary['collection_timestamp']}
                """
                
                self.send_email_alert("VIX Data Collection - Success", success_message)
                self.send_slack_webhook(success_message)
                
                print("🎉 VIX data collection completed successfully!")
                return True
            else:
                raise Exception("Failed to save data")
                
        except Exception as e:
            error_message = f"❌ VIX Data Collection Failed\n\nError: {str(e)}\nTimestamp: {datetime.now()}"
            self.send_email_alert("VIX Data Collection - ERROR", error_message)
            self.send_slack_webhook(error_message)
            print(f"💥 Collection failed: {e}")
            return False
        
        finally:
            self.disconnect()
    
    def disconnect(self):
        """Disconnect from Bloomberg"""
        if self.session:
            self.session.stop()
            print("✅ Bloomberg session disconnected")

def main():
    """Main execution function"""
    print("="*70)
    print("VIX FUTURES & OPTIONS DATA COLLECTION SYSTEM")
    print("="*70)
    print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Create fetcher instance
    fetcher = VIXDataFetcher()
    
    # Run full collection
    success = fetcher.run_full_collection()
    
    if success:
        print("\n🎊 VIX data collection pipeline completed successfully!")
    else:
        print("\n💥 VIX data collection pipeline failed!")
    
    return success

if __name__ == "__main__":
    main()
