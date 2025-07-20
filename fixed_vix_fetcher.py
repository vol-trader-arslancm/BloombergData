"""
Fixed VIX Data Fetcher - Proper Bloomberg API Response Handling
Fixes the fieldData access error and collects working VIX options
"""

import sys
import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, date
import json
import time
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

class FixedVIXDataFetcher:
    """
    Fixed VIX Data Collection with proper Bloomberg API response handling
    """
    
    def __init__(self, years_back=10):
        self.session = None
        self.refDataService = None
        self.project_root = project_root
        self.data_dir = self.project_root / 'data' / 'vix_data'
        
        # Create directories
        self.data_dir.mkdir(parents=True, exist_ok=True)
        
        # Working securities from discovery
        self.vix_securities = {
            'vix_spot': 'VIX3 Index',  # Cboe Volatility Index
            'vix_front_month': 'CBOE VIX1 Index',  # Generic 1st VX Future
        }
        
        # Data fields
        self.basic_fields = {
            'last_price': 'PX_LAST',
            'open_price': 'PX_OPEN',
            'high_price': 'PX_HIGH', 
            'low_price': 'PX_LOW',
            'volume': 'PX_VOLUME'
        }
        
        self.options_fields = {
            'last_price': 'PX_LAST',
            'mid_price': 'PX_MID',
            'volume': 'PX_VOLUME',
            'delta': 'DELTA_MID',
            'vega': 'VEGA_MID'
        }
        
        # Date range
        self.end_date = datetime.now()
        self.start_date = self.end_date - timedelta(days=years_back*365 + 3)
        
        print(f"🔥 Fixed VIX Data Collection")
        print(f"📅 Period: {self.start_date.strftime('%Y-%m-%d')} to {self.end_date.strftime('%Y-%m-%d')}")
        print(f"💾 Output: {self.data_dir}")
    
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
    
    def get_historical_data_fixed(self, ticker, fields, data_type):
        """
        Fixed historical data collection with proper Bloomberg API response handling
        """
        print(f"📊 Collecting {data_type} for {ticker}...")
        
        try:
            request = self.refDataService.createRequest("HistoricalDataRequest")
            request.getElement("securities").appendValue(ticker)
            
            bloomberg_fields = list(fields.values())
            for field in bloomberg_fields:
                request.getElement("fields").appendValue(field)
            
            request.set("startDate", self.start_date.strftime('%Y%m%d'))
            request.set("endDate", self.end_date.strftime('%Y%m%d'))
            request.set("periodicitySelection", "DAILY")
            
            self.session.sendRequest(request)
            
            # Process response with fixed parsing
            all_data = []
            while True:
                event = self.session.nextEvent(15000)  # 15 second timeout
                
                if event.eventType() == blpapi.Event.RESPONSE:
                    for msg in event:
                        # Fixed: Proper access to securityData
                        securityDataArray = msg.getElement("securityData")
                        
                        for i in range(securityDataArray.numValues()):
                            securityData = securityDataArray.getValue(i)
                            
                            # Check for security error
                            if securityData.hasElement("securityError"):
                                error = securityData.getElement("securityError")
                                print(f"   ❌ Security error for {ticker}: {error}")
                                return pd.DataFrame()
                            
                            # Check for field data
                            if not securityData.hasElement("fieldData"):
                                print(f"   ❌ No field data for {ticker}")
                                return pd.DataFrame()
                            
                            # Process field data array
                            fieldDataArray = securityData.getElement("fieldData")
                            
                            for j in range(fieldDataArray.numValues()):
                                fieldData = fieldDataArray.getValue(j)
                                
                                # Get date
                                if not fieldData.hasElement("date"):
                                    continue
                                
                                data_date = fieldData.getElement("date").getValue()
                                
                                row_data = {
                                    'date': data_date.strftime('%Y-%m-%d'),
                                    'ticker': ticker,
                                    'data_type': data_type
                                }
                                
                                # Extract field data
                                for clean_name, bloomberg_field in fields.items():
                                    if fieldData.hasElement(bloomberg_field):
                                        value = fieldData.getElement(bloomberg_field).getValue()
                                        row_data[clean_name] = value if value is not None else np.nan
                                    else:
                                        row_data[clean_name] = np.nan
                                
                                all_data.append(row_data)
                    break
                
                if event.eventType() == blpapi.Event.TIMEOUT:
                    print(f"   ⏱️ Timeout for {ticker}")
                    break
            
            df = pd.DataFrame(all_data)
            print(f"   ✅ Collected {len(df)} records for {ticker}")
            return df
            
        except Exception as e:
            print(f"   ❌ Error collecting {ticker}: {e}")
            import traceback
            traceback.print_exc()
            return pd.DataFrame()
    
    def generate_vix_options_working_format(self):
        """
        Generate VIX options using the working format we discovered
        """
        print("📋 Generating VIX options using working format...")
        
        # Generate monthly expiry dates for the last 2 years (faster collection)
        option_tickers = []
        
        # Start from 2 years ago for faster collection
        start_date = datetime.now() - timedelta(days=2*365)
        current_date = start_date
        
        while current_date <= datetime.now() + timedelta(days=60):  # Include next 2 months
            year = current_date.year
            month = current_date.month
            
            # Find 3rd Wednesday of month
            first_day = date(year, month, 1)
            first_weekday = first_day.weekday()
            days_to_first_wed = (2 - first_weekday) % 7
            first_wed = first_day + timedelta(days=days_to_first_wed)
            third_wed = first_wed + timedelta(days=14)
            
            # Use the working format: "VIX MM/DD/YY C{strike} Index"
            date_str = third_wed.strftime('%m/%d/%y')
            
            # Liquid strikes around typical VIX levels
            strikes = [12, 13, 14, 15, 16, 17, 18, 19, 20, 22, 25, 30, 35, 40]
            
            for strike in strikes:
                option_ticker = f"VIX {date_str} C{strike} Index"
                option_tickers.append({
                    'ticker': option_ticker,
                    'expiry_date': third_wed,
                    'strike': strike
                })
            
            # Move to next month
            if month == 12:
                current_date = datetime.datetime(year + 1, 1, 1)
            else:
                current_date = datetime.datetime(year, month + 1, 1)
        
        print(f"   Generated {len(option_tickers)} VIX option contracts")
        return option_tickers
    
    def collect_vix_options_batch(self, option_tickers, batch_size=10):
        """
        Collect VIX options data in batches with the working format
        """
        print(f"📊 Collecting VIX options data in batches...")
        
        all_data = []
        total_batches = (len(option_tickers) - 1) // batch_size + 1
        successful_requests = 0
        
        for i in range(0, len(option_tickers), batch_size):
            batch = option_tickers[i:i + batch_size]
            batch_num = i // batch_size + 1
            
            print(f"   Batch {batch_num}/{total_batches} ({len(batch)} options)")
            
            for option_info in batch:
                ticker = option_info['ticker']
                
                try:
                    # Use current price request for options (faster than historical)
                    request = self.refDataService.createRequest("ReferenceDataRequest")
                    request.getElement("securities").appendValue(ticker)
                    
                    # Request essential option fields
                    request.getElement("fields").appendValue("PX_LAST")
                    request.getElement("fields").appendValue("PX_MID")
                    request.getElement("fields").appendValue("DELTA_MID")
                    request.getElement("fields").appendValue("VEGA_MID")
                    request.getElement("fields").appendValue("PX_VOLUME")
                    
                    self.session.sendRequest(request)
                    event = self.session.nextEvent(5000)
                    
                    if event.eventType() == blpapi.Event.RESPONSE:
                        for msg in event:
                            securityDataArray = msg.getElement("securityData")
                            
                            for j in range(securityDataArray.numValues()):
                                securityData = securityDataArray.getValue(j)
                                
                                if securityData.hasElement("securityError"):
                                    # Skip options that don't exist
                                    continue
                                
                                if securityData.hasElement("fieldData"):
                                    fieldData = securityData.getElement("fieldData")
                                    
                                    row_data = {
                                        'date': datetime.now().strftime('%Y-%m-%d'),
                                        'ticker': ticker,
                                        'expiry_date': option_info['expiry_date'].strftime('%Y-%m-%d'),
                                        'strike': option_info['strike'],
                                        'data_type': 'VIX_Option'
                                    }
                                    
                                    # Extract option data
                                    fields_map = {
                                        'last_price': 'PX_LAST',
                                        'mid_price': 'PX_MID', 
                                        'delta': 'DELTA_MID',
                                        'vega': 'VEGA_MID',
                                        'volume': 'PX_VOLUME'
                                    }
                                    
                                    for clean_name, bloomberg_field in fields_map.items():
                                        if fieldData.hasElement(bloomberg_field):
                                            value = fieldData.getElement(bloomberg_field).getValue()
                                            row_data[clean_name] = value if value is not None else np.nan
                                        else:
                                            row_data[clean_name] = np.nan
                                    
                                    all_data.append(row_data)
                                    successful_requests += 1
                
                except Exception as e:
                    continue  # Skip problematic options
                
                # Rate limiting
                time.sleep(0.05)
        
        df = pd.DataFrame(all_data)
        print(f"   ✅ Collected {len(df)} option records from {successful_requests} contracts")
        return df
    
    def filter_target_delta_options(self, options_df):
        """
        Filter options to 10Δ and 50Δ calls
        """
        if len(options_df) == 0:
            return pd.DataFrame()
        
        print(f"🎯 Filtering for 10Δ and 50Δ options...")
        
        # Remove options without delta data
        valid_options = options_df.dropna(subset=['delta'])
        print(f"   Valid options with delta: {len(valid_options)}")
        
        if len(valid_options) == 0:
            return pd.DataFrame()
        
        # Only keep calls (positive delta)
        call_options = valid_options[valid_options['delta'] > 0]
        print(f"   Call options: {len(call_options)}")
        
        target_deltas = [0.10, 0.50]
        filtered_data = []
        
        # Group by expiry date
        for expiry, group in call_options.groupby('expiry_date'):
            for target_delta in target_deltas:
                # Find closest delta
                group['delta_diff'] = abs(group['delta'] - target_delta)
                
                if len(group) > 0:
                    closest_idx = group['delta_diff'].idxmin()
                    closest_option = group.loc[closest_idx].copy()
                    
                    closest_option['target_delta'] = target_delta
                    closest_option['delta_label'] = f"{int(target_delta*100)}Δ"
                    
                    filtered_data.append(closest_option.to_dict())
        
        filtered_df = pd.DataFrame(filtered_data)
        print(f"   ✅ Found {len(filtered_df)} target delta options")
        return filtered_df
    
    def save_vix_data(self, data_dict):
        """
        Save VIX data to files
        """
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        try:
            files_created = []
            
            for data_name, df in data_dict.items():
                if len(df) > 0:
                    filename = f'vix_{data_name}_fixed_{timestamp}.csv'
                    file_path = self.data_dir / filename
                    df.to_csv(file_path, index=False)
                    files_created.append(str(file_path))
                    print(f"✅ {data_name}: {len(df)} records → {filename}")
            
            # Summary
            summary = {
                'collection_timestamp': timestamp,
                'collection_type': 'fixed_vix_data',
                'data_period': {
                    'start_date': self.start_date.strftime('%Y-%m-%d'),
                    'end_date': self.end_date.strftime('%Y-%m-%d')
                },
                'data_summary': {
                    data_name: len(df) for data_name, df in data_dict.items()
                },
                'files_created': files_created
            }
            
            summary_file = self.data_dir / f'vix_fixed_summary_{timestamp}.json'
            with open(summary_file, 'w') as f:
                json.dump(summary, f, indent=2)
            
            print(f"✅ Summary: {summary_file.name}")
            return summary
            
        except Exception as e:
            print(f"❌ Failed to save data: {e}")
            return None
    
    def run_fixed_collection(self):
        """
        Run the fixed VIX data collection
        """
        print("🚀 Starting fixed VIX data collection...")
        
        try:
            if not self.connect():
                return False
            
            vix_data = {}
            
            # Try to collect VIX spot data
            print(f"\n📊 Attempting VIX spot data collection...")
            vix_spot_df = self.get_historical_data_fixed(
                self.vix_securities['vix_spot'], 
                self.basic_fields, 
                'VIX_Spot'
            )
            if len(vix_spot_df) > 0:
                vix_data['spot'] = vix_spot_df
            
            # Try to collect VIX futures data  
            print(f"\n📊 Attempting VIX futures data collection...")
            vix_futures_df = self.get_historical_data_fixed(
                self.vix_securities['vix_front_month'], 
                self.basic_fields, 
                'VIX_Future'
            )
            if len(vix_futures_df) > 0:
                vix_data['futures'] = vix_futures_df
            
            # Collect VIX options using working format
            print(f"\n📊 Collecting VIX options (current data)...")
            option_tickers = self.generate_vix_options_working_format()
            options_df = self.collect_vix_options_batch(option_tickers[:100])  # Limit for speed
            
            if len(options_df) > 0:
                vix_data['options_all'] = options_df
                
                # Filter for target deltas
                target_delta_df = self.filter_target_delta_options(options_df)
                if len(target_delta_df) > 0:
                    vix_data['options_target_deltas'] = target_delta_df
            
            # Save data
            if vix_data:
                print(f"\n💾 Saving VIX data...")
                summary = self.save_vix_data(vix_data)
                
                if summary:
                    print("\n🎉 Fixed VIX data collection completed!")
                    print("=" * 60)
                    for data_name, count in summary['data_summary'].items():
                        print(f"📊 {data_name}: {count:,} records")
                    return True
            else:
                print("❌ No data collected")
                return False
                
        except Exception as e:
            print(f"💥 Collection failed: {e}")
            import traceback
            traceback.print_exc()
            return False
        
        finally:
            self.disconnect()
    
    def disconnect(self):
        """Disconnect from Bloomberg"""
        if self.session:
            self.session.stop()
            print("✅ Bloomberg session disconnected")

def main():
    """Main execution"""
    print("=" * 70)
    print("FIXED VIX DATA COLLECTION - BLOOMBERG API RESPONSE FIX")
    print("=" * 70)
    print(f"Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    fetcher = FixedVIXDataFetcher(years_back=10)
    success = fetcher.run_fixed_collection()
    
    if success:
        print("\n🎊 Success! VIX data collected and ready for analysis")
    else:
        print("\n💥 Collection failed")
    
    return success

if __name__ == "__main__":
    main()
