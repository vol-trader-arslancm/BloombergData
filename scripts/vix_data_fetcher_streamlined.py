"""
Streamlined VIX Futures and Options Data Fetcher
Focused on getting data ready for analysis quickly
- Disabled email alerts
- Optimized for faster collection
- Better error handling for missing contracts
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

class StreamlinedVIXDataFetcher:
    """
    Streamlined VIX Data Collection - Analysis Ready
    Focus on getting clean data for VIX futures and options analysis
    """
    
    def __init__(self, years_back=10):
        self.session = None
        self.refDataService = None
        self.project_root = project_root
        self.data_dir = self.project_root / 'data' / 'vix_data'
        
        # Create directories
        self.data_dir.mkdir(parents=True, exist_ok=True)
        
        # VIX contract mappings - only essential fields
        self.vix_futures_fields = {
            'last_price': 'PX_LAST',
            'settle_price': 'PX_SETTLE',
            'volume': 'PX_VOLUME',
            'open_interest': 'OPEN_INT'
        }
        
        self.vix_options_fields = {
            'last_price': 'PX_LAST',
            'mid_price': 'PX_MID',
            'volume': 'PX_VOLUME',
            'open_interest': 'OPEN_INT',
            'delta': 'DELTA_MID',
            'vega': 'VEGA_MID',
            'underlying_price': 'UNDL_PX'
        }
        
        # Date range
        self.end_date = datetime.now()
        self.start_date = self.end_date - timedelta(days=years_back*365 + 3)
        
        print(f"🔥 Streamlined VIX Data Collection")
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
    
    def generate_vix_future_tickers(self):
        """Generate front-month VIX futures tickers"""
        tickers = []
        current_date = self.start_date.date()
        
        while current_date <= self.end_date.date():
            year = current_date.year
            month = current_date.month
            
            # Find 3rd Wednesday of month (VIX futures expiry)
            first_day = date(year, month, 1)
            first_weekday = first_day.weekday()
            days_to_first_wed = (2 - first_weekday) % 7
            first_wed = first_day + timedelta(days=days_to_first_wed)
            third_wed = first_wed + timedelta(days=14)
            
            # VIX futures ticker format
            month_codes = ['F', 'G', 'H', 'J', 'K', 'M', 'N', 'Q', 'U', 'V', 'X', 'Z']
            month_code = month_codes[month - 1]
            year_code = str(year)[-2:]
            
            ticker = f"VIX{month_code}{year_code} Curncy"
            
            tickers.append({
                'ticker': ticker,
                'expiry_date': third_wed,
                'year': year,
                'month': month
            })
            
            # Move to next month
            if month == 12:
                current_date = date(year + 1, 1, 1)
            else:
                current_date = date(year, month + 1, 1)
        
        print(f"📋 Generated {len(tickers)} VIX futures contracts")
        return tickers
    
    def generate_vix_option_tickers(self, futures_info, max_per_expiry=20):
        """
        Generate VIX options tickers - limited set for efficiency
        Focus on liquid strikes around typical VIX levels
        """
        option_tickers = []
        
        # Reduced strike set for faster collection
        liquid_strikes = [12, 13, 14, 15, 16, 17, 18, 19, 20, 22, 25, 27, 30, 35, 40]
        
        count = 0
        for future_info in futures_info:
            if count >= max_per_expiry:
                break
                
            expiry_date = future_info['expiry_date']
            expiry_str = expiry_date.strftime('%m/%d/%y')
            
            for strike in liquid_strikes:
                call_ticker = f"VIX {expiry_str} C{strike} Index"
                
                option_tickers.append({
                    'ticker': call_ticker,
                    'expiry_date': expiry_date,
                    'strike': strike,
                    'underlying_future': future_info['ticker']
                })
            
            count += 1
        
        print(f"📋 Generated {len(option_tickers)} VIX option contracts (liquid strikes only)")
        return option_tickers
    
    def get_historical_data_batch(self, tickers, fields, data_type, batch_size=5):
        """
        Optimized batch data collection with better error handling
        """
        print(f"📊 Collecting {data_type} data...")
        all_data = []
        
        total_batches = (len(tickers) - 1) // batch_size + 1
        successful_requests = 0
        
        for i in range(0, len(tickers), batch_size):
            batch = tickers[i:i + batch_size]
            batch_num = i // batch_size + 1
            
            print(f"   Batch {batch_num}/{total_batches} ({len(batch)} contracts)")
            
            for ticker_info in batch:
                ticker = ticker_info['ticker']
                
                try:
                    # Create request
                    request = self.refDataService.createRequest("HistoricalDataRequest")
                    request.getElement("securities").appendValue(ticker)
                    
                    for field in fields:
                        request.getElement("fields").appendValue(field)
                    
                    request.set("startDate", self.start_date.strftime('%Y%m%d'))
                    request.set("endDate", self.end_date.strftime('%Y%m%d'))
                    request.set("periodicitySelection", "DAILY")
                    
                    self.session.sendRequest(request)
                    
                    # Process response with timeout
                    ticker_data = []
                    response_received = False
                    
                    while True:
                        event = self.session.nextEvent(10000)  # 10 second timeout
                        
                        if event.eventType() == blpapi.Event.RESPONSE:
                            for msg in event:
                                securityData = msg.getElement("securityData")
                                
                                if securityData.hasElement("securityError"):
                                    # Skip contracts with errors (normal for missing options)
                                    break
                                
                                fieldDataArray = securityData.getElement("fieldData")
                                
                                for j in range(fieldDataArray.numValues()):
                                    fieldData = fieldDataArray.getValue(j)
                                    data_date = fieldData.getElement("date").getValue()
                                    
                                    row_data = {
                                        'date': data_date.strftime('%Y-%m-%d'),
                                        'ticker': ticker,
                                        'contract_type': data_type
                                    }
                                    
                                    # Add expiry info for options
                                    if 'expiry_date' in ticker_info:
                                        row_data['expiry_date'] = ticker_info['expiry_date'].strftime('%Y-%m-%d')
                                    if 'strike' in ticker_info:
                                        row_data['strike'] = ticker_info['strike']
                                    
                                    # Extract field data
                                    for clean_name, bloomberg_field in fields.items():
                                        if fieldData.hasElement(bloomberg_field):
                                            value = fieldData.getElement(bloomberg_field).getValue()
                                            row_data[clean_name] = value if value is not None else np.nan
                                        else:
                                            row_data[clean_name] = np.nan
                                    
                                    ticker_data.append(row_data)
                                
                                successful_requests += 1
                                response_received = True
                            break
                        
                        if event.eventType() == blpapi.Event.TIMEOUT:
                            break
                    
                    all_data.extend(ticker_data)
                    
                    # Rate limiting
                    time.sleep(0.05)
                    
                except Exception as e:
                    print(f"      Error processing {ticker}: {e}")
                    continue
        
        df = pd.DataFrame(all_data)
        print(f"✅ Collected {len(df)} data points from {successful_requests} successful requests")
        return df
    
    def filter_target_delta_options(self, options_df, target_deltas=[0.10, 0.50]):
        """
        Filter options to target deltas with improved logic
        """
        if len(options_df) == 0:
            return pd.DataFrame()
        
        print(f"🎯 Filtering for target deltas: {[int(d*100) for d in target_deltas]}Δ")
        
        filtered_data = []
        
        # Group by date and expiry
        groups = options_df.groupby(['date', 'expiry_date'])
        total_groups = len(groups)
        processed = 0
        
        for (date, expiry), group in groups:
            processed += 1
            if processed % 100 == 0:
                print(f"   Processed {processed}/{total_groups} date/expiry combinations")
            
            # Filter valid options with delta data
            valid_options = group.dropna(subset=['delta'])
            
            if len(valid_options) == 0:
                continue
            
            # Only keep call options (positive delta)
            call_options = valid_options[valid_options['delta'] > 0]
            
            if len(call_options) == 0:
                continue
            
            for target_delta in target_deltas:
                # Find closest delta
                call_options['delta_diff'] = abs(call_options['delta'] - target_delta)
                
                if len(call_options) > 0:
                    closest_idx = call_options['delta_diff'].idxmin()
                    closest_option = call_options.loc[closest_idx].copy()
                    
                    # Add target delta info
                    closest_option['target_delta'] = target_delta
                    closest_option['delta_label'] = f"{int(target_delta*100)}Δ"
                    
                    filtered_data.append(closest_option.to_dict())
        
        filtered_df = pd.DataFrame(filtered_data)
        print(f"✅ Found {len(filtered_df)} target delta options")
        return filtered_df
    
    def save_analysis_ready_data(self, futures_df, options_df, target_delta_df):
        """
        Save data in analysis-ready format
        """
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        try:
            files_created = []
            
            # Save futures data
            if len(futures_df) > 0:
                futures_file = self.data_dir / f'vix_futures_analysis_{timestamp}.csv'
                futures_df.to_csv(futures_file, index=False)
                files_created.append(str(futures_file))
                print(f"✅ VIX futures: {len(futures_df)} records → {futures_file.name}")
            
            # Save target delta options
            if len(target_delta_df) > 0:
                target_file = self.data_dir / f'vix_options_target_deltas_{timestamp}.csv'
                target_delta_df.to_csv(target_file, index=False)
                files_created.append(str(target_file))
                print(f"✅ Target delta options: {len(target_delta_df)} records → {target_file.name}")
            
            # Create analysis summary
            summary = {
                'collection_timestamp': timestamp,
                'data_period': {
                    'start_date': self.start_date.strftime('%Y-%m-%d'),
                    'end_date': self.end_date.strftime('%Y-%m-%d')
                },
                'data_summary': {
                    'futures_records': len(futures_df),
                    'total_options_records': len(options_df),
                    'target_delta_records': len(target_delta_df),
                    'unique_futures': len(futures_df['ticker'].unique()) if len(futures_df) > 0 else 0,
                    'target_delta_breakdown': target_delta_df['delta_label'].value_counts().to_dict() if len(target_delta_df) > 0 else {}
                },
                'files_created': files_created
            }
            
            summary_file = self.data_dir / f'vix_analysis_summary_{timestamp}.json'
            with open(summary_file, 'w') as f:
                json.dump(summary, f, indent=2)
            
            print(f"✅ Analysis summary: {summary_file.name}")
            return summary
            
        except Exception as e:
            print(f"❌ Failed to save data: {e}")
            return None
    
    def run_streamlined_collection(self):
        """
        Run streamlined data collection focused on analysis
        """
        print("🚀 Starting streamlined VIX data collection for analysis...")
        
        try:
            # Connect
            if not self.connect():
                return False
            
            # Generate contracts
            print("\n📋 Generating contract lists...")
            futures_info = self.generate_vix_future_tickers()
            options_info = self.generate_vix_option_tickers(futures_info, max_per_expiry=15)  # Limit for speed
            
            # Collect futures data
            print("\n📊 Collecting VIX futures data...")
            futures_df = self.get_historical_data_batch(
                futures_info, 
                self.vix_futures_fields, 
                'VIX_Future'
            )
            
            # Collect options data
            print("\n📊 Collecting VIX options data...")
            options_df = self.get_historical_data_batch(
                options_info, 
                self.vix_options_fields, 
                'VIX_Option',
                batch_size=3  # Smaller batches for options
            )
            
            # Filter target deltas
            print("\n🎯 Processing target delta options...")
            target_delta_df = self.filter_target_delta_options(options_df)
            
            # Save analysis-ready data
            print("\n💾 Saving analysis-ready datasets...")
            summary = self.save_analysis_ready_data(futures_df, options_df, target_delta_df)
            
            if summary:
                print("\n🎉 Data collection completed successfully!")
                print("=" * 50)
                print(f"📊 Futures records: {summary['data_summary']['futures_records']:,}")
                print(f"📊 Target delta options: {summary['data_summary']['target_delta_records']:,}")
                print(f"📁 Files saved to: {self.data_dir}")
                
                if summary['data_summary']['target_delta_breakdown']:
                    print("\n📈 Target delta breakdown:")
                    for delta_label, count in summary['data_summary']['target_delta_breakdown'].items():
                        print(f"   {delta_label}: {count:,} records")
                
                return True
            else:
                return False
                
        except Exception as e:
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
    """Main execution - analysis focused"""
    print("=" * 60)
    print("STREAMLINED VIX DATA COLLECTION - ANALYSIS READY")
    print("=" * 60)
    print(f"Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Create fetcher
    fetcher = StreamlinedVIXDataFetcher(years_back=10)
    
    # Run collection
    success = fetcher.run_streamlined_collection()
    
    if success:
        print("\n🎊 Ready for analysis! Check the data/vix_data/ directory")
        print("💡 Next: Load the CSV files into your analysis notebooks")
    else:
        print("\n💥 Collection incomplete - check Bloomberg connection")
    
    return success

if __name__ == "__main__":
    main()
