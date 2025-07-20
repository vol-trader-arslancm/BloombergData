"""
Simple VIX Options Data Fetcher
Focus on collecting the working VIX options data for analysis
Skip the problematic historical data API calls
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

class SimpleVIXOptionsFetcher:
    """
    Simple VIX Options Data Collection
    Focus on current VIX options using the working format we discovered
    """
    
    def __init__(self):
        self.session = None
        self.refDataService = None
        self.project_root = project_root
        self.data_dir = self.project_root / 'data' / 'vix_data'
        
        # Create directories
        self.data_dir.mkdir(parents=True, exist_ok=True)
        
        print(f"🔥 Simple VIX Options Data Collection")
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
    
    def generate_current_vix_options(self):
        """
        Generate VIX options for current and next few monthly expiries
        Using the working format: VIX MM/DD/YY C{strike} Index
        """
        print("📋 Generating current VIX options...")
        
        option_tickers = []
        current_date = datetime.now()
        
        # Generate next 6 monthly expiries
        for i in range(6):
            target_month = current_date.month + i
            target_year = current_date.year
            
            # Handle year rollover
            while target_month > 12:
                target_month -= 12
                target_year += 1
            
            # Find 3rd Wednesday of target month
            first_day = date(target_year, target_month, 1)
            first_weekday = first_day.weekday()  # Monday = 0, Sunday = 6
            days_to_first_wed = (2 - first_weekday) % 7  # Wednesday = 2
            first_wed = first_day + timedelta(days=days_to_first_wed)
            third_wed = first_wed + timedelta(days=14)
            
            # Format date as MM/DD/YY
            date_str = third_wed.strftime('%m/%d/%y')
            
            # Generate strikes around typical VIX levels (12-50)
            strikes = [12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 
                      27, 30, 32, 35, 37, 40, 45, 50]
            
            for strike in strikes:
                option_ticker = f"VIX {date_str} C{strike} Index"
                option_tickers.append({
                    'ticker': option_ticker,
                    'expiry_date': third_wed,
                    'strike': strike,
                    'expiry_month': f"{target_year}-{target_month:02d}"
                })
        
        print(f"   Generated {len(option_tickers)} VIX option contracts")
        return option_tickers
    
    def collect_vix_options_simple(self, option_tickers, max_options=200):
        """
        Collect VIX options data using reference data (current values)
        """
        print(f"📊 Collecting VIX options data (current values)...")
        print(f"   Processing up to {max_options} options for speed...")
        
        all_data = []
        successful_count = 0
        
        # Limit options for faster collection
        limited_tickers = option_tickers[:max_options]
        
        for i, option_info in enumerate(limited_tickers):
            ticker = option_info['ticker']
            
            if i % 20 == 0:
                print(f"   Progress: {i}/{len(limited_tickers)} ({successful_count} successful)")
            
            try:
                # Create reference data request
                request = self.refDataService.createRequest("ReferenceDataRequest")
                request.getElement("securities").appendValue(ticker)
                
                # Essential option fields
                fields = [
                    "PX_LAST",      # Last price
                    "PX_MID",       # Mid price  
                    "PX_BID",       # Bid price
                    "PX_ASK",       # Ask price
                    "PX_VOLUME",    # Volume
                    "DELTA_MID",    # Delta
                    "GAMMA_MID",    # Gamma
                    "THETA_MID",    # Theta
                    "VEGA_MID",     # Vega
                    "IVOL_MID"      # Implied volatility
                ]
                
                for field in fields:
                    request.getElement("fields").appendValue(field)
                
                self.session.sendRequest(request)
                event = self.session.nextEvent(3000)  # 3 second timeout
                
                if event.eventType() == blpapi.Event.RESPONSE:
                    for msg in event:
                        securityDataArray = msg.getElement("securityData")
                        
                        for j in range(securityDataArray.numValues()):
                            securityData = securityDataArray.getValue(j)
                            
                            # Skip if security error
                            if securityData.hasElement("securityError"):
                                continue
                            
                            # Check for field data
                            if not securityData.hasElement("fieldData"):
                                continue
                            
                            fieldData = securityData.getElement("fieldData")
                            
                            # Create data row
                            row_data = {
                                'collection_date': datetime.now().strftime('%Y-%m-%d'),
                                'ticker': ticker,
                                'expiry_date': option_info['expiry_date'].strftime('%Y-%m-%d'),
                                'strike': option_info['strike'],
                                'expiry_month': option_info['expiry_month'],
                                'data_type': 'VIX_Call_Option'
                            }
                            
                            # Extract all available fields
                            field_mapping = {
                                'last_price': 'PX_LAST',
                                'mid_price': 'PX_MID',
                                'bid_price': 'PX_BID', 
                                'ask_price': 'PX_ASK',
                                'volume': 'PX_VOLUME',
                                'delta': 'DELTA_MID',
                                'gamma': 'GAMMA_MID',
                                'theta': 'THETA_MID',
                                'vega': 'VEGA_MID',
                                'implied_vol': 'IVOL_MID'
                            }
                            
                            for clean_name, bloomberg_field in field_mapping.items():
                                if fieldData.hasElement(bloomberg_field):
                                    value = fieldData.getElement(bloomberg_field).getValue()
                                    row_data[clean_name] = value if value is not None else np.nan
                                else:
                                    row_data[clean_name] = np.nan
                            
                            all_data.append(row_data)
                            successful_count += 1
                            break  # Only process first security in response
                
            except Exception as e:
                # Skip problematic options
                continue
            
            # Small delay to avoid overloading Bloomberg
            time.sleep(0.02)
        
        df = pd.DataFrame(all_data)
        print(f"   ✅ Successfully collected {len(df)} VIX options")
        return df
    
    def filter_target_deltas(self, options_df):
        """
        Filter options to find 10 delta and 50 delta calls
        """
        if len(options_df) == 0:
            return pd.DataFrame()
        
        print(f"🎯 Filtering for target delta options...")
        
        # Remove options without delta data
        valid_options = options_df.dropna(subset=['delta']).copy()
        print(f"   Options with delta data: {len(valid_options)}")
        
        if len(valid_options) == 0:
            return pd.DataFrame()
        
        # Only keep calls (positive delta)
        call_options = valid_options[valid_options['delta'] > 0].copy()
        print(f"   Call options (delta > 0): {len(call_options)}")
        
        if len(call_options) == 0:
            return pd.DataFrame()
        
        target_deltas = [0.10, 0.50]  # 10 delta and 50 delta
        filtered_data = []
        
        # Group by expiry month
        for expiry_month, group in call_options.groupby('expiry_month'):
            print(f"   Processing {expiry_month}: {len(group)} options")
            
            for target_delta in target_deltas:
                # Find option closest to target delta
                group['delta_diff'] = abs(group['delta'] - target_delta)
                
                if len(group) > 0:
                    closest_idx = group['delta_diff'].idxmin()
                    closest_option = group.loc[closest_idx].copy()
                    
                    # Add target delta labels
                    closest_option['target_delta'] = target_delta
                    closest_option['delta_label'] = f"{int(target_delta*100)}Δ"
                    
                    filtered_data.append(closest_option.to_dict())
        
        target_df = pd.DataFrame(filtered_data)
        print(f"   ✅ Found {len(target_df)} target delta options")
        
        if len(target_df) > 0:
            # Show breakdown
            breakdown = target_df['delta_label'].value_counts()
            for label, count in breakdown.items():
                print(f"     {label}: {count} options")
        
        return target_df
    
    def save_vix_options_data(self, options_df, target_delta_df):
        """
        Save VIX options data to CSV files
        """
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        try:
            files_created = []
            
            # Save all options data
            if len(options_df) > 0:
                all_options_file = self.data_dir / f'vix_options_current_{timestamp}.csv'
                options_df.to_csv(all_options_file, index=False)
                files_created.append(str(all_options_file))
                print(f"✅ All options: {len(options_df)} records → {all_options_file.name}")
            
            # Save target delta options
            if len(target_delta_df) > 0:
                target_file = self.data_dir / f'vix_options_10d_50d_{timestamp}.csv'
                target_delta_df.to_csv(target_file, index=False)
                files_created.append(str(target_file))
                print(f"✅ Target deltas: {len(target_delta_df)} records → {target_file.name}")
            
            # Create summary
            summary = {
                'collection_timestamp': timestamp,
                'collection_type': 'vix_options_current',
                'collection_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'data_summary': {
                    'total_options': len(options_df),
                    'target_delta_options': len(target_delta_df),
                    'delta_breakdown': target_delta_df['delta_label'].value_counts().to_dict() if len(target_delta_df) > 0 else {},
                    'expiry_months': sorted(options_df['expiry_month'].unique().tolist()) if len(options_df) > 0 else []
                },
                'files_created': files_created
            }
            
            summary_file = self.data_dir / f'vix_options_summary_{timestamp}.json'
            with open(summary_file, 'w') as f:
                json.dump(summary, f, indent=2)
            
            print(f"✅ Summary: {summary_file.name}")
            return summary
            
        except Exception as e:
            print(f"❌ Failed to save data: {e}")
            return None
    
    def run_simple_collection(self):
        """
        Run simple VIX options collection
        """
        print("🚀 Starting simple VIX options collection...")
        
        try:
            if not self.connect():
                return False
            
            # Generate option tickers
            print("\n📋 Generating VIX option contracts...")
            option_tickers = self.generate_current_vix_options()
            
            # Collect options data
            print("\n📊 Collecting VIX options data...")
            options_df = self.collect_vix_options_simple(option_tickers, max_options=150)
            
            if len(options_df) == 0:
                print("❌ No options data collected")
                return False
            
            # Filter for target deltas
            print("\n🎯 Filtering for 10Δ and 50Δ calls...")
            target_delta_df = self.filter_target_deltas(options_df)
            
            # Save data
            print("\n💾 Saving VIX options data...")
            summary = self.save_vix_options_data(options_df, target_delta_df)
            
            if summary:
                print("\n🎉 VIX options collection completed successfully!")
                print("=" * 60)
                print(f"📊 Total options collected: {summary['data_summary']['total_options']:,}")
                print(f"📊 Target delta options: {summary['data_summary']['target_delta_options']:,}")
                
                if summary['data_summary']['delta_breakdown']:
                    print("📈 Delta breakdown:")
                    for label, count in summary['data_summary']['delta_breakdown'].items():
                        print(f"   {label}: {count}")
                
                print(f"📁 Files saved to: {self.data_dir}")
                return True
            else:
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
    print("SIMPLE VIX OPTIONS DATA COLLECTION")
    print("=" * 70)
    print(f"Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    fetcher = SimpleVIXOptionsFetcher()
    success = fetcher.run_simple_collection()
    
    if success:
        print("\n🎊 Success! VIX options data ready for analysis")
        print("💡 You can now copy this data to your shared drive")
    else:
        print("\n💥 Collection failed")
    
    return success

if __name__ == "__main__":
    main()
