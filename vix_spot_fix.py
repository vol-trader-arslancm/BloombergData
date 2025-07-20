"""
VIX Spot Data Fix and Dataset Completion
Get VIX spot historical data using correct ticker and complete the dataset
"""

import sys
import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import json
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

class VIXSpotFix:
    """
    Fix VIX spot data collection and complete the dataset
    """
    
    def __init__(self):
        self.session = None
        self.refDataService = None
        self.project_root = project_root
        self.data_dir = self.project_root / 'data' / 'vix_data'
        
        # Test different VIX spot tickers
        self.vix_spot_candidates = [
            'VIX Index',        # Standard VIX
            'VIX3 Index',       # We found this before
            'SPX Index',        # S&P 500 for reference
            '.VIX Index',       # Alternative format
            'CBOE VIX Index',   # CBOE format
        ]
        
        # Date range for 10 years
        self.end_date = datetime.now()
        self.start_date = self.end_date - timedelta(days=10*365 + 3)
        
        print(f"🔧 VIX Spot Data Fix")
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
    
    def find_working_vix_spot_ticker(self):
        """
        Test different VIX spot tickers to find the working one
        """
        print("🔍 Testing VIX spot ticker variations...")
        
        for ticker in self.vix_spot_candidates:
            try:
                request = self.refDataService.createRequest("ReferenceDataRequest")
                request.getElement("securities").appendValue(ticker)
                request.getElement("fields").appendValue("PX_LAST")
                request.getElement("fields").appendValue("NAME")
                
                self.session.sendRequest(request)
                event = self.session.nextEvent(3000)
                
                if event.eventType() == blpapi.Event.RESPONSE:
                    for msg in event:
                        securityDataArray = msg.getElement("securityData")
                        
                        for j in range(securityDataArray.numValues()):
                            securityData = securityDataArray.getValue(j)
                            
                            if securityData.hasElement("securityError"):
                                print(f"   ❌ {ticker:15} - Not found")
                                continue
                            
                            if securityData.hasElement("fieldData"):
                                fieldData = securityData.getElement("fieldData")
                                
                                price = "N/A"
                                name = "Unknown"
                                
                                if fieldData.hasElement("PX_LAST"):
                                    price = fieldData.getElement("PX_LAST").getValue()
                                if fieldData.hasElement("NAME"):
                                    name = fieldData.getElement("NAME").getValue()
                                
                                print(f"   ✅ {ticker:15} - Price: {price:>8} | {name[:40]}")
                                
                                # Test if this ticker has historical data
                                if self.test_historical_data(ticker):
                                    print(f"   🎯 {ticker:15} - HAS HISTORICAL DATA!")
                                    return ticker
                                else:
                                    print(f"   ⚠️ {ticker:15} - No historical data")
                
            except Exception as e:
                print(f"   ❌ {ticker:15} - Error: {e}")
                continue
        
        return None
    
    def test_historical_data(self, ticker):
        """
        Test if ticker has historical data available
        """
        try:
            request = self.refDataService.createRequest("HistoricalDataRequest")
            request.getElement("securities").appendValue(ticker)
            request.getElement("fields").appendValue("PX_LAST")
            
            # Test last 5 days
            test_start = self.end_date - timedelta(days=5)
            request.set("startDate", test_start.strftime('%Y%m%d'))
            request.set("endDate", self.end_date.strftime('%Y%m%d'))
            request.set("periodicitySelection", "DAILY")
            
            self.session.sendRequest(request)
            event = self.session.nextEvent(5000)
            
            if event.eventType() == blpapi.Event.RESPONSE:
                for msg in event:
                    if msg.hasElement("securityData"):
                        securityData = msg.getElement("securityData")
                        if securityData.hasElement("fieldData"):
                            fieldDataArray = securityData.getElement("fieldData")
                            if fieldDataArray.numValues() > 0:
                                return True
            
            return False
            
        except Exception as e:
            return False
    
    def collect_vix_spot_historical(self, vix_ticker):
        """
        Collect 10 years of VIX spot historical data
        """
        print(f"📊 Collecting 10-year VIX spot data for {vix_ticker}...")
        
        try:
            request = self.refDataService.createRequest("HistoricalDataRequest")
            request.getElement("securities").appendValue(vix_ticker)
            
            # VIX spot fields
            fields = ["PX_LAST", "PX_OPEN", "PX_HIGH", "PX_LOW"]
            for field in fields:
                request.getElement("fields").appendValue(field)
            
            request.set("startDate", self.start_date.strftime('%Y%m%d'))
            request.set("endDate", self.end_date.strftime('%Y%m%d'))
            request.set("periodicitySelection", "DAILY")
            
            self.session.sendRequest(request)
            
            all_data = []
            while True:
                event = self.session.nextEvent(30000)
                
                if event.eventType() == blpapi.Event.RESPONSE:
                    for msg in event:
                        if msg.hasElement("securityData"):
                            securityData = msg.getElement("securityData")
                            
                            if securityData.hasElement("securityError"):
                                print(f"   ❌ Error for {vix_ticker}")
                                return pd.DataFrame()
                            
                            if securityData.hasElement("fieldData"):
                                fieldDataArray = securityData.getElement("fieldData")
                                
                                for i in range(fieldDataArray.numValues()):
                                    fieldData = fieldDataArray.getValue(i)
                                    
                                    if not fieldData.hasElement("date"):
                                        continue
                                    
                                    data_date = fieldData.getElement("date").getValue()
                                    
                                    row_data = {
                                        'date': data_date.strftime('%Y-%m-%d'),
                                        'ticker': vix_ticker,
                                        'data_type': 'VIX_Spot'
                                    }
                                    
                                    # Extract VIX data
                                    field_mapping = {
                                        'vix_level': 'PX_LAST',
                                        'vix_open': 'PX_OPEN',
                                        'vix_high': 'PX_HIGH',
                                        'vix_low': 'PX_LOW'
                                    }
                                    
                                    for clean_name, bloomberg_field in field_mapping.items():
                                        if fieldData.hasElement(bloomberg_field):
                                            value = fieldData.getElement(bloomberg_field).getValue()
                                            row_data[clean_name] = value if value is not None else np.nan
                                        else:
                                            row_data[clean_name] = np.nan
                                    
                                    all_data.append(row_data)
                    break
                
                if event.eventType() == blpapi.Event.TIMEOUT:
                    print(f"   ⏱️ Timeout for {vix_ticker}")
                    break
            
            df = pd.DataFrame(all_data)
            print(f"   ✅ Collected {len(df):,} VIX spot records")
            return df
            
        except Exception as e:
            print(f"   ❌ Error collecting VIX spot: {e}")
            return pd.DataFrame()
    
    def load_existing_futures_data(self):
        """
        Load the existing VIX futures data that was successfully collected
        """
        print("📂 Loading existing VIX futures data...")
        
        # Find the most recent futures file
        futures_files = list(self.data_dir.glob("vix_futures_10yr_historical_*.csv"))
        
        if not futures_files:
            print("   ❌ No existing futures data found")
            return pd.DataFrame()
        
        # Get the most recent file
        latest_file = max(futures_files, key=lambda x: x.stat().st_mtime)
        
        try:
            futures_df = pd.read_csv(latest_file)
            print(f"   ✅ Loaded {len(futures_df):,} futures records from {latest_file.name}")
            return futures_df
        except Exception as e:
            print(f"   ❌ Error loading futures data: {e}")
            return pd.DataFrame()
    
    def create_complete_dataset(self, spot_df, futures_df):
        """
        Create complete dataset with term structure metrics
        """
        if len(spot_df) == 0 or len(futures_df) == 0:
            print("❌ Cannot create complete dataset - missing spot or futures data")
            return pd.DataFrame()
        
        print(f"📊 Creating complete dataset with term structure metrics...")
        
        # Pivot futures data
        futures_pivot = futures_df.pivot_table(
            index='date',
            columns='ticker', 
            values='last_price',
            aggfunc='first'
        ).reset_index()
        
        # Merge with spot data
        spot_clean = spot_df[['date', 'vix_level']].copy()
        complete_df = pd.merge(futures_pivot, spot_clean, on='date', how='inner')
        
        # Calculate term structure metrics
        if 'UX4 Index' in complete_df.columns:
            complete_df['front_month_spread'] = complete_df['UX4 Index'] - complete_df['vix_level']
            complete_df['front_month_ratio'] = complete_df['UX4 Index'] / complete_df['vix_level']
            
            # Market structure
            complete_df['market_structure'] = np.where(
                complete_df['front_month_spread'] > 0, 'Contango', 'Backwardation'
            )
        
        if 'UX4 Index' in complete_df.columns and 'UX5 Index' in complete_df.columns:
            complete_df['m1_m2_spread'] = complete_df['UX5 Index'] - complete_df['UX4 Index']
            complete_df['calendar_spread_1_2'] = complete_df['UX5 Index'] - complete_df['UX4 Index']
        
        if 'UX4 Index' in complete_df.columns and 'UX9 Index' in complete_df.columns:
            complete_df['front_back_spread'] = complete_df['UX9 Index'] - complete_df['UX4 Index']
            complete_df['calendar_spread_1_6'] = complete_df['UX9 Index'] - complete_df['UX4 Index']
        
        print(f"   ✅ Created complete dataset with {len(complete_df):,} records")
        return complete_df
    
    def save_complete_dataset(self, spot_df, futures_df, complete_df):
        """
        Save the complete VIX dataset
        """
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        try:
            files_created = []
            
            # Save VIX spot
            if len(spot_df) > 0:
                spot_file = self.data_dir / f'vix_spot_10yr_complete_{timestamp}.csv'
                spot_df.to_csv(spot_file, index=False)
                files_created.append(str(spot_file))
                print(f"✅ VIX spot: {len(spot_df):,} records → {spot_file.name}")
            
            # Save complete term structure dataset
            if len(complete_df) > 0:
                complete_file = self.data_dir / f'vix_complete_dataset_10yr_{timestamp}.csv'
                complete_df.to_csv(complete_file, index=False)
                files_created.append(str(complete_file))
                print(f"✅ Complete dataset: {len(complete_df):,} records → {complete_file.name}")
            
            # Create final summary
            summary = {
                'completion_timestamp': timestamp,
                'dataset_type': 'complete_10yr_vix_dataset',
                'data_summary': {
                    'vix_spot_records': len(spot_df),
                    'vix_futures_records': len(futures_df),
                    'complete_dataset_records': len(complete_df),
                    'futures_contracts': len(futures_df['ticker'].unique()) if len(futures_df) > 0 else 0,
                    'date_range': {
                        'start': complete_df['date'].min() if len(complete_df) > 0 else None,
                        'end': complete_df['date'].max() if len(complete_df) > 0 else None
                    }
                },
                'analysis_ready': True,
                'files_created': files_created
            }
            
            summary_file = self.data_dir / f'vix_complete_summary_{timestamp}.json'
            with open(summary_file, 'w') as f:
                json.dump(summary, f, indent=2, default=str)
            
            print(f"✅ Summary: {summary_file.name}")
            return summary
            
        except Exception as e:
            print(f"❌ Failed to save complete dataset: {e}")
            return None
    
    def run_fix_and_complete(self):
        """
        Fix VIX spot data and complete the dataset
        """
        print("🚀 Starting VIX dataset completion...")
        
        try:
            if not self.connect():
                return False
            
            # Find working VIX spot ticker
            vix_ticker = self.find_working_vix_spot_ticker()
            
            if not vix_ticker:
                print("❌ Could not find working VIX spot ticker")
                return False
            
            # Collect VIX spot historical data
            spot_df = self.collect_vix_spot_historical(vix_ticker)
            
            if len(spot_df) == 0:
                print("❌ Failed to collect VIX spot data")
                return False
            
            # Load existing futures data
            futures_df = self.load_existing_futures_data()
            
            if len(futures_df) == 0:
                print("❌ No existing futures data to complete")
                return False
            
            # Create complete dataset
            complete_df = self.create_complete_dataset(spot_df, futures_df)
            
            # Save everything
            summary = self.save_complete_dataset(spot_df, futures_df, complete_df)
            
            if summary:
                print("\n🎉 VIX dataset completion successful!")
                print("=" * 70)
                print(f"📊 VIX spot: {summary['data_summary']['vix_spot_records']:,} records")
                print(f"📊 VIX futures: {summary['data_summary']['vix_futures_records']:,} records") 
                print(f"📊 Complete dataset: {summary['data_summary']['complete_dataset_records']:,} records")
                print(f"📊 Futures contracts: {summary['data_summary']['futures_contracts']}")
                print(f"📅 Date range: {summary['data_summary']['date_range']['start']} to {summary['data_summary']['date_range']['end']}")
                print(f"📁 Files saved to: {self.data_dir}")
                return True
            else:
                return False
                
        except Exception as e:
            print(f"💥 Fix failed: {e}")
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
    print("VIX DATASET COMPLETION - SPOT DATA FIX")
    print("=" * 70)
    print(f"Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    fixer = VIXSpotFix()
    success = fixer.run_fix_and_complete()
    
    if success:
        print("\n🎊 Complete! 10-year VIX dataset ready for analysis")
    else:
        print("\n💥 Fix failed")
    
    return success

if __name__ == "__main__":
    main()
