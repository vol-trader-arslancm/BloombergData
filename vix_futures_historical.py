"""
10-Year VIX Futures Historical Data Fetcher
Collect 10 years of daily historical data for UX VIX futures
Using the working UX format (UX4-UX9 Index)
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

class VIXFuturesHistoricalFetcher:
    """
    10-Year VIX Futures Historical Data Collection
    Using UX format that we confirmed works
    """
    
    def __init__(self, years_back=10):
        self.session = None
        self.refDataService = None
        self.project_root = project_root
        self.data_dir = self.project_root / 'data' / 'vix_data'
        
        # Create directories
        self.data_dir.mkdir(parents=True, exist_ok=True)
        
        # Working UX VIX futures (confirmed working)
        self.ux_futures = [
            'UX4 Index',   # 1st month VIX future
            'UX5 Index',   # 2nd month VIX future
            'UX6 Index',   # 3rd month VIX future
            'UX7 Index',   # 4th month VIX future
            'UX8 Index',   # 5th month VIX future
            'UX9 Index',   # 6th month VIX future
        ]
        
        # VIX spot (confirmed working)
        self.vix_spot = 'VIX1 Index'
        
        # Date range for 10 years
        self.end_date = datetime.now()
        self.start_date = self.end_date - timedelta(days=years_back*365 + 3)
        
        # Essential fields for VIX futures analysis
        self.futures_fields = {
            'last_price': 'PX_LAST',
            'open_price': 'PX_OPEN',
            'high_price': 'PX_HIGH',
            'low_price': 'PX_LOW',
            'settle_price': 'PX_SETTLE',
            'volume': 'PX_VOLUME',
            'open_interest': 'OPEN_INT'
        }
        
        self.spot_fields = {
            'vix_level': 'PX_LAST',
            'vix_open': 'PX_OPEN',
            'vix_high': 'PX_HIGH',
            'vix_low': 'PX_LOW'
        }
        
        print(f"🔥 10-Year VIX Futures Historical Collection")
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
    
    def get_historical_data_for_security(self, ticker, fields, data_type):
        """
        Get 10 years of historical data for a specific security
        """
        print(f"📊 Collecting 10-year {data_type} data for {ticker}...")
        
        try:
            # Create historical data request
            request = self.refDataService.createRequest("HistoricalDataRequest")
            request.getElement("securities").appendValue(ticker)
            
            # Add all fields
            bloomberg_fields = list(fields.values())
            for field in bloomberg_fields:
                request.getElement("fields").appendValue(field)
            
            # Set date range
            request.set("startDate", self.start_date.strftime('%Y%m%d'))
            request.set("endDate", self.end_date.strftime('%Y%m%d'))
            request.set("periodicitySelection", "DAILY")
            
            # Send request
            self.session.sendRequest(request)
            
            # Process response with proper Bloomberg API structure
            all_data = []
            while True:
                event = self.session.nextEvent(30000)  # 30 second timeout for large data
                
                if event.eventType() == blpapi.Event.RESPONSE:
                    for msg in event:
                        # Handle securityData properly for historical requests
                        if msg.hasElement("securityData"):
                            securityData = msg.getElement("securityData")
                            
                            # Check for security error
                            if securityData.hasElement("securityError"):
                                error = securityData.getElement("securityError")
                                print(f"   ❌ Security error for {ticker}: {error}")
                                return pd.DataFrame()
                            
                            # Process field data
                            if securityData.hasElement("fieldData"):
                                fieldDataArray = securityData.getElement("fieldData")
                                
                                for i in range(fieldDataArray.numValues()):
                                    fieldData = fieldDataArray.getValue(i)
                                    
                                    # Get date
                                    if not fieldData.hasElement("date"):
                                        continue
                                    
                                    data_date = fieldData.getElement("date").getValue()
                                    
                                    # Create row
                                    row_data = {
                                        'date': data_date.strftime('%Y-%m-%d'),
                                        'ticker': ticker,
                                        'data_type': data_type
                                    }
                                    
                                    # Extract all field data
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
            print(f"   ✅ Collected {len(df):,} records for {ticker}")
            return df
            
        except Exception as e:
            print(f"   ❌ Error collecting {ticker}: {e}")
            import traceback
            traceback.print_exc()
            return pd.DataFrame()
    
    def collect_vix_spot_historical(self):
        """
        Collect 10 years of VIX spot historical data
        """
        print(f"\n📊 Collecting VIX Spot Historical Data...")
        return self.get_historical_data_for_security(
            self.vix_spot, 
            self.spot_fields, 
            'VIX_Spot'
        )
    
    def collect_vix_futures_historical(self):
        """
        Collect 10 years of VIX futures historical data
        """
        print(f"\n📊 Collecting VIX Futures Historical Data...")
        
        all_futures_data = []
        successful_futures = 0
        
        for ticker in self.ux_futures:
            futures_df = self.get_historical_data_for_security(
                ticker, 
                self.futures_fields, 
                'VIX_Future'
            )
            
            if len(futures_df) > 0:
                all_futures_data.append(futures_df)
                successful_futures += 1
            
            # Rate limiting
            time.sleep(0.5)
        
        # Combine all futures data
        if all_futures_data:
            combined_df = pd.concat(all_futures_data, ignore_index=True)
            print(f"\n✅ Total VIX futures data: {len(combined_df):,} records from {successful_futures} contracts")
            return combined_df
        else:
            print(f"\n❌ No VIX futures data collected")
            return pd.DataFrame()
    
    def calculate_term_structure_metrics(self, futures_df, spot_df):
        """
        Calculate daily term structure metrics
        """
        if len(futures_df) == 0 or len(spot_df) == 0:
            return pd.DataFrame()
        
        print(f"📊 Calculating term structure metrics...")
        
        # Pivot futures data by date and ticker
        futures_pivot = futures_df.pivot_table(
            index='date', 
            columns='ticker', 
            values='last_price', 
            aggfunc='first'
        ).reset_index()
        
        # Merge with VIX spot
        spot_clean = spot_df[['date', 'vix_level']].copy()
        
        term_structure = pd.merge(futures_pivot, spot_clean, on='date', how='inner')
        
        # Calculate spreads and ratios
        if 'UX4 Index' in term_structure.columns:
            term_structure['front_month_spread'] = term_structure['UX4 Index'] - term_structure['vix_level']
            term_structure['front_month_ratio'] = term_structure['UX4 Index'] / term_structure['vix_level']
        
        if 'UX4 Index' in term_structure.columns and 'UX5 Index' in term_structure.columns:
            term_structure['m1_m2_spread'] = term_structure['UX5 Index'] - term_structure['UX4 Index']
            term_structure['m1_m2_ratio'] = term_structure['UX5 Index'] / term_structure['UX4 Index']
        
        if 'UX4 Index' in term_structure.columns and 'UX9 Index' in term_structure.columns:
            term_structure['front_back_spread'] = term_structure['UX9 Index'] - term_structure['UX4 Index']
            term_structure['front_back_ratio'] = term_structure['UX9 Index'] / term_structure['UX4 Index']
        
        # Market structure classification
        term_structure['market_structure'] = np.where(
            term_structure['front_month_spread'] > 0, 'Contango', 'Backwardation'
        )
        
        print(f"   ✅ Calculated metrics for {len(term_structure):,} trading days")
        return term_structure
    
    def save_historical_vix_data(self, spot_df, futures_df, term_structure_df):
        """
        Save 10-year VIX historical data
        """
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        try:
            files_created = []
            
            # Save VIX spot historical
            if len(spot_df) > 0:
                spot_file = self.data_dir / f'vix_spot_10yr_historical_{timestamp}.csv'
                spot_df.to_csv(spot_file, index=False)
                files_created.append(str(spot_file))
                print(f"✅ VIX spot 10yr: {len(spot_df):,} records → {spot_file.name}")
            
            # Save VIX futures historical
            if len(futures_df) > 0:
                futures_file = self.data_dir / f'vix_futures_10yr_historical_{timestamp}.csv'
                futures_df.to_csv(futures_file, index=False)
                files_created.append(str(futures_file))
                print(f"✅ VIX futures 10yr: {len(futures_df):,} records → {futures_file.name}")
            
            # Save term structure metrics
            if len(term_structure_df) > 0:
                term_file = self.data_dir / f'vix_term_structure_10yr_{timestamp}.csv'
                term_structure_df.to_csv(term_file, index=False)
                files_created.append(str(term_file))
                print(f"✅ Term structure 10yr: {len(term_structure_df):,} records → {term_file.name}")
            
            # Create comprehensive summary
            summary = {
                'collection_timestamp': timestamp,
                'collection_type': '10_year_vix_historical',
                'data_period': {
                    'start_date': self.start_date.strftime('%Y-%m-%d'),
                    'end_date': self.end_date.strftime('%Y-%m-%d'),
                    'total_days': (self.end_date - self.start_date).days
                },
                'data_summary': {
                    'vix_spot_records': len(spot_df),
                    'vix_futures_records': len(futures_df),
                    'term_structure_records': len(term_structure_df),
                    'futures_contracts_collected': len(futures_df['ticker'].unique()) if len(futures_df) > 0 else 0,
                    'date_range_actual': {
                        'start': spot_df['date'].min() if len(spot_df) > 0 else None,
                        'end': spot_df['date'].max() if len(spot_df) > 0 else None
                    }
                },
                'files_created': files_created,
                'analysis_ready': {
                    'spot_data': len(spot_df) > 0,
                    'futures_data': len(futures_df) > 0,
                    'term_structure': len(term_structure_df) > 0,
                    'complete_dataset': len(spot_df) > 0 and len(futures_df) > 0
                }
            }
            
            summary_file = self.data_dir / f'vix_10yr_historical_summary_{timestamp}.json'
            with open(summary_file, 'w') as f:
                json.dump(summary, f, indent=2, default=str)
            
            print(f"✅ Summary: {summary_file.name}")
            return summary
            
        except Exception as e:
            print(f"❌ Failed to save data: {e}")
            return None
    
    def run_historical_collection(self):
        """
        Run 10-year VIX historical data collection
        """
        print("🚀 Starting 10-year VIX historical data collection...")
        
        try:
            if not self.connect():
                return False
            
            # Collect VIX spot historical
            spot_df = self.collect_vix_spot_historical()
            
            # Collect VIX futures historical
            futures_df = self.collect_vix_futures_historical()
            
            # Calculate term structure metrics
            term_structure_df = self.calculate_term_structure_metrics(futures_df, spot_df)
            
            # Save all data
            print(f"\n💾 Saving 10-year VIX historical data...")
            summary = self.save_historical_vix_data(spot_df, futures_df, term_structure_df)
            
            if summary and summary['analysis_ready']['complete_dataset']:
                print("\n🎉 10-year VIX historical collection completed successfully!")
                print("=" * 70)
                print(f"📊 VIX spot records: {summary['data_summary']['vix_spot_records']:,}")
                print(f"📊 VIX futures records: {summary['data_summary']['vix_futures_records']:,}")
                print(f"📊 Term structure records: {summary['data_summary']['term_structure_records']:,}")
                print(f"📊 Futures contracts: {summary['data_summary']['futures_contracts_collected']}")
                
                if summary['data_summary']['date_range_actual']['start']:
                    print(f"📅 Actual date range: {summary['data_summary']['date_range_actual']['start']} to {summary['data_summary']['date_range_actual']['end']}")
                
                print(f"📁 Files saved to: {self.data_dir}")
                return True
            else:
                print("❌ Incomplete dataset collected")
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
    print("=" * 80)
    print("10-YEAR VIX FUTURES HISTORICAL DATA COLLECTION")
    print("=" * 80)
    print(f"Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    fetcher = VIXFuturesHistoricalFetcher(years_back=10)
    success = fetcher.run_historical_collection()
    
    if success:
        print("\n🎊 Success! 10 years of VIX data ready for analysis")
        print("💡 You now have the complete historical VIX dataset!")
    else:
        print("\n💥 Collection failed")
    
    return success

if __name__ == "__main__":
    main()
