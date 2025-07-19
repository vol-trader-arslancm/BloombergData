"""
10-Year Historical Volatility Data Fetcher
Comprehensive collection of 10 years of historical implied and realized volatility data
for SPX Index and top 50 components with robust error handling and progress tracking.
"""

import sys
import os
import pandas as pd
from datetime import datetime, timedelta
import json
import numpy as np
import time

# Add project root to path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

try:
    import blpapi
    from config.bloomberg_config import SPX_TICKER
except ImportError as e:
    print(f"IMPORT ERROR: {e}")
    SPX_TICKER = 'SPX Index'

class TenYearVolatilityFetcher:
    """Fetch 10 years of comprehensive historical volatility data"""
    
    def __init__(self):
        self.session = None
        self.refDataService = None
        self.project_root = project_root
        self.data_dir = os.path.join(project_root, 'data', 'historical_volatility')
        self.progress_file = os.path.join(self.data_dir, 'ten_year_progress.json')
        
        # Volatility field mappings with clean labels
        self.realized_fields = {
            'realized_vol_30d': 'VOLATILITY_30D',
            'realized_vol_90d': 'VOLATILITY_90D', 
            'realized_vol_180d': 'VOLATILITY_180D',
            'realized_vol_252d': 'VOLATILITY_260D'  # Bloomberg uses 260D for annual
        }
        
        self.implied_fields = {
            'implied_vol_1m_atm': '1MTH_IMPVOL_100.0%MNY_DF',
            'implied_vol_3m_atm': '3MTH_IMPVOL_100.0%MNY_DF',
            'implied_vol_6m_atm': '6MTH_IMPVOL_100.0%MNY_DF',
            'implied_vol_12m_atm': '12MTH_IMPVOL_100.0%MNY_DF'
        }
        
        # Create directories
        os.makedirs(self.data_dir, exist_ok=True)
        
        # Set 10-year date range
        self.end_date = datetime.now()
        self.start_date = self.end_date - timedelta(days=10*365 + 3)  # 10 years + leap days
        
        print(f"📅 10-Year Collection Period:")
        print(f"   Start: {self.start_date.strftime('%Y-%m-%d')}")
        print(f"   End: {self.end_date.strftime('%Y-%m-%d')}")
        print(f"   Total period: {(self.end_date - self.start_date).days:,} days")
    
    def connect(self):
        """Connect to Bloomberg Terminal"""
        try:
            sessionOptions = blpapi.SessionOptions()
            self.session = blpapi.Session(sessionOptions)
            
            if not self.session.start():
                print("ERROR: Failed to start Bloomberg session")
                return False
            
            if not self.session.openService("//blp/refdata"):
                print("ERROR: Failed to open Bloomberg reference data service")
                return False
            
            self.refDataService = self.session.getService("//blp/refdata")
            print("SUCCESS: Connected to Bloomberg for 10-year historical volatility data")
            return True
            
        except Exception as e:
            print(f"ERROR: Bloomberg connection failed: {e}")
            return False
    
    def load_target_securities(self):
        """Load SPX components and define target securities"""
        try:
            # Try to load from existing SPX weights file
            weights_file = os.path.join(project_root, 'data', 'processed', 'spx_weights', 'spx_weights_latest.csv')
            
            if os.path.exists(weights_file):
                weights_df = pd.read_csv(weights_file)
                top_50_tickers = weights_df.head(50)['ticker'].tolist()
                print(f"SUCCESS: Loaded top 50 SPX components from weights file")
            else:
                # Fallback to predefined list
                print("WARNING: SPX weights file not found, using predefined top 50")
                top_50_tickers = [
                    'AAPL US Equity', 'MSFT US Equity', 'NVDA US Equity', 'AMZN US Equity',
                    'META US Equity', 'GOOGL US Equity', 'GOOG US Equity', 'BRK/B US Equity',
                    'LLY US Equity', 'AVGO US Equity', 'JPM US Equity', 'TSLA US Equity',
                    'WMT US Equity', 'V US Equity', 'UNH US Equity', 'XOM US Equity',
                    'MA US Equity', 'PG US Equity', 'JNJ US Equity', 'COST US Equity',
                    'HD US Equity', 'NFLX US Equity', 'BAC US Equity', 'ABBV US Equity',
                    'CRM US Equity', 'CVX US Equity', 'KO US Equity', 'AMD US Equity',
                    'PEP US Equity', 'TMO US Equity', 'LIN US Equity', 'CSCO US Equity',
                    'ACN US Equity', 'ADBE US Equity', 'MRK US Equity', 'ORCL US Equity',
                    'ABT US Equity', 'COP US Equity', 'DHR US Equity', 'NKE US Equity',
                    'VZ US Equity', 'TXN US Equity', 'QCOM US Equity', 'PM US Equity',
                    'WFC US Equity', 'DIS US Equity', 'IBM US Equity', 'CAT US Equity',
                    'GE US Equity', 'MS US Equity'
                ]
            
            # SPX Index + top components
            all_tickers = [SPX_TICKER] + top_50_tickers
            
            print(f"📊 Target Securities for 10-Year Collection:")
            print(f"   Total securities: {len(all_tickers)}")
            print(f"   SPX Index + Top 50 components")
            print(f"   Sample: {all_tickers[:5]}")
            
            return all_tickers
            
        except Exception as e:
            print(f"ERROR: Failed to load target securities: {e}")
            return None
    
    def load_progress(self):
        """Load collection progress to enable resumption"""
        try:
            if os.path.exists(self.progress_file):
                with open(self.progress_file, 'r') as f:
                    progress = json.load(f)
                print(f"INFO: Loaded existing progress - {len(progress.get('completed_securities', []))} securities completed")
                return progress
            else:
                return {
                    'start_date': self.start_date.strftime('%Y-%m-%d'),
                    'end_date': self.end_date.strftime('%Y-%m-%d'),
                    'completed_securities': [],
                    'failed_securities': [],
                    'collection_started': datetime.now().isoformat(),
                    'last_updated': None
                }
        except Exception as e:
            print(f"WARNING: Could not load progress: {e}")
            return {}
    
    def save_progress(self, progress):
        """Save collection progress"""
        try:
            progress['last_updated'] = datetime.now().isoformat()
            with open(self.progress_file, 'w') as f:
                json.dump(progress, f, indent=2)
        except Exception as e:
            print(f"WARNING: Could not save progress: {e}")
    
    def fetch_security_volatility_data(self, ticker, vol_fields, data_type):
        """Fetch 10 years of volatility data for a single security"""
        try:
            print(f"      Fetching {data_type} data for {ticker}...")
            
            bloomberg_fields = list(vol_fields.values())
            start_date_str = self.start_date.strftime('%Y%m%d')
            end_date_str = self.end_date.strftime('%Y%m%d')
            
            request = self.refDataService.createRequest("HistoricalDataRequest")
            request.getElement("securities").appendValue(ticker)
            
            for field in bloomberg_fields:
                request.getElement("fields").appendValue(field)
            
            request.set("startDate", start_date_str)
            request.set("endDate", end_date_str)
            request.set("periodicitySelection", "DAILY")
            
            self.session.sendRequest(request)
            
            # Process response with longer timeout for 10-year data
            ticker_data = []
            while True:
                event = self.session.nextEvent(30000)  # 30 second timeout
                
                if event.eventType() == blpapi.Event.RESPONSE:
                    for msg in event:
                        securityData = msg.getElement("securityData")
                        
                        if securityData.hasElement("securityError"):
                            print(f"         WARNING: Error for {ticker}")
                            return pd.DataFrame()
                        
                        fieldDataArray = securityData.getElement("fieldData")
                        
                        for i in range(fieldDataArray.numValues()):
                            fieldData = fieldDataArray.getValue(i)
                            date = fieldData.getElement("date").getValue()
                            
                            row_data = {
                                'date': date.strftime('%Y-%m-%d'),
                                'ticker': ticker,
                                'data_type': data_type
                            }
                            
                            # Map Bloomberg fields to clean names
                            for clean_name, bloomberg_field in vol_fields.items():
                                if fieldData.hasElement(bloomberg_field):
                                    value = fieldData.getElement(bloomberg_field).getValue()
                                    row_data[clean_name] = value if value is not None else np.nan
                                else:
                                    row_data[clean_name] = np.nan
                            
                            ticker_data.append(row_data)
                    break
                
                if event.eventType() == blpapi.Event.TIMEOUT:
                    print(f"         WARNING: Timeout for {ticker}")
                    return pd.DataFrame()
            
            if ticker_data:
                df = pd.DataFrame(ticker_data)
                df['date'] = pd.to_datetime(df['date'])
                print(f"         SUCCESS: {len(df):,} observations for {ticker}")
                return df
            else:
                print(f"         WARNING: No data for {ticker}")
                return pd.DataFrame()
                
        except Exception as e:
            print(f"         ERROR: Failed to fetch {ticker}: {e}")
            return pd.DataFrame()
    
    def collect_ten_year_data(self, securities):
        """Main collection function for 10-year data"""
        progress = self.load_progress()
        completed_securities = set(progress.get('completed_securities', []))
        failed_securities = set(progress.get('failed_securities', []))
        
        all_realized_data = []
        all_implied_data = []
        
        total_securities = len(securities)
        
        print(f"\n🚀 STARTING 10-YEAR VOLATILITY DATA COLLECTION")
        print(f"   Total securities: {total_securities}")
        print(f"   Already completed: {len(completed_securities)}")
        print(f"   Remaining: {total_securities - len(completed_securities)}")
        print("=" * 60)
        
        for i, ticker in enumerate(securities):
            if ticker in completed_securities:
                print(f"⏭️  Skipping {ticker} (already completed)")
                continue
                
            if ticker in failed_securities:
                print(f"⚠️  Retrying {ticker} (previously failed)")
            
            print(f"\n📊 Processing {ticker} ({i+1}/{total_securities})")
            print(f"   Progress: {((i+1)/total_securities)*100:.1f}%")
            
            try:
                # Fetch realized volatility data
                realized_df = self.fetch_security_volatility_data(
                    ticker, self.realized_fields, 'realized'
                )
                
                # Fetch implied volatility data
                implied_df = self.fetch_security_volatility_data(
                    ticker, self.implied_fields, 'implied'
                )
                
                # Check if we got meaningful data
                realized_success = len(realized_df) > 100  # At least 100 observations
                implied_success = len(implied_df) > 100
                
                if realized_success:
                    all_realized_data.append(realized_df)
                    print(f"      ✅ Realized data: {len(realized_df):,} obs")
                
                if implied_success:
                    all_implied_data.append(implied_df)
                    print(f"      ✅ Implied data: {len(implied_df):,} obs")
                
                if realized_success or implied_success:
                    completed_securities.add(ticker)
                    progress['completed_securities'] = list(completed_securities)
                    
                    # Remove from failed if it was there
                    if ticker in failed_securities:
                        failed_securities.remove(ticker)
                        progress['failed_securities'] = list(failed_securities)
                    
                    print(f"      ✅ {ticker} completed successfully")
                else:
                    failed_securities.add(ticker)
                    progress['failed_securities'] = list(failed_securities)
                    print(f"      ❌ {ticker} failed - insufficient data")
                
                # Save progress every 5 securities
                if (i + 1) % 5 == 0:
                    self.save_progress(progress)
                    print(f"      💾 Progress saved")
                
                # Brief pause to avoid overwhelming Bloomberg
                time.sleep(1)
                
            except Exception as e:
                print(f"      ❌ Error processing {ticker}: {e}")
                failed_securities.add(ticker)
                progress['failed_securities'] = list(failed_securities)
                continue
        
        # Final progress save
        self.save_progress(progress)
        
        # Combine all data
        print(f"\n📊 COMBINING 10-YEAR DATA...")
        combined_data = []
        
        if all_realized_data:
            realized_combined = pd.concat(all_realized_data, ignore_index=True)
            combined_data.append(realized_combined)
            print(f"   Realized data: {len(realized_combined):,} total observations")
        
        if all_implied_data:
            implied_combined = pd.concat(all_implied_data, ignore_index=True)
            combined_data.append(implied_combined)
            print(f"   Implied data: {len(implied_combined):,} total observations")
        
        if combined_data:
            final_df = pd.concat(combined_data, ignore_index=True)
            final_df = final_df.sort_values(['ticker', 'date'])
            
            print(f"\n✅ 10-YEAR COLLECTION SUMMARY:")
            print(f"   Total observations: {len(final_df):,}")
            print(f"   Securities: {final_df['ticker'].nunique()}")
            print(f"   Date range: {final_df['date'].min().strftime('%Y-%m-%d')} to {final_df['date'].max().strftime('%Y-%m-%d')}")
            print(f"   Successful securities: {len(completed_securities)}")
            print(f"   Failed securities: {len(failed_securities)}")
            
            return final_df
        else:
            print("❌ No data collected")
            return pd.DataFrame()
    
    def save_ten_year_data(self, df):
        """Save 10-year volatility data with comprehensive metadata"""
        try:
            if len(df) == 0:
                print("No data to save")
                return None
            
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            
            print(f"\n💾 SAVING 10-YEAR VOLATILITY DATA...")
            
            # Save main dataset in multiple formats
            base_filename = f'ten_year_volatility_data_{timestamp}'
            
            # CSV format
            csv_file = os.path.join(self.data_dir, f'{base_filename}.csv')
            df.to_csv(csv_file, index=False)
            print(f"   ✅ CSV: {csv_file}")
            
            # Parquet format (faster loading)
            parquet_file = os.path.join(self.data_dir, f'{base_filename}.parquet')
            df.to_parquet(parquet_file, index=False)
            print(f"   ✅ Parquet: {parquet_file}")
            
            # Update latest files
            latest_csv = os.path.join(self.data_dir, 'ten_year_volatility_latest.csv')
            latest_parquet = os.path.join(self.data_dir, 'ten_year_volatility_latest.parquet')
            
            df.to_csv(latest_csv, index=False)
            df.to_parquet(latest_parquet, index=False)
            print(f"   ✅ Latest files updated")
            
            # Create comprehensive summary
            summary = {
                'collection_info': {
                    'timestamp': timestamp,
                    'collection_period_days': (self.end_date - self.start_date).days,
                    'start_date': self.start_date.strftime('%Y-%m-%d'),
                    'end_date': self.end_date.strftime('%Y-%m-%d')
                },
                'data_summary': {
                    'total_observations': len(df),
                    'securities_count': df['ticker'].nunique(),
                    'data_types': df['data_type'].unique().tolist(),
                    'date_coverage': {
                        'first_date': df['date'].min().isoformat(),
                        'last_date': df['date'].max().isoformat(),
                        'trading_days': len(df['date'].unique())
                    }
                },
                'data_quality': {
                    'realized_observations': len(df[df['data_type'] == 'realized']),
                    'implied_observations': len(df[df['data_type'] == 'implied']),
                    'spx_observations': len(df[df['ticker'] == 'SPX Index']),
                    'component_observations': len(df[df['ticker'] != 'SPX Index']),
                    'completeness_by_field': {}
                },
                'file_info': {
                    'csv_file': csv_file,
                    'parquet_file': parquet_file,
                    'latest_csv': latest_csv,
                    'latest_parquet': latest_parquet
                }
            }
            
            # Add field completeness analysis
            numeric_columns = df.select_dtypes(include=[np.number]).columns
            for col in numeric_columns:
                if 'vol' in col:
                    completeness = (df[col].notna().sum() / len(df)) * 100
                    summary['data_quality']['completeness_by_field'][col] = round(completeness, 2)
            
            # Save summary
            summary_file = os.path.join(self.data_dir, f'ten_year_collection_summary_{timestamp}.json')
            with open(summary_file, 'w') as f:
                json.dump(summary, f, indent=2, default=str)
            
            print(f"   ✅ Summary: {summary_file}")
            
            print(f"\n🎉 10-YEAR DATA COLLECTION COMPLETE!")
            print(f"   Files ready for advanced volatility analysis")
            
            return csv_file, parquet_file, summary_file
            
        except Exception as e:
            print(f"ERROR: Failed to save 10-year data: {e}")
            return None
    
    def disconnect(self):
        """Disconnect from Bloomberg"""
        if self.session:
            self.session.stop()
            print("SUCCESS: Bloomberg session disconnected")

def main():
    """Main execution function for 10-year volatility collection"""
    print("🚀 10-YEAR HISTORICAL VOLATILITY DATA COLLECTION")
    print("=" * 70)
    print(f"Collecting comprehensive 10-year volatility dataset...")
    print(f"This may take 2-4 hours depending on Bloomberg performance")
    
    fetcher = TenYearVolatilityFetcher()
    
    try:
        # Connect to Bloomberg
        if not fetcher.connect():
            return False
        
        # Load target securities
        print("\n1. Loading target securities...")
        securities = fetcher.load_target_securities()
        if not securities:
            return False
        
        # Collect 10-year data
        print(f"\n2. Starting 10-year data collection...")
        ten_year_df = fetcher.collect_ten_year_data(securities)
        
        if len(ten_year_df) == 0:
            print("❌ No data collected")
            return False
        
        # Save data
        print(f"\n3. Saving 10-year dataset...")
        result = fetcher.save_ten_year_data(ten_year_df)
        
        if result:
            print(f"\n✅ SUCCESS: 10-year volatility dataset created!")
            print(f"   Ready for professional-grade risk premium analysis")
            print(f"   Use this data in your advanced volatility notebooks")
            return True
        else:
            print("❌ Failed to save data")
            return False
            
    except KeyboardInterrupt:
        print("\n⚠️ Collection interrupted by user")
        print("Progress has been saved - you can resume later")
        return False
    except Exception as e:
        print(f"❌ Error in main execution: {e}")
        return False
    
    finally:
        fetcher.disconnect()

if __name__ == "__main__":
    main()