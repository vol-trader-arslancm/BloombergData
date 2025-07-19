"""
Historical Volatility Data Fetcher
Collect comprehensive historical implied and realized volatility data for top 50 SPX components + SPX Index
"""

import sys
import os
import pandas as pd
from datetime import datetime, timedelta
import json
import numpy as np

# Add project root to path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

try:
    import blpapi
    from config.bloomberg_config import SPX_TICKER
except ImportError as e:
    print(f"IMPORT ERROR: {e}")
    SPX_TICKER = 'SPX Index'

class HistoricalVolatilityFetcher:
    """Fetch comprehensive historical volatility data with incremental updates"""
    
    def __init__(self):
        self.session = None
        self.refDataService = None
        self.project_root = project_root
        self.data_dir = os.path.join(project_root, 'data', 'historical_volatility')
        self.log_file = os.path.join(self.data_dir, 'collection_log.json')
        
        # Volatility field mappings
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
            'implied_vol_12m_atm': '12MTH_IMPVOL_100.0%MNY_DF',
            'implied_vol_1m_50delta': '1MTH_IMPVOL_50.0DELTA_DF',
            'implied_vol_3m_50delta': '3MTH_IMPVOL_50.0DELTA_DF',
            'implied_vol_6m_50delta': '6MTH_IMPVOL_50.0DELTA_DF',
            'implied_vol_12m_50delta': '12MTH_IMPVOL_50.0DELTA_DF'
        }
        
        # Create directories
        os.makedirs(self.data_dir, exist_ok=True)
    
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
            print("SUCCESS: Connected to Bloomberg for historical volatility data")
            return True
            
        except Exception as e:
            print(f"ERROR: Bloomberg connection failed: {e}")
            return False
    
    def load_spx_components(self):
        """Load top 50 SPX components from weights file"""
        try:
            weights_file = os.path.join(project_root, 'data', 'processed', 'spx_weights', 'spx_weights_latest.csv')
            
            if not os.path.exists(weights_file):
                print(f"ERROR: SPX weights file not found: {weights_file}")
                print("Please run fetch_spx_weights.py first to generate the weights file")
                return None
            
            weights_df = pd.read_csv(weights_file)
            
            # Get top 50 components + SPX Index
            top_50_tickers = weights_df.head(50)['ticker'].tolist()
            all_tickers = [SPX_TICKER] + top_50_tickers
            
            print(f"SUCCESS: Loaded {len(all_tickers)} securities for historical volatility collection")
            print(f"   SPX Index + Top 50 components")
            print(f"   Sample tickers: {all_tickers[:5]}")
            
            return all_tickers
            
        except Exception as e:
            print(f"ERROR: Failed to load SPX components: {e}")
            return None
    
    def load_collection_log(self):
        """Load collection log to track what's been collected"""
        try:
            if os.path.exists(self.log_file):
                with open(self.log_file, 'r') as f:
                    return json.load(f)
            else:
                # Initialize new log
                return {
                    'last_collection_date': None,
                    'securities_collected': [],
                    'date_ranges': {},
                    'total_observations': 0,
                    'created': datetime.now().isoformat()
                }
        except Exception as e:
            print(f"WARNING: Could not load collection log: {e}")
            return {}
    
    def save_collection_log(self, log_data):
        """Save collection log"""
        try:
            log_data['last_updated'] = datetime.now().isoformat()
            with open(self.log_file, 'w') as f:
                json.dump(log_data, f, indent=2, default=str)
        except Exception as e:
            print(f"WARNING: Could not save collection log: {e}")
    
    def get_collection_date_range(self, incremental=True):
        """Determine date range for collection"""
        try:
            end_date = datetime.now()
            
            if incremental:
                log_data = self.load_collection_log()
                last_date = log_data.get('last_collection_date')
                
                if last_date:
                    start_date = datetime.fromisoformat(last_date) + timedelta(days=1)
                    print(f"INFO: Incremental collection from {start_date.strftime('%Y-%m-%d')}")
                else:
                    # First time collection - go back 3 years
                    start_date = end_date - timedelta(days=3*365)
                    print(f"INFO: Initial collection - 3 years back from {start_date.strftime('%Y-%m-%d')}")
            else:
                # Full refresh - 3 years back
                start_date = end_date - timedelta(days=3*365)
                print(f"INFO: Full collection - 3 years back from {start_date.strftime('%Y-%m-%d')}")
            
            return start_date.strftime('%Y%m%d'), end_date.strftime('%Y%m%d')
            
        except Exception as e:
            print(f"ERROR: Failed to determine date range: {e}")
            return None, None
    
    def fetch_historical_volatility(self, tickers, vol_fields, start_date, end_date, data_type):
        """Fetch historical volatility data for given tickers and date range"""
        try:
            print(f"INFO: Fetching {data_type} volatility data...")
            print(f"   Securities: {len(tickers)}")
            print(f"   Fields: {list(vol_fields.keys())}")
            print(f"   Date range: {start_date} to {end_date}")
            
            all_data = []
            bloomberg_fields = list(vol_fields.values())
            
            # Process securities in batches
            batch_size = 10
            for batch_idx in range(0, len(tickers), batch_size):
                batch_tickers = tickers[batch_idx:batch_idx + batch_size]
                batch_num = batch_idx // batch_size + 1
                total_batches = (len(tickers) + batch_size - 1) // batch_size
                
                print(f"   Processing batch {batch_num}/{total_batches}: {len(batch_tickers)} securities")
                
                for ticker in batch_tickers:
                    print(f"     Fetching {ticker}...")
                    
                    try:
                        request = self.refDataService.createRequest("HistoricalDataRequest")
                        request.getElement("securities").appendValue(ticker)
                        
                        for field in bloomberg_fields:
                            request.getElement("fields").appendValue(field)
                        
                        request.set("startDate", start_date)
                        request.set("endDate", end_date)
                        request.set("periodicitySelection", "DAILY")
                        
                        self.session.sendRequest(request)
                        
                        # Process response
                        ticker_data = []
                        while True:
                            event = self.session.nextEvent(10000)  # 10 second timeout
                            
                            if event.eventType() == blpapi.Event.RESPONSE:
                                for msg in event:
                                    securityData = msg.getElement("securityData")
                                    
                                    if securityData.hasElement("securityError"):
                                        print(f"       WARNING: Error for {ticker}")
                                        break
                                    
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
                                print(f"       WARNING: Timeout for {ticker}")
                                break
                        
                        all_data.extend(ticker_data)
                        print(f"       Retrieved {len(ticker_data)} observations")
                        
                    except Exception as e:
                        print(f"       ERROR: Failed to fetch {ticker}: {e}")
                        continue
            
            if all_data:
                df = pd.DataFrame(all_data)
                df['date'] = pd.to_datetime(df['date'])
                df = df.sort_values(['date', 'ticker'])
                
                print(f"SUCCESS: Retrieved {len(df)} total observations for {data_type}")
                return df
            else:
                print(f"WARNING: No data retrieved for {data_type}")
                return pd.DataFrame()
                
        except Exception as e:
            print(f"ERROR: Failed to fetch {data_type} data: {e}")
            return pd.DataFrame()
    
    def save_volatility_data(self, realized_df, implied_df, start_date, end_date):
        """Save volatility data in both CSV and Parquet formats"""
        try:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            
            # Combine realized and implied data
            if not realized_df.empty and not implied_df.empty:
                combined_df = pd.concat([realized_df, implied_df], ignore_index=True)
            elif not realized_df.empty:
                combined_df = realized_df
            elif not implied_df.empty:
                combined_df = implied_df
            else:
                print("WARNING: No data to save")
                return None
            
            print(f"INFO: Saving {len(combined_df)} observations...")
            
            # Save main time series files
            base_filename = f'historical_volatility_timeseries_{start_date}_{end_date}_{timestamp}'
            
            # CSV format
            csv_file = os.path.join(self.data_dir, f'{base_filename}.csv')
            combined_df.to_csv(csv_file, index=False)
            print(f"SUCCESS: CSV saved to: {csv_file}")
            
            # Parquet format
            parquet_file = os.path.join(self.data_dir, f'{base_filename}.parquet')
            combined_df.to_parquet(parquet_file, index=False)
            print(f"SUCCESS: Parquet saved to: {parquet_file}")
            
            # Also save latest versions (for easy access)
            latest_csv = os.path.join(self.data_dir, 'historical_volatility_latest.csv')
            latest_parquet = os.path.join(self.data_dir, 'historical_volatility_latest.parquet')
            
            combined_df.to_csv(latest_csv, index=False)
            combined_df.to_parquet(latest_parquet, index=False)
            
            print(f"SUCCESS: Latest files updated")
            
            # Create summary
            summary = {
                'collection_timestamp': timestamp,
                'date_range': f"{start_date} to {end_date}",
                'total_observations': len(combined_df),
                'securities_count': combined_df['ticker'].nunique(),
                'data_types': combined_df['data_type'].unique().tolist(),
                'date_coverage': {
                    'start_date': combined_df['date'].min().isoformat(),
                    'end_date': combined_df['date'].max().isoformat(),
                    'total_days': len(combined_df['date'].unique())
                },
                'data_quality': {
                    'realized_vol_observations': len(combined_df[combined_df['data_type'] == 'realized']),
                    'implied_vol_observations': len(combined_df[combined_df['data_type'] == 'implied']),
                    'missing_data_pct': (combined_df.isnull().sum().sum() / (len(combined_df) * len(combined_df.columns))) * 100
                }
            }
            
            summary_file = os.path.join(self.data_dir, f'collection_summary_{timestamp}.json')
            with open(summary_file, 'w') as f:
                json.dump(summary, f, indent=2, default=str)
            
            print(f"SUCCESS: Summary saved to: {summary_file}")
            
            return csv_file, parquet_file, summary
            
        except Exception as e:
            print(f"ERROR: Failed to save volatility data: {e}")
            return None
    
    def disconnect(self):
        """Disconnect from Bloomberg"""
        if self.session:
            self.session.stop()
            print("SUCCESS: Bloomberg session disconnected")

def main():
    """Main execution function for historical volatility collection"""
    print("="*80)
    print("HISTORICAL VOLATILITY DATA COLLECTION")
    print("="*80)
    print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("Collecting 3 years of historical volatility data for SPX + Top 50 components...")
    
    fetcher = HistoricalVolatilityFetcher()
    
    try:
        # Connect to Bloomberg
        if not fetcher.connect():
            return False
        
        # Load SPX components
        print("\n1. Loading SPX components...")
        tickers = fetcher.load_spx_components()
        if not tickers:
            return False
        
        # Determine collection date range
        print("\n2. Determining collection date range...")
        start_date, end_date = fetcher.get_collection_date_range(incremental=True)
        if not start_date or not end_date:
            return False
        
        # Collect realized volatility data
        print("\n3. Collecting historical realized volatility...")
        realized_df = fetcher.fetch_historical_volatility(
            tickers, fetcher.realized_fields, start_date, end_date, 'realized'
        )
        
        # Collect implied volatility data  
        print("\n4. Collecting historical implied volatility...")
        implied_df = fetcher.fetch_historical_volatility(
            tickers, fetcher.implied_fields, start_date, end_date, 'implied'
        )
        
        # Save all data
        print("\n5. Saving historical volatility data...")
        result = fetcher.save_volatility_data(realized_df, implied_df, start_date, end_date)
        
        if result:
            csv_file, parquet_file, summary = result
            
            # Update collection log
            log_data = fetcher.load_collection_log()
            log_data['last_collection_date'] = end_date
            log_data['securities_collected'] = tickers
            log_data['total_observations'] = summary['total_observations']
            fetcher.save_collection_log(log_data)
            
            print(f"\nSUCCESS: Historical volatility collection completed!")
            print(f"   Total observations: {summary['total_observations']:,}")
            print(f"   Securities: {summary['securities_count']}")
            print(f"   Date range: {summary['date_coverage']['start_date']} to {summary['date_coverage']['end_date']}")
            print(f"   Files saved in: {fetcher.data_dir}")
            print(f"\n   Ready for volatility analysis!")
            
            return True
        else:
            print("ERROR: Failed to save data")
            return False
            
    except Exception as e:
        print(f"ERROR: Error in main execution: {e}")
        return False
    
    finally:
        fetcher.disconnect()

if __name__ == "__main__":
    main()