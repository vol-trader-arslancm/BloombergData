"""
Volatility Data Fetcher
Collect implied and realized volatility data for SPX and components
"""

import sys
import os
import pandas as pd
from datetime import datetime, timedelta
import json

# Add project root to path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

try:
    import blpapi
    from config.bloomberg_config import SPX_TICKER, VOLATILITY_FIELDS
except ImportError as e:
    print(f"❌ Import error: {e}")
    sys.exit(1)

class VolatilityDataFetcher:
    """Fetch volatility data for SPX and top components"""
    
    def __init__(self):
        self.session = None
        self.refDataService = None
    
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
            print("✅ Connected to Bloomberg for volatility data")
            return True
            
        except Exception as e:
            print(f"❌ Bloomberg connection failed: {e}")
            return False
    
    def get_current_volatility(self, tickers, vol_fields):
        """Get current volatility data for given tickers"""
        try:
            if isinstance(tickers, str):
                tickers = [tickers]
            
            print(f"📊 Fetching volatility data for {len(tickers)} securities...")
            
            vol_data = []
            
            # Process in batches
            batch_size = 15
            for i in range(0, len(tickers), batch_size):
                batch = tickers[i:i+batch_size]
                print(f"   Processing batch {i//batch_size + 1}: {len(batch)} securities")
                
                request = self.refDataService.createRequest("ReferenceDataRequest")
                
                for ticker in batch:
                    request.getElement("securities").appendValue(ticker)
                
                for field in vol_fields:
                    request.getElement("fields").appendValue(field)
                
                self.session.sendRequest(request)
                
                # Process response
                while True:
                    event = self.session.nextEvent(3000)  # 3 second timeout
                    
                    if event.eventType() == blpapi.Event.RESPONSE:
                        for msg in event:
                            securityData = msg.getElement("securityData")
                            for j in range(securityData.numValues()):
                                security = securityData.getValue(j)
                                ticker = security.getElement("security").getValue()
                                
                                if security.hasElement("securityError"):
                                    print(f"⚠️ Error for {ticker}")
                                    continue
                                
                                fieldData = security.getElement("fieldData")
                                vol_record = {
                                    'ticker': ticker,
                                    'timestamp': datetime.now().isoformat()
                                }
                                
                                for field in vol_fields:
                                    if fieldData.hasElement(field):
                                        vol_record[field] = fieldData.getElement(field).getValue()
                                    else:
                                        vol_record[field] = None
                                
                                vol_data.append(vol_record)
                        break
                    
                    if event.eventType() == blpapi.Event.TIMEOUT:
                        print("⚠️ Timeout in volatility batch processing")
                        break
            
            df = pd.DataFrame(vol_data)
            print(f"✅ Retrieved volatility data for {len(df)} securities")
            
            return df
            
        except Exception as e:
            print(f"❌ Failed to get volatility data: {e}")
            return None
    
    def get_spx_volatility_surface(self):
        """Get SPX implied volatility surface data"""
        try:
            print("📊 Fetching SPX volatility surface...")
            
            # SPX implied volatility fields for different strikes and tenors
            iv_fields = [
                # At-the-money across tenors
                '1MTH_IMPVOL_100.0%MNY_DF',
                '2MTH_IMPVOL_100.0%MNY_DF',
                '3MTH_IMPVOL_100.0%MNY_DF',
                '6MTH_IMPVOL_100.0%MNY_DF',
                '12MTH_IMPVOL_100.0%MNY_DF',
                
                # 3-month across strikes
                '3MTH_IMPVOL_90.0%MNY_DF',
                '3MTH_IMPVOL_95.0%MNY_DF',
                '3MTH_IMPVOL_100.0%MNY_DF',
                '3MTH_IMPVOL_105.0%MNY_DF',
                '3MTH_IMPVOL_110.0%MNY_DF',
            ]
            
            request = self.refDataService.createRequest("ReferenceDataRequest")
            request.getElement("securities").appendValue(SPX_TICKER)
            
            for field in iv_fields:
                request.getElement("fields").appendValue(field)
            
            self.session.sendRequest(request)
            
            # Process response
            iv_data = {}
            while True:
                event = self.session.nextEvent(2000)
                
                if event.eventType() == blpapi.Event.RESPONSE:
                    for msg in event:
                        securityData = msg.getElement("securityData")
                        security = securityData.getValue(0)
                        
                        if security.hasElement("securityError"):
                            print("❌ Error getting SPX volatility surface")
                            return None
                        
                        fieldData = security.getElement("fieldData")
                        for field in iv_fields:
                            if fieldData.hasElement(field):
                                iv_data[field] = fieldData.getElement(field).getValue()
                            else:
                                iv_data[field] = None
                    break
                
                if event.eventType() == blpapi.Event.TIMEOUT:
                    print("⚠️ Timeout getting SPX volatility surface")
                    break
            
            if iv_data:
                print(f"✅ Retrieved SPX volatility surface: {len([v for v in iv_data.values() if v is not None])} fields with data")
                return iv_data
            else:
                return None
                
        except Exception as e:
            print(f"❌ Failed to get SPX volatility surface: {e}")
            return None
    
    def save_volatility_data(self, vol_df, spx_surface, tickers_processed):
        """Save volatility data to files"""
        try:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            
            # Create output directory
            output_dir = os.path.join(project_root, 'data', 'raw', 'volatility')
            os.makedirs(output_dir, exist_ok=True)
            
            # Save main volatility DataFrame
            vol_file = os.path.join(output_dir, f'volatility_data_{timestamp}.csv')
            vol_df.to_csv(vol_file, index=False)
            print(f"✅ Volatility data saved to: {vol_file}")
            
            # Save SPX volatility surface
            if spx_surface:
                surface_file = os.path.join(output_dir, f'spx_vol_surface_{timestamp}.json')
                with open(surface_file, 'w') as f:
                    # Clean data for JSON serialization
                    surface_clean = {}
                    for k, v in spx_surface.items():
                        surface_clean[k] = float(v) if v is not None else None
                    
                    json.dump({
                        'timestamp': timestamp,
                        'spx_ticker': SPX_TICKER,
                        'volatility_surface': surface_clean
                    }, f, indent=2)
                print(f"✅ SPX volatility surface saved to: {surface_file}")
            
            # Create summary
            summary = {
                'timestamp': timestamp,
                'total_securities': int(len(vol_df)),
                'securities_processed': tickers_processed,
                'fields_collected': list(VOLATILITY_FIELDS.values()),
                'spx_surface_available': spx_surface is not None,
                'data_quality': {
                    'vol_30d_available': int(vol_df['VOLATILITY_30D'].notna().sum()),
                    'vol_90d_available': int(vol_df['VOLATILITY_90D'].notna().sum()),
                    'implied_vol_available': int(len([c for c in vol_df.columns if 'IMPVOL' in c]))
                }
            }
            
            summary_file = os.path.join(output_dir, f'volatility_summary_{timestamp}.json')
            with open(summary_file, 'w') as f:
                json.dump(summary, f, indent=2)
            print(f"✅ Summary saved to: {summary_file}")
            
            return vol_file, surface_file if spx_surface else None, summary_file
            
        except Exception as e:
            print(f"❌ Failed to save volatility data: {e}")
            return None, None, None
    
    def disconnect(self):
        """Disconnect from Bloomberg"""
        if self.session:
            self.session.stop()
            print("✅ Bloomberg session disconnected")

def main():
    """Main execution function"""
    print("="*60)
    print("VOLATILITY DATA COLLECTION")
    print("="*60)
    print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Define securities to process
    securities = [
        SPX_TICKER,  # SPX Index first
        'SPY US Equity',  # SPY ETF
        # Top 20 SPX components
        'AAPL US Equity', 'MSFT US Equity', 'NVDA US Equity', 'AMZN US Equity',
        'META US Equity', 'GOOGL US Equity', 'GOOG US Equity', 'LLY US Equity',
        'AVGO US Equity', 'JPM US Equity', 'TSLA US Equity', 'WMT US Equity',
        'V US Equity', 'UNH US Equity', 'XOM US Equity', 'MA US Equity',
        'PG US Equity', 'JNJ US Equity', 'COST US Equity', 'HD US Equity'
    ]
    
    # Volatility fields to collect
    vol_fields = [
        'VOLATILITY_30D', 'VOLATILITY_90D', 'VOLATILITY_120D', 'VOLATILITY_260D',
        '1MTH_IMPVOL_100.0%MNY_DF', '3MTH_IMPVOL_100.0%MNY_DF', '6MTH_IMPVOL_100.0%MNY_DF'
    ]
    
    # Create fetcher instance
    fetcher = VolatilityDataFetcher()
    
    try:
        # Connect to Bloomberg
        if not fetcher.connect():
            return False
        
        print(f"\nProcessing {len(securities)} securities for volatility data...")
        print(f"Fields to collect: {vol_fields}")
        
        # Get current volatility data
        print("\n1. Fetching current volatility data...")
        vol_df = fetcher.get_current_volatility(securities, vol_fields)
        
        if vol_df is not None and not vol_df.empty:
            print(f"\n✅ Volatility data summary:")
            print(f"   Securities processed: {len(vol_df)}")
            print(f"   Data points collected: {vol_df.notna().sum().sum()}")
            
            # Show sample data
            print("\n📊 Sample volatility data:")
            sample_cols = ['ticker', 'VOLATILITY_30D', 'VOLATILITY_90D', '3MTH_IMPVOL_100.0%MNY_DF']
            available_cols = [col for col in sample_cols if col in vol_df.columns]
            print(vol_df[available_cols].head(10).to_string(index=False))
        
        # Get SPX volatility surface
        print("\n2. Fetching SPX volatility surface...")
        spx_surface = fetcher.get_spx_volatility_surface()
        
        if spx_surface:
            print("✅ SPX volatility surface retrieved:")
            for field, value in spx_surface.items():
                if value is not None:
                    print(f"   {field}: {value:.2f}%")
        
        # Save all data
        if vol_df is not None and not vol_df.empty:
            print("\n3. Saving data...")
            vol_file, surface_file, summary_file = fetcher.save_volatility_data(
                vol_df, spx_surface, securities
            )
            
            if vol_file:
                print(f"\n✅ Volatility data collection completed successfully!")
                print(f"Files saved in: data/raw/volatility/")
                return True
        else:
            print("❌ No volatility data to save")
            return False
            
    except Exception as e:
        print(f"❌ Error in main execution: {e}")
        return False
    
    finally:
        fetcher.disconnect()

if __name__ == "__main__":
    main()