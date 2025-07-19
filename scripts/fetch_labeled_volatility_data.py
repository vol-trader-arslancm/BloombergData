"""
WINDOWS-COMPATIBLE Volatility Data Fetcher
Collect implied and realized volatility data with proper labeling (no emojis)
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
    from config.bloomberg_config import SPX_TICKER, VOLATILITY_FIELDS, CLEAN_COLUMN_NAMES
except ImportError as e:
    print(f"IMPORT ERROR: {e}")
    # Fallback values if config import fails
    SPX_TICKER = 'SPX Index'
    VOLATILITY_FIELDS = {
        'realized_vol_30d': 'VOLATILITY_30D',
        'realized_vol_90d': 'VOLATILITY_90D',
        'implied_vol_3m_atm': '3MTH_IMPVOL_100.0%MNY_DF'
    }

class VolatilityDataFetcher:
    """Fetch volatility data with proper implied/realized labeling"""
    
    def __init__(self):
        self.session = None
        self.refDataService = None
    
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
            print("SUCCESS: Connected to Bloomberg for volatility data")
            return True
            
        except Exception as e:
            print(f"ERROR: Bloomberg connection failed: {e}")
            return False
    
    def get_current_volatility_with_labels(self, tickers, vol_field_mapping):
        """Get current volatility data with proper column labels"""
        try:
            if isinstance(tickers, str):
                tickers = [tickers]
            
            # Get Bloomberg field names from mapping
            bloomberg_fields = list(vol_field_mapping.values())
            
            print(f"INFO: Fetching volatility data for {len(tickers)} securities...")
            print(f"   Realized vol fields: {[f for f in bloomberg_fields if 'VOLATILITY' in f]}")
            print(f"   Implied vol fields: {[f for f in bloomberg_fields if 'IMPVOL' in f]}")
            
            vol_data = []
            
            # Process in batches
            batch_size = 15
            for i in range(0, len(tickers), batch_size):
                batch = tickers[i:i+batch_size]
                print(f"   Processing batch {i//batch_size + 1}: {len(batch)} securities")
                
                request = self.refDataService.createRequest("ReferenceDataRequest")
                
                for ticker in batch:
                    request.getElement("securities").appendValue(ticker)
                
                for field in bloomberg_fields:
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
                                    print(f"WARNING: Error for {ticker}")
                                    continue
                                
                                fieldData = security.getElement("fieldData")
                                vol_record = {
                                    'ticker': ticker,
                                    'collection_timestamp': datetime.now().isoformat()
                                }
                                
                                # Map Bloomberg fields to clean labels
                                for clean_name, bloomberg_field in vol_field_mapping.items():
                                    if fieldData.hasElement(bloomberg_field):
                                        vol_record[clean_name] = fieldData.getElement(bloomberg_field).getValue()
                                    else:
                                        vol_record[clean_name] = None
                                
                                vol_data.append(vol_record)
                        break
                    
                    if event.eventType() == blpapi.Event.TIMEOUT:
                        print("WARNING: Timeout in volatility batch processing")
                        break
            
            df = pd.DataFrame(vol_data)
            print(f"SUCCESS: Retrieved volatility data for {len(df)} securities")
            
            return df
            
        except Exception as e:
            print(f"ERROR: Failed to get volatility data: {e}")
            return None
    
    def get_spx_volatility_surface_labeled(self):
        """Get SPX implied volatility surface with proper labels"""
        try:
            print("INFO: Fetching SPX volatility surface with proper labels...")
            
            # SPX implied volatility fields with clean labels
            surface_mapping = {
                'implied_vol_1m_atm': '1MTH_IMPVOL_100.0%MNY_DF',
                'implied_vol_2m_atm': '2MTH_IMPVOL_100.0%MNY_DF',
                'implied_vol_3m_atm': '3MTH_IMPVOL_100.0%MNY_DF',
                'implied_vol_6m_atm': '6MTH_IMPVOL_100.0%MNY_DF',
                'implied_vol_12m_atm': '12MTH_IMPVOL_100.0%MNY_DF',
                'implied_vol_3m_90_moneyness': '3MTH_IMPVOL_90.0%MNY_DF',
                'implied_vol_3m_95_moneyness': '3MTH_IMPVOL_95.0%MNY_DF',
                'implied_vol_3m_100_moneyness': '3MTH_IMPVOL_100.0%MNY_DF',
                'implied_vol_3m_105_moneyness': '3MTH_IMPVOL_105.0%MNY_DF',
                'implied_vol_3m_110_moneyness': '3MTH_IMPVOL_110.0%MNY_DF'
            }
            
            bloomberg_fields = list(surface_mapping.values())
            
            request = self.refDataService.createRequest("ReferenceDataRequest")
            request.getElement("securities").appendValue(SPX_TICKER)
            
            for field in bloomberg_fields:
                request.getElement("fields").appendValue(field)
            
            self.session.sendRequest(request)
            
            # Process response
            surface_data = {}
            while True:
                event = self.session.nextEvent(2000)
                
                if event.eventType() == blpapi.Event.RESPONSE:
                    for msg in event:
                        securityData = msg.getElement("securityData")
                        security = securityData.getValue(0)
                        
                        if security.hasElement("securityError"):
                            print("ERROR: Error getting SPX volatility surface")
                            return None
                        
                        fieldData = security.getElement("fieldData")
                        for clean_name, bloomberg_field in surface_mapping.items():
                            if fieldData.hasElement(bloomberg_field):
                                surface_data[clean_name] = fieldData.getElement(bloomberg_field).getValue()
                            else:
                                surface_data[clean_name] = None
                    break
                
                if event.eventType() == blpapi.Event.TIMEOUT:
                    print("WARNING: Timeout getting SPX volatility surface")
                    break
            
            if surface_data:
                valid_fields = len([v for v in surface_data.values() if v is not None])
                print(f"SUCCESS: Retrieved SPX volatility surface: {valid_fields} fields with data")
                return surface_data
            else:
                return None
                
        except Exception as e:
            print(f"ERROR: Failed to get SPX volatility surface: {e}")
            return None
    
    def save_volatility_data_labeled(self, vol_df, spx_surface, tickers_processed):
        """Save volatility data with proper labels and metadata"""
        try:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            
            # Create output directory
            output_dir = os.path.join(project_root, 'data', 'processed', 'volatility')
            os.makedirs(output_dir, exist_ok=True)
            
            # Save main volatility DataFrame
            vol_file = os.path.join(output_dir, f'labeled_volatility_data_{timestamp}.csv')
            vol_df.to_csv(vol_file, index=False)
            print(f"SUCCESS: Labeled volatility data saved to: {vol_file}")
            
            # Save SPX volatility surface
            if spx_surface:
                surface_file = os.path.join(output_dir, f'spx_labeled_vol_surface_{timestamp}.json')
                with open(surface_file, 'w') as f:
                    # Clean data for JSON serialization
                    surface_clean = {}
                    for k, v in spx_surface.items():
                        surface_clean[k] = float(v) if v is not None else None
                    
                    json.dump({
                        'collection_timestamp': timestamp,
                        'spx_ticker': SPX_TICKER,
                        'volatility_surface_labeled': surface_clean,
                        'field_descriptions': {
                            'implied_vol_1m_atm': '1-month at-the-money implied volatility',
                            'implied_vol_3m_atm': '3-month at-the-money implied volatility',
                            'implied_vol_3m_90_moneyness': '3-month 90% moneyness implied volatility (put side)',
                            'implied_vol_3m_110_moneyness': '3-month 110% moneyness implied volatility (call side)'
                        }
                    }, f, indent=2)
                print(f"SUCCESS: SPX labeled volatility surface saved to: {surface_file}")
            
            # Create comprehensive summary with field descriptions
            summary = {
                'collection_timestamp': timestamp,
                'total_securities': int(len(vol_df)),
                'securities_processed': tickers_processed,
                'field_descriptions': {
                    'realized_volatility': {
                        'realized_vol_30d': '30-day realized (historical) volatility',
                        'realized_vol_90d': '90-day realized (historical) volatility',
                        'realized_vol_120d': '120-day realized (historical) volatility',
                        'realized_vol_260d': '260-day realized (historical) volatility'
                    },
                    'implied_volatility': {
                        'implied_vol_1m_atm': '1-month at-the-money implied volatility',
                        'implied_vol_3m_atm': '3-month at-the-money implied volatility',
                        'implied_vol_6m_atm': '6-month at-the-money implied volatility'
                    }
                },
                'data_quality': {
                    'realized_vol_30d_coverage': int(vol_df['realized_vol_30d'].notna().sum()) if 'realized_vol_30d' in vol_df.columns else 0,
                    'realized_vol_90d_coverage': int(vol_df['realized_vol_90d'].notna().sum()) if 'realized_vol_90d' in vol_df.columns else 0,
                    'implied_vol_3m_atm_coverage': int(vol_df['implied_vol_3m_atm'].notna().sum()) if 'implied_vol_3m_atm' in vol_df.columns else 0
                },
                'spx_surface_available': spx_surface is not None
            }
            
            summary_file = os.path.join(output_dir, f'labeled_volatility_summary_{timestamp}.json')
            with open(summary_file, 'w') as f:
                json.dump(summary, f, indent=2)
            print(f"SUCCESS: Comprehensive summary saved to: {summary_file}")
            
            return vol_file, surface_file if spx_surface else None, summary_file
            
        except Exception as e:
            print(f"ERROR: Failed to save labeled volatility data: {e}")
            return None, None, None
    
    def disconnect(self):
        """Disconnect from Bloomberg"""
        if self.session:
            self.session.stop()
            print("SUCCESS: Bloomberg session disconnected")

def main():
    """Main execution function with proper volatility labeling"""
    print("="*70)
    print("LABELED VOLATILITY DATA COLLECTION")
    print("="*70)
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
    
    # Volatility fields with proper labels
    vol_field_mapping = {
        'realized_vol_30d': 'VOLATILITY_30D',
        'realized_vol_90d': 'VOLATILITY_90D',
        'realized_vol_120d': 'VOLATILITY_120D',
        'realized_vol_260d': 'VOLATILITY_260D',
        'implied_vol_1m_atm': '1MTH_IMPVOL_100.0%MNY_DF',
        'implied_vol_3m_atm': '3MTH_IMPVOL_100.0%MNY_DF',
        'implied_vol_6m_atm': '6MTH_IMPVOL_100.0%MNY_DF'
    }
    
    # Create fetcher instance
    fetcher = VolatilityDataFetcher()
    
    try:
        # Connect to Bloomberg
        if not fetcher.connect():
            return False
        
        print(f"\nProcessing {len(securities)} securities for LABELED volatility data...")
        print(f"Realized vol fields: {[k for k in vol_field_mapping.keys() if 'realized' in k]}")
        print(f"Implied vol fields: {[k for k in vol_field_mapping.keys() if 'implied' in k]}")
        
        # Get current volatility data with labels
        print("\n1. Fetching LABELED volatility data...")
        vol_df = fetcher.get_current_volatility_with_labels(securities, vol_field_mapping)
        
        if vol_df is not None and not vol_df.empty:
            print(f"\nSUCCESS: Labeled volatility data summary:")
            print(f"   Securities processed: {len(vol_df)}")
            print(f"   Total data points: {vol_df.select_dtypes(include='number').notna().sum().sum()}")
            
            # Show sample data with proper labels
            print("\nINFO: Sample LABELED volatility data:")
            sample_cols = ['ticker', 'realized_vol_30d', 'realized_vol_90d', 'implied_vol_3m_atm']
            available_cols = [col for col in sample_cols if col in vol_df.columns]
            print(vol_df[available_cols].head(10).to_string(index=False))
        
        # Get SPX volatility surface with labels
        print("\n2. Fetching LABELED SPX volatility surface...")
        spx_surface = fetcher.get_spx_volatility_surface_labeled()
        
        if spx_surface:
            print("SUCCESS: SPX labeled volatility surface retrieved:")
            for field, value in spx_surface.items():
                if value is not None:
                    print(f"   {field}: {value:.2f}%")
        
        # Save all labeled data
        if vol_df is not None and not vol_df.empty:
            print("\n3. Saving LABELED data...")
            vol_file, surface_file, summary_file = fetcher.save_volatility_data_labeled(
                vol_df, spx_surface, securities
            )
            
            if vol_file:
                print(f"\nSUCCESS: LABELED volatility data collection completed successfully!")
                print(f"Files saved in: data/processed/volatility/")
                print(f"\nKEY IMPROVEMENTS:")
                print(f"   SUCCESS: Proper realized vs implied volatility labeling")
                print(f"   SUCCESS: Clear time period indicators (30d, 90d, etc.)")
                print(f"   SUCCESS: Moneyness labels for implied vol")
                print(f"   SUCCESS: Comprehensive field descriptions")
                return True
        else:
            print("ERROR: No volatility data to save")
            return False
            
    except Exception as e:
        print(f"ERROR: Error in main execution: {e}")
        return False
    
    finally:
        fetcher.disconnect()

if __name__ == "__main__":
    main()