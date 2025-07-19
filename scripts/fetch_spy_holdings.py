"""
SPY Holdings Data Fetcher
Collect SPY ETF holdings and weights for index replication
"""

import sys
import os
import pandas as pd
from datetime import datetime
import json

# Add project root to path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

try:
    import blpapi
    from config.bloomberg_config import SPY_TICKER, MARKET_DATA_FIELDS
except ImportError as e:
    print(f"❌ Import error: {e}")
    sys.exit(1)

class SPYHoldingsFetcher:
    """Fetch and process SPY ETF holdings data"""
    
    def __init__(self):
        self.session = None
        self.refDataService = None
        self.holdings_data = None
    
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
            print("✅ Connected to Bloomberg for SPY holdings data")
            return True
            
        except Exception as e:
            print(f"❌ Bloomberg connection failed: {e}")
            return False
    
    def get_spy_basic_data(self):
        """Get basic SPY ETF information"""
        try:
            request = self.refDataService.createRequest("ReferenceDataRequest")
            request.getElement("securities").appendValue(SPY_TICKER)
            
            # Basic ETF fields
            fields = ['PX_LAST', 'CUR_MKT_CAP', 'FUND_NET_ASSET_VAL', 'FUND_TOTAL_ASSETS']
            for field in fields:
                request.getElement("fields").appendValue(field)
            
            self.session.sendRequest(request)
            
            # Process response
            data = {}
            while True:
                event = self.session.nextEvent(1000)
                
                if event.eventType() == blpapi.Event.RESPONSE:
                    for msg in event:
                        securityData = msg.getElement("securityData")
                        security = securityData.getValue(0)
                        
                        if security.hasElement("securityError"):
                            print("❌ Error getting SPY basic data")
                            return None
                        
                        fieldData = security.getElement("fieldData")
                        for field in fields:
                            if fieldData.hasElement(field):
                                data[field] = fieldData.getElement(field).getValue()
                    break
                
                if event.eventType() == blpapi.Event.TIMEOUT:
                    print("⚠️ Request timeout for SPY basic data")
                    break
            
            return data
            
        except Exception as e:
            print(f"❌ Failed to get SPY basic data: {e}")
            return None
    
    def get_top_holdings_manual(self):
        """Get top SPX components manually (since direct holdings might not be available)"""
        # Top 50 SPX components by market cap (approximate)
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
        
        try:
            print(f"📊 Fetching market cap data for top 50 holdings...")
            
            # Get market cap data for weight calculation
            holdings_data = []
            
            # Process in batches to avoid overwhelming Bloomberg
            batch_size = 10
            for i in range(0, len(top_50_tickers), batch_size):
                batch = top_50_tickers[i:i+batch_size]
                print(f"   Processing batch {i//batch_size + 1}: {len(batch)} securities")
                
                request = self.refDataService.createRequest("ReferenceDataRequest")
                
                for ticker in batch:
                    request.getElement("securities").appendValue(ticker)
                
                # Fields for weight calculation
                fields = ['PX_LAST', 'CUR_MKT_CAP', 'EQY_SH_OUT', 'NAME']
                for field in fields:
                    request.getElement("fields").appendValue(field)
                
                self.session.sendRequest(request)
                
                # Process response
                while True:
                    event = self.session.nextEvent(2000)  # 2 second timeout
                    
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
                                holding_data = {'ticker': ticker}
                                
                                for field in fields:
                                    if fieldData.hasElement(field):
                                        holding_data[field] = fieldData.getElement(field).getValue()
                                    else:
                                        holding_data[field] = None
                                
                                holdings_data.append(holding_data)
                        break
                    
                    if event.eventType() == blpapi.Event.TIMEOUT:
                        print("⚠️ Timeout in batch processing")
                        break
            
            # Convert to DataFrame and calculate weights
            df = pd.DataFrame(holdings_data)
            
            # Filter out securities with no market cap data
            df = df[df['CUR_MKT_CAP'].notna()]
            
            # Calculate weights (market cap / total market cap)
            total_market_cap = df['CUR_MKT_CAP'].sum()
            df['weight_pct'] = (df['CUR_MKT_CAP'] / total_market_cap) * 100
            
            # Sort by weight
            df = df.sort_values('weight_pct', ascending=False)
            
            print(f"✅ Retrieved data for {len(df)} holdings")
            print(f"   Total market cap: ${total_market_cap/1e12:.2f}T")
            
            return df
            
        except Exception as e:
            print(f"❌ Failed to get holdings data: {e}")
            return None
    
    def save_holdings_data(self, holdings_df, spy_basic_data):
        """Save holdings data to files"""
        try:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            
            # Create output directory
            output_dir = os.path.join(project_root, 'data', 'raw', 'spy_holdings')
            os.makedirs(output_dir, exist_ok=True)
            
            # Save holdings DataFrame
            holdings_file = os.path.join(output_dir, f'spy_holdings_{timestamp}.csv')
            holdings_df.to_csv(holdings_file, index=False)
            print(f"✅ Holdings data saved to: {holdings_file}")
            
            # Save basic SPY data
            basic_file = os.path.join(output_dir, f'spy_basic_data_{timestamp}.json')
            with open(basic_file, 'w') as f:
                # Convert numpy types to Python types for JSON serialization
                basic_data_clean = {}
                for k, v in spy_basic_data.items():
                    if v is not None:
                        basic_data_clean[k] = float(v) if isinstance(v, (int, float)) else str(v)
                    else:
                        basic_data_clean[k] = None
                
                json.dump({
                    'timestamp': timestamp,
                    'spy_data': basic_data_clean
                }, f, indent=2)
            print(f"✅ SPY basic data saved to: {basic_file}")
            
            # Save summary
            summary = {
                'timestamp': timestamp,
                'total_holdings': len(holdings_df),
                'total_market_cap': float(holdings_df['CUR_MKT_CAP'].sum()),
                'top_10_holdings': holdings_df.head(10)[['ticker', 'NAME', 'weight_pct']].to_dict('records')
            }
            
            summary_file = os.path.join(output_dir, f'holdings_summary_{timestamp}.json')
            with open(summary_file, 'w') as f:
                json.dump(summary, f, indent=2)
            print(f"✅ Summary saved to: {summary_file}")
            
            return holdings_file, basic_file, summary_file
            
        except Exception as e:
            print(f"❌ Failed to save data: {e}")
            return None, None, None
    
    def disconnect(self):
        """Disconnect from Bloomberg"""
        if self.session:
            self.session.stop()
            print("✅ Bloomberg session disconnected")

def main():
    """Main execution function"""
    print("="*60)
    print("SPY HOLDINGS DATA COLLECTION")
    print("="*60)
    print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Create fetcher instance
    fetcher = SPYHoldingsFetcher()
    
    try:
        # Connect to Bloomberg
        if not fetcher.connect():
            return False
        
        # Get SPY basic data
        print("\n1. Fetching SPY basic information...")
        spy_basic = fetcher.get_spy_basic_data()
        if spy_basic:
            print("✅ SPY basic data retrieved:")
            for key, value in spy_basic.items():
                if value is not None:
                    if 'CAP' in key or 'ASSETS' in key:
                        print(f"   {key}: ${value/1e9:.2f}B")
                    else:
                        print(f"   {key}: {value}")
        
        # Get holdings data
        print("\n2. Fetching top holdings data...")
        holdings_df = fetcher.get_top_holdings_manual()
        
        if holdings_df is not None and not holdings_df.empty:
            print(f"\n✅ Holdings data summary:")
            print(f"   Total holdings: {len(holdings_df)}")
            print(f"   Top 10 holdings by weight:")
            print(holdings_df.head(10)[['ticker', 'NAME', 'weight_pct', 'CUR_MKT_CAP']].to_string(index=False))
            
            # Save data
            print("\n3. Saving data...")
            holdings_file, basic_file, summary_file = fetcher.save_holdings_data(holdings_df, spy_basic)
            
            if holdings_file:
                print(f"\n✅ Data collection completed successfully!")
                print(f"Files saved in: data/raw/spy_holdings/")
                return True
        else:
            print("❌ Failed to retrieve holdings data")
            return False
            
    except Exception as e:
        print(f"❌ Error in main execution: {e}")
        return False
    
    finally:
        fetcher.disconnect()

if __name__ == "__main__":
    main()