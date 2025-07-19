"""
PROPER SPY ETF Holdings Fetcher
Get actual SPY ETF holdings and weights using Bloomberg bulk data requests
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
    from config.bloomberg_config import SPY_TICKER
except ImportError as e:
    print(f"IMPORT ERROR: {e}")
    SPY_TICKER = 'SPY US Equity'

class SPYHoldingsFetcher:
    """Fetch actual SPY ETF holdings and weights"""
    
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
            
            print("SUCCESS: Connected to Bloomberg for SPY holdings data")
            return True
            
        except Exception as e:
            print(f"ERROR: Bloomberg connection failed: {e}")
            return False
    
    def get_spy_holdings_bulk(self):
        """Get SPY holdings using bulk data fields"""
        try:
            print("INFO: Fetching SPY holdings using bulk data fields...")
            
            request = self.refDataService.createRequest("ReferenceDataRequest")
            request.getElement("securities").appendValue(SPY_TICKER)
            
            # Try multiple bulk data fields for ETF holdings
            bulk_fields = [
                'FUND_HOLDINGS',
                'FUND_HOLDING_DETAILS', 
                'PORTFOLIO_HOLDINGS',
                'FUND_HOLDINGS_WEIGHTS',
                'FUND_HOLDINGS_SHARES',
                'FUND_TOP_10_HOLDINGS'
            ]
            
            for field in bulk_fields:
                request.getElement("fields").appendValue(field)
            
            # Add override to get latest data
            overrides = request.getElement("overrides")
            override = overrides.appendElement()
            override.setElement("fieldId", "FUND_HOLDINGS_AS_OF_DT")
            override.setElement("value", datetime.now().strftime('%Y%m%d'))
            
            self.session.sendRequest(request)
            
            holdings_data = {}
            while True:
                event = self.session.nextEvent(10000)  # 10 second timeout for bulk data
                
                if event.eventType() == blpapi.Event.RESPONSE:
                    for msg in event:
                        securityData = msg.getElement("securityData")
                        security = securityData.getValue(0)
                        
                        if security.hasElement("securityError"):
                            error = security.getElement("securityError")
                            print(f"WARNING: {error.getElement('message').getValue()}")
                            continue
                        
                        fieldData = security.getElement("fieldData")
                        
                        for field in bulk_fields:
                            if fieldData.hasElement(field):
                                element = fieldData.getElement(field)
                                
                                if element.isArray():
                                    # Handle array data (multiple holdings)
                                    holdings_list = []
                                    for i in range(element.numValues()):
                                        holding = element.getValue(i)
                                        holding_dict = self._parse_holding_element(holding)
                                        if holding_dict:
                                            holdings_list.append(holding_dict)
                                    holdings_data[field] = holdings_list
                                else:
                                    # Handle single value
                                    holdings_data[field] = element.getValue()
                                
                                print(f"   Found data for field: {field}")
                    break
                
                if event.eventType() == blpapi.Event.TIMEOUT:
                    print("WARNING: Timeout getting SPY holdings bulk data")
                    break
            
            return holdings_data if holdings_data else None
            
        except Exception as e:
            print(f"ERROR: Failed to get SPY holdings bulk data: {e}")
            return None
    
    def _parse_holding_element(self, holding):
        """Parse individual holding element from Bloomberg response"""
        try:
            holding_dict = {}
            
            # Common field names for holdings
            field_mappings = {
                'Security Name': 'name',
                'Security': 'ticker', 
                'Ticker': 'ticker',
                'Weight': 'weight',
                'Percent of Fund': 'weight_pct',
                'Market Value': 'market_value',
                'Shares': 'shares',
                'Position': 'position'
            }
            
            # Try to extract all available fields
            if hasattr(holding, 'numElements'):
                for i in range(holding.numElements()):
                    element = holding.getElement(i)
                    field_name = element.name()
                    
                    try:
                        value = element.getValue()
                        mapped_name = field_mappings.get(field_name, field_name.lower().replace(' ', '_'))
                        holding_dict[mapped_name] = value
                    except:
                        continue
            
            return holding_dict if holding_dict else None
            
        except Exception as e:
            print(f"WARNING: Could not parse holding element: {e}")
            return None
    
    def get_spy_top_holdings(self):
        """Get SPY top holdings using a simpler approach"""
        try:
            print("INFO: Fetching SPY top holdings...")
            
            request = self.refDataService.createRequest("ReferenceDataRequest")
            request.getElement("securities").appendValue(SPY_TICKER)
            
            # Try comprehensive fields for ETF data
            fields = [
                'FUND_TOP_10_HOLDINGS',
                'FUND_TOP_5_HOLDINGS', 
                'FUND_HOLDINGS_NAME',
                'FUND_TOTAL_ASSETS',
                'FUND_NET_ASSET_VAL',
                'FUND_HOLDINGS_COUNT',
                'FUND_PORTFOLIO_TURNOVER',
                'FUND_EXPENSE_RATIO'
            ]
            
            for field in fields:
                request.getElement("fields").appendValue(field)
            
            self.session.sendRequest(request)
            
            holdings_data = {}
            while True:
                event = self.session.nextEvent(5000)
                
                if event.eventType() == blpapi.Event.RESPONSE:
                    for msg in event:
                        securityData = msg.getElement("securityData")
                        security = securityData.getValue(0)
                        
                        if security.hasElement("securityError"):
                            print("WARNING: Error getting top holdings")
                            continue
                        
                        fieldData = security.getElement("fieldData")
                        
                        for field in fields:
                            if fieldData.hasElement(field):
                                element = fieldData.getElement(field)
                                
                                # Handle different data types
                                if element.isArray():
                                    # Array data (like top holdings list)
                                    array_data = []
                                    for i in range(element.numValues()):
                                        item = element.getValue(i)
                                        if hasattr(item, 'toString'):
                                            array_data.append(str(item))
                                        else:
                                            array_data.append(item)
                                    holdings_data[field] = array_data
                                    print(f"   Retrieved: {field} ({len(array_data)} items)")
                                else:
                                    # Single value
                                    value = element.getValue()
                                    holdings_data[field] = value
                                    print(f"   Retrieved: {field} = {value}")
                    break
                
                if event.eventType() == blpapi.Event.TIMEOUT:
                    print("WARNING: Timeout getting top holdings")
                    break
            
            return holdings_data if holdings_data else None
            
        except Exception as e:
            print(f"ERROR: Failed to get top holdings: {e}")
            return None
    
    def create_holdings_from_spx_weights(self):
        """Fallback: Create approximate SPY holdings from S&P 500 market cap weights"""
        try:
            print("INFO: Creating approximate SPY holdings from S&P 500 weights...")
            
            # Get top 50 S&P 500 stocks (approximate SPY top holdings)
            top_stocks = [
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
            
            holdings_data = []
            
            # Get data for top stocks
            request = self.refDataService.createRequest("ReferenceDataRequest")
            
            for stock in top_stocks:
                request.getElement("securities").appendValue(stock)
            
            fields = ['PX_LAST', 'CUR_MKT_CAP', 'NAME']
            for field in fields:
                request.getElement("fields").appendValue(field)
            
            self.session.sendRequest(request)
            
            while True:
                event = self.session.nextEvent(5000)
                
                if event.eventType() == blpapi.Event.RESPONSE:
                    for msg in event:
                        securityData = msg.getElement("securityData")
                        for i in range(securityData.numValues()):
                            security = securityData.getValue(i)
                            ticker = security.getElement("security").getValue()
                            
                            if security.hasElement("securityError"):
                                continue
                            
                            fieldData = security.getElement("fieldData")
                            
                            holding = {
                                'ticker': ticker,
                                'name': fieldData.getElement('NAME').getValue() if fieldData.hasElement('NAME') else None,
                                'price': fieldData.getElement('PX_LAST').getValue() if fieldData.hasElement('PX_LAST') else None,
                                'market_cap': fieldData.getElement('CUR_MKT_CAP').getValue() if fieldData.hasElement('CUR_MKT_CAP') else None
                            }
                            
                            holdings_data.append(holding)
                    break
                
                if event.eventType() == blpapi.Event.TIMEOUT:
                    break
            
            # Calculate approximate weights
            df = pd.DataFrame(holdings_data)
            df = df[df['market_cap'].notna()]
            
            if len(df) > 0:
                total_cap = df['market_cap'].sum()
                df['weight_pct'] = (df['market_cap'] / total_cap) * 100
                df = df.sort_values('weight_pct', ascending=False)
                
                print(f"   Created approximate holdings for {len(df)} stocks")
                return df
            else:
                print("   WARNING: No market cap data available")
                return None
                
        except Exception as e:
            print(f"ERROR: Failed to create approximate holdings: {e}")
            return None
    
    def save_holdings_data(self, holdings_data, data_type):
        """Save holdings data"""
        try:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            
            output_dir = os.path.join(project_root, 'data', 'processed', 'spy_holdings')
            os.makedirs(output_dir, exist_ok=True)
            
            if isinstance(holdings_data, pd.DataFrame):
                # Save DataFrame
                filename = f'spy_holdings_{data_type}_{timestamp}.csv'
                filepath = os.path.join(output_dir, filename)
                holdings_data.to_csv(filepath, index=False)
                print(f"SUCCESS: SPY holdings saved to: {filepath}")
                
                # Create summary
                summary = {
                    'timestamp': timestamp,
                    'data_type': data_type,
                    'total_holdings': len(holdings_data),
                    'top_10_holdings': holdings_data.head(10).to_dict('records') if len(holdings_data) > 0 else []
                }
                
            else:
                # Save raw data
                filename = f'spy_holdings_raw_{data_type}_{timestamp}.json'
                filepath = os.path.join(output_dir, filename)
                with open(filepath, 'w') as f:
                    json.dump(holdings_data, f, indent=2, default=str)
                print(f"SUCCESS: Raw SPY data saved to: {filepath}")
                
                summary = {
                    'timestamp': timestamp,
                    'data_type': data_type,
                    'raw_data_fields': list(holdings_data.keys()) if holdings_data else []
                }
            
            # Save summary
            summary_file = os.path.join(output_dir, f'spy_holdings_summary_{data_type}_{timestamp}.json')
            with open(summary_file, 'w') as f:
                json.dump(summary, f, indent=2, default=str)
            
            return filepath, summary_file
            
        except Exception as e:
            print(f"ERROR: Failed to save holdings data: {e}")
            return None, None
    
    def disconnect(self):
        """Disconnect from Bloomberg"""
        if self.session:
            self.session.stop()
            print("SUCCESS: Bloomberg session disconnected")

def main():
    """Main execution function"""
    print("="*70)
    print("SPY ETF HOLDINGS DATA COLLECTION")
    print("="*70)
    print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    fetcher = SPYHoldingsFetcher()
    
    try:
        if not fetcher.connect():
            return False
        
        success = False
        
        # Method 1: Try bulk holdings data
        print("\n1. Attempting bulk SPY holdings collection...")
        bulk_holdings = fetcher.get_spy_holdings_bulk()
        if bulk_holdings:
            print("SUCCESS: Bulk holdings data retrieved!")
            fetcher.save_holdings_data(bulk_holdings, "bulk")
            success = True
        
        # Method 2: Try top holdings
        if not success:
            print("\n2. Attempting top holdings collection...")
            top_holdings = fetcher.get_spy_top_holdings()
            if top_holdings:
                print("SUCCESS: Top holdings data retrieved!")
                print(f"   Data fields found: {list(top_holdings.keys())}")
                fetcher.save_holdings_data(top_holdings, "top")
                
                # Check if we got actual holdings list
                has_holdings_list = any('HOLDINGS' in field and isinstance(top_holdings[field], list) 
                                      for field in top_holdings.keys())
                if has_holdings_list:
                    success = True
                else:
                    print("   Note: Got fund info but no holdings list")

        # Method 3: Always try approximate weights as backup
        print("\n3. Getting approximate SPY holdings from S&P 500 weights...")
        approx_holdings = fetcher.create_holdings_from_spx_weights()
        if approx_holdings is not None:
            print("SUCCESS: Approximate holdings created!")
            print(f"   Top 10 holdings:")
            print(approx_holdings[['ticker', 'name', 'weight_pct']].head(10).to_string(index=False))
            fetcher.save_holdings_data(approx_holdings, "approximate")
            success = True
        
        if success:
            print(f"\nSUCCESS: SPY holdings data collection completed!")
            print(f"Data saved in: data/processed/spy_holdings/")
            return True
        else:
            print("ERROR: All methods failed to retrieve SPY holdings")
            return False
            
    except Exception as e:
        print(f"ERROR: Error in main execution: {e}")
        return False
    
    finally:
        fetcher.disconnect()

if __name__ == "__main__":
    main()