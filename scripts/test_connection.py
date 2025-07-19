"""
Direct Bloomberg API Connection Test
Using blpapi directly instead of xbbg wrapper
"""

import sys
import os
from datetime import datetime, timedelta
import pandas as pd

# Add project root to path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

try:
    import blpapi
    print("✅ Bloomberg API imported successfully")
    print(f"   blpapi version: {blpapi.__version__}")
except ImportError as e:
    print(f"❌ Bloomberg API import error: {e}")
    sys.exit(1)

try:
    from config.bloomberg_config import SPX_TICKER, VOLATILITY_FIELDS, MARKET_DATA_FIELDS
    print("✅ Config imported successfully")
except ImportError as e:
    print(f"⚠️ Config import failed: {e}")
    # Fallback values
    SPX_TICKER = 'SPX Index'
    VOLATILITY_FIELDS = {'vol_30d': 'VOLATILITY_30D', 'vol_90d': 'VOLATILITY_90D'}
    MARKET_DATA_FIELDS = {'price': 'PX_LAST'}

class BloombergDataAPI:
    """Direct Bloomberg API wrapper"""
    
    def __init__(self):
        self.session = None
        self.refDataService = None
    
    def connect(self):
        """Connect to Bloomberg Terminal"""
        try:
            # Create session options
            sessionOptions = blpapi.SessionOptions()
            
            # Create and start session
            self.session = blpapi.Session(sessionOptions)
            
            if not self.session.start():
                print("❌ Failed to start Bloomberg session")
                return False
            
            # Open reference data service
            if not self.session.openService("//blp/refdata"):
                print("❌ Failed to open Bloomberg reference data service")
                return False
            
            self.refDataService = self.session.getService("//blp/refdata")
            print("✅ Bloomberg session connected successfully")
            return True
            
        except Exception as e:
            print(f"❌ Bloomberg connection failed: {e}")
            return False
    
    def get_reference_data(self, tickers, fields):
        """Get reference data (current values)"""
        if not self.session or not self.refDataService:
            print("❌ Not connected to Bloomberg")
            return None
        
        try:
            # Create request
            request = self.refDataService.createRequest("ReferenceDataRequest")
            
            # Add securities
            if isinstance(tickers, str):
                tickers = [tickers]
            for ticker in tickers:
                request.getElement("securities").appendValue(ticker)
            
            # Add fields
            if isinstance(fields, str):
                fields = [fields]
            for field in fields:
                request.getElement("fields").appendValue(field)
            
            # Send request
            cid = self.session.sendRequest(request)
            
            # Process response
            data = {}
            while True:
                event = self.session.nextEvent(500)  # 500ms timeout
                
                if event.eventType() == blpapi.Event.RESPONSE:
                    for msg in event:
                        securityData = msg.getElement("securityData")
                        for i in range(securityData.numValues()):
                            security = securityData.getValue(i)
                            ticker = security.getElement("security").getValue()
                            
                            if security.hasElement("securityError"):
                                print(f"❌ Error for {ticker}")
                                continue
                            
                            fieldData = security.getElement("fieldData")
                            ticker_data = {}
                            for field in fields:
                                if fieldData.hasElement(field):
                                    ticker_data[field] = fieldData.getElement(field).getValue()
                                else:
                                    ticker_data[field] = None
                            
                            data[ticker] = ticker_data
                    break
                
                if event.eventType() == blpapi.Event.TIMEOUT:
                    print("⚠️ Request timeout")
                    break
            
            return pd.DataFrame(data).T if data else pd.DataFrame()
            
        except Exception as e:
            print(f"❌ Reference data request failed: {e}")
            return None
    
    def disconnect(self):
        """Disconnect from Bloomberg"""
        if self.session:
            self.session.stop()
            print("✅ Bloomberg session disconnected")

def test_bloomberg_connection():
    """Test Bloomberg connection and data retrieval"""
    print("\n" + "="*50)
    print("DIRECT BLOOMBERG API TEST")
    print("="*50)
    
    # Create API instance
    bb_api = BloombergDataAPI()
    
    # Test connection
    if not bb_api.connect():
        return False
    
    try:
        # Test 1: Get SPX current price
        print("\n1. Testing SPX current price...")
        price_data = bb_api.get_reference_data(SPX_TICKER, 'PX_LAST')
        
        if price_data is not None and not price_data.empty:
            spx_price = price_data.iloc[0]['PX_LAST']
            print(f"✅ SPX current price: {spx_price:.2f}")
        else:
            print("❌ Failed to get SPX price")
        
        # Test 2: Get SPX volatility
        print("\n2. Testing SPX volatility data...")
        vol_data = bb_api.get_reference_data(SPX_TICKER, ['VOLATILITY_30D', 'VOLATILITY_90D'])
        
        if vol_data is not None and not vol_data.empty:
            print("✅ SPX volatility data retrieved:")
            for col in vol_data.columns:
                val = vol_data.iloc[0][col]
                if val is not None:
                    print(f"   {col}: {val:.2f}%")
                else:
                    print(f"   {col}: No data available")
        else:
            print("❌ Failed to get volatility data")
        
        # Test 3: Get SPY price
        print("\n3. Testing SPY ETF price...")
        spy_data = bb_api.get_reference_data('SPY US Equity', 'PX_LAST')
        
        if spy_data is not None and not spy_data.empty:
            spy_price = spy_data.iloc[0]['PX_LAST']
            print(f"✅ SPY current price: ${spy_price:.2f}")
        else:
            print("❌ Failed to get SPY price")
        
        # Test 4: Get multiple securities
        print("\n4. Testing multiple securities...")
        multi_data = bb_api.get_reference_data(
            ['SPX Index', 'SPY US Equity', 'AAPL US Equity'],
            ['PX_LAST', 'CUR_MKT_CAP']
        )
        
        if multi_data is not None and not multi_data.empty:
            print("✅ Multiple securities data:")
            print(multi_data)
        else:
            print("❌ Failed to get multiple securities data")
        
    except Exception as e:
        print(f"❌ Test failed with error: {e}")
    
    finally:
        bb_api.disconnect()
    
    print("\n" + "="*50)
    print("BLOOMBERG API TEST COMPLETE")
    print("="*50)
    
    return True

if __name__ == "__main__":
    print("Bloomberg Data Collection - Direct API Test")
    print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Make sure Bloomberg Terminal is running and logged in!")
    
    input("\nPress Enter when Bloomberg Terminal is ready...")
    
    test_bloomberg_connection()
    
    print(f"\nNext steps:")
    print("1. If tests passed, we can build the data collection modules")
    print("2. Create SPY holdings fetcher")
    print("3. Create volatility data collector")
    print("4. Build analysis notebooks")