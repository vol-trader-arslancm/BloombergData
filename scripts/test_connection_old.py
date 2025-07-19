"""
Bloomberg Connection Test Script
Test Bloomberg API connectivity and basic data retrieval
"""

import sys
import os
from datetime import datetime, timedelta

# Add project root to path (fix the import issue)
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

try:
    import xbbg
    import pandas as pd
    print("✅ Required packages imported successfully")
except ImportError as e:
    print(f"❌ Import error: {e}")
    print("Please install requirements: pip install -r requirements.txt")
    sys.exit(1)

# Now import config (after fixing path)
try:
    from config.bloomberg_config import SPX_TICKER, VOLATILITY_FIELDS, MARKET_DATA_FIELDS
    print("✅ Config imported successfully")
except ImportError as e:
    print(f"⚠️ Config import failed: {e}")
    print("Using default values...")
    # Fallback values
    SPX_TICKER = 'SPX Index'
    VOLATILITY_FIELDS = {
        'vol_30d': 'VOLATILITY_30D',
        'vol_90d': 'VOLATILITY_90D'
    }
    MARKET_DATA_FIELDS = {'price': 'PX_LAST'}

from config.bloomberg_config import SPX_TICKER, VOLATILITY_FIELDS, MARKET_DATA_FIELDS

def test_bloomberg_connection():
    """Test basic Bloomberg Terminal connection"""
    print("\n" + "="*50)
    print("BLOOMBERG CONNECTION TEST")
    print("="*50)
    
    try:
        # Test 1: Basic price data
        print("\n1. Testing SPX price data...")
        end_date = datetime.now().strftime('%Y-%m-%d')
        start_date = (datetime.now() - timedelta(days=5)).strftime('%Y-%m-%d')
        
        price_data = xbbg.bdh(
            tickers=SPX_TICKER,
            flds='PX_LAST',
            start_date=start_date,
            end_date=end_date
        )
        
        if not price_data.empty:
            print(f"✅ SPX price data retrieved: {price_data.shape[0]} rows")
            print(f"   Latest SPX price: {price_data.iloc[-1, 0]:.2f}")
        else:
            print("❌ No price data retrieved")
            return False
            
    except Exception as e:
        print(f"❌ Price data test failed: {e}")
        return False
    
    try:
        # Test 2: Volatility data
        print("\n2. Testing SPX volatility data...")
        vol_data = xbbg.bdp(
            tickers=SPX_TICKER,
            flds=['VOLATILITY_30D', 'VOLATILITY_90D']
        )
        
        if not vol_data.empty:
            print("✅ SPX volatility data retrieved:")
            for col in vol_data.columns:
                val = vol_data.iloc[0][col]
                if pd.notna(val):
                    print(f"   {col}: {val:.2f}%")
                else:
                    print(f"   {col}: No data")
        else:
            print("❌ No volatility data retrieved")
            
    except Exception as e:
        print(f"❌ Volatility data test failed: {e}")
        
    try:
        # Test 3: SPY holdings test
        print("\n3. Testing SPY ETF data...")
        spy_data = xbbg.bdp(
            tickers='SPY US Equity',
            flds='PX_LAST'
        )
        
        if not spy_data.empty:
            spy_price = spy_data.iloc[0, 0]
            print(f"✅ SPY price retrieved: ${spy_price:.2f}")
        else:
            print("❌ SPY data not available")
            
    except Exception as e:
        print(f"❌ SPY data test failed: {e}")
    
    print("\n" + "="*50)
    print("CONNECTION TEST COMPLETE")
    print("="*50)
    print("\nIf you see ✅ marks above, Bloomberg connection is working!")
    print("If you see ❌ marks, check:")
    print("  - Bloomberg Terminal is running")
    print("  - You're logged into Bloomberg")
    print("  - blpapi package is installed correctly")
    
    return True

def test_field_availability():
    """Test availability of key Bloomberg fields"""
    print("\n" + "="*50)
    print("BLOOMBERG FIELDS TEST")
    print("="*50)
    
    test_fields = list(VOLATILITY_FIELDS.values())[:4]  # Test first 4 vol fields
    test_fields.extend(['PX_LAST', 'CUR_MKT_CAP'])
    
    try:
        field_data = xbbg.bdp(
            tickers=SPX_TICKER,
            flds=test_fields
        )
        
        print(f"\nTesting {len(test_fields)} key fields:")
        for field in test_fields:
            if field in field_data.columns:
                val = field_data[field].iloc[0]
                status = "✅" if pd.notna(val) else "⚠️"
                print(f"  {status} {field}: {val if pd.notna(val) else 'No data'}")
            else:
                print(f"  ❌ {field}: Field not found")
                
    except Exception as e:
        print(f"❌ Fields test failed: {e}")

if __name__ == "__main__":
    print("Bloomberg Data Collection - Connection Test")
    print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Run tests
    test_bloomberg_connection()
    test_field_availability()
    
    print(f"\nNext steps:")
    print("1. Run: python scripts/fetch_spy_holdings.py")
    print("2. Run: python scripts/fetch_volatility_data.py")
    print("3. Open notebooks/01_data_exploration.ipynb")