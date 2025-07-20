"""
Bloomberg Security Discovery Script
Find what VIX-related securities are actually available in Bloomberg
"""

import blpapi
import pandas as pd
from datetime import datetime

def test_security_variations():
    """Test various VIX security naming conventions"""
    try:
        session = blpapi.Session()
        session.start()
        session.openService("//blp/refdata")
        service = session.getService("//blp/refdata")
        
        # Try different VIX naming conventions
        test_securities = [
            # VIX Index variations
            "VIX Index",
            "VIX US Index", 
            "VXX Index",
            "CBOE VIX Index",
            
            # VIX Futures variations
            "VIX1 Index",
            "VIX2 Index", 
            "VIX3 Index",
            "VIXF25 Curncy",
            "VIXG25 Curncy",
            "VIX F25 Index",
            "VIX G25 Index",
            "VXF25 Index",
            "VXG25 Index",
            
            # Alternative VIX futures
            "VX1 Comdty",
            "VX2 Comdty",
            "VXF25 Comdty",
            "VXG25 Comdty",
            
            # CBOE VIX futures
            "CBOE VIX1 Index",
            "CBOE VIX2 Index",
        ]
        
        print("🔍 Testing VIX security variations...")
        print("=" * 50)
        
        results = {}
        working_securities = []
        
        for security in test_securities:
            print(f"Testing {security:20} ... ", end="")
            
            request = service.createRequest("ReferenceDataRequest")
            request.getElement("securities").appendValue(security)
            request.getElement("fields").appendValue("PX_LAST")
            request.getElement("fields").appendValue("NAME")
            
            session.sendRequest(request)
            event = session.nextEvent(5000)
            
            if event.eventType() == blpapi.Event.RESPONSE:
                for msg in event:
                    securityData = msg.getElement("securityData")
                    security_data = securityData.getValue(0)
                    
                    if security_data.hasElement("securityError"):
                        print("❌ Not Found")
                        results[security] = "Not Found"
                    elif security_data.hasElement("fieldData"):
                        fieldData = security_data.getElement("fieldData")
                        
                        name = "Unknown"
                        price = "N/A"
                        
                        if fieldData.hasElement("NAME"):
                            name = fieldData.getElement("NAME").getValue()
                        if fieldData.hasElement("PX_LAST"):
                            price = fieldData.getElement("PX_LAST").getValue()
                        
                        print(f"✅ Found: {name} (Price: {price})")
                        results[security] = f"Found: {name}"
                        working_securities.append(security)
                    else:
                        print("❌ No Data")
                        results[security] = "No Data"
            else:
                print("❌ Timeout")
                results[security] = "Timeout"
        
        session.stop()
        return working_securities, results
        
    except Exception as e:
        print(f"❌ Security discovery failed: {e}")
        return [], {}

def test_options_format():
    """Test different VIX options naming formats"""
    try:
        session = blpapi.Session()
        session.start()
        session.openService("//blp/refdata")
        service = session.getService("//blp/refdata")
        
        # Try different option formats with current/near dates
        option_formats = [
            # Standard format
            "VIX 01/22/25 C20 Index",
            "VIX 02/19/25 C25 Index",
            
            # Alternative formats
            "VIX1 01/22/25 C20 Index",
            "VX 01/22/25 C20 Index", 
            "CBOE VIX 01/22/25 C20 Index",
            
            # Different date formats
            "VIX 22JAN25 C20 Index",
            "VIX 220125 C20 Index",
            
            # Different suffixes
            "VIX 01/22/25 C20 Equity",
            "VIX 01/22/25 C20 Curncy",
        ]
        
        print("\n🔍 Testing VIX options formats...")
        print("=" * 50)
        
        working_options = []
        
        for option in option_formats:
            print(f"Testing {option:25} ... ", end="")
            
            request = service.createRequest("ReferenceDataRequest")
            request.getElement("securities").appendValue(option)
            request.getElement("fields").appendValue("PX_LAST")
            request.getElement("fields").appendValue("DELTA_MID")
            
            session.sendRequest(request)
            event = session.nextEvent(5000)
            
            if event.eventType() == blpapi.Event.RESPONSE:
                for msg in event:
                    securityData = msg.getElement("securityData")
                    security_data = securityData.getValue(0)
                    
                    if security_data.hasElement("securityError"):
                        print("❌ Not Found")
                    elif security_data.hasElement("fieldData"):
                        fieldData = security_data.getElement("fieldData")
                        
                        price = "N/A"
                        delta = "N/A"
                        
                        if fieldData.hasElement("PX_LAST"):
                            price = fieldData.getElement("PX_LAST").getValue()
                        if fieldData.hasElement("DELTA_MID"):
                            delta = fieldData.getElement("DELTA_MID").getValue()
                        
                        print(f"✅ Found (Price: {price}, Delta: {delta})")
                        working_options.append(option)
                    else:
                        print("❌ No Data")
            else:
                print("❌ Timeout")
        
        session.stop()
        return working_options
        
    except Exception as e:
        print(f"❌ Options discovery failed: {e}")
        return []

def check_original_data():
    """Check what was actually collected in the original run"""
    import json
    from pathlib import Path
    
    print("\n🔍 Analyzing original collection data...")
    print("=" * 50)
    
    data_dir = Path("data/vix_data")
    
    # Find the summary file
    summary_files = list(data_dir.glob("vix_analysis_summary_*.json"))
    if summary_files:
        with open(summary_files[0]) as f:
            summary = json.load(f)
        
        print(f"Original collection summary:")
        print(f"   Futures records: {summary['data_summary']['futures_records']}")
        print(f"   Total options records: {summary['data_summary']['total_options_records']}")
        print(f"   Target delta records: {summary['data_summary']['target_delta_records']}")
        
        # Check if there are any data files we missed
        csv_files = list(data_dir.glob("*.csv"))
        if csv_files:
            print(f"\nFound CSV files:")
            for csv_file in csv_files:
                size_kb = csv_file.stat().st_size / 1024
                print(f"   {csv_file.name} ({size_kb:.1f} KB)")
                
                # Quick peek at the data
                try:
                    import pandas as pd
                    df = pd.read_csv(csv_file, nrows=5)
                    print(f"      Columns: {list(df.columns)}")
                    if 'ticker' in df.columns:
                        unique_tickers = df['ticker'].unique()[:3]
                        print(f"      Sample tickers: {list(unique_tickers)}")
                except Exception as e:
                    print(f"      Error reading file: {e}")
        else:
            print("No CSV files found")
    else:
        print("No summary file found")

def main():
    """Run comprehensive Bloomberg discovery"""
    print("🔍 BLOOMBERG VIX SECURITY DISCOVERY")
    print("=" * 60)
    print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Test security variations
    working_securities, all_results = test_security_variations()
    
    # Test options formats
    working_options = test_options_format()
    
    # Check original data
    check_original_data()
    
    # Summary
    print("\n" + "=" * 60)
    print("📋 DISCOVERY SUMMARY")
    print("=" * 60)
    
    if working_securities:
        print("✅ Working VIX Securities Found:")
        for sec in working_securities:
            print(f"   {sec}")
    else:
        print("❌ No working VIX securities found")
    
    if working_options:
        print("\n✅ Working VIX Options Found:")
        for opt in working_options:
            print(f"   {opt}")
    else:
        print("\n❌ No working VIX options found")
    
    print(f"\n💡 Recommendations:")
    if working_securities:
        print("   - Use the working security tickers for futures collection")
    if working_options:
        print("   - Use the working option format for options collection")
    if not working_securities and not working_options:
        print("   - Check if VIX data is available in your Bloomberg subscription")
        print("   - Try searching Bloomberg terminal for VIX securities manually")
        print("   - Verify Bloomberg terminal is fully logged in and operational")

if __name__ == "__main__":
    main()
