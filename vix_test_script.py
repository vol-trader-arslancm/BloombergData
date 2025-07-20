"""
VIX Contract Test Script
Test specific VIX contracts to see what Bloomberg has available
"""

import blpapi
import pandas as pd
from datetime import datetime, timedelta

def test_bloomberg_connection():
    """Test basic Bloomberg connection"""
    try:
        session = blpapi.Session()
        if not session.start():
            print("❌ Failed to start Bloomberg session")
            return False
        
        if not session.openService("//blp/refdata"):
            print("❌ Failed to open reference data service")
            session.stop()
            return False
        
        print("✅ Bloomberg connection successful")
        session.stop()
        return True
    except Exception as e:
        print(f"❌ Bloomberg connection failed: {e}")
        return False

def test_vix_spot():
    """Test VIX Index (spot) data"""
    try:
        session = blpapi.Session()
        session.start()
        session.openService("//blp/refdata")
        service = session.getService("//blp/refdata")
        
        request = service.createRequest("ReferenceDataRequest")
        request.getElement("securities").appendValue("VIX Index")
        request.getElement("fields").appendValue("PX_LAST")
        
        session.sendRequest(request)
        event = session.nextEvent(5000)
        
        if event.eventType() == blpapi.Event.RESPONSE:
            for msg in event:
                securityData = msg.getElement("securityData")
                security = securityData.getValue(0)
                if security.hasElement("fieldData"):
                    fieldData = security.getElement("fieldData")
                    if fieldData.hasElement("PX_LAST"):
                        vix_level = fieldData.getElement("PX_LAST").getValue()
                        print(f"✅ VIX Index current level: {vix_level}")
                        session.stop()
                        return True
        
        print("❌ No VIX Index data received")
        session.stop()
        return False
    except Exception as e:
        print(f"❌ VIX Index test failed: {e}")
        return False

def test_vix_futures():
    """Test known VIX futures contracts"""
    try:
        session = blpapi.Session()
        session.start()
        session.openService("//blp/refdata")
        service = session.getService("//blp/refdata")
        
        # Test current front month contracts (these should exist)
        test_contracts = [
            "VIX1 Index",  # Front month VIX futures
            "VIX2 Index",  # Second month VIX futures
            "VIXF25 Curncy",  # January 2025 VIX futures
            "VIXG25 Curncy",  # February 2025 VIX futures
        ]
        
        print("🧪 Testing VIX futures contracts...")
        results = {}
        
        for contract in test_contracts:
            request = service.createRequest("ReferenceDataRequest")
            request.getElement("securities").appendValue(contract)
            request.getElement("fields").appendValue("PX_LAST")
            
            session.sendRequest(request)
            event = session.nextEvent(5000)
            
            if event.eventType() == blpapi.Event.RESPONSE:
                for msg in event:
                    securityData = msg.getElement("securityData")
                    security = securityData.getValue(0)
                    
                    if security.hasElement("securityError"):
                        results[contract] = "❌ Error/Not Found"
                    elif security.hasElement("fieldData"):
                        fieldData = security.getElement("fieldData")
                        if fieldData.hasElement("PX_LAST"):
                            price = fieldData.getElement("PX_LAST").getValue()
                            results[contract] = f"✅ Price: {price}"
                        else:
                            results[contract] = "❌ No price data"
                    else:
                        results[contract] = "❌ No data"
        
        for contract, result in results.items():
            print(f"   {contract}: {result}")
        
        session.stop()
        return any("✅" in result for result in results.values())
        
    except Exception as e:
        print(f"❌ VIX futures test failed: {e}")
        return False

def test_vix_options():
    """Test VIX options contracts"""
    try:
        session = blpapi.Session()
        session.start()
        session.openService("//blp/refdata")
        service = session.getService("//blp/refdata")
        
        # Test current month VIX options (these should exist)
        test_options = [
            "VIX 01/22/25 C20 Index",  # January 2025 VIX call option
            "VIX 02/19/25 C25 Index",  # February 2025 VIX call option
        ]
        
        print("🧪 Testing VIX options contracts...")
        results = {}
        
        for option in test_options:
            request = service.createRequest("ReferenceDataRequest")
            request.getElement("securities").appendValue(option)
            fields = ["PX_LAST", "DELTA_MID", "VEGA_MID"]
            
            for field in fields:
                request.getElement("fields").appendValue(field)
            
            session.sendRequest(request)
            event = session.nextEvent(5000)
            
            if event.eventType() == blpapi.Event.RESPONSE:
                for msg in event:
                    securityData = msg.getElement("securityData")
                    security = securityData.getValue(0)
                    
                    if security.hasElement("securityError"):
                        results[option] = "❌ Error/Not Found"
                    elif security.hasElement("fieldData"):
                        fieldData = security.getElement("fieldData")
                        data = {}
                        for field in fields:
                            if fieldData.hasElement(field):
                                data[field] = fieldData.getElement(field).getValue()
                        
                        if data:
                            delta = data.get('DELTA_MID', 'N/A')
                            price = data.get('PX_LAST', 'N/A')
                            results[option] = f"✅ Price: {price}, Delta: {delta}"
                        else:
                            results[option] = "❌ No data fields"
                    else:
                        results[option] = "❌ No data"
        
        for option, result in results.items():
            print(f"   {option}: {result}")
        
        session.stop()
        return any("✅" in result for result in results.values())
        
    except Exception as e:
        print(f"❌ VIX options test failed: {e}")
        return False

def main():
    """Run all VIX contract tests"""
    print("🔍 VIX CONTRACT AVAILABILITY TEST")
    print("=" * 40)
    
    # Test basic connection
    if not test_bloomberg_connection():
        return
    
    # Test VIX spot
    print("\n📊 Testing VIX Index (Spot)...")
    test_vix_spot()
    
    # Test VIX futures
    print("\n📈 Testing VIX Futures...")
    futures_ok = test_vix_futures()
    
    # Test VIX options
    print("\n📊 Testing VIX Options...")
    options_ok = test_vix_options()
    
    print("\n" + "=" * 40)
    print("📋 TEST SUMMARY:")
    print(f"   VIX Futures Available: {'✅' if futures_ok else '❌'}")
    print(f"   VIX Options Available: {'✅' if options_ok else '❌'}")
    
    if not futures_ok:
        print("\n💡 VIX Futures Issue:")
        print("   - Try VIX1 Index / VIX2 Index instead of monthly codes")
        print("   - Check if your Bloomberg has VIX futures data")
    
    if not options_ok:
        print("\n💡 VIX Options Issue:")
        print("   - Check VIX option ticker format")
        print("   - Verify expiry dates are correct")
        print("   - Ensure delta data is available")

if __name__ == "__main__":
    main()
