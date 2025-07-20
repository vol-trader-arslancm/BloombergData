#!/usr/bin/env python3
"""
Debug VIX Options Data
Test what fields are actually available for VIX options
"""

import sys
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import json

try:
    import blpapi
    print("✅ Bloomberg API imported successfully")
except ImportError as e:
    print(f"❌ Bloomberg API import error: {e}")
    sys.exit(1)

class VIXOptionsDebugger:
    """Debug VIX options to see what data is available"""
    
    def __init__(self):
        self.session = None
        self.refDataService = None
    
    def connect_bloomberg(self):
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
            print("✅ Bloomberg connection established")
            return True
            
        except Exception as e:
            print(f"❌ Bloomberg connection failed: {e}")
            return False
    
    def test_single_option_all_fields(self, ticker):
        """Test a single option with basic fields first"""
        print(f"\n🔍 Testing option: {ticker}")
        
        # Start with basic fields only
        basic_fields = ['PX_LAST', 'PX_BID', 'PX_ASK', 'PX_VOLUME']
        
        try:
            request = self.refDataService.createRequest("ReferenceDataRequest")
            request.getElement("securities").appendValue(ticker)
            
            for field in basic_fields:
                request.getElement("fields").appendValue(field)
            
            self.session.sendRequest(request)
            event = self.session.nextEvent(5000)
            
            if event.eventType() == blpapi.Event.RESPONSE:
                for msg in event:
                    print(f"📄 Full message: {msg}")
                    
                    securityDataArray = msg.getElement("securityData")
                    
                    for j in range(securityDataArray.numValues()):
                        securityData = securityDataArray.getValue(j)
                        security = securityData.getElement("security").getValue()
                        print(f"🔍 Security: {security}")
                        
                        if securityData.hasElement("securityError"):
                            error = securityData.getElement("securityError")
                            print(f"❌ Security error: {error}")
                            return False
                        
                        if securityData.hasElement("fieldExceptions"):
                            exceptions = securityData.getElement("fieldExceptions")
                            if exceptions.numValues() > 0:
                                print(f"⚠️  Field exceptions:")
                                for k in range(exceptions.numValues()):
                                    exc = exceptions.getValue(k)
                                    print(f"   {exc}")
                        
                        if securityData.hasElement("fieldData"):
                            fieldData = securityData.getElement("fieldData")
                            print(f"✅ Field data available:")
                            
                            # Show all available fields
                            for field in basic_fields:
                                if fieldData.hasElement(field):
                                    value = fieldData.getElement(field).getValue()
                                    print(f"   {field}: {value}")
                                else:
                                    print(f"   {field}: Not available")
                            
                            return True
                        else:
                            print(f"❌ No field data")
                            return False
            else:
                print(f"⚠️  No response received")
                return False
                
        except Exception as e:
            print(f"❌ Error testing {ticker}: {e}")
            return False
    
    def test_option_greeks(self, ticker):
        """Test Greeks fields specifically"""
        print(f"\n🎯 Testing Greeks for: {ticker}")
        
        # Greeks fields to test
        greeks_fields = [
            'DELTA_MID', 'DELTA_BID', 'DELTA_ASK', 'DELTA',
            'GAMMA_MID', 'GAMMA_BID', 'GAMMA_ASK', 'GAMMA',
            'THETA_MID', 'THETA_BID', 'THETA_ASK', 'THETA',
            'VEGA_MID', 'VEGA_BID', 'VEGA_ASK', 'VEGA',
            'IVOL_MID', 'IVOL_BID', 'IVOL_ASK', 'IVOL'
        ]
        
        working_greeks = []
        
        for field in greeks_fields:
            try:
                request = self.refDataService.createRequest("ReferenceDataRequest")
                request.getElement("securities").appendValue(ticker)
                request.getElement("fields").appendValue(field)
                
                self.session.sendRequest(request)
                event = self.session.nextEvent(3000)
                
                if event.eventType() == blpapi.Event.RESPONSE:
                    for msg in event:
                        securityDataArray = msg.getElement("securityData")
                        securityData = securityDataArray.getValue(0)
                        
                        if securityData.hasElement("fieldData"):
                            fieldData = securityData.getElement("fieldData")
                            if fieldData.hasElement(field):
                                value = fieldData.getElement(field).getValue()
                                if value is not None:
                                    print(f"✅ {field}: {value}")
                                    working_greeks.append(field)
                                else:
                                    print(f"⚠️  {field}: Null value")
                            else:
                                print(f"❌ {field}: Not in response")
                        else:
                            print(f"❌ {field}: No field data")
                
            except Exception as e:
                print(f"❌ {field}: Error - {e}")
        
        print(f"\n✅ Working Greeks fields: {working_greeks}")
        return working_greeks
    
    def run_comprehensive_test(self):
        """Run comprehensive test of VIX options"""
        if not self.connect_bloomberg():
            return
        
        try:
            # Test the working options we found earlier
            test_options = [
                'VIX 250820 C 15 Index',
                'VIX 250820 C 20 Index', 
                'VIX 250820 C 25 Index'
            ]
            
            results = {}
            
            for ticker in test_options:
                print(f"\n{'='*60}")
                print(f"TESTING: {ticker}")
                print(f"{'='*60}")
                
                # Test basic fields
                basic_works = self.test_single_option_all_fields(ticker)
                
                if basic_works:
                    # Test Greeks
                    working_greeks = self.test_option_greeks(ticker)
                    
                    results[ticker] = {
                        'basic_data': basic_works,
                        'working_greeks': working_greeks
                    }
                else:
                    results[ticker] = {
                        'basic_data': False,
                        'working_greeks': []
                    }
            
            # Summary
            print(f"\n{'='*60}")
            print("SUMMARY OF TESTS")
            print(f"{'='*60}")
            
            all_working_greeks = set()
            for ticker, result in results.items():
                print(f"\n{ticker}:")
                print(f"  Basic data: {'✅' if result['basic_data'] else '❌'}")
                print(f"  Greeks available: {len(result['working_greeks'])}")
                if result['working_greeks']:
                    print(f"  Working Greeks: {result['working_greeks']}")
                    all_working_greeks.update(result['working_greeks'])
            
            print(f"\n🎯 UNIVERSAL WORKING GREEKS: {list(all_working_greeks)}")
            
            # Save results
            with open('vix_options_debug_results.json', 'w') as f:
                json.dump(results, f, indent=2)
            
            print(f"\n💾 Results saved to: vix_options_debug_results.json")
            
            return results
            
        finally:
            if self.session:
                self.session.stop()

def main():
    print("🔧 VIX OPTIONS DEBUGGER")
    print("Testing what fields are actually available for VIX options")
    print("=" * 60)
    
    debugger = VIXOptionsDebugger()
    results = debugger.run_comprehensive_test()
    
    if results:
        print("\n✅ Debug complete! Check the output above for working fields.")
    else:
        print("❌ Debug failed")

if __name__ == "__main__":
    main()
