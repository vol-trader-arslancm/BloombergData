#!/usr/bin/env python3
"""
Robust VIX Options Test
Better handling of Bloomberg API responses with proper event processing
"""

import sys
import pandas as pd
import numpy as np
from datetime import datetime
import json
import time

try:
    import blpapi
    print("✅ Bloomberg API imported successfully")
except ImportError as e:
    print(f"❌ Bloomberg API import error: {e}")
    sys.exit(1)

class RobustVIXOptionsTest:
    """Robust VIX options testing with proper Bloomberg API event handling"""
    
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
    
    def test_option_robust(self, ticker, fields=['PX_LAST']):
        """Test option with robust event handling"""
        print(f"\n🔍 Testing: {ticker}")
        print(f"   Fields: {fields}")
        
        try:
            request = self.refDataService.createRequest("ReferenceDataRequest")
            request.getElement("securities").appendValue(ticker)
            
            for field in fields:
                request.getElement("fields").appendValue(field)
            
            print("   📤 Sending request...")
            self.session.sendRequest(request)
            
            # Better event processing
            print("   🔄 Processing events...")
            response_data = {}
            
            while True:
                event = self.session.nextEvent(10000)  # 10 second timeout
                
                print(f"   📨 Event type: {event.eventType()}")
                
                if event.eventType() == blpapi.Event.PARTIAL_RESPONSE:
                    print("   🔄 Partial response - continuing...")
                    for msg in event:
                        print(f"      Partial message: {msg}")
                
                elif event.eventType() == blpapi.Event.RESPONSE:
                    print("   ✅ Full response received!")
                    
                    for msg in event:
                        print(f"   📄 Message: {msg}")
                        
                        if msg.hasElement("securityData"):
                            securityDataArray = msg.getElement("securityData")
                            print(f"   📊 Security data elements: {securityDataArray.numValues()}")
                            
                            for j in range(securityDataArray.numValues()):
                                securityData = securityDataArray.getValue(j)
                                security = securityData.getElement("security").getValue()
                                print(f"   🎯 Security: {security}")
                                
                                response_data['security'] = security
                                
                                # Check for errors
                                if securityData.hasElement("securityError"):
                                    error = securityData.getElement("securityError")
                                    print(f"   ❌ Security error: {error}")
                                    response_data['error'] = str(error)
                                    return response_data
                                
                                # Check for field exceptions
                                if securityData.hasElement("fieldExceptions"):
                                    exceptions = securityData.getElement("fieldExceptions")
                                    if exceptions.numValues() > 0:
                                        print(f"   ⚠️  Field exceptions:")
                                        for k in range(exceptions.numValues()):
                                            exc = exceptions.getValue(k)
                                            print(f"      {exc}")
                                
                                # Get field data
                                if securityData.hasElement("fieldData"):
                                    fieldData = securityData.getElement("fieldData")
                                    print(f"   ✅ Field data found!")
                                    
                                    response_data['fields'] = {}
                                    
                                    for field in fields:
                                        if fieldData.hasElement(field):
                                            value = fieldData.getElement(field).getValue()
                                            print(f"      {field}: {value}")
                                            response_data['fields'][field] = value
                                        else:
                                            print(f"      {field}: NOT FOUND")
                                            response_data['fields'][field] = None
                                    
                                    return response_data
                                else:
                                    print(f"   ❌ No field data found")
                                    response_data['error'] = "No field data"
                                    return response_data
                    break
                
                elif event.eventType() == blpapi.Event.TIMEOUT:
                    print("   ⏰ Request timed out")
                    response_data['error'] = "Timeout"
                    return response_data
                
                else:
                    print(f"   ℹ️  Other event: {event.eventType()}")
            
            return response_data
            
        except Exception as e:
            print(f"   ❌ Exception: {e}")
            return {'error': str(e)}
    
    def test_multiple_fields_progressive(self, ticker):
        """Test fields progressively to isolate issues"""
        print(f"\n🧪 PROGRESSIVE FIELD TESTING: {ticker}")
        print("=" * 50)
        
        # Test fields one by one
        test_fields = [
            ['PX_LAST'],
            ['PX_BID'],
            ['PX_ASK'],
            ['PX_VOLUME'],
            ['PX_LAST', 'PX_BID'],
            ['PX_LAST', 'PX_BID', 'PX_ASK'],
            ['DELTA'],
            ['DELTA_MID'],
            ['GAMMA'],
            ['IVOL']
        ]
        
        results = {}
        
        for fields in test_fields:
            field_key = "_".join(fields)
            print(f"\n📋 Testing fields: {fields}")
            
            result = self.test_option_robust(ticker, fields)
            results[field_key] = result
            
            if 'error' not in result:
                print(f"   ✅ SUCCESS with fields: {fields}")
            else:
                print(f"   ❌ FAILED: {result.get('error', 'Unknown error')}")
            
            time.sleep(0.5)  # Brief pause between tests
        
        return results
    
    def run_comprehensive_option_test(self):
        """Run comprehensive option testing"""
        if not self.connect_bloomberg():
            return
        
        try:
            # Test the options we know should work
            test_options = [
                'VIX 250820 C 20 Index'  # Start with just one
            ]
            
            all_results = {}
            
            for ticker in test_options:
                print(f"\n{'='*80}")
                print(f"COMPREHENSIVE TEST: {ticker}")
                print(f"{'='*80}")
                
                # Progressive field testing
                results = self.test_multiple_fields_progressive(ticker)
                all_results[ticker] = results
                
                # Analyze results
                print(f"\n📊 ANALYSIS FOR {ticker}:")
                print("-" * 40)
                
                working_combinations = []
                failed_combinations = []
                
                for field_combo, result in results.items():
                    if 'error' not in result and 'fields' in result:
                        working_combinations.append(field_combo)
                        print(f"   ✅ {field_combo}: {result['fields']}")
                    else:
                        failed_combinations.append(field_combo)
                        print(f"   ❌ {field_combo}: {result.get('error', 'Failed')}")
                
                print(f"\n🎯 WORKING COMBINATIONS: {len(working_combinations)}")
                print(f"   {working_combinations}")
                print(f"\n⚠️  FAILED COMBINATIONS: {len(failed_combinations)}")
                print(f"   {failed_combinations}")
            
            # Save detailed results
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            results_file = f'robust_vix_options_test_{timestamp}.json'
            
            with open(results_file, 'w') as f:
                json.dump(all_results, f, indent=2, default=str)
            
            print(f"\n💾 Detailed results saved to: {results_file}")
            
            return all_results
            
        finally:
            if self.session:
                self.session.stop()

def main():
    print("🔧 ROBUST VIX OPTIONS TEST")
    print("Comprehensive testing with proper Bloomberg API event handling")
    print("=" * 70)
    
    tester = RobustVIXOptionsTest()
    results = tester.run_comprehensive_option_test()
    
    if results:
        print("\n✅ Comprehensive test complete!")
        print("Check the detailed output above and the saved JSON file")
    else:
        print("❌ Test failed")

if __name__ == "__main__":
    main()
