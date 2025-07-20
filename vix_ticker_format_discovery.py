#!/usr/bin/env python3
"""
VIX Ticker Format Discovery
Test different VIX option ticker formats to find what works
"""

import sys
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, date
import json
import time

try:
    import blpapi
    print("✅ Bloomberg API imported successfully")
except ImportError as e:
    print(f"❌ Bloomberg API import error: {e}")
    sys.exit(1)

class VIXTickerFormatDiscovery:
    """Discover the correct VIX option ticker format for your Bloomberg setup"""
    
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
    
    def test_single_ticker(self, ticker):
        """Test a single ticker to see if it exists"""
        try:
            request = self.refDataService.createRequest("ReferenceDataRequest")
            request.getElement("securities").appendValue(ticker)
            request.getElement("fields").appendValue("PX_LAST")
            
            self.session.sendRequest(request)
            
            while True:
                event = self.session.nextEvent(5000)
                
                if event.eventType() == blpapi.Event.RESPONSE:
                    for msg in event:
                        securityDataArray = msg.getElement("securityData")
                        securityData = securityDataArray.getValue(0)
                        
                        if securityData.hasElement("securityError"):
                            error = securityData.getElement("securityError")
                            return False, f"Error: {error.getElement('message').getValue()}"
                        
                        if securityData.hasElement("fieldData"):
                            fieldData = securityData.getElement("fieldData")
                            if fieldData.hasElement("PX_LAST"):
                                price = fieldData.getElement("PX_LAST").getValue()
                                return True, f"Price: {price}"
                    break
                
                if event.eventType() == blpapi.Event.TIMEOUT:
                    return False, "Timeout"
            
            return False, "No response"
            
        except Exception as e:
            return False, f"Exception: {e}"
    
    def generate_option_format_variations(self):
        """Generate different VIX option ticker format variations to test"""
        
        # Use next month's expiry (around August 2025)
        target_date = datetime(2025, 8, 20)  # 3rd Wednesday of August 2025
        strike = 20  # Common strike
        
        # Different date formats
        date_formats = [
            target_date.strftime('%y%m%d'),    # 250820
            target_date.strftime('%m/%d/%y'),  # 08/20/25
            target_date.strftime('%m%d%y'),    # 082025
            target_date.strftime('%Y%m%d'),    # 20250820
            target_date.strftime('%b%y'),      # Aug25
            target_date.strftime('%b %y'),     # Aug 25
        ]
        
        # Different ticker patterns
        ticker_patterns = []
        
        for date_str in date_formats:
            # Pattern variations
            patterns = [
                f"VIX {date_str} C {strike} Index",      # VIX 250820 C 20 Index
                f"VIX {date_str} C{strike} Index",       # VIX 250820 C20 Index  
                f"VIX{date_str} C {strike} Index",       # VIX250820 C 20 Index
                f"VIX{date_str} C{strike} Index",        # VIX250820 C20 Index
                f"VIX {date_str} C {strike}.00 Index",   # VIX 250820 C 20.00 Index
                f"VIX {date_str} Call {strike} Index",   # VIX 250820 Call 20 Index
                f"VIX {date_str}C{strike} Index",        # VIX 250820C20 Index
                f".VIX {date_str} C {strike} Index",     # .VIX 250820 C 20 Index
                f"VIX {date_str} C {strike}",            # No Index suffix
                f"VIX{date_str}C{strike}",               # Compact format
            ]
            ticker_patterns.extend(patterns)
        
        # Also test some known working patterns from project knowledge
        known_patterns = [
            "VIX 08/20/25 C20 Index",   # MM/DD/YY format
            "VIX Aug25 C20 Index",      # Month abbreviation
        ]
        ticker_patterns.extend(known_patterns)
        
        return list(set(ticker_patterns))  # Remove duplicates
    
    def test_all_vix_option_formats(self):
        """Test all VIX option format variations"""
        if not self.connect_bloomberg():
            return
        
        try:
            print("🔍 Testing VIX option ticker format variations...")
            
            format_variations = self.generate_option_format_variations()
            print(f"Generated {len(format_variations)} format variations to test")
            
            working_formats = []
            failed_formats = []
            
            for i, ticker in enumerate(format_variations):
                print(f"\n[{i+1}/{len(format_variations)}] Testing: {ticker}")
                
                works, message = self.test_single_ticker(ticker)
                
                if works:
                    print(f"   ✅ WORKS: {message}")
                    working_formats.append({'ticker': ticker, 'result': message})
                else:
                    print(f"   ❌ Failed: {message}")
                    failed_formats.append({'ticker': ticker, 'error': message})
                
                time.sleep(0.2)  # Rate limiting
            
            # Results summary
            print(f"\n{'='*80}")
            print("VIX OPTION FORMAT DISCOVERY RESULTS")
            print(f"{'='*80}")
            
            print(f"\n✅ WORKING FORMATS ({len(working_formats)}):")
            if working_formats:
                for fmt in working_formats:
                    print(f"   {fmt['ticker']} → {fmt['result']}")
            else:
                print("   None found")
            
            print(f"\n❌ FAILED FORMATS ({len(failed_formats)}):")
            if len(failed_formats) <= 10:  # Show all if not too many
                for fmt in failed_formats:
                    print(f"   {fmt['ticker']} → {fmt['error']}")
            else:
                print(f"   {len(failed_formats)} formats failed (see JSON file for details)")
            
            # Save detailed results
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            results = {
                'working_formats': working_formats,
                'failed_formats': failed_formats,
                'total_tested': len(format_variations),
                'working_count': len(working_formats),
                'timestamp': timestamp
            }
            
            results_file = f'vix_format_discovery_{timestamp}.json'
            with open(results_file, 'w') as f:
                json.dump(results, f, indent=2)
            
            print(f"\n💾 Detailed results saved to: {results_file}")
            
            # If we found working formats, test them with Greeks
            if working_formats:
                print(f"\n🎯 Testing Greeks for working formats...")
                self.test_greeks_for_working_formats([fmt['ticker'] for fmt in working_formats])
            
            return working_formats
            
        finally:
            if self.session:
                self.session.stop()
    
    def test_greeks_for_working_formats(self, working_tickers):
        """Test Greeks availability for working ticker formats"""
        greeks_fields = ['DELTA', 'DELTA_MID', 'GAMMA', 'GAMMA_MID', 'THETA', 'THETA_MID', 'VEGA', 'VEGA_MID', 'IVOL', 'IVOL_MID']
        
        for ticker in working_tickers[:3]:  # Test first 3 working tickers
            print(f"\n📊 Testing Greeks for: {ticker}")
            
            working_greeks = []
            
            for field in greeks_fields:
                works, message = self.test_single_field(ticker, field)
                if works:
                    print(f"   ✅ {field}: {message}")
                    working_greeks.append(field)
                else:
                    print(f"   ❌ {field}: {message}")
            
            print(f"   🎯 Working Greeks: {working_greeks}")
    
    def test_single_field(self, ticker, field):
        """Test a specific field for a ticker"""
        try:
            request = self.refDataService.createRequest("ReferenceDataRequest")
            request.getElement("securities").appendValue(ticker)
            request.getElement("fields").appendValue(field)
            
            self.session.sendRequest(request)
            
            while True:
                event = self.session.nextEvent(3000)
                
                if event.eventType() == blpapi.Event.RESPONSE:
                    for msg in event:
                        securityDataArray = msg.getElement("securityData")
                        securityData = securityDataArray.getValue(0)
                        
                        if securityData.hasElement("fieldData"):
                            fieldData = securityData.getElement("fieldData")
                            if fieldData.hasElement(field):
                                value = fieldData.getElement(field).getValue()
                                return True, f"{value}"
                    break
                
                if event.eventType() == blpapi.Event.TIMEOUT:
                    return False, "Timeout"
            
            return False, "No data"
            
        except Exception as e:
            return False, f"Error: {e}"

def main():
    print("🔍 VIX OPTION TICKER FORMAT DISCOVERY")
    print("Testing different ticker formats to find what works in your Bloomberg setup")
    print("=" * 80)
    
    discoverer = VIXTickerFormatDiscovery()
    working_formats = discoverer.test_all_vix_option_formats()
    
    if working_formats:
        print(f"\n🎉 SUCCESS! Found {len(working_formats)} working VIX option formats")
        print("Use these formats for your VIX volatility strategy!")
    else:
        print("\n❌ No working VIX option formats found")
        print("This could mean:")
        print("- VIX options not available in your Bloomberg subscription")
        print("- Different ticker format needed")
        print("- Options not actively trading for this expiry")

if __name__ == "__main__":
    main()
