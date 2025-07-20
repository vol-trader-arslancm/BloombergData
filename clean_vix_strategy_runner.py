#!/usr/bin/env python3
"""
Clean VIX Strategy Runner - No Import Issues
Simplified version that focuses on getting the strategy data
"""

import sys
import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, date
import json
import time
from pathlib import Path
import logging

try:
    import blpapi
    print("✅ Bloomberg API imported successfully")
except ImportError as e:
    print(f"❌ Bloomberg API import error: {e}")
    sys.exit(1)

class CleanVIXStrategyRunner:
    """
    Clean implementation of VIX volatility strategy data collection
    Strategy: SHORT 1x 50Δ call + LONG 2x 10Δ calls + VIX futures hedge
    """
    
    def __init__(self, years_back=5):
        self.session = None
        self.refDataService = None
        self.data_dir = Path('./data/clean_vix_strategy')
        self.data_dir.mkdir(parents=True, exist_ok=True)
        
        # Date range
        self.end_date = datetime.now()
        self.start_date = self.end_date - timedelta(days=years_back*365)
        
        # Setup logging
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
        
        print(f"🔥 Clean VIX Strategy Runner initialized")
        print(f"📅 Period: {self.start_date.date()} to {self.end_date.date()}")
    
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
    
    def get_vix_spot_data(self):
        """Get VIX spot index data"""
        print("📊 Collecting VIX spot data...")
        
        try:
            request = self.refDataService.createRequest("HistoricalDataRequest")
            request.getElement("securities").appendValue("VIX Index")
            
            # Basic fields
            for field in ["PX_LAST", "PX_OPEN", "PX_HIGH", "PX_LOW", "PX_VOLUME"]:
                request.getElement("fields").appendValue(field)
            
            request.set("startDate", self.start_date.strftime('%Y%m%d'))
            request.set("endDate", self.end_date.strftime('%Y%m%d'))
            request.set("periodicitySelection", "DAILY")
            
            self.session.sendRequest(request)
            
            # Collect data
            vix_data = []
            
            while True:
                event = self.session.nextEvent(30000)
                
                if event.eventType() == blpapi.Event.RESPONSE:
                    for msg in event:
                        securityData = msg.getElement("securityData")
                        
                        if securityData.hasElement("fieldData"):
                            fieldDataArray = securityData.getElement("fieldData")
                            
                            for j in range(fieldDataArray.numValues()):
                                fieldData = fieldDataArray.getValue(j)
                                trade_date = fieldData.getElement("date").getValue()
                                
                                row = {
                                    'date': trade_date.strftime('%Y-%m-%d'),
                                    'ticker': 'VIX Index',
                                    'close': fieldData.getElement("PX_LAST").getValue() if fieldData.hasElement("PX_LAST") else np.nan,
                                    'open': fieldData.getElement("PX_OPEN").getValue() if fieldData.hasElement("PX_OPEN") else np.nan,
                                    'high': fieldData.getElement("PX_HIGH").getValue() if fieldData.hasElement("PX_HIGH") else np.nan,
                                    'low': fieldData.getElement("PX_LOW").getValue() if fieldData.hasElement("PX_LOW") else np.nan,
                                    'volume': fieldData.getElement("PX_VOLUME").getValue() if fieldData.hasElement("PX_VOLUME") else np.nan
                                }
                                vix_data.append(row)
                    break
                
                if event.eventType() == blpapi.Event.TIMEOUT:
                    print("⚠️  Timeout getting VIX data")
                    break
            
            df = pd.DataFrame(vix_data)
            print(f"✅ Collected {len(df)} VIX spot records")
            return df
            
        except Exception as e:
            print(f"❌ Error collecting VIX spot data: {e}")
            return pd.DataFrame()
    
    def generate_vix_futures_tickers(self):
        """Generate VIX futures tickers for the date range"""
        tickers = []
        current_date = self.start_date.replace(day=1).date()  # Convert to date
        end_date = self.end_date.date()  # Convert to date
        
        while current_date <= end_date + timedelta(days=90):
            year = current_date.year
            month = current_date.month
            
            # VIX futures month codes
            month_codes = ['F','G','H','J','K','M','N','Q','U','V','X','Z']
            month_code = month_codes[month - 1]
            year_code = str(year)[-2:]
            
            ticker = f"VIX{month_code}{year_code} Curncy"
            
            # Find expiry (3rd Wednesday)
            first_day = date(year, month, 1)
            first_weekday = first_day.weekday()
            days_to_first_wed = (2 - first_weekday) % 7
            first_wed = first_day + timedelta(days=days_to_first_wed)
            third_wed = first_wed + timedelta(days=14)
            
            tickers.append({
                'ticker': ticker,
                'expiry_date': third_wed,
                'year': year,
                'month': month
            })
            
            # Next month
            if month == 12:
                current_date = date(year + 1, 1, 1)
            else:
                current_date = date(year, month + 1, 1)
        
        return tickers
    
    def get_vix_futures_data(self):
        """Get VIX futures data using the correct UX format"""
        print("📊 Collecting VIX futures data...")
        
        # Use the working UX format from project knowledge
        test_tickers = [
            {"ticker": "UX1 Index", "description": "VIX Front Month Future"},
            {"ticker": "UX2 Index", "description": "VIX 2nd Month Future"},
            {"ticker": "UX3 Index", "description": "VIX 3rd Month Future"},
            {"ticker": "UX4 Index", "description": "VIX 4th Month Future"},
            {"ticker": "UX5 Index", "description": "VIX 5th Month Future"},
            {"ticker": "UX6 Index", "description": "VIX 6th Month Future"}
        ]
        
        print(f"Testing {len(test_tickers)} VIX futures contracts (UX format)")
        
        all_futures_data = []
        
        for ticker_info in test_tickers:
            ticker = ticker_info['ticker']
            description = ticker_info['description']
            print(f"  Testing: {ticker} ({description})")
            
            try:
                request = self.refDataService.createRequest("HistoricalDataRequest")
                request.getElement("securities").appendValue(ticker)
                
                for field in ["PX_LAST", "PX_SETTLE", "PX_VOLUME", "OPEN_INT"]:
                    request.getElement("fields").appendValue(field)
                
                # Get last 90 days for testing
                test_start = self.end_date - timedelta(days=90)
                request.set("startDate", test_start.strftime('%Y%m%d'))
                request.set("endDate", self.end_date.strftime('%Y%m%d'))
                request.set("periodicitySelection", "DAILY")
                
                self.session.sendRequest(request)
                
                # Get response
                event = self.session.nextEvent(10000)
                
                if event.eventType() == blpapi.Event.RESPONSE:
                    for msg in event:
                        securityData = msg.getElement("securityData")
                        
                        if securityData.hasElement("securityError"):
                            print(f"    ❌ Error: {ticker}")
                            continue
                        
                        if securityData.hasElement("fieldData"):
                            fieldDataArray = securityData.getElement("fieldData")
                            print(f"    ✅ {ticker}: {fieldDataArray.numValues()} records")
                            
                            for j in range(fieldDataArray.numValues()):
                                fieldData = fieldDataArray.getValue(j)
                                trade_date = fieldData.getElement("date").getValue()
                                
                                row = {
                                    'date': trade_date.strftime('%Y-%m-%d'),
                                    'ticker': ticker,
                                    'description': description,
                                    'contract_month': ticker.replace(' Index', ''),  # UX1, UX2, etc.
                                    'last': fieldData.getElement("PX_LAST").getValue() if fieldData.hasElement("PX_LAST") else np.nan,
                                    'settle': fieldData.getElement("PX_SETTLE").getValue() if fieldData.hasElement("PX_SETTLE") else np.nan,
                                    'volume': fieldData.getElement("PX_VOLUME").getValue() if fieldData.hasElement("PX_VOLUME") else np.nan,
                                    'open_interest': fieldData.getElement("OPEN_INT").getValue() if fieldData.hasElement("OPEN_INT") else np.nan
                                }
                                all_futures_data.append(row)
                        else:
                            print(f"    ⚠️  {ticker}: No field data")
                
            except Exception as e:
                print(f"    ❌ Error with {ticker}: {e}")
            
            time.sleep(0.1)  # Rate limiting
        
        df = pd.DataFrame(all_futures_data)
        print(f"✅ Collected {len(df)} VIX futures records")
        return df
    
    def test_vix_options(self):
        """Test a few VIX options to see if they work"""
        print("📊 Testing VIX options...")
        
        # Generate some test option tickers for current month
        current_date = datetime.now()
        year = current_date.year
        month = current_date.month
        
        # Find next expiry
        first_day = date(year, month, 1)
        first_weekday = first_day.weekday()
        days_to_first_wed = (2 - first_weekday) % 7
        first_wed = first_day + timedelta(days=days_to_first_wed)
        third_wed = first_wed + timedelta(days=14)
        
        # If we're past this month's expiry, use next month
        if third_wed < current_date.date():
            if month == 12:
                next_month = 1
                next_year = year + 1
            else:
                next_month = month + 1
                next_year = year
            
            first_day = date(next_year, next_month, 1)
            first_weekday = first_day.weekday()
            days_to_first_wed = (2 - first_weekday) % 7
            first_wed = first_day + timedelta(days=days_to_first_wed)
            third_wed = first_wed + timedelta(days=14)
        
        exp_str = third_wed.strftime('%y%m%d')
        
        # Test a few option strikes
        test_options = []
        strikes = [15, 18, 20, 25, 30]
        
        for strike in strikes:
            ticker = f"VIX {exp_str} C {strike} Index"
            test_options.append(ticker)
        
        print(f"Testing {len(test_options)} VIX options for expiry {third_wed}")
        
        working_options = []
        
        for ticker in test_options:
            try:
                request = self.refDataService.createRequest("ReferenceDataRequest")
                request.getElement("securities").appendValue(ticker)
                request.getElement("fields").appendValue("PX_LAST")
                
                self.session.sendRequest(request)
                event = self.session.nextEvent(5000)
                
                if event.eventType() == blpapi.Event.RESPONSE:
                    for msg in event:
                        securityData = msg.getElement("securityData").getValue(0)
                        
                        if securityData.hasElement("fieldData"):
                            print(f"  ✅ {ticker}: Working")
                            working_options.append(ticker)
                        else:
                            print(f"  ❌ {ticker}: No data")
                
            except Exception as e:
                print(f"  ❌ {ticker}: Error - {e}")
            
            time.sleep(0.1)
        
        print(f"✅ Found {len(working_options)} working VIX options")
        return working_options
    
    def run_strategy_data_collection(self):
        """Run the complete strategy data collection"""
        if not self.connect_bloomberg():
            return None
        
        strategy_data = {}
        
        try:
            # 1. VIX Spot
            vix_spot = self.get_vix_spot_data()
            if len(vix_spot) > 0:
                strategy_data['vix_spot'] = vix_spot
            
            # 2. VIX Futures
            vix_futures = self.get_vix_futures_data()
            if len(vix_futures) > 0:
                strategy_data['vix_futures'] = vix_futures
            
            # 3. Test VIX Options
            working_options = self.test_vix_options()
            strategy_data['working_options'] = working_options
            
            # Save data
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            
            for data_type, data in strategy_data.items():
                if isinstance(data, pd.DataFrame) and len(data) > 0:
                    filename = f"clean_vix_{data_type}_{timestamp}.csv"
                    filepath = self.data_dir / filename
                    data.to_csv(filepath, index=False)
                    print(f"💾 Saved: {filename}")
            
            # Save working options list
            if working_options:
                options_file = self.data_dir / f"working_options_{timestamp}.json"
                with open(options_file, 'w') as f:
                    json.dump(working_options, f, indent=2)
                print(f"💾 Saved: working_options_{timestamp}.json")
            
            # Summary
            summary = {
                'timestamp': timestamp,
                'vix_spot_records': len(strategy_data.get('vix_spot', [])),
                'vix_futures_records': len(strategy_data.get('vix_futures', [])),
                'working_options_count': len(working_options),
                'working_options': working_options
            }
            
            summary_file = self.data_dir / f"collection_summary_{timestamp}.json"
            with open(summary_file, 'w') as f:
                json.dump(summary, f, indent=2)
            
            print(f"\n🎉 Strategy data collection completed!")
            print(f"📊 Summary: {summary}")
            
            return summary
            
        finally:
            if self.session:
                self.session.stop()

def main():
    print("🚀 Clean VIX Strategy Data Collection")
    print("=" * 50)
    
    # Quick collection for testing
    runner = CleanVIXStrategyRunner(years_back=1)  # Just 1 year for now
    
    print("⚠️  Ensure Bloomberg Terminal is running!")
    proceed = input("Continue with data collection? (y/n): ").lower().strip()
    
    if proceed == 'y':
        summary = runner.run_strategy_data_collection()
        
        if summary:
            print(f"\n✅ Success! Data saved in: {runner.data_dir}")
            print("Next: Use this data to build the full strategy backtester")
        else:
            print("❌ Collection failed")
    else:
        print("Collection cancelled")

if __name__ == "__main__":
    main()
