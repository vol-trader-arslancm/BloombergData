"""
Corrected VIX Data Fetcher - Using Proper Bloomberg Tickers
Based on discovery results: VIX3 Index, CBOE VIX1 Index
"""

import sys
import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, date
import json
import time
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent.absolute()
sys.path.insert(0, str(project_root))

try:
    import blpapi
    print("✅ Bloomberg API imported successfully")
except ImportError as e:
    print(f"❌ Bloomberg API import error: {e}")
    sys.exit(1)

class CorrectedVIXDataFetcher:
    """
    VIX Data Collection using correct Bloomberg tickers
    Focus on VIX spot, VIX futures generic contracts, and working options
    """
    
    def __init__(self, years_back=10):
        self.session = None
        self.refDataService = None
        self.project_root = project_root
        self.data_dir = self.project_root / 'data' / 'vix_data'
        
        # Create directories
        self.data_dir.mkdir(parents=True, exist_ok=True)
        
        # Working VIX securities from discovery
        self.vix_securities = {
            'vix_spot': 'VIX3 Index',  # Cboe Volatility Index
            'vix_front_month': 'CBOE VIX1 Index',  # Generic 1st VX Future
            'vix_etn': 'VIX2 Index'  # VIX ETN for reference
        }
        
        # Try to find additional VIX futures generics
        self.additional_vix_futures = [
            'CBOE VIX2 Index',  # Generic 2nd VX Future
            'CBOE VIX3 Index',  # Generic 3rd VX Future
            'CBOE VIX4 Index',  # Generic 4th VX Future
        ]
        
        # Essential fields for analysis
        self.price_fields = {
            'last_price': 'PX_LAST',
            'open_price': 'PX_OPEN',
            'high_price': 'PX_HIGH',
            'low_price': 'PX_LOW',
            'volume': 'PX_VOLUME'
        }
        
        self.futures_fields = {
            'last_price': 'PX_LAST',
            'settle_price': 'PX_SETTLE',
            'volume': 'PX_VOLUME',
            'open_interest': 'OPEN_INT'
        }
        
        # Date range
        self.end_date = datetime.now()
        self.start_date = self.end_date - timedelta(days=years_back*365 + 3)
        
        print(f"🔥 Corrected VIX Data Collection")
        print(f"📅 Period: {self.start_date.strftime('%Y-%m-%d')} to {self.end_date.strftime('%Y-%m-%d')}")
        print(f"💾 Output: {self.data_dir}")
    
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
            print("✅ Bloomberg connection established")
            return True
            
        except Exception as e:
            print(f"❌ Bloomberg connection failed: {e}")
            return False
    
    def test_additional_futures(self):
        """Test for additional VIX futures generics"""
        print("🔍 Testing for additional VIX futures...")
        
        working_futures = []
        
        for ticker in self.additional_vix_futures:
            try:
                request = self.refDataService.createRequest("ReferenceDataRequest")
                request.getElement("securities").appendValue(ticker)
                request.getElement("fields").appendValue("PX_LAST")
                request.getElement("fields").appendValue("NAME")
                
                self.session.sendRequest(request)
                event = self.session.nextEvent(5000)
                
                if event.eventType() == blpapi.Event.RESPONSE:
                    for msg in event:
                        securityData = msg.getElement("securityData")
                        security = securityData.getValue(0)
                        
                        if not security.hasElement("securityError") and security.hasElement("fieldData"):
                            fieldData = security.getElement("fieldData")
                            name = "Unknown"
                            price = "N/A"
                            
                            if fieldData.hasElement("NAME"):
                                name = fieldData.getElement("NAME").getValue()
                            if fieldData.hasElement("PX_LAST"):
                                price = fieldData.getElement("PX_LAST").getValue()
                            
                            print(f"   ✅ {ticker}: {name} (Price: {price})")
                            working_futures.append(ticker)
                        else:
                            print(f"   ❌ {ticker}: Not available")
                            
            except Exception as e:
                print(f"   ❌ {ticker}: Error - {e}")
        
        return working_futures
    
    def get_historical_data(self, ticker, fields, data_type):
        """
        Get historical data for a specific ticker
        """
        print(f"📊 Collecting {data_type} data for {ticker}...")
        
        try:
            request = self.refDataService.createRequest("HistoricalDataRequest")
            request.getElement("securities").appendValue(ticker)
            
            bloomberg_fields = list(fields.values())
            for field in bloomberg_fields:
                request.getElement("fields").appendValue(field)
            
            request.set("startDate", self.start_date.strftime('%Y%m%d'))
            request.set("endDate", self.end_date.strftime('%Y%m%d'))
            request.set("periodicitySelection", "DAILY")
            
            self.session.sendRequest(request)
            
            # Process response
            all_data = []
            while True:
                event = self.session.nextEvent(15000)  # 15 second timeout
                
                if event.eventType() == blpapi.Event.RESPONSE:
                    for msg in event:
                        securityData = msg.getElement("securityData")
                        
                        if securityData.hasElement("securityError"):
                            print(f"   ❌ Error for {ticker}")
                            return pd.DataFrame()
                        
                        fieldDataArray = securityData.getElement("fieldData")
                        
                        for i in range(fieldDataArray.numValues()):
                            fieldData = fieldDataArray.getValue(i)
                            data_date = fieldData.getElement("date").getValue()
                            
                            row_data = {
                                'date': data_date.strftime('%Y-%m-%d'),
                                'ticker': ticker,
                                'data_type': data_type
                            }
                            
                            # Extract field data
                            for clean_name, bloomberg_field in fields.items():
                                if fieldData.hasElement(bloomberg_field):
                                    value = fieldData.getElement(bloomberg_field).getValue()
                                    row_data[clean_name] = value if value is not None else np.nan
                                else:
                                    row_data[clean_name] = np.nan
                            
                            all_data.append(row_data)
                    break
                
                if event.eventType() == blpapi.Event.TIMEOUT:
                    print(f"   ⏱️ Timeout for {ticker}")
                    break
            
            df = pd.DataFrame(all_data)
            print(f"   ✅ Collected {len(df)} records for {ticker}")
            return df
            
        except Exception as e:
            print(f"   ❌ Error collecting {ticker}: {e}")
            return pd.DataFrame()
    
    def test_vix_options_with_known_format(self):
        """
        Test VIX options using the format that generated 21,430 records
        Try to find a working option to understand the correct format
        """
        print("🔍 Testing VIX options format discovery...")
        
        # Generate some current/near-term option tickers to test
        current_date = datetime.now()
        
        # Try next few monthly expiries
        test_dates = []
        for i in range(6):  # Next 6 months
            if current_date.month + i <= 12:
                test_month = current_date.month + i
                test_year = current_date.year
            else:
                test_month = (current_date.month + i) % 12
                test_year = current_date.year + 1
            
            # Find 3rd Wednesday
            first_day = date(test_year, test_month, 1)
            first_weekday = first_day.weekday()
            days_to_first_wed = (2 - first_weekday) % 7
            first_wed = first_day + timedelta(days=days_to_first_wed)
            third_wed = first_wed + timedelta(days=14)
            
            test_dates.append(third_wed)
        
        # Try multiple option formats
        option_formats = [
            "VIX {date} C{strike} Index",
            "VIX3 {date} C{strike} Index",
            "CBOE VIX {date} C{strike} Index",
        ]
        
        strikes = [15, 16, 17, 18, 19, 20, 25, 30]
        working_options = []
        
        for test_date in test_dates[:2]:  # Test first 2 months only
            date_str = test_date.strftime('%m/%d/%y')
            
            for fmt in option_formats:
                for strike in strikes:
                    option_ticker = fmt.format(date=date_str, strike=strike)
                    
                    try:
                        request = self.refDataService.createRequest("ReferenceDataRequest")
                        request.getElement("securities").appendValue(option_ticker)
                        request.getElement("fields").appendValue("PX_LAST")
                        
                        self.session.sendRequest(request)
                        event = self.session.nextEvent(3000)
                        
                        if event.eventType() == blpapi.Event.RESPONSE:
                            for msg in event:
                                securityData = msg.getElement("securityData")
                                security = securityData.getValue(0)
                                
                                if not security.hasElement("securityError") and security.hasElement("fieldData"):
                                    print(f"   ✅ Working option format found: {option_ticker}")
                                    working_options.append(option_ticker)
                                    if len(working_options) >= 3:  # Found enough examples
                                        return working_options
                    except:
                        continue
        
        return working_options
    
    def save_corrected_data(self, vix_data_dict):
        """
        Save the corrected VIX data
        """
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        try:
            files_created = []
            
            # Save each dataset
            for data_name, df in vix_data_dict.items():
                if len(df) > 0:
                    filename = f'vix_{data_name}_corrected_{timestamp}.csv'
                    file_path = self.data_dir / filename
                    df.to_csv(file_path, index=False)
                    files_created.append(str(file_path))
                    print(f"✅ {data_name}: {len(df)} records → {filename}")
            
            # Create summary
            summary = {
                'collection_timestamp': timestamp,
                'collection_type': 'corrected_vix_data',
                'data_period': {
                    'start_date': self.start_date.strftime('%Y-%m-%d'),
                    'end_date': self.end_date.strftime('%Y-%m-%d')
                },
                'working_tickers': self.vix_securities,
                'data_summary': {
                    data_name: len(df) for data_name, df in vix_data_dict.items()
                },
                'files_created': files_created
            }
            
            summary_file = self.data_dir / f'vix_corrected_summary_{timestamp}.json'
            with open(summary_file, 'w') as f:
                json.dump(summary, f, indent=2)
            
            print(f"✅ Summary: {summary_file.name}")
            return summary
            
        except Exception as e:
            print(f"❌ Failed to save data: {e}")
            return None
    
    def run_corrected_collection(self):
        """
        Run data collection with corrected tickers
        """
        print("🚀 Starting corrected VIX data collection...")
        
        try:
            # Connect
            if not self.connect():
                return False
            
            # Test for additional futures
            additional_futures = self.test_additional_futures()
            
            # Collect data for working securities
            vix_data = {}
            
            # VIX Spot (VIX3 Index)
            print(f"\n📊 Collecting VIX Spot data...")
            vix_spot_df = self.get_historical_data(
                self.vix_securities['vix_spot'], 
                self.price_fields, 
                'VIX_Spot'
            )
            if len(vix_spot_df) > 0:
                vix_data['spot'] = vix_spot_df
            
            # VIX Front Month Futures (CBOE VIX1 Index)
            print(f"\n📊 Collecting VIX Front Month Futures...")
            vix_front_df = self.get_historical_data(
                self.vix_securities['vix_front_month'], 
                self.futures_fields, 
                'VIX_Front_Month_Future'
            )
            if len(vix_front_df) > 0:
                vix_data['front_month_future'] = vix_front_df
            
            # Additional futures if available
            if additional_futures:
                for ticker in additional_futures:
                    data_type = f'VIX_Future_{ticker.split()[-1]}'  # Extract the number
                    print(f"\n📊 Collecting {ticker}...")
                    future_df = self.get_historical_data(ticker, self.futures_fields, data_type)
                    if len(future_df) > 0:
                        vix_data[f'future_{ticker.replace(" ", "_").lower()}'] = future_df
            
            # Test options format
            print(f"\n🔍 Testing VIX options...")
            working_options = self.test_vix_options_with_known_format()
            if working_options:
                print(f"Found working options format! Examples:")
                for opt in working_options[:3]:
                    print(f"   {opt}")
            else:
                print("No working VIX options format found")
            
            # Save data
            if vix_data:
                print(f"\n💾 Saving corrected VIX data...")
                summary = self.save_corrected_data(vix_data)
                
                if summary:
                    print("\n🎉 Corrected VIX data collection completed!")
                    print("=" * 60)
                    for data_name, count in summary['data_summary'].items():
                        print(f"📊 {data_name}: {count:,} records")
                    print(f"📁 Files saved to: {self.data_dir}")
                    return True
            else:
                print("❌ No data collected")
                return False
                
        except Exception as e:
            print(f"💥 Collection failed: {e}")
            return False
        
        finally:
            self.disconnect()
    
    def disconnect(self):
        """Disconnect from Bloomberg"""
        if self.session:
            self.session.stop()
            print("✅ Bloomberg session disconnected")

def main():
    """Main execution"""
    print("=" * 70)
    print("CORRECTED VIX DATA COLLECTION - PROPER BLOOMBERG TICKERS")
    print("=" * 70)
    print(f"Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Create fetcher
    fetcher = CorrectedVIXDataFetcher(years_back=10)
    
    # Run corrected collection
    success = fetcher.run_corrected_collection()
    
    if success:
        print("\n🎊 Success! You now have proper VIX data for analysis")
        print("💡 Next: Copy files to shared drive and start analysis")
    else:
        print("\n💥 Collection failed - check Bloomberg connection")
    
    return success

if __name__ == "__main__":
    main()
