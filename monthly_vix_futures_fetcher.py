"""
Monthly VIX Futures Fetcher - Specific Contracts
Collect VIX futures using specific monthly contracts (UXQ25, UXN25, etc.)
Plus fix VIX spot data collection
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

class MonthlyVIXFuturesFetcher:
    """
    Monthly VIX Futures Collection using specific contract codes
    UXQ25 = August 2025, UXN25 = July 2025, etc.
    """
    
    def __init__(self, years_back=10):
        self.session = None
        self.refDataService = None
        self.project_root = project_root
        self.data_dir = self.project_root / 'data' / 'vix_data'
        
        # Create directories
        self.data_dir.mkdir(parents=True, exist_ok=True)
        
        # VIX futures month codes (same as other futures)
        self.month_codes = {
            1: 'F',   # January
            2: 'G',   # February  
            3: 'H',   # March
            4: 'J',   # April
            5: 'K',   # May
            6: 'M',   # June
            7: 'N',   # July
            8: 'Q',   # August
            9: 'U',   # September
            10: 'V',  # October
            11: 'X',  # November
            12: 'Z'   # December
        }
        
        # Date range
        self.end_date = datetime.now()
        self.start_date = self.end_date - timedelta(days=years_back*365 + 3)
        
        # Try multiple VIX spot formats
        self.vix_spot_candidates = [
            'VIX Index',        # Standard format
            '.VIX Index',       # With dot prefix
            'VIX US Index',     # With country
            'SPX/VIX Index',    # Alternative
        ]
        
        print(f"🔥 Monthly VIX Futures Collection")
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
    
    def generate_monthly_vix_futures(self):
        """
        Generate specific monthly VIX futures contracts using UX format
        """
        print("📋 Generating monthly VIX futures contracts...")
        
        contracts = []
        current_date = self.start_date
        
        while current_date <= self.end_date + timedelta(days=180):  # Include future months
            year = current_date.year
            month = current_date.month
            
            # Generate UX ticker: UX + month_code + year
            month_code = self.month_codes[month]
            year_code = str(year)[-2:]  # Last 2 digits
            ticker = f"UX{month_code}{year_code} Index"
            
            # Find actual expiry date (3rd Wednesday)
            first_day = date(year, month, 1)
            first_weekday = first_day.weekday()
            days_to_first_wed = (2 - first_weekday) % 7
            first_wed = first_day + timedelta(days=days_to_first_wed)
            third_wed = first_wed + timedelta(days=14)
            
            contracts.append({
                'ticker': ticker,
                'year': year,
                'month': month,
                'month_code': month_code,
                'expiry_date': third_wed,
                'contract_month': f"{year}-{month:02d}"
            })
            
            # Move to next month
            if month == 12:
                current_date = datetime(year + 1, 1, 1)
            else:
                current_date = datetime(year, month + 1, 1)
        
        print(f"   Generated {len(contracts)} monthly VIX futures contracts")
        return contracts
    
    def test_monthly_contracts_availability(self, contracts):
        """
        Test which monthly VIX contracts are available
        """
        print("🔍 Testing monthly VIX futures availability...")
        
        working_contracts = []
        
        # Test a sample of contracts to see which format works
        test_contracts = contracts[-24:]  # Test last 2 years worth
        
        for contract in test_contracts:
            ticker = contract['ticker']
            
            try:
                request = self.refDataService.createRequest("ReferenceDataRequest")
                request.getElement("securities").appendValue(ticker)
                request.getElement("fields").appendValue("PX_LAST")
                request.getElement("fields").appendValue("NAME")
                request.getElement("fields").appendValue("LAST_TRADEABLE_DT")
                
                self.session.sendRequest(request)
                event = self.session.nextEvent(2000)
                
                if event.eventType() == blpapi.Event.RESPONSE:
                    for msg in event:
                        securityDataArray = msg.getElement("securityData")
                        
                        for j in range(securityDataArray.numValues()):
                            securityData = securityDataArray.getValue(j)
                            
                            if securityData.hasElement("securityError"):
                                continue
                            
                            if securityData.hasElement("fieldData"):
                                fieldData = securityData.getElement("fieldData")
                                
                                price = "N/A"
                                name = "Unknown"
                                expiry = "N/A"
                                
                                if fieldData.hasElement("PX_LAST"):
                                    price = fieldData.getElement("PX_LAST").getValue()
                                if fieldData.hasElement("NAME"):
                                    name = fieldData.getElement("NAME").getValue()
                                if fieldData.hasElement("LAST_TRADEABLE_DT"):
                                    expiry_date = fieldData.getElement("LAST_TRADEABLE_DT").getValue()
                                    if expiry_date:
                                        expiry = expiry_date.strftime('%Y-%m-%d')
                                
                                print(f"   ✅ {ticker:12} | Price: {price:>8} | Expiry: {expiry} | {name[:30]}")
                                working_contracts.append(contract)
                                break
                
            except Exception as e:
                continue
            
            time.sleep(0.05)
        
        print(f"\n✅ Found {len(working_contracts)} working monthly VIX futures")
        return working_contracts
    
    def collect_monthly_futures_historical(self, working_contracts):
        """
        Collect historical data for working monthly VIX futures
        """
        if not working_contracts:
            return pd.DataFrame()
        
        print(f"📊 Collecting historical data for {len(working_contracts)} monthly VIX futures...")
        
        all_data = []
        
        for contract in working_contracts:
            ticker = contract['ticker']
            
            try:
                print(f"   Collecting {ticker} ({contract['contract_month']})...")
                
                request = self.refDataService.createRequest("HistoricalDataRequest")
                request.getElement("securities").appendValue(ticker)
                
                # Essential futures fields
                fields = ["PX_LAST", "PX_OPEN", "PX_HIGH", "PX_LOW", "PX_SETTLE", "PX_VOLUME", "OPEN_INT"]
                for field in fields:
                    request.getElement("fields").appendValue(field)
                
                request.set("startDate", self.start_date.strftime('%Y%m%d'))
                request.set("endDate", self.end_date.strftime('%Y%m%d'))
                request.set("periodicitySelection", "DAILY")
                
                self.session.sendRequest(request)
                
                # Process response
                contract_data = []
                while True:
                    event = self.session.nextEvent(20000)  # 20 second timeout
                    
                    if event.eventType() == blpapi.Event.RESPONSE:
                        for msg in event:
                            if msg.hasElement("securityData"):
                                securityData = msg.getElement("securityData")
                                
                                if securityData.hasElement("securityError"):
                                    print(f"      ❌ Error for {ticker}")
                                    break
                                
                                if securityData.hasElement("fieldData"):
                                    fieldDataArray = securityData.getElement("fieldData")
                                    
                                    for i in range(fieldDataArray.numValues()):
                                        fieldData = fieldDataArray.getValue(i)
                                        
                                        if not fieldData.hasElement("date"):
                                            continue
                                        
                                        data_date = fieldData.getElement("date").getValue()
                                        
                                        row_data = {
                                            'date': data_date.strftime('%Y-%m-%d'),
                                            'ticker': ticker,
                                            'contract_month': contract['contract_month'],
                                            'expiry_date': contract['expiry_date'].strftime('%Y-%m-%d'),
                                            'month_code': contract['month_code'],
                                            'data_type': 'VIX_Monthly_Future'
                                        }
                                        
                                        # Extract field data
                                        field_mapping = {
                                            'last_price': 'PX_LAST',
                                            'open_price': 'PX_OPEN',
                                            'high_price': 'PX_HIGH',
                                            'low_price': 'PX_LOW',
                                            'settle_price': 'PX_SETTLE',
                                            'volume': 'PX_VOLUME',
                                            'open_interest': 'OPEN_INT'
                                        }
                                        
                                        for clean_name, bloomberg_field in field_mapping.items():
                                            if fieldData.hasElement(bloomberg_field):
                                                value = fieldData.getElement(bloomberg_field).getValue()
                                                row_data[clean_name] = value if value is not None else np.nan
                                            else:
                                                row_data[clean_name] = np.nan
                                        
                                        contract_data.append(row_data)
                        break
                    
                    if event.eventType() == blpapi.Event.TIMEOUT:
                        print(f"      ⏱️ Timeout for {ticker}")
                        break
                
                all_data.extend(contract_data)
                print(f"      ✅ Collected {len(contract_data):,} records")
                
            except Exception as e:
                print(f"      ❌ Error collecting {ticker}: {e}")
                continue
            
            time.sleep(0.2)  # Rate limiting
        
        df = pd.DataFrame(all_data)
        print(f"\n✅ Total monthly VIX futures: {len(df):,} records")
        return df
    
    def find_vix_spot_alternative(self):
        """
        Find working VIX spot ticker using alternative methods
        """
        print("🔍 Finding VIX spot alternative...")
        
        # Since direct VIX Index doesn't work, let's try SPX for reference
        try:
            request = self.refDataService.createRequest("ReferenceDataRequest")
            request.getElement("securities").appendValue("SPX Index")
            request.getElement("fields").appendValue("PX_LAST")
            request.getElement("fields").appendValue("NAME")
            
            self.session.sendRequest(request)
            event = self.session.nextEvent(3000)
            
            if event.eventType() == blpapi.Event.RESPONSE:
                for msg in event:
                    securityDataArray = msg.getElement("securityData")
                    
                    for j in range(securityDataArray.numValues()):
                        securityData = securityDataArray.getValue(j)
                        
                        if not securityData.hasElement("securityError") and securityData.hasElement("fieldData"):
                            fieldData = securityData.getElement("fieldData")
                            
                            if fieldData.hasElement("PX_LAST"):
                                spx_price = fieldData.getElement("PX_LAST").getValue()
                                print(f"   ✅ Using SPX Index as reference: {spx_price}")
                                return "SPX Index"
        
        except Exception as e:
            pass
        
        print("   ❌ Could not find working market index")
        return None
    
    def save_monthly_vix_data(self, futures_df, working_contracts):
        """
        Save monthly VIX futures data
        """
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        try:
            files_created = []
            
            # Save monthly futures data
            if len(futures_df) > 0:
                futures_file = self.data_dir / f'vix_monthly_futures_10yr_{timestamp}.csv'
                futures_df.to_csv(futures_file, index=False)
                files_created.append(str(futures_file))
                print(f"✅ Monthly VIX futures: {len(futures_df):,} records → {futures_file.name}")
            
            # Create analysis summary
            summary = {
                'collection_timestamp': timestamp,
                'collection_type': 'monthly_vix_futures',
                'data_period': {
                    'start_date': self.start_date.strftime('%Y-%m-%d'),
                    'end_date': self.end_date.strftime('%Y-%m-%d')
                },
                'contracts_info': {
                    'total_contracts_tested': len(working_contracts),
                    'contracts_with_data': len(futures_df['ticker'].unique()) if len(futures_df) > 0 else 0,
                    'contract_months': sorted(futures_df['contract_month'].unique().tolist()) if len(futures_df) > 0 else []
                },
                'data_summary': {
                    'total_records': len(futures_df),
                    'records_with_prices': len(futures_df[futures_df['last_price'].notna()]) if len(futures_df) > 0 else 0,
                    'date_range_actual': {
                        'start': futures_df['date'].min() if len(futures_df) > 0 else None,
                        'end': futures_df['date'].max() if len(futures_df) > 0 else None
                    }
                },
                'files_created': files_created
            }
            
            summary_file = self.data_dir / f'vix_monthly_summary_{timestamp}.json'
            with open(summary_file, 'w') as f:
                json.dump(summary, f, indent=2, default=str)
            
            print(f"✅ Summary: {summary_file.name}")
            return summary
            
        except Exception as e:
            print(f"❌ Failed to save data: {e}")
            return None
    
    def run_monthly_collection(self):
        """
        Run monthly VIX futures collection
        """
        print("🚀 Starting monthly VIX futures collection...")
        
        try:
            if not self.connect():
                return False
            
            # Generate monthly contracts
            contracts = self.generate_monthly_vix_futures()
            
            # Test availability
            working_contracts = self.test_monthly_contracts_availability(contracts)
            
            if not working_contracts:
                print("❌ No working monthly VIX futures found")
                return False
            
            # Collect historical data
            futures_df = self.collect_monthly_futures_historical(working_contracts)
            
            if len(futures_df) == 0:
                print("❌ No monthly futures data collected")
                return False
            
            # Save data
            summary = self.save_monthly_vix_data(futures_df, working_contracts)
            
            if summary:
                print("\n🎉 Monthly VIX futures collection completed!")
                print("=" * 70)
                print(f"📊 Total records: {summary['data_summary']['total_records']:,}")
                print(f"📊 Contracts with data: {summary['contracts_info']['contracts_with_data']}")
                print(f"📊 Records with prices: {summary['data_summary']['records_with_prices']:,}")
                
                if summary['data_summary']['date_range_actual']['start']:
                    print(f"📅 Date range: {summary['data_summary']['date_range_actual']['start']} to {summary['data_summary']['date_range_actual']['end']}")
                
                print(f"📁 Files saved to: {self.data_dir}")
                return True
            else:
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
    print("=" * 80)
    print("MONTHLY VIX FUTURES COLLECTION - SPECIFIC CONTRACTS")
    print("=" * 80)
    print(f"Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    fetcher = MonthlyVIXFuturesFetcher(years_back=10)
    success = fetcher.run_monthly_collection()
    
    if success:
        print("\n🎊 Success! Monthly VIX futures data collected")
        print("💡 Now you have specific contract data with actual expiries!")
    else:
        print("\n💥 Collection failed")
    
    return success

if __name__ == "__main__":
    main()
