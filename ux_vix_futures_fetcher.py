"""
UX VIX Futures Data Fetcher
Collect VIX futures using the UX format (UX1 Index, UX2 Index, etc.)
Based on user finding: "ux1 index des" shows VIX futures in Bloomberg terminal
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

class UXVIXFuturesFetcher:
    """
    UX VIX Futures Data Collection
    Use UX1 Index, UX2 Index format for VIX futures
    """
    
    def __init__(self):
        self.session = None
        self.refDataService = None
        self.project_root = project_root
        self.data_dir = self.project_root / 'data' / 'vix_data'
        
        # Create directories
        self.data_dir.mkdir(parents=True, exist_ok=True)
        
        # UX VIX futures format
        self.ux_futures = [
            'UX1 Index',   # 1st month VIX future
            'UX2 Index',   # 2nd month VIX future
            'UX3 Index',   # 3rd month VIX future
            'UX4 Index',   # 4th month VIX future
            'UX5 Index',   # 5th month VIX future
            'UX6 Index',   # 6th month VIX future
            'UX7 Index',   # 7th month VIX future
            'UX8 Index',   # 8th month VIX future
            'UX9 Index',   # 9th month VIX future
        ]
        
        # Also test VIX spot
        self.vix_spot = 'VIX1 Index'  # From our discovery
        
        print(f"🔥 UX VIX Futures Data Collection")
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
    
    def test_ux_futures_availability(self):
        """
        Test which UX VIX futures are available
        """
        print("🔍 Testing UX VIX futures availability...")
        
        working_futures = []
        futures_info = {}
        
        # Test UX futures
        for ticker in self.ux_futures:
            try:
                request = self.refDataService.createRequest("ReferenceDataRequest")
                request.getElement("securities").appendValue(ticker)
                
                # Test comprehensive futures fields
                fields = [
                    "PX_LAST", "NAME", "SECURITY_TYP", "CRNCY", "EXCH_CODE",
                    "LAST_TRADEABLE_DT", "DAYS_TO_EXP", "PX_SETTLE",
                    "PX_VOLUME", "OPEN_INT", "CONTRACT_VALUE"
                ]
                
                for field in fields:
                    request.getElement("fields").appendValue(field)
                
                self.session.sendRequest(request)
                event = self.session.nextEvent(3000)
                
                if event.eventType() == blpapi.Event.RESPONSE:
                    for msg in event:
                        securityDataArray = msg.getElement("securityData")
                        
                        for j in range(securityDataArray.numValues()):
                            securityData = securityDataArray.getValue(j)
                            
                            if securityData.hasElement("securityError"):
                                continue
                            
                            if securityData.hasElement("fieldData"):
                                fieldData = securityData.getElement("fieldData")
                                
                                info = {'ticker': ticker}
                                
                                # Extract all available info
                                if fieldData.hasElement("PX_LAST"):
                                    info['price'] = fieldData.getElement("PX_LAST").getValue()
                                if fieldData.hasElement("NAME"):
                                    info['name'] = fieldData.getElement("NAME").getValue()
                                if fieldData.hasElement("SECURITY_TYP"):
                                    info['security_type'] = fieldData.getElement("SECURITY_TYP").getValue()
                                if fieldData.hasElement("CRNCY"):
                                    info['currency'] = fieldData.getElement("CRNCY").getValue()
                                if fieldData.hasElement("EXCH_CODE"):
                                    info['exchange'] = fieldData.getElement("EXCH_CODE").getValue()
                                if fieldData.hasElement("LAST_TRADEABLE_DT"):
                                    info['expiry_date'] = fieldData.getElement("LAST_TRADEABLE_DT").getValue()
                                if fieldData.hasElement("DAYS_TO_EXP"):
                                    info['days_to_expiry'] = fieldData.getElement("DAYS_TO_EXP").getValue()
                                if fieldData.hasElement("PX_VOLUME"):
                                    info['volume'] = fieldData.getElement("PX_VOLUME").getValue()
                                if fieldData.hasElement("OPEN_INT"):
                                    info['open_interest'] = fieldData.getElement("OPEN_INT").getValue()
                                
                                working_futures.append(ticker)
                                futures_info[ticker] = info
                                
                                # Display info
                                price = info.get('price', 'N/A')
                                name = info.get('name', 'Unknown')[:40]
                                currency = info.get('currency', 'Unknown')
                                days_to_exp = info.get('days_to_expiry', 'N/A')
                                
                                print(f"   ✅ {ticker:12} | {price:>8} | {currency} | Days: {days_to_exp:>3} | {name}")
                                break
                
            except Exception as e:
                continue
            
            time.sleep(0.05)
        
        print(f"\n✅ Found {len(working_futures)} working UX VIX futures")
        return working_futures, futures_info
    
    def collect_vix_spot_data(self):
        """
        Collect VIX spot data for reference
        """
        print(f"📊 Collecting VIX spot data ({self.vix_spot})...")
        
        try:
            request = self.refDataService.createRequest("ReferenceDataRequest")
            request.getElement("securities").appendValue(self.vix_spot)
            
            fields = ["PX_LAST", "PX_OPEN", "PX_HIGH", "PX_LOW", "NAME"]
            for field in fields:
                request.getElement("fields").appendValue(field)
            
            self.session.sendRequest(request)
            event = self.session.nextEvent(5000)
            
            spot_data = None
            
            if event.eventType() == blpapi.Event.RESPONSE:
                for msg in event:
                    securityDataArray = msg.getElement("securityData")
                    
                    for j in range(securityDataArray.numValues()):
                        securityData = securityDataArray.getValue(j)
                        
                        if not securityData.hasElement("securityError") and securityData.hasElement("fieldData"):
                            fieldData = securityData.getElement("fieldData")
                            
                            spot_data = {
                                'collection_date': datetime.now().strftime('%Y-%m-%d'),
                                'ticker': self.vix_spot,
                                'data_type': 'VIX_Spot'
                            }
                            
                            if fieldData.hasElement("PX_LAST"):
                                spot_data['vix_level'] = fieldData.getElement("PX_LAST").getValue()
                            if fieldData.hasElement("PX_OPEN"):
                                spot_data['vix_open'] = fieldData.getElement("PX_OPEN").getValue()
                            if fieldData.hasElement("PX_HIGH"):
                                spot_data['vix_high'] = fieldData.getElement("PX_HIGH").getValue()
                            if fieldData.hasElement("PX_LOW"):
                                spot_data['vix_low'] = fieldData.getElement("PX_LOW").getValue()
                            if fieldData.hasElement("NAME"):
                                spot_data['name'] = fieldData.getElement("NAME").getValue()
                            
                            vix_level = spot_data.get('vix_level', 'N/A')
                            print(f"   ✅ VIX Spot Level: {vix_level}")
                            break
            
            return spot_data
            
        except Exception as e:
            print(f"   ❌ Error collecting VIX spot: {e}")
            return None
    
    def collect_ux_futures_current_data(self, working_futures):
        """
        Collect current data for UX VIX futures
        """
        if not working_futures:
            return pd.DataFrame()
        
        print(f"📊 Collecting current data for {len(working_futures)} UX VIX futures...")
        
        all_data = []
        
        for ticker in working_futures:
            try:
                request = self.refDataService.createRequest("ReferenceDataRequest")
                request.getElement("securities").appendValue(ticker)
                
                # Comprehensive VIX futures fields
                fields = [
                    "PX_LAST", "PX_OPEN", "PX_HIGH", "PX_LOW", "PX_SETTLE",
                    "PX_VOLUME", "OPEN_INT", "NAME", "CRNCY", "EXCH_CODE",
                    "LAST_TRADEABLE_DT", "DAYS_TO_EXP", "CONTRACT_VALUE"
                ]
                
                for field in fields:
                    request.getElement("fields").appendValue(field)
                
                self.session.sendRequest(request)
                event = self.session.nextEvent(5000)
                
                if event.eventType() == blpapi.Event.RESPONSE:
                    for msg in event:
                        securityDataArray = msg.getElement("securityData")
                        
                        for j in range(securityDataArray.numValues()):
                            securityData = securityDataArray.getValue(j)
                            
                            if securityData.hasElement("securityError"):
                                continue
                            
                            if securityData.hasElement("fieldData"):
                                fieldData = securityData.getElement("fieldData")
                                
                                row_data = {
                                    'collection_date': datetime.now().strftime('%Y-%m-%d'),
                                    'ticker': ticker,
                                    'data_type': 'VIX_Future_UX'
                                }
                                
                                # Map Bloomberg fields
                                field_mapping = {
                                    'last_price': 'PX_LAST',
                                    'open_price': 'PX_OPEN',
                                    'high_price': 'PX_HIGH',
                                    'low_price': 'PX_LOW',
                                    'settle_price': 'PX_SETTLE',
                                    'volume': 'PX_VOLUME',
                                    'open_interest': 'OPEN_INT',
                                    'name': 'NAME',
                                    'currency': 'CRNCY',
                                    'exchange': 'EXCH_CODE',
                                    'expiry_date': 'LAST_TRADEABLE_DT',
                                    'days_to_expiry': 'DAYS_TO_EXP',
                                    'contract_value': 'CONTRACT_VALUE'
                                }
                                
                                for clean_name, bloomberg_field in field_mapping.items():
                                    if fieldData.hasElement(bloomberg_field):
                                        value = fieldData.getElement(bloomberg_field).getValue()
                                        if bloomberg_field == 'LAST_TRADEABLE_DT' and value:
                                            try:
                                                row_data[clean_name] = value.strftime('%Y-%m-%d')
                                            except:
                                                row_data[clean_name] = str(value)
                                        else:
                                            row_data[clean_name] = value if value is not None else np.nan
                                    else:
                                        row_data[clean_name] = np.nan
                                
                                all_data.append(row_data)
                                break
                
            except Exception as e:
                print(f"   ❌ Error collecting {ticker}: {e}")
                continue
            
            time.sleep(0.1)
        
        df = pd.DataFrame(all_data)
        print(f"✅ Collected data for {len(df)} UX VIX futures")
        return df
    
    def analyze_vix_term_structure(self, futures_df, spot_data):
        """
        Analyze VIX futures term structure
        """
        print("📊 Analyzing VIX term structure...")
        
        analysis = {
            'collection_timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'vix_spot_level': None,
            'futures_count': len(futures_df),
            'term_structure': {}
        }
        
        # Add VIX spot level
        if spot_data and 'vix_level' in spot_data:
            analysis['vix_spot_level'] = float(spot_data['vix_level'])
            print(f"   VIX Spot: {analysis['vix_spot_level']:.2f}")
        
        if len(futures_df) > 0:
            # Sort by days to expiry
            valid_futures = futures_df.dropna(subset=['last_price', 'days_to_expiry']).copy()
            
            if len(valid_futures) > 0:
                valid_futures = valid_futures.sort_values('days_to_expiry')
                
                # Build term structure
                for _, row in valid_futures.iterrows():
                    analysis['term_structure'][row['ticker']] = {
                        'price': float(row['last_price']),
                        'days_to_expiry': int(row['days_to_expiry']),
                        'volume': float(row['volume']) if pd.notna(row['volume']) else 0
                    }
                
                # Calculate market structure
                futures_prices = valid_futures['last_price'].tolist()
                if analysis['vix_spot_level'] and len(futures_prices) > 0:
                    front_future = futures_prices[0]
                    if front_future > analysis['vix_spot_level']:
                        analysis['market_structure'] = 'Contango'
                        analysis['contango_level'] = front_future - analysis['vix_spot_level']
                    else:
                        analysis['market_structure'] = 'Backwardation'
                        analysis['backwardation_level'] = analysis['vix_spot_level'] - front_future
                
                print(f"   Futures Chain: {len(valid_futures)} contracts")
                if 'market_structure' in analysis:
                    print(f"   Market Structure: {analysis['market_structure']}")
                    if 'contango_level' in analysis:
                        print(f"   Contango Level: +{analysis['contango_level']:.2f}")
                    if 'backwardation_level' in analysis:
                        print(f"   Backwardation Level: -{analysis['backwardation_level']:.2f}")
        
        return analysis
    
    def save_ux_vix_data(self, futures_df, spot_data, analysis):
        """
        Save UX VIX futures data
        """
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        try:
            files_created = []
            
            # Save futures data
            if len(futures_df) > 0:
                futures_file = self.data_dir / f'vix_ux_futures_{timestamp}.csv'
                futures_df.to_csv(futures_file, index=False)
                files_created.append(str(futures_file))
                print(f"✅ UX VIX futures: {len(futures_df)} records → {futures_file.name}")
            
            # Save spot data
            if spot_data:
                spot_df = pd.DataFrame([spot_data])
                spot_file = self.data_dir / f'vix_spot_{timestamp}.csv'
                spot_df.to_csv(spot_file, index=False)
                files_created.append(str(spot_file))
                print(f"✅ VIX spot: 1 record → {spot_file.name}")
            
            # Save analysis
            summary = {
                'collection_timestamp': timestamp,
                'collection_type': 'ux_vix_futures',
                'term_structure_analysis': analysis,
                'data_summary': {
                    'futures_collected': len(futures_df),
                    'spot_collected': 1 if spot_data else 0,
                    'total_records': len(futures_df) + (1 if spot_data else 0)
                },
                'files_created': files_created
            }
            
            summary_file = self.data_dir / f'vix_ux_summary_{timestamp}.json'
            with open(summary_file, 'w') as f:
                json.dump(summary, f, indent=2, default=str)
            
            print(f"✅ Summary: {summary_file.name}")
            return summary
            
        except Exception as e:
            print(f"❌ Failed to save data: {e}")
            return None
    
    def run_ux_collection(self):
        """
        Run UX VIX futures collection
        """
        print("🚀 Starting UX VIX futures collection...")
        
        try:
            if not self.connect():
                return False
            
            # Test UX futures availability
            working_futures, futures_info = self.test_ux_futures_availability()
            
            if not working_futures:
                print("❌ No UX VIX futures found")
                return False
            
            # Collect VIX spot
            spot_data = self.collect_vix_spot_data()
            
            # Collect UX futures data
            futures_df = self.collect_ux_futures_current_data(working_futures)
            
            if len(futures_df) == 0:
                print("❌ No UX futures data collected")
                return False
            
            # Analyze term structure
            analysis = self.analyze_vix_term_structure(futures_df, spot_data)
            
            # Save data
            summary = self.save_ux_vix_data(futures_df, spot_data, analysis)
            
            if summary:
                print("\n🎉 UX VIX futures collection completed successfully!")
                print("=" * 60)
                print(f"📊 UX VIX futures: {len(futures_df)}")
                print(f"📊 VIX spot: {'✅' if spot_data else '❌'}")
                
                if analysis.get('vix_spot_level'):
                    print(f"📈 VIX Level: {analysis['vix_spot_level']:.2f}")
                if analysis.get('market_structure'):
                    print(f"📈 Structure: {analysis['market_structure']}")
                
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
    print("=" * 70)
    print("UX VIX FUTURES DATA COLLECTION")
    print("=" * 70)
    print(f"Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    fetcher = UXVIXFuturesFetcher()
    success = fetcher.run_ux_collection()
    
    if success:
        print("\n🎊 Success! UX VIX futures data collected")
        print("💡 You now have the complete VIX derivatives dataset!")
    else:
        print("\n💥 Collection failed")
    
    return success

if __name__ == "__main__":
    main()
