"""
VIX Futures Data Fetcher
Focus on collecting VIX futures using reference data (current prices)
and trying to find additional VIX futures contracts
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

class VIXFuturesFetcher:
    """
    VIX Futures Data Collection
    Focus on VIX futures using reference data and testing multiple formats
    """
    
    def __init__(self):
        self.session = None
        self.refDataService = None
        self.project_root = project_root
        self.data_dir = self.project_root / 'data' / 'vix_data'
        
        # Create directories
        self.data_dir.mkdir(parents=True, exist_ok=True)
        
        # Known working VIX securities
        self.known_vix_securities = [
            'VIX3 Index',      # Cboe Volatility Index (spot)
            'CBOE VIX1 Index', # Generic 1st VX Future
            'VIX2 Index'       # IPATH VIX ETN
        ]
        
        # Test additional VIX futures formats
        self.test_vix_futures = [
            # Generic VIX futures
            'CBOE VIX1 Index', 'CBOE VIX2 Index', 'CBOE VIX3 Index', 'CBOE VIX4 Index',
            'CBOE VIX5 Index', 'CBOE VIX6 Index', 'CBOE VIX7 Index', 'CBOE VIX8 Index',
            
            # Alternative formats
            'VIX1 Index', 'VIX4 Index', 'VIX5 Index', 'VIX6 Index',
            
            # Direct VX futures (if available)
            'VX1 Comdty', 'VX2 Comdty', 'VX3 Comdty', 'VX4 Comdty',
            
            # Monthly VIX futures (current year)
            'VIXF25 Curncy', 'VIXG25 Curncy', 'VIXH25 Curncy', 'VIXJ25 Curncy',
            'VIXK25 Curncy', 'VIXM25 Curncy', 'VIXN25 Curncy', 'VIXQ25 Curncy',
            'VIXU25 Curncy', 'VIXV25 Curncy', 'VIXX25 Curncy', 'VIXZ25 Curncy'
        ]
        
        print(f"🔥 VIX Futures Data Collection")
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
    
    def test_vix_futures_availability(self):
        """
        Test which VIX futures are actually available
        """
        print("🔍 Testing VIX futures availability...")
        
        working_futures = []
        futures_info = {}
        
        for ticker in self.test_vix_futures:
            try:
                request = self.refDataService.createRequest("ReferenceDataRequest")
                request.getElement("securities").appendValue(ticker)
                
                # Test basic fields
                fields = ["PX_LAST", "NAME", "CRNCY", "LAST_TRADEABLE_DT"]
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
                                
                                # Extract available info
                                if fieldData.hasElement("PX_LAST"):
                                    info['price'] = fieldData.getElement("PX_LAST").getValue()
                                if fieldData.hasElement("NAME"):
                                    info['name'] = fieldData.getElement("NAME").getValue()
                                if fieldData.hasElement("CRNCY"):
                                    info['currency'] = fieldData.getElement("CRNCY").getValue()
                                if fieldData.hasElement("LAST_TRADEABLE_DT"):
                                    info['expiry'] = fieldData.getElement("LAST_TRADEABLE_DT").getValue()
                                
                                working_futures.append(ticker)
                                futures_info[ticker] = info
                                
                                price = info.get('price', 'N/A')
                                name = info.get('name', 'Unknown')[:50]
                                print(f"   ✅ {ticker:20} | Price: {price:>8} | {name}")
                                break
                
            except Exception as e:
                continue
            
            time.sleep(0.05)  # Rate limiting
        
        print(f"\n✅ Found {len(working_futures)} working VIX futures")
        return working_futures, futures_info
    
    def collect_vix_futures_current_data(self, working_futures):
        """
        Collect current data for working VIX futures
        """
        if not working_futures:
            print("❌ No working futures to collect")
            return pd.DataFrame()
        
        print(f"📊 Collecting current data for {len(working_futures)} VIX futures...")
        
        all_data = []
        
        for ticker in working_futures:
            try:
                request = self.refDataService.createRequest("ReferenceDataRequest")
                request.getElement("securities").appendValue(ticker)
                
                # Comprehensive futures fields
                fields = [
                    "PX_LAST",           # Last price
                    "PX_OPEN",           # Open price
                    "PX_HIGH",           # High price
                    "PX_LOW",            # Low price
                    "PX_SETTLE",         # Settlement price
                    "PX_VOLUME",         # Volume
                    "OPEN_INT",          # Open interest
                    "NAME",              # Security name
                    "CRNCY",             # Currency
                    "LAST_TRADEABLE_DT", # Expiry date
                    "DAYS_TO_EXP",       # Days to expiry
                    "CONTRACT_VALUE",    # Contract value
                    "FUT_CONT_SIZE"      # Contract size
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
                                print(f"   ❌ Error for {ticker}")
                                continue
                            
                            if securityData.hasElement("fieldData"):
                                fieldData = securityData.getElement("fieldData")
                                
                                row_data = {
                                    'collection_date': datetime.now().strftime('%Y-%m-%d'),
                                    'ticker': ticker,
                                    'data_type': 'VIX_Future'
                                }
                                
                                # Map Bloomberg fields to clean names
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
                                    'expiry_date': 'LAST_TRADEABLE_DT',
                                    'days_to_expiry': 'DAYS_TO_EXP',
                                    'contract_value': 'CONTRACT_VALUE',
                                    'contract_size': 'FUT_CONT_SIZE'
                                }
                                
                                for clean_name, bloomberg_field in field_mapping.items():
                                    if fieldData.hasElement(bloomberg_field):
                                        value = fieldData.getElement(bloomberg_field).getValue()
                                        if bloomberg_field == 'LAST_TRADEABLE_DT' and value:
                                            # Format date properly
                                            try:
                                                row_data[clean_name] = value.strftime('%Y-%m-%d')
                                            except:
                                                row_data[clean_name] = str(value)
                                        else:
                                            row_data[clean_name] = value if value is not None else np.nan
                                    else:
                                        row_data[clean_name] = np.nan
                                
                                all_data.append(row_data)
                                print(f"   ✅ Collected data for {ticker}")
                                break
                
            except Exception as e:
                print(f"   ❌ Error collecting {ticker}: {e}")
                continue
            
            time.sleep(0.1)  # Rate limiting
        
        df = pd.DataFrame(all_data)
        print(f"✅ Collected data for {len(df)} VIX futures")
        return df
    
    def analyze_vix_futures_structure(self, futures_df, futures_info):
        """
        Analyze the VIX futures term structure
        """
        if len(futures_df) == 0:
            return {}
        
        print("📊 Analyzing VIX futures term structure...")
        
        # Separate spot vs futures
        spot_data = futures_df[futures_df['ticker'].str.contains('VIX3|VIX2')].copy()
        futures_data = futures_df[~futures_df['ticker'].str.contains('VIX3|VIX2')].copy()
        
        analysis = {
            'spot_instruments': len(spot_data),
            'futures_instruments': len(futures_data),
            'total_instruments': len(futures_df)
        }
        
        if len(spot_data) > 0:
            spot_prices = spot_data['last_price'].dropna()
            if len(spot_prices) > 0:
                analysis['vix_spot_level'] = float(spot_prices.iloc[0])
        
        if len(futures_data) > 0:
            # Sort by days to expiry if available
            futures_valid = futures_data.dropna(subset=['last_price'])
            if len(futures_valid) > 0:
                futures_prices = futures_valid['last_price'].tolist()
                analysis['futures_prices'] = futures_prices
                analysis['futures_range'] = {
                    'min': float(min(futures_prices)),
                    'max': float(max(futures_prices)),
                    'mean': float(np.mean(futures_prices))
                }
                
                # Check for contango/backwardation if we have spot
                if 'vix_spot_level' in analysis and len(futures_prices) > 0:
                    front_future = futures_prices[0] if len(futures_prices) > 0 else None
                    if front_future:
                        if front_future > analysis['vix_spot_level']:
                            analysis['market_structure'] = 'Contango'
                        else:
                            analysis['market_structure'] = 'Backwardation'
                        analysis['spot_future_spread'] = front_future - analysis['vix_spot_level']
        
        print(f"   VIX Spot Level: {analysis.get('vix_spot_level', 'N/A')}")
        print(f"   Futures Count: {analysis['futures_instruments']}")
        if 'futures_range' in analysis:
            print(f"   Futures Range: {analysis['futures_range']['min']:.2f} - {analysis['futures_range']['max']:.2f}")
        if 'market_structure' in analysis:
            print(f"   Market Structure: {analysis['market_structure']}")
        
        return analysis
    
    def save_vix_futures_data(self, futures_df, futures_info, analysis):
        """
        Save VIX futures data and analysis
        """
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        try:
            files_created = []
            
            # Save futures data
            if len(futures_df) > 0:
                futures_file = self.data_dir / f'vix_futures_current_{timestamp}.csv'
                futures_df.to_csv(futures_file, index=False)
                files_created.append(str(futures_file))
                print(f"✅ VIX futures: {len(futures_df)} records → {futures_file.name}")
            
            # Save futures info and analysis
            summary = {
                'collection_timestamp': timestamp,
                'collection_type': 'vix_futures_current',
                'collection_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'working_futures': list(futures_info.keys()),
                'futures_details': futures_info,
                'term_structure_analysis': analysis,
                'data_summary': {
                    'total_futures': len(futures_df),
                    'futures_with_prices': len(futures_df[futures_df['last_price'].notna()]) if len(futures_df) > 0 else 0
                },
                'files_created': files_created
            }
            
            summary_file = self.data_dir / f'vix_futures_summary_{timestamp}.json'
            with open(summary_file, 'w') as f:
                json.dump(summary, f, indent=2, default=str)
            
            print(f"✅ Summary: {summary_file.name}")
            return summary
            
        except Exception as e:
            print(f"❌ Failed to save data: {e}")
            return None
    
    def run_vix_futures_collection(self):
        """
        Run VIX futures data collection
        """
        print("🚀 Starting VIX futures data collection...")
        
        try:
            if not self.connect():
                return False
            
            # Test VIX futures availability
            print("\n🔍 Testing VIX futures availability...")
            working_futures, futures_info = self.test_vix_futures_availability()
            
            if not working_futures:
                print("❌ No working VIX futures found")
                return False
            
            # Collect current data
            print(f"\n📊 Collecting current data for VIX futures...")
            futures_df = self.collect_vix_futures_current_data(working_futures)
            
            if len(futures_df) == 0:
                print("❌ No futures data collected")
                return False
            
            # Analyze term structure
            print(f"\n📊 Analyzing VIX futures term structure...")
            analysis = self.analyze_vix_futures_structure(futures_df, futures_info)
            
            # Save data
            print(f"\n💾 Saving VIX futures data...")
            summary = self.save_vix_futures_data(futures_df, futures_info, analysis)
            
            if summary:
                print("\n🎉 VIX futures collection completed successfully!")
                print("=" * 60)
                print(f"📊 Working futures found: {len(working_futures)}")
                print(f"📊 Data records collected: {len(futures_df)}")
                
                if 'vix_spot_level' in analysis:
                    print(f"📈 VIX Spot Level: {analysis['vix_spot_level']:.2f}")
                
                if 'market_structure' in analysis:
                    print(f"📈 Market Structure: {analysis['market_structure']}")
                
                print(f"📁 Files saved to: {self.data_dir}")
                return True
            else:
                return False
                
        except Exception as e:
            print(f"💥 Collection failed: {e}")
            import traceback
            traceback.print_exc()
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
    print("VIX FUTURES DATA COLLECTION")
    print("=" * 70)
    print(f"Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    fetcher = VIXFuturesFetcher()
    success = fetcher.run_vix_futures_collection()
    
    if success:
        print("\n🎊 Success! VIX futures data collected")
        print("💡 You can now copy this data to your shared drive")
    else:
        print("\n💥 Collection failed")
    
    return success

if __name__ == "__main__":
    main()
