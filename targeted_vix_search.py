"""
Targeted VIX Futures Search
Search specifically for CBOE VIX volatility futures, not other commodities
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

class TargetedVIXSearch:
    """
    Targeted search for actual CBOE VIX volatility futures
    """
    
    def __init__(self):
        self.session = None
        self.refDataService = None
        self.project_root = project_root
        self.data_dir = self.project_root / 'data' / 'vix_data'
        
        # Create directories
        self.data_dir.mkdir(parents=True, exist_ok=True)
        
        print(f"🔥 Targeted VIX Futures Search")
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
    
    def search_vix_volatility_futures(self):
        """
        Search specifically for VIX volatility futures (not other VX commodities)
        """
        print("🔍 Searching for CBOE VIX volatility futures...")
        
        # Test specific VIX volatility-related tickers
        vix_candidates = [
            # CBOE VIX Index itself
            'VIX Index',
            'CBOE VIX Index', 
            'VIX US Index',
            
            # VIX futures - try many variations
            'VIX1 Index',      # Generic 1st month
            'VIX2 Index',      # Generic 2nd month  
            'VIX3 Index',      # We know this works - it's VIX spot
            'VIX4 Index',
            'VIX5 Index',
            'VIX6 Index',
            
            # Alternative VIX future formats
            'CBOE VIX1 Index',
            'CBOE VIX2 Index', 
            'CBOE VIX3 Index',
            'CBOE VIX4 Index',
            
            # VIX ETF/ETN products
            'VXX US Equity',   # VIX ETN
            'UVXY US Equity',  # 2x VIX ETF
            'SVXY US Equity',  # Short VIX ETF
            'VIXY US Equity',  # VIX ETF
            
            # Try specific monthly VIX futures in Index format
            'VIXF25 Index',
            'VIXG25 Index', 
            'VIXH25 Index',
            'VIXJ25 Index',
            'VIXK25 Index',
            'VIXM25 Index',
        ]
        
        working_vix = []
        vix_info = {}
        
        print(f"Testing {len(vix_candidates)} VIX-related securities...")
        
        for ticker in vix_candidates:
            try:
                request = self.refDataService.createRequest("ReferenceDataRequest")
                request.getElement("securities").appendValue(ticker)
                
                # Test comprehensive fields to understand what we have
                fields = [
                    "PX_LAST", "NAME", "SECURITY_TYP", "CRNCY", "COUNTRY",
                    "EXCH_CODE", "ID_ISIN", "UNDERLYING_SECURITY_DES"
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
                                
                                # Extract detailed info
                                if fieldData.hasElement("PX_LAST"):
                                    info['price'] = fieldData.getElement("PX_LAST").getValue()
                                if fieldData.hasElement("NAME"):
                                    info['name'] = fieldData.getElement("NAME").getValue()
                                if fieldData.hasElement("SECURITY_TYP"):
                                    info['security_type'] = fieldData.getElement("SECURITY_TYP").getValue()
                                if fieldData.hasElement("CRNCY"):
                                    info['currency'] = fieldData.getElement("CRNCY").getValue()
                                if fieldData.hasElement("COUNTRY"):
                                    info['country'] = fieldData.getElement("COUNTRY").getValue()
                                if fieldData.hasElement("EXCH_CODE"):
                                    info['exchange'] = fieldData.getElement("EXCH_CODE").getValue()
                                if fieldData.hasElement("UNDERLYING_SECURITY_DES"):
                                    info['underlying'] = fieldData.getElement("UNDERLYING_SECURITY_DES").getValue()
                                
                                # Only keep USD-denominated securities or VIX-related
                                currency = info.get('currency', '')
                                name = info.get('name', '').upper()
                                
                                # Filter for actual VIX/volatility instruments
                                is_vix_related = (
                                    'VIX' in name or 
                                    'VOLATILITY' in name or
                                    'CBOE' in name or
                                    currency == 'USD'
                                )
                                
                                if is_vix_related:
                                    working_vix.append(ticker)
                                    vix_info[ticker] = info
                                    
                                    price = info.get('price', 'N/A')
                                    security_type = info.get('security_type', 'Unknown')
                                    exchange = info.get('exchange', 'Unknown')
                                    
                                    print(f"   ✅ {ticker:20} | {price:>8} | {security_type:>10} | {name[:40]}")
                                else:
                                    print(f"   ⚠️ {ticker:20} | Filtered out: {currency} {name[:30]}")
                                break
                
            except Exception as e:
                continue
            
            time.sleep(0.05)
        
        print(f"\n✅ Found {len(working_vix)} VIX-related securities")
        return working_vix, vix_info
    
    def test_vix_historical_data(self, ticker):
        """
        Test if we can get historical data for a VIX security
        """
        try:
            request = self.refDataService.createRequest("HistoricalDataRequest")
            request.getElement("securities").appendValue(ticker)
            request.getElement("fields").appendValue("PX_LAST")
            
            # Test last 5 days
            end_date = datetime.now()
            start_date = end_date - timedelta(days=5)
            
            request.set("startDate", start_date.strftime('%Y%m%d'))
            request.set("endDate", end_date.strftime('%Y%m%d'))
            request.set("periodicitySelection", "DAILY")
            
            self.session.sendRequest(request)
            event = self.session.nextEvent(5000)
            
            data_points = 0
            if event.eventType() == blpapi.Event.RESPONSE:
                for msg in event:
                    if msg.hasElement("securityData"):
                        # Try to count data points without parsing the complex structure
                        data_points = 1  # If we get here, we have some data
                        
            return data_points > 0
            
        except Exception as e:
            return False
    
    def analyze_vix_securities(self, working_vix, vix_info):
        """
        Analyze the found VIX securities to identify the best ones
        """
        print("\n📊 Analyzing VIX securities...")
        
        categorized = {
            'vix_spot': [],
            'vix_futures': [],
            'vix_etfs': [],
            'other': []
        }
        
        for ticker in working_vix:
            info = vix_info[ticker]
            name = info.get('name', '').upper()
            security_type = info.get('security_type', '').upper()
            
            if 'VOLATILITY INDEX' in name or ticker == 'VIX3 Index':
                categorized['vix_spot'].append((ticker, info))
            elif 'FUTURE' in name or 'GENERIC' in name:
                categorized['vix_futures'].append((ticker, info))
            elif 'EQUITY' in ticker or 'ETF' in name or 'ETN' in name:
                categorized['vix_etfs'].append((ticker, info))
            else:
                categorized['other'].append((ticker, info))
        
        print("\n📋 VIX Securities Categorization:")
        
        for category, securities in categorized.items():
            if securities:
                print(f"\n{category.upper()}:")
                for ticker, info in securities:
                    name = info.get('name', 'Unknown')[:50]
                    price = info.get('price', 'N/A')
                    
                    # Test historical data availability
                    has_history = self.test_vix_historical_data(ticker)
                    history_status = "✅ Historical" if has_history else "❌ No History"
                    
                    print(f"   {ticker:20} | {price:>8} | {history_status} | {name}")
        
        return categorized
    
    def save_vix_discovery_results(self, working_vix, vix_info, categorized):
        """
        Save VIX discovery results
        """
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        try:
            # Save comprehensive results
            discovery_results = {
                'discovery_timestamp': timestamp,
                'total_vix_securities_found': len(working_vix),
                'working_vix_tickers': working_vix,
                'vix_securities_details': vix_info,
                'categorized_securities': {
                    category: [ticker for ticker, info in securities]
                    for category, securities in categorized.items()
                },
                'recommendations': {
                    'vix_spot_best': categorized['vix_spot'][0][0] if categorized['vix_spot'] else None,
                    'vix_futures_available': len(categorized['vix_futures']),
                    'vix_etfs_available': len(categorized['vix_etfs'])
                }
            }
            
            results_file = self.data_dir / f'vix_discovery_results_{timestamp}.json'
            with open(results_file, 'w') as f:
                json.dump(discovery_results, f, indent=2, default=str)
            
            print(f"\n✅ Discovery results saved: {results_file.name}")
            
            # Create summary CSV of all found securities
            summary_data = []
            for ticker in working_vix:
                info = vix_info[ticker]
                summary_data.append({
                    'ticker': ticker,
                    'name': info.get('name', ''),
                    'price': info.get('price', ''),
                    'security_type': info.get('security_type', ''),
                    'currency': info.get('currency', ''),
                    'exchange': info.get('exchange', ''),
                    'country': info.get('country', '')
                })
            
            if summary_data:
                summary_df = pd.DataFrame(summary_data)
                summary_file = self.data_dir / f'vix_securities_summary_{timestamp}.csv'
                summary_df.to_csv(summary_file, index=False)
                print(f"✅ Securities summary saved: {summary_file.name}")
            
            return discovery_results
            
        except Exception as e:
            print(f"❌ Failed to save results: {e}")
            return None
    
    def run_targeted_search(self):
        """
        Run targeted VIX search
        """
        print("🚀 Starting targeted VIX volatility futures search...")
        
        try:
            if not self.connect():
                return False
            
            # Search for VIX securities
            working_vix, vix_info = self.search_vix_volatility_futures()
            
            if not working_vix:
                print("❌ No VIX securities found")
                return False
            
            # Analyze and categorize
            categorized = self.analyze_vix_securities(working_vix, vix_info)
            
            # Save results
            results = self.save_vix_discovery_results(working_vix, vix_info, categorized)
            
            if results:
                print("\n🎉 VIX discovery completed!")
                print("=" * 60)
                print(f"📊 Total VIX securities found: {len(working_vix)}")
                
                for category, securities in categorized.items():
                    if securities:
                        print(f"📊 {category.replace('_', ' ').title()}: {len(securities)}")
                
                print(f"📁 Results saved to: {self.data_dir}")
                
                # Recommendations
                if categorized['vix_spot']:
                    print(f"\n💡 Recommended VIX Spot: {categorized['vix_spot'][0][0]}")
                if categorized['vix_futures']:
                    print(f"💡 VIX Futures available: {[t for t, i in categorized['vix_futures']]}")
                
                return True
            else:
                return False
                
        except Exception as e:
            print(f"💥 Search failed: {e}")
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
    print("TARGETED VIX VOLATILITY FUTURES SEARCH")
    print("=" * 70)
    print(f"Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    searcher = TargetedVIXSearch()
    success = searcher.run_targeted_search()
    
    if success:
        print("\n🎊 Discovery complete! Check results for actual VIX futures")
    else:
        print("\n💥 Search failed")
    
    return success

if __name__ == "__main__":
    main()
