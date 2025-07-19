"""
SPX Index Market Cap Weighted Components
Build our own S&P 500 market cap weighted index using Bloomberg SPX INDEX MEMB
"""

import sys
import os
import pandas as pd
from datetime import datetime
import json

# Add project root to path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

try:
    import blpapi
    from config.bloomberg_config import SPX_TICKER
except ImportError as e:
    print(f"IMPORT ERROR: {e}")
    SPX_TICKER = 'SPX Index'

class SPXIndexWeights:
    """Build S&P 500 market cap weighted index from SPX components"""
    
    def __init__(self):
        self.session = None
        self.refDataService = None
    
    def connect(self):
        """Connect to Bloomberg Terminal"""
        try:
            sessionOptions = blpapi.SessionOptions()
            self.session = blpapi.Session(sessionOptions)
            
            if not self.session.start():
                print("ERROR: Failed to start Bloomberg session")
                return False
            
            if not self.session.openService("//blp/refdata"):
                print("ERROR: Failed to open Bloomberg reference data service")
                return False
            
            self.refDataService = self.session.getService("//blp/refdata")
            print("SUCCESS: Connected to Bloomberg for SPX index data")
            return True
            
        except Exception as e:
            print(f"ERROR: Bloomberg connection failed: {e}")
            return False
    
    def get_spx_members(self):
        """Get SPX Index members using INDX_MEMBERS field"""
        try:
            print("INFO: Fetching SPX Index members...")
            
            request = self.refDataService.createRequest("ReferenceDataRequest")
            request.getElement("securities").appendValue("SPX Index")
            request.getElement("fields").appendValue("INDX_MEMBERS")
            
            self.session.sendRequest(request)
            
            members = []
            while True:
                event = self.session.nextEvent(5000)  # 5 second timeout
                
                if event.eventType() == blpapi.Event.RESPONSE:
                    for msg in event:
                        securityData = msg.getElement("securityData")
                        security = securityData.getValue(0)
                        
                        if security.hasElement("securityError"):
                            error = security.getElement("securityError")
                            print(f"ERROR: {error.getElement('message').getValue()}")
                            return None
                        
                        fieldData = security.getElement("fieldData")
                        if fieldData.hasElement("INDX_MEMBERS"):
                            members_element = fieldData.getElement("INDX_MEMBERS")
                            
                            for i in range(members_element.numValues()):
                                member = members_element.getValue(i)
                                
                                # Extract member data
                                member_ticker = member.getElement("Member Ticker and Exchange Code").getValue()
                                members.append(member_ticker)
                    break
                
                if event.eventType() == blpapi.Event.TIMEOUT:
                    print("ERROR: Timeout getting SPX members")
                    return None
            
            print(f"SUCCESS: Retrieved {len(members)} SPX Index members")
            
            # Convert Bloomberg composite tickers to equity tickers
            equity_tickers = []
            for ticker in members:
                if ticker.endswith(' UN') or ticker.endswith(' UW') or ticker.endswith(' UR'):
                    # Convert to US Equity format
                    base_ticker = ticker.split(' ')[0]
                    equity_ticker = f"{base_ticker} US Equity"
                    equity_tickers.append(equity_ticker)
                elif not ticker.endswith(' Equity'):
                    # Add US Equity if not already present
                    base_ticker = ticker.split(' ')[0]
                    equity_ticker = f"{base_ticker} US Equity"
                    equity_tickers.append(equity_ticker)
                else:
                    # Already in correct format
                    equity_tickers.append(ticker)
            
            print(f"SUCCESS: Converted to {len(equity_tickers)} equity tickers")
            print(f"   Sample tickers: {equity_tickers[:5]}")
            
            return equity_tickers
            
        except Exception as e:
            print(f"ERROR: Failed to get SPX members: {e}")
            return None
    
    def get_market_cap_data(self, tickers, batch_size=20):
        """Get market cap and company data for SPX components"""
        try:
            print(f"INFO: Fetching market cap data for {len(tickers)} components...")
            
            components_data = []
            
            # Process in batches
            for i in range(0, len(tickers), batch_size):
                batch = tickers[i:i+batch_size]
                batch_num = i//batch_size + 1
                total_batches = (len(tickers) + batch_size - 1) // batch_size
                print(f"   Processing batch {batch_num}/{total_batches}: {len(batch)} components")
                
                request = self.refDataService.createRequest("ReferenceDataRequest")
                
                for ticker in batch:
                    request.getElement("securities").appendValue(ticker)
                
                # Fields for market cap weighting
                fields = [
                    'PX_LAST',           # Last price
                    'CUR_MKT_CAP',       # Current market cap
                    'EQY_SH_OUT',        # Shares outstanding
                    'NAME',              # Company name
                    'GICS_SECTOR_NAME',  # GICS sector
                    'COUNTRY_ISO',       # Country
                    'EQY_FLOAT_SHS'      # Float shares
                ]
                
                for field in fields:
                    request.getElement("fields").appendValue(field)
                
                self.session.sendRequest(request)
                
                while True:
                    event = self.session.nextEvent(5000)
                    
                    if event.eventType() == blpapi.Event.RESPONSE:
                        for msg in event:
                            securityData = msg.getElement("securityData")
                            for j in range(securityData.numValues()):
                                security = securityData.getValue(j)
                                ticker = security.getElement("security").getValue()
                                
                                if security.hasElement("securityError"):
                                    print(f"   WARNING: Error for {ticker}")
                                    continue
                                
                                fieldData = security.getElement("fieldData")
                                component_data = {
                                    'ticker': ticker,
                                    'collection_date': datetime.now().strftime('%Y-%m-%d')
                                }
                                
                                for field in fields:
                                    if fieldData.hasElement(field):
                                        component_data[field] = fieldData.getElement(field).getValue()
                                    else:
                                        component_data[field] = None
                                
                                components_data.append(component_data)
                        break
                    
                    if event.eventType() == blpapi.Event.TIMEOUT:
                        print(f"   WARNING: Timeout for batch {batch_num}")
                        break
            
            print(f"SUCCESS: Retrieved market cap data for {len(components_data)} components")
            return components_data
            
        except Exception as e:
            print(f"ERROR: Failed to get market cap data: {e}")
            return None
    
    def calculate_market_cap_weights(self, components_data):
        """Calculate market cap weights for S&P 500 components"""
        try:
            print("INFO: Calculating market cap weights...")
            
            df = pd.DataFrame(components_data)
            
            # Filter components with valid market cap data
            initial_count = len(df)
            df = df[df['CUR_MKT_CAP'].notna() & (df['CUR_MKT_CAP'] > 0)]
            final_count = len(df)
            
            print(f"   Components with valid market cap: {final_count}/{initial_count}")
            
            if final_count == 0:
                print("ERROR: No components have valid market cap data")
                return None
            
            # Calculate total market cap
            total_market_cap = df['CUR_MKT_CAP'].sum()
            
            # Calculate weights as percentage
            df['market_cap_weight_pct'] = (df['CUR_MKT_CAP'] / total_market_cap) * 100
            
            # Sort by weight (largest first)
            df = df.sort_values('market_cap_weight_pct', ascending=False)
            
            # Add ranking
            df['rank'] = range(1, len(df) + 1)
            
            print(f"SUCCESS: Calculated weights for {len(df)} components")
            print(f"   Total S&P 500 market cap: ${total_market_cap/1e12:.2f}T")
            print(f"   Top 5 components by weight:")
            
            top_5 = df.head(5)[['rank', 'ticker', 'NAME', 'market_cap_weight_pct']].copy()
            for _, row in top_5.iterrows():
                print(f"     {row['rank']:2d}. {row['ticker']:12s} {row['market_cap_weight_pct']:5.2f}% - {row['NAME']}")
            
            return df
            
        except Exception as e:
            print(f"ERROR: Failed to calculate weights: {e}")
            return None
    
    def save_spx_weights(self, weights_df):
        """Save S&P 500 market cap weights data"""
        try:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            date_str = datetime.now().strftime('%Y-%m-%d')
            
            # Create output directory
            output_dir = os.path.join(project_root, 'data', 'processed', 'spx_weights')
            os.makedirs(output_dir, exist_ok=True)
            
            # Save main weights file
            weights_file = os.path.join(output_dir, f'spx_market_cap_weights_{timestamp}.csv')
            weights_df.to_csv(weights_file, index=False)
            print(f"SUCCESS: SPX weights saved to: {weights_file}")
            
            # Save latest weights (always current)
            latest_file = os.path.join(output_dir, 'spx_weights_latest.csv')
            weights_df.to_csv(latest_file, index=False)
            print(f"SUCCESS: Latest weights saved to: {latest_file}")
            
            # Create summary
            summary = {
                'collection_timestamp': timestamp,
                'collection_date': date_str,
                'total_components': len(weights_df),
                'total_market_cap_usd': float(weights_df['CUR_MKT_CAP'].sum()),
                'top_10_holdings': weights_df.head(10)[['rank', 'ticker', 'NAME', 'market_cap_weight_pct']].to_dict('records'),
                'sector_breakdown': weights_df.groupby('GICS_SECTOR_NAME')['market_cap_weight_pct'].sum().to_dict() if 'GICS_SECTOR_NAME' in weights_df.columns else {},
                'data_quality': {
                    'components_with_market_cap': int(weights_df['CUR_MKT_CAP'].notna().sum()),
                    'components_with_names': int(weights_df['NAME'].notna().sum()),
                    'components_with_sectors': int(weights_df['GICS_SECTOR_NAME'].notna().sum()) if 'GICS_SECTOR_NAME' in weights_df.columns else 0,
                    'weight_coverage_pct': float(weights_df['market_cap_weight_pct'].sum())
                }
            }
            
            summary_file = os.path.join(output_dir, f'spx_weights_summary_{timestamp}.json')
            with open(summary_file, 'w') as f:
                json.dump(summary, f, indent=2, default=str)
            print(f"SUCCESS: Summary saved to: {summary_file}")
            
            return weights_file, latest_file, summary_file
            
        except Exception as e:
            print(f"ERROR: Failed to save SPX weights: {e}")
            return None, None, None
    
    def disconnect(self):
        """Disconnect from Bloomberg"""
        if self.session:
            self.session.stop()
            print("SUCCESS: Bloomberg session disconnected")

def main():
    """Main execution function"""
    print("="*70)
    print("SPX INDEX MARKET CAP WEIGHTED COMPONENTS")
    print("="*70)
    print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("Building S&P 500 market cap weighted index from SPX INDEX MEMB...")
    
    spx = SPXIndexWeights()
    
    try:
        if not spx.connect():
            return False
        
        # Step 1: Get SPX Index members
        print("\n1. Getting SPX Index members...")
        members = spx.get_spx_members()
        if not members:
            print("ERROR: Failed to get SPX members")
            return False
        
        # Step 2: Get market cap data for all components
        print(f"\n2. Getting market cap data for {len(members)} components...")
        components_data = spx.get_market_cap_data(members)
        if not components_data:
            print("ERROR: Failed to get market cap data")
            return False
        
        # Step 3: Calculate market cap weights
        print("\n3. Calculating market cap weights...")
        weights_df = spx.calculate_market_cap_weights(components_data)
        if weights_df is None:
            print("ERROR: Failed to calculate weights")
            return False
        
        # Step 4: Save the data
        print("\n4. Saving SPX market cap weights...")
        weights_file, latest_file, summary_file = spx.save_spx_weights(weights_df)
        
        if weights_file:
            print(f"\nSUCCESS: SPX market cap weighted index created!")
            print(f"   Components: {len(weights_df)}")
            print(f"   Total market cap: ${weights_df['CUR_MKT_CAP'].sum()/1e12:.2f}T")
            print(f"   Weight coverage: {weights_df['market_cap_weight_pct'].sum():.1f}%")
            print(f"   Files saved in: data/processed/spx_weights/")
            print(f"\nThis can now be used as SPY proxy for volatility analysis!")
            return True
        else:
            print("ERROR: Failed to save data")
            return False
            
    except Exception as e:
        print(f"ERROR: Error in main execution: {e}")
        return False
    
    finally:
        spx.disconnect()

if __name__ == "__main__":
    main()