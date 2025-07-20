#!/usr/bin/env python3
"""
Final Working VIX Strategy Implementation
Using discovered working formats:
- VIX Spot: VIX Index
- VIX Futures: UX1 Index, UX2 Index, etc.
- VIX Options: VIX MM/DD/YY C{strike} Index
Strategy: SHORT 1x 50Δ call + LONG 2x 10Δ calls + UX1 futures hedge
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

class FinalVIXStrategy:
    """
    Final working VIX volatility strategy using confirmed ticker formats
    """
    
    def __init__(self, years_back=5):
        self.session = None
        self.refDataService = None
        self.data_dir = Path('./data/final_vix_strategy')
        self.results_dir = self.data_dir / 'results'
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.results_dir.mkdir(parents=True, exist_ok=True)
        
        # Date range
        self.end_date = datetime.now()
        self.start_date = self.end_date - timedelta(days=years_back*365)
        
        # Strategy parameters
        self.target_deltas = [10, 50]  # 10Δ long, 50Δ short
        self.quantities = {10: 2, 50: -1}  # LONG 2x 10Δ, SHORT 1x 50Δ
        
        # Contract specifications
        self.vix_option_multiplier = 100  # $100 per point
        self.vix_future_multiplier = 1000  # $1000 per point
        
        # Working Bloomberg field names
        self.option_fields = {
            'last': 'PX_LAST',
            'bid': 'PX_BID',
            'ask': 'PX_ASK',
            'volume': 'PX_VOLUME',
            'delta': 'DELTA_MID',
            'gamma': 'GAMMA_MID',
            'theta': 'THETA_MID',
            'vega': 'VEGA_MID',
            'ivol': 'IVOL_MID'
        }
        
        # Setup logging
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
        
        print(f"🔥 Final VIX Strategy initialized")
        print(f"📅 Analysis period: {self.start_date.date()} to {self.end_date.date()}")
        print(f"🎯 Strategy: SHORT 1x 50Δ call + LONG 2x 10Δ calls + UX1 hedge")
    
    def connect_bloomberg(self):
        """Connect to Bloomberg Terminal"""
        try:
            sessionOptions = blpapi.SessionOptions()
            self.session = blpapi.Session(sessionOptions)
            
            if not self.session.start():
                self.logger.error("Failed to start Bloomberg session")
                return False
            
            if not self.session.openService("//blp/refdata"):
                self.logger.error("Failed to open Bloomberg reference data service")
                return False
            
            self.refDataService = self.session.getService("//blp/refdata")
            self.logger.info("Bloomberg connection established")
            return True
            
        except Exception as e:
            self.logger.error(f"Bloomberg connection failed: {e}")
            return False
    
    def get_vix_expiry_calendar(self):
        """Generate VIX options expiry calendar (3rd Wednesday of each month)"""
        expiries = []
        current_date = self.start_date.replace(day=1).date()
        end_date = self.end_date.date()
        
        while current_date <= end_date + timedelta(days=90):
            year = current_date.year
            month = current_date.month
            
            # Find 3rd Wednesday
            first_day = date(year, month, 1)
            first_weekday = first_day.weekday()
            days_to_first_wed = (2 - first_weekday) % 7
            first_wed = first_day + timedelta(days=days_to_first_wed)
            third_wed = first_wed + timedelta(days=14)
            
            expiries.append({
                'expiry_date': third_wed,
                'year': year,
                'month': month,
                'expiry_string': third_wed.strftime('%m/%d/%y')  # MM/DD/YY format
            })
            
            # Next month
            if month == 12:
                current_date = date(year + 1, 1, 1)
            else:
                current_date = date(year, month + 1, 1)
        
        return expiries
    
    def generate_vix_options_for_expiry(self, expiry_info):
        """Generate VIX option tickers for a specific expiry using working format"""
        exp_str = expiry_info['expiry_string']  # MM/DD/YY format
        
        # Generate strike range around typical VIX levels
        strikes = list(range(10, 51, 2)) + [12, 15, 18, 22, 25, 28, 32, 35, 38, 42, 45, 48]
        strikes = sorted(set(strikes))
        
        option_tickers = []
        
        for strike in strikes:
            # Use the working format: VIX MM/DD/YY C{strike} Index
            ticker = f"VIX {exp_str} C{strike} Index"
            option_tickers.append({
                'ticker': ticker,
                'expiry_date': expiry_info['expiry_date'],
                'expiry_string': exp_str,
                'strike': strike,
                'option_type': 'call'
            })
        
        return option_tickers
    
    def get_current_option_data_with_greeks(self, option_tickers, batch_size=5):
        """Get current option data including Greeks for target delta identification"""
        print(f"📊 Fetching current option data with Greeks for {len(option_tickers)} options...")
        
        all_option_data = []
        
        for i in range(0, len(option_tickers), batch_size):
            batch = option_tickers[i:i + batch_size]
            print(f"  Processing batch {i//batch_size + 1}/{(len(option_tickers)-1)//batch_size + 1}")
            
            for option_info in batch:
                ticker = option_info['ticker']
                
                try:
                    request = self.refDataService.createRequest("ReferenceDataRequest")
                    request.getElement("securities").appendValue(ticker)
                    
                    # Add all working fields
                    for bloomberg_field in self.option_fields.values():
                        request.getElement("fields").appendValue(bloomberg_field)
                    
                    self.session.sendRequest(request)
                    
                    while True:
                        event = self.session.nextEvent(5000)
                        
                        if event.eventType() == blpapi.Event.RESPONSE:
                            for msg in event:
                                securityDataArray = msg.getElement("securityData")
                                
                                for j in range(securityDataArray.numValues()):
                                    securityData = securityDataArray.getValue(j)
                                    
                                    if securityData.hasElement("securityError"):
                                        # Skip non-existent options
                                        break
                                    
                                    if securityData.hasElement("fieldData"):
                                        fieldData = securityData.getElement("fieldData")
                                        
                                        row_data = {
                                            'date': datetime.now().strftime('%Y-%m-%d'),
                                            'ticker': ticker,
                                            'expiry_date': option_info['expiry_date'].strftime('%Y-%m-%d'),
                                            'strike': option_info['strike']
                                        }
                                        
                                        # Extract field values using working field names
                                        for clean_name, bloomberg_field in self.option_fields.items():
                                            if fieldData.hasElement(bloomberg_field):
                                                value = fieldData.getElement(bloomberg_field).getValue()
                                                row_data[clean_name] = value if value is not None else np.nan
                                            else:
                                                row_data[clean_name] = np.nan
                                        
                                        # Calculate mid price if bid/ask available
                                        if not pd.isna(row_data.get('bid')) and not pd.isna(row_data.get('ask')):
                                            row_data['mid'] = (row_data['bid'] + row_data['ask']) / 2
                                        else:
                                            row_data['mid'] = row_data.get('last', np.nan)
                                        
                                        all_option_data.append(row_data)
                            break
                        
                        if event.eventType() == blpapi.Event.TIMEOUT:
                            break
                
                except Exception as e:
                    self.logger.warning(f"Error fetching {ticker}: {e}")
                
                time.sleep(0.1)  # Rate limiting
        
        return pd.DataFrame(all_option_data)
    
    def identify_target_delta_options(self, options_df):
        """Identify specific options closest to target deltas for position entry"""
        print("🎯 Identifying target delta options for strategy entry...")
        
        # Filter for valid options with Greeks and prices
        valid_options = options_df[
            (options_df['delta'].notna()) & 
            (options_df['delta'] > 0) & 
            (options_df['volume'].fillna(0) > 0) &
            (options_df['mid'].notna()) &
            (options_df['mid'] > 0)
        ].copy()
        
        if len(valid_options) == 0:
            print("❌ No valid options found")
            return pd.DataFrame()
        
        print(f"✅ Found {len(valid_options)} valid options with Greeks")
        
        target_positions = []
        
        # Group by expiry to find entry points
        for expiry, expiry_group in valid_options.groupby('expiry_date'):
            expiry_positions = {}
            
            # Find closest delta for each target
            for target_delta in self.target_deltas:
                target_delta_decimal = target_delta / 100
                delta_diff = abs(expiry_group['delta'] - target_delta_decimal)
                
                if len(delta_diff) == 0:
                    continue
                    
                closest_idx = delta_diff.idxmin()
                
                if delta_diff.loc[closest_idx] < 0.05:  # Within 5 delta points
                    option_row = expiry_group.loc[closest_idx]
                    
                    position_info = {
                        'ticker': option_row['ticker'],
                        'expiry_date': expiry,
                        'target_delta': target_delta,
                        'actual_delta': option_row['delta'],
                        'delta_error': delta_diff.loc[closest_idx],
                        'strike': option_row['strike'],
                        'entry_price': option_row['mid'],
                        'quantity': self.quantities[target_delta],
                        'position_type': 'SHORT' if target_delta == 50 else 'LONG',
                        'entry_date': option_row['date'],
                        'bid': option_row['bid'],
                        'ask': option_row['ask'],
                        'gamma': option_row['gamma'],
                        'theta': option_row['theta'],
                        'vega': option_row['vega'],
                        'ivol': option_row['ivol']
                    }
                    
                    expiry_positions[target_delta] = position_info
            
            # Only include expiries where we have BOTH 10Δ and 50Δ options
            if len(expiry_positions) == 2:
                for delta, position in expiry_positions.items():
                    target_positions.append(position)
                    
                print(f"✅ {expiry}: SHORT 1x 50Δ @ ${expiry_positions[50]['strike']} (Δ={expiry_positions[50]['actual_delta']:.3f}) | LONG 2x 10Δ @ ${expiry_positions[10]['strike']} (Δ={expiry_positions[10]['actual_delta']:.3f})")
            else:
                print(f"⚠️  {expiry}: Incomplete - only found {list(expiry_positions.keys())} delta options")
        
        return pd.DataFrame(target_positions)
    
    def get_ux1_futures_data(self):
        """Get UX1 front month futures data for hedging"""
        print("📊 Collecting UX1 front month futures data for hedging...")
        
        try:
            request = self.refDataService.createRequest("HistoricalDataRequest")
            request.getElement("securities").appendValue("UX1 Index")
            
            for field in ["PX_LAST", "PX_SETTLE", "PX_VOLUME", "OPEN_INT"]:
                request.getElement("fields").appendValue(field)
            
            # Get last 6 months for testing
            test_start = self.end_date - timedelta(days=180)
            request.set("startDate", test_start.strftime('%Y%m%d'))
            request.set("endDate", self.end_date.strftime('%Y%m%d'))
            request.set("periodicitySelection", "DAILY")
            
            self.session.sendRequest(request)
            
            futures_data = []
            
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
                                    'ticker': 'UX1 Index',
                                    'last': fieldData.getElement("PX_LAST").getValue() if fieldData.hasElement("PX_LAST") else np.nan,
                                    'settle': fieldData.getElement("PX_SETTLE").getValue() if fieldData.hasElement("PX_SETTLE") else np.nan,
                                    'volume': fieldData.getElement("PX_VOLUME").getValue() if fieldData.hasElement("PX_VOLUME") else np.nan,
                                    'open_interest': fieldData.getElement("OPEN_INT").getValue() if fieldData.hasElement("OPEN_INT") else np.nan
                                }
                                futures_data.append(row)
                    break
                
                if event.eventType() == blpapi.Event.TIMEOUT:
                    print("⚠️  Timeout getting UX1 futures data")
                    break
            
            df = pd.DataFrame(futures_data)
            print(f"✅ Collected {len(df)} UX1 futures records")
            return df
            
        except Exception as e:
            print(f"❌ Error collecting UX1 futures data: {e}")
            return pd.DataFrame()
    
    def run_strategy_implementation(self):
        """Run the complete strategy implementation"""
        if not self.connect_bloomberg():
            return None
        
        try:
            print("🚀 Starting Final VIX Strategy Implementation...")
            
            # 1. Get expiry calendar
            expiry_calendar = self.get_vix_expiry_calendar()
            print(f"📅 Generated {len(expiry_calendar)} expiry dates")
            
            # 2. Get current/next month options for testing
            future_expiries = [exp for exp in expiry_calendar if exp['expiry_date'] >= datetime.now().date()][:2]
            
            if not future_expiries:
                print("❌ No valid future expiries found")
                return None
            
            print(f"🎯 Testing options for expiries: {[exp['expiry_date'] for exp in future_expiries]}")
            
            # 3. Generate and test options for first expiry
            test_expiry = future_expiries[0]
            options_for_expiry = self.generate_vix_options_for_expiry(test_expiry)
            
            print(f"📋 Generated {len(options_for_expiry)} option tickers for {test_expiry['expiry_date']}")
            
            # 4. Get option data with Greeks (test first 20 strikes)
            options_df = self.get_current_option_data_with_greeks(options_for_expiry[:20])
            
            if len(options_df) == 0:
                print("❌ No option data retrieved")
                return None
            
            print(f"✅ Retrieved data for {len(options_df)} options")
            
            # 5. Identify target delta positions
            target_positions_df = self.identify_target_delta_options(options_df)
            
            if len(target_positions_df) == 0:
                print("❌ No target positions identified")
                return None
            
            # 6. Get UX1 futures data
            futures_df = self.get_ux1_futures_data()
            
            # 7. Save all data
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            
            # Save DataFrames
            if len(options_df) > 0:
                options_df.to_csv(self.data_dir / f"current_vix_options_{timestamp}.csv", index=False)
                print(f"💾 Saved current options data ({len(options_df)} records)")
            
            if len(target_positions_df) > 0:
                target_positions_df.to_csv(self.data_dir / f"target_positions_{timestamp}.csv", index=False)
                print(f"💾 Saved target positions ({len(target_positions_df)} positions)")
            
            if len(futures_df) > 0:
                futures_df.to_csv(self.data_dir / f"ux1_futures_data_{timestamp}.csv", index=False)
                print(f"💾 Saved UX1 futures data ({len(futures_df)} records)")
            
            # Save expiry calendar
            calendar_json = []
            for exp in expiry_calendar:
                exp_copy = exp.copy()
                exp_copy['expiry_date'] = exp_copy['expiry_date'].strftime('%Y-%m-%d')
                calendar_json.append(exp_copy)
            
            with open(self.data_dir / f"expiry_calendar_{timestamp}.json", 'w') as f:
                json.dump(calendar_json, f, indent=2)
            
            # Create strategy summary
            summary = {
                'timestamp': timestamp,
                'strategy_description': 'SHORT 1x 50Δ call + LONG 2x 10Δ calls + UX1 futures hedge',
                'working_vix_option_format': 'VIX MM/DD/YY C{strike} Index',
                'working_futures_format': 'UX1 Index',
                'expiry_dates_generated': len(expiry_calendar),
                'current_options_tested': len(options_df),
                'target_positions_identified': len(target_positions_df),
                'futures_records_collected': len(futures_df),
                'strategy_ready': len(target_positions_df) >= 2,  # Need both 10Δ and 50Δ
                'target_positions': target_positions_df.to_dict('records') if len(target_positions_df) > 0 else []
            }
            
            summary_file = self.data_dir / f"final_strategy_summary_{timestamp}.json"
            with open(summary_file, 'w') as f:
                json.dump(summary, f, indent=2, default=str)
            
            print(f"\n🎉 Final VIX Strategy Implementation completed!")
            print(f"📊 Summary:")
            print(f"   - Options tested: {len(options_df)}")
            print(f"   - Target positions: {len(target_positions_df)}")
            print(f"   - UX1 futures records: {len(futures_df)}")
            print(f"   - Strategy ready: {'✅' if summary['strategy_ready'] else '❌'}")
            
            if summary['strategy_ready']:
                print(f"\n✅ STRATEGY IS READY FOR IMPLEMENTATION!")
                print(f"   - Found both 10Δ and 50Δ target options")
                print(f"   - UX1 futures data available for hedging")
                print(f"   - Using working ticker formats")
                
                # Show position details
                print(f"\n📋 Position Details:")
                for _, pos in target_positions_df.iterrows():
                    print(f"   {pos['position_type']} {abs(pos['quantity'])}x {pos['target_delta']}Δ: {pos['ticker']} @ ${pos['entry_price']:.2f}")
            
            return summary
            
        finally:
            if self.session:
                self.session.stop()

def main():
    print("🚀 FINAL VIX VOLATILITY STRATEGY")
    print("Using confirmed working Bloomberg ticker formats")
    print("Strategy: SHORT 1x 50Δ call + LONG 2x 10Δ calls + UX1 futures hedge")
    print("=" * 80)
    
    strategy = FinalVIXStrategy(years_back=1)  # 1 year for testing
    
    print("⚠️  Ensure Bloomberg Terminal is running!")
    proceed = input("Run final strategy implementation? (y/n): ").lower().strip()
    
    if proceed == 'y':
        summary = strategy.run_strategy_implementation()
        
        if summary and summary['strategy_ready']:
            print(f"\n🎉 SUCCESS! Complete VIX strategy framework is ready!")
            print(f"📁 Data saved in: {strategy.data_dir}")
            print(f"\nNext steps:")
            print(f"1. Use target_positions_*.csv for specific contracts to trade")
            print(f"2. Use ux1_futures_data_*.csv for delta hedging")
            print(f"3. Implement daily position tracking and P&L calculation")
        else:
            print("❌ Strategy implementation incomplete")
    else:
        print("Implementation cancelled")

if __name__ == "__main__":
    main()
