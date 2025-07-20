#!/usr/bin/env python3
"""
Complete VIX Strategy Implementation
Using confirmed working ticker formats from Bloomberg
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

class CompleteVIXStrategy:
    """
    Complete VIX volatility strategy implementation
    Uses confirmed working formats: VIX Index, UX1 Index, VIX YYMMDD C STRIKE Index
    """
    
    def __init__(self, years_back=5):
        self.session = None
        self.refDataService = None
        self.data_dir = Path('./data/complete_vix_strategy')
        self.results_dir = self.data_dir / 'results'
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.results_dir.mkdir(parents=True, exist_ok=True)
        
        # Date range for historical analysis
        self.end_date = datetime.now()
        self.start_date = self.end_date - timedelta(days=years_back*365)
        
        # Strategy parameters
        self.target_deltas = [10, 50]  # 10Δ long, 50Δ short
        self.quantities = {10: 2, 50: -1}  # LONG 2x 10Δ, SHORT 1x 50Δ
        
        # VIX contract specifications
        self.vix_option_multiplier = 100  # $100 per point
        self.vix_future_multiplier = 1000  # $1000 per point
        
        # Setup logging
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
        
        print(f"🔥 Complete VIX Strategy initialized")
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
                'expiry_string': third_wed.strftime('%y%m%d')
            })
            
            # Next month
            if month == 12:
                current_date = date(year + 1, 1, 1)
            else:
                current_date = date(year, month + 1, 1)
        
        return expiries
    
    def generate_vix_options_for_expiry(self, expiry_info):
        """Generate VIX option tickers for a specific expiry"""
        exp_str = expiry_info['expiry_string']
        
        # Generate reasonable strike range around typical VIX levels (10-50)
        strikes = list(range(10, 51, 5)) + [12, 15, 18, 22, 25, 28, 32, 35, 38, 42, 45, 48]
        strikes = sorted(set(strikes))
        
        option_tickers = []
        
        for strike in strikes:
            ticker = f"VIX {exp_str} C {strike} Index"
            option_tickers.append({
                'ticker': ticker,
                'expiry_date': expiry_info['expiry_date'],
                'expiry_string': exp_str,
                'strike': strike,
                'option_type': 'call'
            })
        
        return option_tickers
    
    def get_option_data_with_greeks(self, option_tickers, batch_size=10):
        """Get option data including Greeks for target delta identification"""
        print(f"📊 Fetching option data with Greeks for {len(option_tickers)} options...")
        
        all_option_data = []
        
        # Fields including Greeks
        option_fields = [
            'PX_LAST', 'PX_BID', 'PX_ASK', 'PX_MID', 'PX_VOLUME', 'OPEN_INT',
            'DELTA_MID', 'GAMMA_MID', 'THETA_MID', 'VEGA_MID', 'IVOL_MID'
        ]
        
        for i in range(0, len(option_tickers), batch_size):
            batch = option_tickers[i:i + batch_size]
            print(f"  Processing batch {i//batch_size + 1}/{(len(option_tickers)-1)//batch_size + 1}")
            
            for option_info in batch:
                ticker = option_info['ticker']
                
                try:
                    # Use current data request (faster than historical for Greeks)
                    request = self.refDataService.createRequest("ReferenceDataRequest")
                    request.getElement("securities").appendValue(ticker)
                    
                    for field in option_fields:
                        request.getElement("fields").appendValue(field)
                    
                    self.session.sendRequest(request)
                    event = self.session.nextEvent(5000)
                    
                    if event.eventType() == blpapi.Event.RESPONSE:
                        for msg in event:
                            securityDataArray = msg.getElement("securityData")
                            
                            for j in range(securityDataArray.numValues()):
                                securityData = securityDataArray.getValue(j)
                                
                                if securityData.hasElement("securityError"):
                                    continue  # Skip non-existent options
                                
                                if securityData.hasElement("fieldData"):
                                    fieldData = securityData.getElement("fieldData")
                                    
                                    row_data = {
                                        'date': datetime.now().strftime('%Y-%m-%d'),
                                        'ticker': ticker,
                                        'expiry_date': option_info['expiry_date'].strftime('%Y-%m-%d'),
                                        'strike': option_info['strike']
                                    }
                                    
                                    # Extract field values
                                    for field in option_fields:
                                        if fieldData.hasElement(field):
                                            value = fieldData.getElement(field).getValue()
                                            clean_name = field.lower().replace('_mid', '').replace('px_', '')
                                            row_data[clean_name] = value if value is not None else np.nan
                                        else:
                                            clean_name = field.lower().replace('_mid', '').replace('px_', '')
                                            row_data[clean_name] = np.nan
                                    
                                    all_option_data.append(row_data)
                
                except Exception as e:
                    self.logger.warning(f"Error fetching {ticker}: {e}")
                
                time.sleep(0.05)  # Rate limiting
        
        return pd.DataFrame(all_option_data)
    
    def identify_target_delta_options(self, options_df):
        """Identify specific options closest to target deltas for position entry"""
        print("🎯 Identifying target delta options for strategy entry...")
        
        target_positions = []
        
        # Group by expiry to find entry points for each monthly cycle
        for expiry, expiry_group in options_df.groupby('expiry_date'):
            # Only consider options with valid data
            valid_options = expiry_group[
                (expiry_group['delta'].notna()) & 
                (expiry_group['delta'] > 0) & 
                (expiry_group['volume'].fillna(0) > 0) &
                (expiry_group['last'].notna()) &
                (expiry_group['last'] > 0)
            ]
            
            if len(valid_options) == 0:
                continue
            
            expiry_positions = {}
            
            # Find closest delta for each target
            for target_delta in self.target_deltas:
                target_delta_decimal = target_delta / 100
                delta_diff = abs(valid_options['delta'] - target_delta_decimal)
                
                if len(delta_diff) == 0:
                    continue
                    
                closest_idx = delta_diff.idxmin()
                
                if delta_diff.loc[closest_idx] < 0.05:  # Within 5 delta points
                    option_row = valid_options.loc[closest_idx].copy()
                    
                    position_info = {
                        'ticker': option_row['ticker'],
                        'expiry_date': expiry,
                        'target_delta': target_delta,
                        'actual_delta': option_row['delta'],
                        'delta_error': delta_diff.loc[closest_idx],
                        'strike': option_row['strike'],
                        'entry_price': option_row.get('mid', option_row.get('last', np.nan)),
                        'quantity': self.quantities[target_delta],
                        'position_type': 'SHORT' if target_delta == 50 else 'LONG',
                        'entry_date': option_row['date']
                    }
                    
                    expiry_positions[target_delta] = position_info
            
            # Only include expiries where we have BOTH 10Δ and 50Δ options
            if len(expiry_positions) == 2:  # Must have both legs
                for delta, position in expiry_positions.items():
                    target_positions.append(position)
                    
                print(f"✅ {expiry}: 50Δ SHORT @ {expiry_positions[50]['strike']} | 10Δ LONG 2x @ {expiry_positions[10]['strike']}")
            else:
                print(f"⚠️  {expiry}: Incomplete - only found {list(expiry_positions.keys())} delta options")
        
        return pd.DataFrame(target_positions)
    
    def get_historical_futures_data(self):
        """Get historical UX1 (front month) futures data for hedging"""
        print("📊 Collecting UX1 front month futures data for hedging...")
        
        try:
            request = self.refDataService.createRequest("HistoricalDataRequest")
            request.getElement("securities").appendValue("UX1 Index")
            
            for field in ["PX_LAST", "PX_SETTLE", "PX_VOLUME", "OPEN_INT"]:
                request.getElement("fields").appendValue(field)
            
            request.set("startDate", self.start_date.strftime('%Y%m%d'))
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
                    print("⚠️  Timeout getting futures data")
                    break
            
            df = pd.DataFrame(futures_data)
            print(f"✅ Collected {len(df)} UX1 futures records")
            return df
            
        except Exception as e:
            print(f"❌ Error collecting futures data: {e}")
            return pd.DataFrame()
    
    def run_strategy_discovery(self):
        """Run strategy discovery to identify positions and collect data"""
        if not self.connect_bloomberg():
            return None
        
        try:
            print("🚀 Starting VIX strategy discovery and data collection...")
            
            # 1. Get expiry calendar
            expiry_calendar = self.get_vix_expiry_calendar()
            print(f"📅 Generated {len(expiry_calendar)} expiry dates")
            
            # 2. Test current month options to find working strikes and get Greeks
            current_expiry = [exp for exp in expiry_calendar if exp['expiry_date'] >= datetime.now().date()][:1]
            
            if not current_expiry:
                print("❌ No valid expiry found")
                return None
            
            print(f"🎯 Testing options for expiry: {current_expiry[0]['expiry_date']}")
            
            # Generate option tickers for current expiry
            current_options = self.generate_vix_options_for_expiry(current_expiry[0])
            
            # Get option data with Greeks
            options_df = self.get_option_data_with_greeks(current_options[:20])  # Test 20 strikes
            
            if len(options_df) == 0:
                print("❌ No option data retrieved")
                return None
            
            # 3. Identify target delta positions
            target_positions_df = self.identify_target_delta_options(options_df)
            
            # 4. Get futures data
            futures_df = self.get_historical_futures_data()
            
            # 5. Collect and save strategy data
            strategy_data = {
                'expiry_calendar': expiry_calendar,
                'current_options': options_df,
                'target_positions': target_positions_df,
                'futures_data': futures_df
            }
            
            # Save data
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            
            # Save DataFrames
            if len(options_df) > 0:
                options_df.to_csv(self.data_dir / f"vix_options_current_{timestamp}.csv", index=False)
                print(f"💾 Saved current options data")
            
            if len(target_positions_df) > 0:
                target_positions_df.to_csv(self.data_dir / f"target_positions_{timestamp}.csv", index=False)
                print(f"💾 Saved target positions")
            
            if len(futures_df) > 0:
                futures_df.to_csv(self.data_dir / f"ux1_futures_{timestamp}.csv", index=False)
                print(f"💾 Saved UX1 futures data")
            
            # Save expiry calendar
            with open(self.data_dir / f"expiry_calendar_{timestamp}.json", 'w') as f:
                # Convert dates to strings for JSON serialization
                calendar_json = []
                for exp in expiry_calendar:
                    exp_copy = exp.copy()
                    exp_copy['expiry_date'] = exp_copy['expiry_date'].strftime('%Y-%m-%d')
                    calendar_json.append(exp_copy)
                json.dump(calendar_json, f, indent=2)
            
            # Summary
            summary = {
                'timestamp': timestamp,
                'expiry_dates_generated': len(expiry_calendar),
                'current_options_tested': len(options_df),
                'target_positions_identified': len(target_positions_df),
                'futures_records': len(futures_df),
                'strategy_ready': len(target_positions_df) == 2  # Need both 10Δ and 50Δ
            }
            
            summary_file = self.data_dir / f"strategy_summary_{timestamp}.json"
            with open(summary_file, 'w') as f:
                json.dump(summary, f, indent=2)
            
            print(f"\n🎉 Strategy discovery completed!")
            print(f"📊 Summary: {summary}")
            
            if summary['strategy_ready']:
                print("✅ Strategy is ready for implementation!")
                print("   - Found both 10Δ and 50Δ target options")
                print("   - UX1 futures data available for hedging")
                print("   - Position entry points identified")
            else:
                print("⚠️  Strategy needs refinement - check target positions")
            
            return summary
            
        finally:
            if self.session:
                self.session.stop()

def main():
    print("🚀 COMPLETE VIX VOLATILITY STRATEGY")
    print("Strategy: SHORT 1x 50Δ call + LONG 2x 10Δ calls + UX1 futures hedge")
    print("=" * 70)
    
    strategy = CompleteVIXStrategy(years_back=1)  # 1 year for initial test
    
    print("⚠️  Ensure Bloomberg Terminal is running!")
    proceed = input("Run strategy discovery? (y/n): ").lower().strip()
    
    if proceed == 'y':
        summary = strategy.run_strategy_discovery()
        
        if summary and summary['strategy_ready']:
            print(f"\n✅ Success! Strategy data saved in: {strategy.data_dir}")
            print("\nNext steps:")
            print("1. Review target positions file")
            print("2. Implement historical position tracking")
            print("3. Calculate full strategy P&L")
        else:
            print("❌ Strategy discovery incomplete")
    else:
        print("Discovery cancelled")

if __name__ == "__main__":
    main()
