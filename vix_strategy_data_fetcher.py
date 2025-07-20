"""
VIX Volatility Strategy Data Fetcher
Specifically designed for 50-delta short / 10-delta long call strategy with futures hedging
"""

import sys
import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, date
import json
import time
import schedule
import smtplib
import requests
from email.mime.text import MimeText
from email.mime.multipart import MimeMultipart
from pathlib import Path
import logging

try:
    import blpapi
    print("✅ Bloomberg API imported successfully")
except ImportError as e:
    print(f"❌ Bloomberg API import error: {e}")
    sys.exit(1)

class VIXStrategyDataFetcher:
    """
    Specialized VIX data fetcher for volatility trading strategy
    - Short 50-delta calls
    - Long 2x 10-delta calls  
    - Delta hedge with VIX futures
    - Daily rehedging and monthly roll
    """
    
    def __init__(self, years_back=5):
        self.session = None
        self.refDataService = None
        self.project_root = Path(__file__).parent.absolute()
        self.data_dir = self.project_root / 'data' / 'vix_strategy'
        self.log_dir = self.project_root / 'logs'
        
        # Create directories
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.log_dir.mkdir(parents=True, exist_ok=True)
        
        # Setup logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(self.log_dir / 'vix_strategy_fetcher.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
        
        # Date range for historical data
        self.end_date = datetime.now()
        self.start_date = self.end_date - timedelta(days=years_back*365 + 60)
        
        # Bloomberg field mappings
        self.spot_fields = {
            'close': 'PX_LAST',
            'open': 'PX_OPEN',
            'high': 'PX_HIGH',
            'low': 'PX_LOW',
            'volume': 'PX_VOLUME'
        }
        
        self.futures_fields = {
            'settle': 'PX_SETTLE',
            'last': 'PX_LAST',
            'volume': 'PX_VOLUME',
            'open_interest': 'OPEN_INT',
            'days_to_expiry': 'DAYS_TO_EXP'
        }
        
        self.options_fields = {
            'last': 'PX_LAST',
            'bid': 'PX_BID',
            'ask': 'PX_ASK',
            'mid': 'PX_MID',
            'volume': 'PX_VOLUME',
            'open_interest': 'OPEN_INT',
            'delta': 'DELTA_MID',
            'gamma': 'GAMMA_MID',
            'theta': 'THETA_MID',
            'vega': 'VEGA_MID',
            'implied_vol': 'IVOL_MID',
            'underlying': 'UNDL_PX'
        }
        
        self.logger.info(f"VIX Strategy Data Fetcher initialized")
        self.logger.info(f"Data period: {self.start_date.date()} to {self.end_date.date()}")
    
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
        """
        Generate VIX futures/options expiry dates
        VIX options expire on 3rd Wednesday of each month
        """
        expiries = []
        current_date = self.start_date.replace(day=1)
        
        while current_date <= self.end_date + timedelta(days=90):
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
                'month_code': ['F','G','H','J','K','M','N','Q','U','V','X','Z'][month-1]
            })
            
            # Next month
            if month == 12:
                current_date = date(year + 1, 1, 1)
            else:
                current_date = date(year, month + 1, 1)
        
        return expiries
    
    def generate_vix_futures_tickers(self):
        """Generate VIX 1st month futures tickers"""
        expiries = self.get_vix_expiry_calendar()
        futures_tickers = []
        
        for exp in expiries:
            year_code = str(exp['year'])[-2:]
            ticker = f"VIX{exp['month_code']}{year_code} Curncy"
            
            futures_tickers.append({
                'ticker': ticker,
                'expiry_date': exp['expiry_date'],
                'contract_month': f"{exp['year']}-{exp['month']:02d}"
            })
        
        return futures_tickers
    
    def generate_vix_options_tickers(self, target_deltas=[10, 50]):
        """
        Generate VIX call options tickers for target deltas
        VIX options format: VIX YYMMDD C STRIKE Index
        """
        expiries = self.get_vix_expiry_calendar()
        options_tickers = []
        
        # Common VIX strike range (10-50 typically)
        strikes = list(range(10, 51, 5)) + [12, 15, 18, 22, 25, 28, 32, 35, 38, 42, 45, 48]
        strikes = sorted(set(strikes))
        
        for exp in expiries:
            exp_str = exp['expiry_date'].strftime('%y%m%d')
            
            for strike in strikes:
                ticker = f"VIX {exp_str} C {strike:g} Index"
                
                options_tickers.append({
                    'ticker': ticker,
                    'expiry_date': exp['expiry_date'],
                    'strike': strike,
                    'option_type': 'call',
                    'contract_month': f"{exp['year']}-{exp['month']:02d}"
                })
        
        return options_tickers
    
    def fetch_historical_data(self, securities, fields, data_type=""):
        """Generic Bloomberg historical data fetch"""
        all_data = []
        batch_size = 10
        
        self.logger.info(f"Fetching {data_type} data for {len(securities)} securities")
        
        for i in range(0, len(securities), batch_size):
            batch = securities[i:i + batch_size]
            self.logger.info(f"Processing batch {i//batch_size + 1}/{(len(securities)-1)//batch_size + 1}")
            
            for security in batch:
                ticker = security if isinstance(security, str) else security['ticker']
                
                try:
                    request = self.refDataService.createRequest("HistoricalDataRequest")
                    request.getElement("securities").appendValue(ticker)
                    
                    for field in fields.values():
                        request.getElement("fields").appendValue(field)
                    
                    request.set("startDate", self.start_date.strftime('%Y%m%d'))
                    request.set("endDate", self.end_date.strftime('%Y%m%d'))
                    request.set("periodicitySelection", "DAILY")
                    
                    self.session.sendRequest(request)
                    
                    # Process response
                    while True:
                        event = self.session.nextEvent(10000)
                        
                        if event.eventType() == blpapi.Event.RESPONSE:
                            for msg in event:
                                securityData = msg.getElement("securityData")
                                
                                if securityData.hasElement("securityError"):
                                    self.logger.warning(f"Error fetching {ticker}")
                                    break
                                
                                fieldDataArray = securityData.getElement("fieldData")
                                
                                for j in range(fieldDataArray.numValues()):
                                    fieldData = fieldDataArray.getValue(j)
                                    trade_date = fieldData.getElement("date").getValue()
                                    
                                    row_data = {
                                        'date': trade_date.strftime('%Y-%m-%d'),
                                        'ticker': ticker,
                                        'data_type': data_type
                                    }
                                    
                                    # Add security-specific info
                                    if isinstance(security, dict):
                                        row_data.update({
                                            'expiry_date': security.get('expiry_date', '').strftime('%Y-%m-%d') if security.get('expiry_date') else '',
                                            'strike': security.get('strike', np.nan),
                                            'contract_month': security.get('contract_month', '')
                                        })
                                    
                                    # Extract field values
                                    for clean_name, bloomberg_field in fields.items():
                                        if fieldData.hasElement(bloomberg_field):
                                            value = fieldData.getElement(bloomberg_field).getValue()
                                            row_data[clean_name] = value if value is not None else np.nan
                                        else:
                                            row_data[clean_name] = np.nan
                                    
                                    all_data.append(row_data)
                            break
                        
                        if event.eventType() == blpapi.Event.TIMEOUT:
                            self.logger.warning(f"Timeout fetching {ticker}")
                            break
                
                except Exception as e:
                    self.logger.error(f"Error processing {ticker}: {e}")
                
                time.sleep(0.05)  # Rate limiting
        
        return pd.DataFrame(all_data)
    
    def identify_target_delta_options(self, options_df, target_deltas=[10, 50]):
        """
        Identify options closest to target deltas for POSITION INITIATION only
        This finds the specific contracts to enter at the start of each expiry cycle
        """
        self.logger.info("Identifying target delta options for position initiation...")
        
        target_options = []
        
        # Group by expiry to find entry points for each monthly cycle
        for expiry, expiry_group in options_df.groupby('expiry_date'):
            expiry_date = pd.to_datetime(expiry).date()
            
            # Find first trading day with valid data for this expiry
            first_valid_date = expiry_group[
                (expiry_group['delta'].notna()) & 
                (expiry_group['delta'] > 0) & 
                (expiry_group['volume'].fillna(0) > 0)
            ]['date'].min()
            
            if pd.isna(first_valid_date):
                continue
            
            # Get options for the entry date
            entry_options = expiry_group[
                (expiry_group['date'] == first_valid_date) &
                (expiry_group['delta'].notna()) & 
                (expiry_group['delta'] > 0) & 
                (expiry_group['volume'].fillna(0) > 0)
            ]
            
            if len(entry_options) == 0:
                continue
            
            # Find the specific contracts to enter for each target delta
            position_contracts = {}
            
            for target_delta in target_deltas:
                # Find closest delta
                delta_diff = abs(entry_options['delta'] - target_delta/100)
                closest_idx = delta_diff.idxmin()
                
                if delta_diff.loc[closest_idx] < 0.05:  # Within 5 delta points
                    option_row = entry_options.loc[closest_idx].copy()
                    option_row['target_delta'] = target_delta
                    option_row['delta_error'] = delta_diff.loc[closest_idx]
                    option_row['position_type'] = 'SHORT' if target_delta == 50 else 'LONG'
                    option_row['quantity'] = -1 if target_delta == 50 else 2  # SHORT 1x 50Δ, LONG 2x 10Δ
                    option_row['entry_date'] = first_valid_date
                    option_row['expiry_date'] = expiry_date
                    option_row['strategy_leg'] = f"{target_delta}delta_{'short' if target_delta == 50 else 'long'}"
                    
                    target_options.append(option_row)
                    position_contracts[target_delta] = option_row['ticker']
            
            # Log the contracts selected for this expiry
            if len(position_contracts) == 2:  # Must have BOTH 50Δ and 10Δ
                self.logger.info(f"✅ Expiry {expiry_date}: SHORT 1x 50Δ = {position_contracts.get(50)}")
                self.logger.info(f"   LONG 2x 10Δ = {position_contracts.get(10)}")
            else:
                self.logger.warning(f"⚠️  Expiry {expiry_date}: Incomplete position - only found {list(position_contracts.keys())} delta options")
        
        return pd.DataFrame(target_options)
    
    def create_position_tracking_manifest(self, target_delta_df):
        """
        Create a manifest of specific option positions to track daily
        This defines exactly which contracts to monitor for P&L
        """
        self.logger.info("Creating position tracking manifest...")
        
        position_manifest = []
        
        for _, row in target_delta_df.iterrows():
            # Calculate roll dates (day before expiry)
            expiry_date = pd.to_datetime(row['expiry_date']).date()
            roll_date = expiry_date - timedelta(days=1)
            
            position_info = {
                'ticker': row['ticker'],
                'target_delta': row['target_delta'],
                'position_type': row['position_type'],
                'quantity': row['quantity'],
                'entry_date': row['entry_date'],
                'expiry_date': expiry_date,
                'roll_date': roll_date,
                'strike': row['strike'],
                'entry_delta': row['delta'],
                'entry_price': row.get('mid', row.get('last', np.nan))
            }
            position_manifest.append(position_info)
        
        manifest_df = pd.DataFrame(position_manifest)
        
        # Save manifest for position tracking
        manifest_file = self.data_dir / 'position_tracking_manifest.csv'
        manifest_df.to_csv(manifest_file, index=False)
        self.logger.info(f"Position manifest saved: {manifest_file}")
        
        return manifest_df
    
    def track_specific_positions(self, position_manifest):
        """
        Track the specific option positions daily for P&L calculation
        This gets daily prices/deltas for the exact contracts you're holding
        """
        self.logger.info("Tracking specific option positions...")
        
        # Get unique tickers to track
        position_tickers = position_manifest['ticker'].unique().tolist()
        
        # Fetch daily data for these specific contracts
        position_data = self.fetch_historical_data(
            position_tickers,
            self.options_fields,
            "Position_Tracking"
        )
        
        if len(position_data) == 0:
            self.logger.warning("No position tracking data retrieved")
            return pd.DataFrame()
        
        # Merge with position info
        position_tracking = position_data.merge(
            position_manifest[['ticker', 'target_delta', 'position_type', 'quantity', 
                             'entry_date', 'roll_date', 'entry_price', 'entry_delta']],
            on='ticker',
            how='left'
        )
        
        # Filter data to position holding period only
        position_tracking['date'] = pd.to_datetime(position_tracking['date'])
        position_tracking['entry_date'] = pd.to_datetime(position_tracking['entry_date'])
        position_tracking['roll_date'] = pd.to_datetime(position_tracking['roll_date'])
        
        # Only include dates when position was held
        active_positions = position_tracking[
            (position_tracking['date'] >= position_tracking['entry_date']) &
            (position_tracking['date'] <= position_tracking['roll_date'])
        ].copy()
        
        # Calculate daily P&L
        active_positions['price_change'] = active_positions['mid'] - active_positions['entry_price']
        active_positions['daily_pnl'] = active_positions['price_change'] * active_positions['quantity'] * 100  # VIX options are $100 multiplier
        active_positions['delta_change'] = active_positions['delta'] - active_positions['entry_delta']
        
        return active_positions
    
    def collect_strategy_data(self):
        """
        Collect all data required for the VIX volatility strategy
        Phase 1: Position Initiation (find contracts to enter)
        Phase 2: Position Tracking (daily P&L on specific contracts)
        """
        if not self.connect_bloomberg():
            return None
        
        try:
            strategy_data = {}
            
            # 1. VIX Spot Data
            self.logger.info("Collecting VIX spot data...")
            vix_spot_df = self.fetch_historical_data(
                ['VIX Index'], 
                self.spot_fields, 
                "VIX_Spot"
            )
            strategy_data['vix_spot'] = vix_spot_df
            
            # 2. VIX Futures Data
            self.logger.info("Collecting VIX futures data...")
            futures_tickers = self.generate_vix_futures_tickers()
            futures_df = self.fetch_historical_data(
                futures_tickers,
                self.futures_fields,
                "VIX_Future"
            )
            strategy_data['vix_futures'] = futures_df
            
            # 3. VIX Options Data (for position discovery)
            self.logger.info("Collecting VIX options data for position identification...")
            options_tickers = self.generate_vix_options_tickers()
            
            # Limit to manageable dataset for initial discovery
            current_expiries = [opt for opt in options_tickers 
                              if opt['expiry_date'] >= self.start_date.date()]
            
            options_df = self.fetch_historical_data(
                current_expiries[:200],  # Reasonable limit for testing
                self.options_fields,
                "VIX_Option_Discovery"
            )
            strategy_data['vix_options_discovery'] = options_df
            
            # 4. CRITICAL: Identify Specific Positions to Enter
            if len(options_df) > 0:
                self.logger.info("*** PHASE 1: Identifying specific option contracts to enter ***")
                target_delta_df = self.identify_target_delta_options(options_df)
                strategy_data['position_entry_points'] = target_delta_df
                
                # 5. CRITICAL: Create Position Tracking Manifest
                if len(target_delta_df) > 0:
                    position_manifest = self.create_position_tracking_manifest(target_delta_df)
                    strategy_data['position_manifest'] = position_manifest
                    
                    # 6. CRITICAL: Track Specific Positions Daily
                    self.logger.info("*** PHASE 2: Tracking specific positions for daily P&L ***")
                    position_tracking_df = self.track_specific_positions(position_manifest)
                    strategy_data['position_tracking'] = position_tracking_df
                    
                    self.logger.info(f"Position tracking: {len(position_tracking_df)} daily observations")
                else:
                    self.logger.warning("No valid position entry points found")
            else:
                self.logger.warning("No options data retrieved")
            
            return strategy_data
            
        except Exception as e:
            self.logger.error(f"Error collecting strategy data: {e}")
            return None
        
        finally:
            if self.session:
                self.session.stop()
    
    def save_strategy_data(self, strategy_data):
        """Save collected data to files"""
        if not strategy_data:
            return
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        for data_type, df in strategy_data.items():
            if len(df) > 0:
                filename = f"vix_strategy_{data_type}_{timestamp}.csv"
                filepath = self.data_dir / filename
                df.to_csv(filepath, index=False)
                self.logger.info(f"Saved {len(df)} records to {filename}")
        
        # Create summary
        summary = {
            'collection_timestamp': timestamp,
            'data_period': f"{self.start_date.date()} to {self.end_date.date()}",
            'datasets': {k: len(v) for k, v in strategy_data.items()}
        }
        
        summary_file = self.data_dir / f"collection_summary_{timestamp}.json"
        with open(summary_file, 'w') as f:
            json.dump(summary, f, indent=2)
        
        self.logger.info(f"Collection complete. Summary: {summary}")
        return summary
    
    def send_alert(self, message, alert_type="INFO"):
        """Send email/Slack alerts"""
        # Implementation depends on your notification preferences
        self.logger.info(f"ALERT [{alert_type}]: {message}")
    
    def run_daily_collection(self):
        """Run daily data collection (for production scheduling)"""
        self.logger.info("Starting daily VIX strategy data collection...")
        
        # Adjust date range for daily updates
        self.end_date = datetime.now()
        self.start_date = self.end_date - timedelta(days=5)  # Get last 5 days
        
        strategy_data = self.collect_strategy_data()
        
        if strategy_data:
            summary = self.save_strategy_data(strategy_data)
            self.send_alert(f"Daily collection completed: {summary['datasets']}")
        else:
            self.send_alert("Daily collection failed", "ERROR")
    
    def schedule_daily_collection(self, run_time="09:30"):
        """Schedule daily data collection"""
        schedule.every().day.at(run_time).do(self.run_daily_collection)
        
        self.logger.info(f"Scheduled daily collection at {run_time}")
        
        while True:
            schedule.run_pending()
            time.sleep(60)

# Usage example
if __name__ == "__main__":
    # For historical backtest data collection
    fetcher = VIXStrategyDataFetcher(years_back=5)
    strategy_data = fetcher.collect_strategy_data()
    
    if strategy_data:
        fetcher.save_strategy_data(strategy_data)
        print("✅ VIX strategy data collection completed!")
    else:
        print("❌ Data collection failed")
    
    # For production daily scheduling (uncomment to run)
    # fetcher.schedule_daily_collection("09:30")
