"""
Bloomberg API Configuration - IMPROVED VERSION
Configuration settings for Bloomberg data collection with proper volatility labeling
"""

# Bloomberg Field Mappings with PROPER VOLATILITY LABELS
VOLATILITY_FIELDS = {
    # Realized (Historical) Volatility - Properly Labeled
    'realized_vol_30d': 'VOLATILITY_30D',
    'realized_vol_90d': 'VOLATILITY_90D', 
    'realized_vol_120d': 'VOLATILITY_120D',
    'realized_vol_260d': 'VOLATILITY_260D',
    
    # Implied Volatility - At-the-Money by Tenor
    'implied_vol_1m_atm': '1MTH_IMPVOL_100.0%MNY_DF',
    'implied_vol_3m_atm': '3MTH_IMPVOL_100.0%MNY_DF',
    'implied_vol_6m_atm': '6MTH_IMPVOL_100.0%MNY_DF',
    'implied_vol_12m_atm': '12MTH_IMPVOL_100.0%MNY_DF',
    
    # Implied Volatility - Moneyness Structure (3M tenor)
    'implied_vol_3m_90_moneyness': '3MTH_IMPVOL_90.0%MNY_DF',
    'implied_vol_3m_95_moneyness': '3MTH_IMPVOL_95.0%MNY_DF',
    'implied_vol_3m_100_moneyness': '3MTH_IMPVOL_100.0%MNY_DF',
    'implied_vol_3m_105_moneyness': '3MTH_IMPVOL_105.0%MNY_DF',
    'implied_vol_3m_110_moneyness': '3MTH_IMPVOL_110.0%MNY_DF'
}

# Volatility Surface Fields for Advanced Analysis
VOL_SURFACE_FIELDS = {
    # Term Structure (ATM across time)
    'term_structure': {
        'implied_vol_1m_atm': '1MTH_IMPVOL_100.0%MNY_DF',
        'implied_vol_2m_atm': '2MTH_IMPVOL_100.0%MNY_DF',
        'implied_vol_3m_atm': '3MTH_IMPVOL_100.0%MNY_DF',
        'implied_vol_6m_atm': '6MTH_IMPVOL_100.0%MNY_DF',
        'implied_vol_12m_atm': '12MTH_IMPVOL_100.0%MNY_DF'
    },
    
    # Skew Structure (3M across strikes)  
    'skew_structure_3m': {
        'implied_vol_3m_80_moneyness': '3MTH_IMPVOL_80.0%MNY_DF',
        'implied_vol_3m_90_moneyness': '3MTH_IMPVOL_90.0%MNY_DF',
        'implied_vol_3m_95_moneyness': '3MTH_IMPVOL_95.0%MNY_DF',
        'implied_vol_3m_100_moneyness': '3MTH_IMPVOL_100.0%MNY_DF',
        'implied_vol_3m_105_moneyness': '3MTH_IMPVOL_105.0%MNY_DF',
        'implied_vol_3m_110_moneyness': '3MTH_IMPVOL_110.0%MNY_DF',
        'implied_vol_3m_120_moneyness': '3MTH_IMPVOL_120.0%MNY_DF'
    }
}

MARKET_DATA_FIELDS = {
    'last_price': 'PX_LAST',
    'open_price': 'PX_OPEN',
    'high_price': 'PX_HIGH', 
    'low_price': 'PX_LOW',
    'volume': 'PX_VOLUME',
    'market_cap_usd': 'CUR_MKT_CAP',
    'shares_outstanding': 'EQY_SH_OUT',
    'company_name': 'NAME'
}

ETF_FIELDS = {
    'holdings_list': 'FUND_HOLDINGS',
    'holdings_weights': 'FUND_HOLDING_WEIGHTS',
    'nav_per_share': 'FUND_NET_ASSET_VAL',
    'total_assets': 'FUND_TOTAL_ASSETS'
}

# Tickers
SPX_TICKER = 'SPX Index'
SPY_TICKER = 'SPY US Equity'
VIX_TICKER = 'VIX Index'

# Top 50 SPX Components (will be dynamically updated from SPY holdings)
TOP_50_TICKERS = [
    'AAPL US Equity', 'MSFT US Equity', 'NVDA US Equity', 'AMZN US Equity',
    'META US Equity', 'GOOGL US Equity', 'GOOG US Equity', 'BRK/B US Equity',
    'LLY US Equity', 'AVGO US Equity', 'JPM US Equity', 'TSLA US Equity',
    'WMT US Equity', 'V US Equity', 'UNH US Equity', 'XOM US Equity',
    'MA US Equity', 'PG US Equity', 'JNJ US Equity', 'COST US Equity',
    'HD US Equity', 'NFLX US Equity', 'BAC US Equity', 'ABBV US Equity',
    'CRM US Equity', 'CVX US Equity', 'KO US Equity', 'AMD US Equity',
    'PEP US Equity', 'TMO US Equity', 'LIN US Equity', 'CSCO US Equity',
    'ACN US Equity', 'ADBE US Equity', 'MRK US Equity', 'ORCL US Equity',
    'ABT US Equity', 'COP US Equity', 'DHR US Equity', 'NKE US Equity',
    'VZ US Equity', 'TXN US Equity', 'QCOM US Equity', 'PM US Equity',
    'WFC US Equity', 'DIS US Equity', 'IBM US Equity', 'CAT US Equity',
    'GE US Equity', 'MS US Equity'
]

# Data Collection Settings
DEFAULT_START_DATE = '2023-01-01'
DEFAULT_END_DATE = None  # Will use current date
MAX_HISTORY_DAYS = 365 * 2  # 2 years max

# Data quality settings
MIN_DATA_POINTS = 20  # Minimum data points required
MAX_MISSING_PCT = 0.1  # Maximum 10% missing data allowed

# Output settings
OUTPUT_FORMATS = ['csv', 'parquet', 'excel']
DEFAULT_OUTPUT_FORMAT = 'csv'

# Column name mapping for clean output
CLEAN_COLUMN_NAMES = {
    'VOLATILITY_30D': 'realized_vol_30d',
    'VOLATILITY_90D': 'realized_vol_90d',
    'VOLATILITY_120D': 'realized_vol_120d', 
    'VOLATILITY_260D': 'realized_vol_260d',
    '1MTH_IMPVOL_100.0%MNY_DF': 'implied_vol_1m_atm',
    '3MTH_IMPVOL_100.0%MNY_DF': 'implied_vol_3m_atm',
    '6MTH_IMPVOL_100.0%MNY_DF': 'implied_vol_6m_atm',
    '12MTH_IMPVOL_100.0%MNY_DF': 'implied_vol_12m_atm',
    '3MTH_IMPVOL_90.0%MNY_DF': 'implied_vol_3m_90_moneyness',
    '3MTH_IMPVOL_95.0%MNY_DF': 'implied_vol_3m_95_moneyness',
    '3MTH_IMPVOL_105.0%MNY_DF': 'implied_vol_3m_105_moneyness',
    '3MTH_IMPVOL_110.0%MNY_DF': 'implied_vol_3m_110_moneyness',
    'PX_LAST': 'last_price',
    'CUR_MKT_CAP': 'market_cap_usd',
    'NAME': 'company_name'
}