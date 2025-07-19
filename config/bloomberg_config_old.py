"""
Bloomberg API Configuration
Configuration settings for Bloomberg data collection
"""

# Bloomberg Field Mappings
VOLATILITY_FIELDS = {
    # Realized Volatility
    'vol_30d': 'VOLATILITY_30D',
    'vol_90d': 'VOLATILITY_90D',
    'vol_120d': 'VOLATILITY_120D',
    'vol_260d': 'VOLATILITY_260D',

    # Implied Volatility - ATM
    'iv_1m_atm': '1MTH_IMPVOL_100.0%MNY_DF',
    'iv_3m_atm': '3MTH_IMPVOL_100.0%MNY_DF',
    'iv_6m_atm': '6MTH_IMPVOL_100.0%MNY_DF',
    'iv_12m_atm': '12MTH_IMPVOL_100.0%MNY_DF',
}

MARKET_DATA_FIELDS = {
    'price': 'PX_LAST',
    'open': 'PX_OPEN',
    'high': 'PX_HIGH',
    'low': 'PX_LOW',
    'volume': 'PX_VOLUME',
    'market_cap': 'CUR_MKT_CAP',
}

# Tickers
SPX_TICKER = 'SPX Index'
SPY_TICKER = 'SPY US Equity'