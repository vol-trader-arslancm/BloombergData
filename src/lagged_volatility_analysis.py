# Properly Lagged Volatility Analysis
# Comparing implied vol with FUTURE realized vol (the correct way)

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import plotly.graph_objects as go
from datetime import datetime, timedelta

def create_lagged_volatility_comparison(df):
    """
    Create proper forward-looking volatility comparison
    """
    
    # Separate realized and implied data
    spx_realized = df[df['data_type'] == 'realized'].copy()
    spx_implied = df[df['data_type'] == 'implied'].copy()
    
    print("🔄 CREATING PROPERLY LAGGED VOLATILITY COMPARISON")
    print("=" * 60)
    
    # Create forward-looking realized volatility
    comparisons = []
    
    # 1M implied vs 30D future realized
    if 'implied_vol_1m_atm' in spx_implied.columns and 'realized_vol_30d' in spx_realized.columns:
        print("📊 Processing 1M Implied vs 30D Future Realized...")
        comp_1m = create_forward_comparison(
            spx_implied, spx_realized, 
            'implied_vol_1m_atm', 'realized_vol_30d', 
            30, '1M Implied vs 30D Future Realized'
        )
        if len(comp_1m) > 0:
            comparisons.append(comp_1m)
    
    # 3M implied vs 90D future realized
    if 'implied_vol_3m_atm' in spx_implied.columns and 'realized_vol_90d' in spx_realized.columns:
        print("📊 Processing 3M Implied vs 90D Future Realized...")
        comp_3m = create_forward_comparison(
            spx_implied, spx_realized,
            'implied_vol_3m_atm', 'realized_vol_90d',
            90, '3M Implied vs 90D Future Realized'
        )
        if len(comp_3m) > 0:
            comparisons.append(comp_3m)
    
    # 6M implied vs 180D future realized  
    if 'implied_vol_6m_atm' in spx_implied.columns and 'realized_vol_180d' in spx_realized.columns:
        print("📊 Processing 6M Implied vs 180D Future Realized...")
        comp_6m = create_forward_comparison(
            spx_implied, spx_realized,
            'implied_vol_6m_atm', 'realized_vol_180d', 
            180, '6M Implied vs 180D Future Realized'
        )
        if len(comp_6m) > 0:
            comparisons.append(comp_6m)
    
    return comparisons

def create_forward_comparison(implied_df, realized_df, implied_col, realized_col, lag_days, comparison_name):
    """
    Create forward-looking comparison by lagging realized volatility
    
    Parameters:
    - implied_df: DataFrame with implied volatility data
    - realized_df: DataFrame with realized volatility data  
    - implied_col: Column name for implied volatility
    - realized_col: Column name for realized volatility
    - lag_days: Number of days to lag realized vol (to make it "future")
    - comparison_name: Name for this comparison
    """
    
    try:
        # Get clean datasets
        implied_clean = implied_df[['date', implied_col]].dropna()
        realized_clean = realized_df[['date', realized_col]].dropna()
        
        # Create future realized volatility by shifting dates backwards
        # This makes realized vol "future" relative to implied vol
        realized_future = realized_clean.copy()
        realized_future['date'] = realized_future['date'] - pd.Timedelta(days=lag_days)
        
        # Merge: implied vol on date X with realized vol that STARTS on date X
        comparison = pd.merge(
            implied_clean, 
            realized_future, 
            on='date', 
            how='inner',
            suffixes=('_implied', '_future_realized')
        )
        
        if len(comparison) > 0:
            # Calculate metrics
            comparison['vol_spread'] = comparison[implied_col] - comparison[realized_col]
            comparison['vol_ratio'] = comparison[implied_col] / comparison[realized_col]
            comparison['comparison_type'] = comparison_name
            
            print(f"   ✅ {comparison_name}: {len(comparison):,} observations")
            print(f"      Date range: {comparison['date'].min().strftime('%Y-%m-%d')} to {comparison['date'].max().strftime('%Y-%m-%d')}")
            print(f"      Avg vol spread: {comparison['vol_spread'].mean():.2f}%")
            print(f"      Implied premium: {(comparison['vol_spread'] > 0).mean()*100:.1f}% of time")
        
        return comparison
        
    except Exception as e:
        print(f"   ❌ Error creating {comparison_name}: {e}")
        return pd.DataFrame()

def analyze_volatility_premium(comparison_df):
    """
    Analyze the true volatility risk premium
    """
    if len(comparison_df) == 0:
        print("No data available for volatility premium analysis")
        return
    
    print(f"\n📈 VOLATILITY RISK PREMIUM ANALYSIS")
    print(f"Comparison: {comparison_df['comparison_type'].iloc[0]}")
    print("=" * 50)
    
    # Get the column names dynamically
    implied_col = [col for col in comparison_df.columns if 'implied_vol' in col][0]
    realized_col = [col for col in comparison_df.columns if 'realized_vol' in col][0]
    
    # Basic statistics
    vol_spread = comparison_df['vol_spread']
    vol_ratio = comparison_df['vol_ratio']
    
    print(f"📊 RISK PREMIUM STATISTICS:")
    print(f"   Average vol spread: {vol_spread.mean():.2f}%")
    print(f"   Median vol spread: {vol_spread.median():.2f}%") 
    print(f"   Std dev of spread: {vol_spread.std():.2f}%")
    print(f"   Sharpe ratio of spread: {vol_spread.mean()/vol_spread.std():.2f}")
    
    print(f"\n📈 DIRECTIONAL ACCURACY:")
    implied_higher = (vol_spread > 0).mean() * 100
    print(f"   Implied > Future Realized: {implied_higher:.1f}% of time")
    print(f"   Average over-pricing: {vol_spread[vol_spread > 0].mean():.2f}%")
    print(f"   Average under-pricing: {vol_spread[vol_spread < 0].mean():.2f}%")
    
    print(f"\n⚡ EXTREME EVENTS:")
    print(f"   Max over-pricing: {vol_spread.max():.2f}% on {comparison_df.loc[vol_spread.idxmax(), 'date'].strftime('%Y-%m-%d')}")
    print(f"   Max under-pricing: {vol_spread.min():.2f}% on {comparison_df.loc[vol_spread.idxmin(), 'date'].strftime('%Y-%m-%d')}")
    
    # Volatility regime analysis
    if implied_col in comparison_df.columns:
        comparison_df['vol_regime'] = pd.cut(
            comparison_df[implied_col], 
            bins=[0, 15, 25, 35, float('inf')], 
            labels=['Low Vol', 'Normal Vol', 'Elevated Vol', 'High Vol']
        )
        
        print(f"\n🎯 PREMIUM BY VOLATILITY REGIME:")
        regime_analysis = comparison_df.groupby('vol_regime')['vol_spread'].agg(['mean', 'std', 'count'])
        for regime, stats in regime_analysis.iterrows():
            print(f"   {regime}: {stats['mean']:.2f}% ± {stats['std']:.2f}% ({int(stats['count'])} obs)")

def plot_lagged_comparison(comparison_df):
    """
    Plot the properly lagged volatility comparison
    """
    if len(comparison_df) == 0:
        print("No data to plot")
        return
    
    # Get column names dynamically
    implied_col = [col for col in comparison_df.columns if 'implied_vol' in col][0]
    realized_col = [col for col in comparison_df.columns if 'realized_vol' in col][0]
    
    fig = go.Figure()
    
    # Implied volatility
    fig.add_trace(
        go.Scatter(
            x=comparison_df['date'],
            y=comparison_df[implied_col],
            mode='lines',
            name='Implied Volatility (Forward-Looking)',
            line=dict(color='#ff7f0e', width=2),
            hovertemplate='<b>Implied Vol</b><br>Date: %{x}<br>Vol: %{y:.2f}%<extra></extra>'
        )
    )
    
    # Future realized volatility  
    fig.add_trace(
        go.Scatter(
            x=comparison_df['date'],
            y=comparison_df[realized_col],
            mode='lines',
            name='Future Realized Volatility',
            line=dict(color='#1f77b4', width=2),
            hovertemplate='<b>Future Realized Vol</b><br>Date: %{x}<br>Vol: %{y:.2f}%<extra></extra>'
        )
    )
    
    # Volatility spread
    fig.add_trace(
        go.Scatter(
            x=comparison_df['date'],
            y=comparison_df['vol_spread'],
            mode='lines',
            name='Vol Risk Premium (Implied - Future Realized)',
            line=dict(color='#2ca02c', width=2, dash='dash'),
            yaxis='y2',
            hovertemplate='<b>Vol Risk Premium</b><br>Date: %{x}<br>Premium: %{y:.2f}%<extra></extra>'
        )
    )
    
    # Add zero line
    fig.add_hline(y=0, line_dash="dot", line_color="gray", opacity=0.5)
    
    fig.update_layout(
        title=f'Properly Lagged Volatility Analysis: {comparison_df["comparison_type"].iloc[0]}',
        xaxis_title='Date',
        yaxis_title='Volatility (%)',
        yaxis2=dict(
            title='Volatility Risk Premium (%)',
            overlaying='y',
            side='right'
        ),
        height=600,
        template='plotly_white',
        legend=dict(x=0.02, y=0.98)
    )
    
    fig.show()

# Example usage function
def run_lagged_analysis(df):
    """
    Run the complete lagged volatility analysis
    """
    print("🚀 RUNNING PROPERLY LAGGED VOLATILITY ANALYSIS")
    print("=" * 60)
    print("This analysis compares implied vol with FUTURE realized vol")
    print("(the theoretically correct approach for risk premium measurement)")
    print()
    
    # Create all available comparisons
    comparisons = create_lagged_volatility_comparison(df)
    
    if len(comparisons) == 0:
        print("❌ No valid comparisons could be created")
        print("This might be because:")
        print("   - Insufficient data history")
        print("   - Missing volatility columns")
        print("   - Date alignment issues")
        return
    
    # Analyze each comparison
    for i, comp_df in enumerate(comparisons):
        print(f"\n{'='*60}")
        print(f"ANALYSIS {i+1}: {comp_df['comparison_type'].iloc[0]}")
        print(f"{'='*60}")
        
        # Statistical analysis
        analyze_volatility_premium(comp_df)
        
        # Plot
        plot_lagged_comparison(comp_df)
    
    return comparisons

# Usage instructions
print("""
📚 HOW TO USE THIS LAGGED ANALYSIS:

1. Load your data:
   df = pd.read_csv('your_volatility_data.csv')
   df['date'] = pd.to_datetime(df['date'])

2. Run the analysis:
   comparisons = run_lagged_analysis(df)

3. The analysis will:
   ✅ Compare implied vol with FUTURE realized vol
   ✅ Calculate true volatility risk premiums  
   ✅ Show regime-based analysis
   ✅ Generate proper forward-looking charts

This gives you the REAL volatility risk premium that traders actually experience!
""")