"""
Advanced Volatility Analysis Module
Sophisticated volatility analysis including proper forward-looking comparisons,
regime analysis, and volatility risk premium calculations.

Key Concepts:
- Index options typically trade at a persistent volatility risk premium
- Volatility is mean-reverting but exhibits regime persistence  
- Proper analysis compares implied vol with FUTURE realized vol
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

class VolatilityAnalyzer:
    """
    Advanced volatility analysis with proper forward-looking comparisons
    """
    
    def __init__(self, data_df):
        """
        Initialize with volatility DataFrame
        
        Parameters:
        - data_df: DataFrame with columns ['date', 'ticker', 'data_type', volatility columns]
        """
        self.df = data_df.copy()
        self.df['date'] = pd.to_datetime(self.df['date'])
        
        # Split by data type
        self.realized_df = self.df[self.df['data_type'] == 'realized'].copy()
        self.implied_df = self.df[self.df['data_type'] == 'implied'].copy()
        
        print(f"📊 VolatilityAnalyzer initialized")
        print(f"   Realized vol observations: {len(self.realized_df):,}")
        print(f"   Implied vol observations: {len(self.implied_df):,}")
    
    def create_forward_looking_comparison(self, ticker='SPX Index'):
        """
        Create properly lagged volatility comparisons for a given ticker
        """
        print(f"\n🔄 CREATING FORWARD-LOOKING ANALYSIS FOR {ticker}")
        print("=" * 60)
        
        # Filter for specific ticker
        realized = self.realized_df[self.realized_df['ticker'] == ticker].copy()
        implied = self.implied_df[self.implied_df['ticker'] == ticker].copy()
        
        comparisons = []
        
        # Define comparison pairs (implied_col, realized_col, lag_days)
        comparison_pairs = [
            ('implied_vol_1m_atm', 'realized_vol_30d', 30),
            ('implied_vol_3m_atm', 'realized_vol_90d', 90),
            ('implied_vol_6m_atm', 'realized_vol_180d', 180),
            ('implied_vol_12m_atm', 'realized_vol_252d', 252)
        ]
        
        for implied_col, realized_col, lag_days in comparison_pairs:
            if implied_col in implied.columns and realized_col in realized.columns:
                print(f"📊 Processing {implied_col} vs {realized_col} (+{lag_days}d forward)...")
                
                comp = self._create_single_comparison(
                    implied, realized, implied_col, realized_col, lag_days
                )
                
                if len(comp) > 0:
                    comp['comparison_type'] = f"{implied_col} vs {realized_col} forward"
                    comp['tenor_days'] = lag_days
                    comparisons.append(comp)
        
        return comparisons
    
    def _create_single_comparison(self, implied_df, realized_df, implied_col, realized_col, lag_days):
        """
        Create a single forward-looking comparison
        """
        try:
            # Clean data
            implied_clean = implied_df[['date', implied_col]].dropna()
            realized_clean = realized_df[['date', realized_col]].dropna()
            
            # Shift realized vol dates backward to create "future" alignment
            realized_future = realized_clean.copy()
            realized_future['date'] = realized_future['date'] - pd.Timedelta(days=lag_days)
            
            # Merge: today's implied vol with realized vol starting today
            comparison = pd.merge(
                implied_clean, 
                realized_future, 
                on='date', 
                how='inner'
            )
            
            if len(comparison) > 0:
                # Calculate metrics
                comparison['vol_spread'] = comparison[implied_col] - comparison[realized_col]
                comparison['vol_ratio'] = comparison[implied_col] / comparison[realized_col]
                comparison['abs_spread'] = abs(comparison['vol_spread'])
                
                # Risk premium statistics
                avg_premium = comparison['vol_spread'].mean()
                premium_frequency = (comparison['vol_spread'] > 0).mean() * 100
                
                print(f"   ✅ {len(comparison):,} observations")
                print(f"      Date range: {comparison['date'].min().strftime('%Y-%m-%d')} to {comparison['date'].max().strftime('%Y-%m-%d')}")
                print(f"      Avg risk premium: {avg_premium:.2f}%")
                print(f"      Premium frequency: {premium_frequency:.1f}%")
                
                return comparison
            else:
                print(f"   ❌ No overlapping data found")
                return pd.DataFrame()
                
        except Exception as e:
            print(f"   ❌ Error: {e}")
            return pd.DataFrame()
    
    def analyze_volatility_risk_premium(self, comparison_df):
        """
        Comprehensive analysis of volatility risk premium
        """
        if len(comparison_df) == 0:
            return None
        
        analysis = {}
        
        # Basic premium statistics
        vol_spread = comparison_df['vol_spread']
        analysis['basic_stats'] = {
            'mean_premium': vol_spread.mean(),
            'median_premium': vol_spread.median(),
            'std_premium': vol_spread.std(),
            'min_premium': vol_spread.min(),
            'max_premium': vol_spread.max(),
            'sharpe_ratio': vol_spread.mean() / vol_spread.std() if vol_spread.std() > 0 else 0
        }
        
        # Frequency analysis
        analysis['frequency'] = {
            'implied_higher_pct': (vol_spread > 0).mean() * 100,
            'avg_overpricing': vol_spread[vol_spread > 0].mean(),
            'avg_underpricing': vol_spread[vol_spread < 0].mean(),
            'extreme_overpricing_pct': (vol_spread > 10).mean() * 100,
            'extreme_underpricing_pct': (vol_spread < -10).mean() * 100
        }
        
        # Regime analysis
        implied_col = [col for col in comparison_df.columns if 'implied_vol' in col][0]
        comparison_df['vol_regime'] = pd.cut(
            comparison_df[implied_col],
            bins=[0, 12, 20, 30, float('inf')],
            labels=['Very Low Vol', 'Low Vol', 'Elevated Vol', 'High Vol']
        )
        
        regime_stats = comparison_df.groupby('vol_regime')['vol_spread'].agg([
            'mean', 'median', 'std', 'count'
        ]).round(2)
        analysis['regime_analysis'] = regime_stats
        
        # Time-based analysis
        comparison_df['year'] = comparison_df['date'].dt.year
        comparison_df['month'] = comparison_df['date'].dt.month
        
        yearly_stats = comparison_df.groupby('year')['vol_spread'].agg([
            'mean', 'count'
        ]).round(2)
        analysis['yearly_analysis'] = yearly_stats
        
        return analysis
    
    def plot_forward_looking_analysis(self, comparison_df, title_suffix=""):
        """
        Create comprehensive plots for forward-looking analysis
        """
        if len(comparison_df) == 0:
            return
        
        # Get column names
        implied_col = [col for col in comparison_df.columns if 'implied_vol' in col][0]
        realized_col = [col for col in comparison_df.columns if 'realized_vol' in col][0]
        
        # Create subplots
        fig = make_subplots(
            rows=3, cols=1,
            subplot_titles=[
                f'Forward-Looking Volatility Comparison {title_suffix}',
                'Volatility Risk Premium Over Time', 
                'Risk Premium Distribution'
            ],
            vertical_spacing=0.08,
            specs=[[{"secondary_y": False}],
                   [{"secondary_y": True}], 
                   [{"secondary_y": False}]]
        )
        
        # Plot 1: Time series comparison
        fig.add_trace(
            go.Scatter(
                x=comparison_df['date'],
                y=comparison_df[implied_col],
                mode='lines',
                name='Implied Vol (Today)',
                line=dict(color='#ff7f0e', width=2)
            ),
            row=1, col=1
        )
        
        fig.add_trace(
            go.Scatter(
                x=comparison_df['date'],
                y=comparison_df[realized_col],
                mode='lines',
                name='Future Realized Vol',
                line=dict(color='#1f77b4', width=2)
            ),
            row=1, col=1
        )
        
        # Plot 2: Risk premium with rolling average
        fig.add_trace(
            go.Scatter(
                x=comparison_df['date'],
                y=comparison_df['vol_spread'],
                mode='lines',
                name='Risk Premium',
                line=dict(color='#2ca02c', width=1, dash='dot'),
                opacity=0.6
            ),
            row=2, col=1
        )
        
        # Add rolling average
        rolling_premium = comparison_df['vol_spread'].rolling(window=60, center=True).mean()
        fig.add_trace(
            go.Scatter(
                x=comparison_df['date'],
                y=rolling_premium,
                mode='lines',
                name='60-Day Rolling Avg Premium',
                line=dict(color='#d62728', width=3)
            ),
            row=2, col=1
        )
        
        # Plot 3: Distribution
        fig.add_trace(
            go.Histogram(
                x=comparison_df['vol_spread'],
                nbinsx=50,
                name='Premium Distribution',
                marker_color='rgba(55, 128, 191, 0.7)',
                showlegend=False
            ),
            row=3, col=1
        )
        
        # Add zero lines
        for row in [1, 2]:
            fig.add_hline(y=0, line_dash="dot", line_color="gray", opacity=0.5, row=row, col=1)
        
        # Update layout
        fig.update_layout(
            title=f'Advanced Volatility Risk Premium Analysis {title_suffix}',
            height=1000,
            template='plotly_white',
            showlegend=True
        )
        
        # Update y-axis labels
        fig.update_yaxes(title_text="Volatility (%)", row=1, col=1)
        fig.update_yaxes(title_text="Risk Premium (%)", row=2, col=1) 
        fig.update_yaxes(title_text="Frequency", row=3, col=1)
        fig.update_xaxes(title_text="Risk Premium (%)", row=3, col=1)
        
        fig.show()
    
    def generate_trading_insights(self, analysis_results):
        """
        Generate actionable trading insights from the analysis
        """
        if not analysis_results:
            return
        
        basic = analysis_results['basic_stats']
        freq = analysis_results['frequency']
        
        print("\n💡 TRADING INSIGHTS & IMPLICATIONS")
        print("=" * 50)
        
        # Risk premium assessment
        avg_premium = basic['mean_premium']
        if avg_premium > 3:
            premium_assessment = "HIGH - Strong systematic risk premium"
        elif avg_premium > 1:
            premium_assessment = "MODERATE - Consistent risk premium" 
        elif avg_premium > -1:
            premium_assessment = "NEUTRAL - No clear premium"
        else:
            premium_assessment = "NEGATIVE - Implied vol underpricing"
        
        print(f"🎯 RISK PREMIUM ASSESSMENT: {premium_assessment}")
        print(f"   Average premium: {avg_premium:.2f}%")
        print(f"   Consistency: {freq['implied_higher_pct']:.1f}% of time implied > realized")
        
        # Volatility selling insights
        print(f"\n📈 VOLATILITY SELLING STRATEGY:")
        if avg_premium > 2 and freq['implied_higher_pct'] > 60:
            print("   ✅ FAVORABLE - Systematic edge for vol sellers")
            print("   ✅ High premium frequency suggests consistent profits")
        elif avg_premium > 0:
            print("   ⚠️ MIXED - Some edge but requires timing")
        else:
            print("   ❌ UNFAVORABLE - No systematic edge")
        
        print(f"   Expected P&L per trade: {avg_premium:.2f}%")
        print(f"   Win rate: {freq['implied_higher_pct']:.1f}%")
        
        # Risk management
        print(f"\n⚠️ RISK MANAGEMENT:")
        print(f"   Max historical loss: {basic['min_premium']:.2f}%")
        print(f"   Volatility of returns: {basic['std_premium']:.2f}%")
        print(f"   Sharpe ratio: {basic['sharpe_ratio']:.2f}")
        
        # Regime insights
        if 'regime_analysis' in analysis_results:
            print(f"\n🎭 REGIME-BASED INSIGHTS:")
            regime_df = analysis_results['regime_analysis']
            for regime, stats in regime_df.iterrows():
                print(f"   {regime}: {stats['mean']:.2f}% avg premium ({int(stats['count'])} obs)")

def run_advanced_volatility_analysis(data_df, ticker='SPX Index'):
    """
    Run complete advanced volatility analysis
    """
    print("🚀 ADVANCED VOLATILITY ANALYSIS")
    print("=" * 60)
    print("Analyzing volatility risk premiums with proper forward-looking methodology")
    print()
    
    # Initialize analyzer
    analyzer = VolatilityAnalyzer(data_df)
    
    # Create forward-looking comparisons
    comparisons = analyzer.create_forward_looking_comparison(ticker)
    
    if len(comparisons) == 0:
        print("❌ No valid forward-looking comparisons created")
        return None
    
    results = {}
    
    # Analyze each comparison
    for i, comp_df in enumerate(comparisons):
        tenor = comp_df['tenor_days'].iloc[0]
        comparison_type = comp_df['comparison_type'].iloc[0]
        
        print(f"\n{'='*60}")
        print(f"ANALYSIS {i+1}: {comparison_type}")
        print(f"{'='*60}")
        
        # Risk premium analysis
        analysis = analyzer.analyze_volatility_risk_premium(comp_df)
        
        if analysis:
            # Print detailed results
            basic = analysis['basic_stats']
            freq = analysis['frequency']
            
            print(f"\n📊 RISK PREMIUM STATISTICS:")
            print(f"   Mean premium: {basic['mean_premium']:.2f}%")
            print(f"   Median premium: {basic['median_premium']:.2f}%")
            print(f"   Volatility: {basic['std_premium']:.2f}%")
            print(f"   Sharpe ratio: {basic['sharpe_ratio']:.2f}")
            
            print(f"\n📈 FREQUENCY ANALYSIS:")
            print(f"   Implied > Realized: {freq['implied_higher_pct']:.1f}% of time")
            print(f"   Avg overpricing: {freq['avg_overpricing']:.2f}%")
            print(f"   Avg underpricing: {freq['avg_underpricing']:.2f}%")
            
            # Generate trading insights
            analyzer.generate_trading_insights(analysis)
            
            # Create plots
            analyzer.plot_forward_looking_analysis(comp_df, f"({tenor} days)")
            
            results[f"{tenor}_day"] = {
                'data': comp_df,
                'analysis': analysis
            }
    
    return results