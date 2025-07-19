"""
10-Year Volatility Data Quality Validation
Comprehensive analysis of the freshly collected 10-year dataset
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
from datetime import datetime
import json

def load_and_validate_ten_year_data():
    """Load and validate the 10-year volatility dataset"""
    
    print("🔍 VALIDATING 10-YEAR VOLATILITY DATASET")
    print("=" * 60)
    
    # Load the data
    data_path = r'C:\Users\acmuser\PycharmProjects\BloombergData\data\historical_volatility\ten_year_volatility_latest.csv'
    
    try:
        df = pd.read_csv(data_path)
        df['date'] = pd.to_datetime(df['date'])
        
        print(f"✅ Successfully loaded dataset")
        print(f"   File: {data_path}")
        print(f"   Shape: {df.shape}")
        print(f"   Memory usage: {df.memory_usage(deep=True).sum() / 1024**2:.1f} MB")
        
        return df
        
    except Exception as e:
        print(f"❌ Error loading data: {e}")
        return None

def analyze_data_coverage(df):
    """Analyze data coverage and completeness"""
    
    print(f"\n📊 DATA COVERAGE ANALYSIS")
    print("=" * 40)
    
    # Basic stats
    print(f"Total observations: {len(df):,}")
    print(f"Securities: {df['ticker'].nunique()}")
    print(f"Date range: {df['date'].min().strftime('%Y-%m-%d')} to {df['date'].max().strftime('%Y-%m-%d')}")
    print(f"Trading days: {len(df['date'].unique()):,}")
    
    # Data type breakdown
    print(f"\nData type breakdown:")
    data_type_counts = df['data_type'].value_counts()
    for data_type, count in data_type_counts.items():
        pct = (count / len(df)) * 100
        print(f"   {data_type}: {count:,} ({pct:.1f}%)")
    
    # Security coverage
    print(f"\nSecurity breakdown:")
    security_counts = df['ticker'].value_counts().head(10)
    for ticker, count in security_counts.items():
        print(f"   {ticker}: {count:,} observations")
    
    # Check for SPX Index specifically
    spx_data = df[df['ticker'] == 'SPX Index']
    if len(spx_data) > 0:
        print(f"\n🎯 SPX Index coverage:")
        print(f"   Total observations: {len(spx_data):,}")
        print(f"   Realized: {len(spx_data[spx_data['data_type'] == 'realized']):,}")
        print(f"   Implied: {len(spx_data[spx_data['data_type'] == 'implied']):,}")
        print(f"   Date range: {spx_data['date'].min().strftime('%Y-%m-%d')} to {spx_data['date'].max().strftime('%Y-%m-%d')}")

def analyze_data_quality(df):
    """Analyze data quality and completeness by field"""
    
    print(f"\n🔍 DATA QUALITY ANALYSIS")
    print("=" * 40)
    
    # Get volatility columns
    vol_columns = [col for col in df.columns if 'vol' in col.lower()]
    
    print(f"Volatility fields available: {len(vol_columns)}")
    
    # Calculate completeness for each volatility field
    completeness_data = []
    
    for col in vol_columns:
        total_records = len(df)
        non_null_records = df[col].notna().sum()
        completeness_pct = (non_null_records / total_records) * 100
        
        completeness_data.append({
            'field': col,
            'total_records': total_records,
            'non_null_records': non_null_records,
            'completeness_pct': completeness_pct,
            'data_range': f"{df[col].min():.2f} - {df[col].max():.2f}" if non_null_records > 0 else "No data"
        })
        
        print(f"   {col}: {completeness_pct:.1f}% complete ({non_null_records:,}/{total_records:,})")
    
    return pd.DataFrame(completeness_data)

def create_data_quality_visualizations(df):
    """Create visualizations showing data quality and coverage"""
    
    print(f"\n📈 CREATING DATA QUALITY VISUALIZATIONS...")
    
    # 1. Timeline coverage by security type
    df['is_spx'] = df['ticker'] == 'SPX Index'
    df['security_type'] = df['is_spx'].map({True: 'SPX Index', False: 'Components'})
    
    # Daily observation counts
    daily_counts = df.groupby(['date', 'security_type', 'data_type']).size().reset_index(name='count')
    
    fig = px.line(
        daily_counts, 
        x='date', 
        y='count',
        color='security_type',
        facet_col='data_type',
        title='Daily Data Coverage: SPX vs Components',
        labels={'count': 'Number of Observations', 'date': 'Date'}
    )
    
    fig.update_layout(height=400)
    fig.show()
    
    # 2. Data completeness heatmap by field and year
    vol_columns = [col for col in df.columns if 'vol' in col.lower()]
    df['year'] = df['date'].dt.year
    
    # Calculate yearly completeness for each field
    yearly_completeness = []
    
    for year in sorted(df['year'].unique()):
        year_data = df[df['year'] == year]
        for col in vol_columns:
            completeness = (year_data[col].notna().sum() / len(year_data)) * 100
            yearly_completeness.append({
                'year': year,
                'field': col,
                'completeness': completeness
            })
    
    completeness_df = pd.DataFrame(yearly_completeness)
    
    # Create heatmap
    pivot_completeness = completeness_df.pivot(index='field', columns='year', values='completeness')
    
    fig = go.Figure(data=go.Heatmap(
        z=pivot_completeness.values,
        x=pivot_completeness.columns,
        y=pivot_completeness.index,
        colorscale='RdYlGn',
        text=np.round(pivot_completeness.values, 1),
        texttemplate="%{text}%",
        textfont={"size": 10},
        colorbar=dict(title="Completeness %")
    ))
    
    fig.update_layout(
        title='Data Completeness Heatmap by Field and Year',
        xaxis_title='Year',
        yaxis_title='Volatility Field',
        height=600
    )
    
    fig.show()

def analyze_spx_data_specifically(df):
    """Deep dive into SPX Index data quality"""
    
    print(f"\n🎯 SPX INDEX DEEP DIVE")
    print("=" * 40)
    
    spx_data = df[df['ticker'] == 'SPX Index'].copy()
    
    if len(spx_data) == 0:
        print("❌ No SPX data found")
        return
    
    # Split by data type
    spx_realized = spx_data[spx_data['data_type'] == 'realized']
    spx_implied = spx_data[spx_data['data_type'] == 'implied']
    
    print(f"SPX Realized observations: {len(spx_realized):,}")
    print(f"SPX Implied observations: {len(spx_implied):,}")
    
    # Check specific fields we need for analysis
    key_fields = {
        'realized': ['realized_vol_30d', 'realized_vol_90d', 'realized_vol_180d', 'realized_vol_252d'],
        'implied': ['implied_vol_1m_atm', 'implied_vol_3m_atm', 'implied_vol_6m_atm', 'implied_vol_12m_atm']
    }
    
    for data_type, fields in key_fields.items():
        print(f"\n{data_type.upper()} VOLATILITY FIELDS:")
        
        if data_type == 'realized':
            subset = spx_realized
        else:
            subset = spx_implied
        
        for field in fields:
            if field in subset.columns:
                non_null = subset[field].notna().sum()
                total = len(subset)
                pct = (non_null / total) * 100 if total > 0 else 0
                
                if non_null > 0:
                    avg_val = subset[field].mean()
                    min_val = subset[field].min()
                    max_val = subset[field].max()
                    print(f"   {field}: {pct:.1f}% complete | Avg: {avg_val:.2f}% | Range: {min_val:.2f}%-{max_val:.2f}%")
                else:
                    print(f"   {field}: {pct:.1f}% complete | No data")
            else:
                print(f"   {field}: Field not found")

def generate_data_readiness_report(df):
    """Generate a comprehensive readiness report for advanced analysis"""
    
    print(f"\n📋 DATA READINESS REPORT")
    print("=" * 50)
    
    spx_data = df[df['ticker'] == 'SPX Index']
    
    # Check readiness for forward-looking analysis
    readiness_checks = []
    
    # Check 1: SPX data availability
    spx_realized = spx_data[spx_data['data_type'] == 'realized']
    spx_implied = spx_data[spx_data['data_type'] == 'implied']
    
    readiness_checks.append({
        'check': 'SPX Realized Data',
        'status': '✅ READY' if len(spx_realized) > 2000 else '⚠️ LIMITED',
        'details': f'{len(spx_realized):,} observations'
    })
    
    readiness_checks.append({
        'check': 'SPX Implied Data', 
        'status': '✅ READY' if len(spx_implied) > 2000 else '⚠️ LIMITED',
        'details': f'{len(spx_implied):,} observations'
    })
    
    # Check 2: Key field availability
    key_fields_check = all([
        'realized_vol_90d' in spx_realized.columns,
        'implied_vol_3m_atm' in spx_implied.columns,
        spx_realized['realized_vol_90d'].notna().sum() > 1000,
        spx_implied['implied_vol_3m_atm'].notna().sum() > 1000
    ])
    
    readiness_checks.append({
        'check': 'Forward-Looking Analysis Fields',
        'status': '✅ READY' if key_fields_check else '❌ MISSING',
        'details': '90D realized + 3M implied available' if key_fields_check else 'Key fields missing'
    })
    
    # Check 3: Historical depth
    date_range = (df['date'].max() - df['date'].min()).days
    
    readiness_checks.append({
        'check': 'Historical Depth',
        'status': '✅ READY' if date_range > 3000 else '⚠️ LIMITED',
        'details': f'{date_range:,} days ({date_range/365:.1f} years)'
    })
    
    # Check 4: Component data
    component_count = df[df['ticker'] != 'SPX Index']['ticker'].nunique()
    
    readiness_checks.append({
        'check': 'Component Coverage',
        'status': '✅ READY' if component_count > 30 else '⚠️ LIMITED',
        'details': f'{component_count} individual securities'
    })
    
    # Display readiness report
    print("READINESS ASSESSMENT:")
    for check in readiness_checks:
        print(f"   {check['status']} {check['check']}: {check['details']}")
    
    # Overall assessment
    ready_count = sum(1 for check in readiness_checks if '✅' in check['status'])
    total_checks = len(readiness_checks)
    
    print(f"\nOVERALL READINESS: {ready_count}/{total_checks} checks passed")
    
    if ready_count == total_checks:
        print("🎉 DATASET IS READY FOR PROFESSIONAL-GRADE ANALYSIS!")
        print("   • Advanced risk premium calculations")
        print("   • Cross-sectional volatility studies") 
        print("   • Regime-based trading strategies")
        print("   • Academic-quality research")
    elif ready_count >= 3:
        print("✅ DATASET IS GOOD FOR MOST ANALYSES")
        print("   • Core volatility risk premium analysis ready")
        print("   • Some advanced features may be limited")
    else:
        print("⚠️ DATASET HAS LIMITATIONS")
        print("   • Basic analysis possible")
        print("   • Advanced features may require additional data")

def main():
    """Main validation function"""
    
    # Load data
    df = load_and_validate_ten_year_data()
    if df is None:
        return
    
    # Run comprehensive validation
    analyze_data_coverage(df)
    
    completeness_df = analyze_data_quality(df)
    
    create_data_quality_visualizations(df)
    
    analyze_spx_data_specifically(df)
    
    generate_data_readiness_report(df)
    
    print(f"\n🎯 NEXT STEPS:")
    print("1. Update your advanced volatility notebook to use the 10-year dataset")
    print("2. Run forward-looking risk premium analysis")
    print("3. Compare results with academic volatility risk premium studies")
    print("4. Develop systematic volatility trading strategies")
    
    return df

if __name__ == "__main__":
    main()