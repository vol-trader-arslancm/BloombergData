#!/usr/bin/env python3
"""
VIX Strategy Complete Execution Script
Run this to collect data and calculate strategy performance
"""

import sys
import os
from pathlib import Path
from datetime import datetime

# Add current directory to path
current_dir = Path(__file__).parent.absolute()
sys.path.insert(0, str(current_dir))

def run_historical_collection():
    """
    Step 1: Collect 5 years of historical VIX data
    """
    print("🚀 STEP 1: Collecting historical VIX strategy data...")
    print("=" * 60)
    
    try:
        from vix_strategy_data_fetcher import VIXStrategyDataFetcher
        
        # Initialize fetcher for 5 years of data
        fetcher = VIXStrategyDataFetcher(years_back=5)
        
        print(f"📅 Collecting data from {fetcher.start_date.date()} to {fetcher.end_date.date()}")
        print("⚠️  Ensure Bloomberg Terminal is running and logged in!")
        
        input("Press Enter to continue with data collection...")
        
        # Collect all strategy data
        strategy_data = fetcher.collect_strategy_data()
        
        if strategy_data and any(len(df) > 0 for df in strategy_data.values() if hasattr(df, '__len__')):
            # Save the data
            summary = fetcher.save_strategy_data(strategy_data)
            
            print("✅ Historical data collection completed!")
            print(f"📊 Data summary: {summary}")
            
            return summary['collection_timestamp'] if summary else None
        else:
            print("❌ No data collected - check Bloomberg connection")
            return None
            
    except Exception as e:
        print(f"❌ Data collection failed: {e}")
        return None

def run_performance_analysis(data_timestamp=None):
    """
    Step 2: Calculate strategy performance and P&L
    """
    print("\n🧮 STEP 2: Calculating strategy performance...")
    print("=" * 60)
    
    try:
        from vix_strategy_pnl_calculator import VIXStrategyPnLCalculator
        
        # Initialize calculator
        calculator = VIXStrategyPnLCalculator('./data/vix_strategy')
        
        # Run full analysis
        results = calculator.run_full_analysis(data_timestamp)
        
        if results:
            print("✅ Performance analysis completed!")
            return results
        else:
            print("❌ Performance analysis failed")
            return None
            
    except Exception as e:
        print(f"❌ Performance analysis failed: {e}")
        return None

def setup_daily_scheduling():
    """
    Step 3: Setup daily data collection
    """
    print("\n⏰ STEP 3: Setting up daily scheduling...")
    print("=" * 60)
    
    try:
        from windows_scheduler_setup import WindowsSchedulerSetup
        
        scheduler = WindowsSchedulerSetup()
        files_created = scheduler.setup_complete_system()
        
        print("✅ Scheduling setup completed!")
        print("\nNext steps for daily scheduling:")
        print(f"1. Edit {files_created['config_file']} with your email/Slack credentials")
        print(f"2. Run {files_created['batch_file']} as Administrator")
        print("3. Verify task: schtasks /query /tn \"VIX_Strategy_Daily_Collection\"")
        
        return files_created
        
    except Exception as e:
        print(f"❌ Scheduling setup failed: {e}")
        return None

def main():
    """
    Complete VIX strategy execution workflow
    """
    print("🔥 VIX VOLATILITY STRATEGY EXECUTION")
    print("Strategy: SHORT 1x 50Δ call + LONG 2x 10Δ calls + VIX futures hedge")
    print("=" * 80)
    
    # Check if user wants to run all steps or specific ones
    print("\nExecution options:")
    print("1. Run complete workflow (data collection + analysis + scheduling)")
    print("2. Run data collection only")
    print("3. Run performance analysis only (requires existing data)")
    print("4. Setup daily scheduling only")
    
    choice = input("\nEnter your choice (1-4): ").strip()
    
    if choice == "1":
        # Complete workflow
        print("\n🚀 Running complete VIX strategy workflow...")
        
        # Step 1: Data collection
        data_timestamp = run_historical_collection()
        
        if data_timestamp:
            # Step 2: Performance analysis
            results = run_performance_analysis(data_timestamp)
            
            if results:
                # Step 3: Setup scheduling
                setup_daily_scheduling()
                
                print("\n🎉 COMPLETE WORKFLOW FINISHED!")
                print("✅ Historical data collected")
                print("✅ Performance analysis completed") 
                print("✅ Daily scheduling configured")
                print("\nCheck ./data/vix_strategy/results/ for detailed results")
            else:
                print("\n⚠️  Workflow stopped - performance analysis failed")
        else:
            print("\n⚠️  Workflow stopped - data collection failed")
    
    elif choice == "2":
        # Data collection only
        data_timestamp = run_historical_collection()
        if data_timestamp:
            print(f"\n✅ Data collection completed! Timestamp: {data_timestamp}")
            print("Run option 3 next to analyze performance")
    
    elif choice == "3":
        # Performance analysis only
        print("📁 Looking for existing data files...")
        data_dir = Path('./data/vix_strategy')
        if data_dir.exists():
            results = run_performance_analysis()
            if results:
                print("\n✅ Performance analysis completed!")
        else:
            print("❌ No data directory found. Run data collection first.")
    
    elif choice == "4":
        # Scheduling only
        setup_daily_scheduling()
    
    else:
        print("❌ Invalid choice")

if __name__ == "__main__":
    main()
