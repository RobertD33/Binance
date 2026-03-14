import sys
import os
import logging

# Add current directory to path (IMPORTANT!)
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Configure logging
logging.basicConfig(level=logging.INFO)

# Import the complete trading engine
from complete_trading_engine import CompleteTradeingEngine

if __name__ == "__main__":
    print("Starting Trading Bot...")
    
    # Create trading engine
    engine = CompleteTradeingEngine(
        symbols=["USDTUSD"],
        interval="5m",
        historical_days=7,
        initial_balance=10000.0,
        exchange="binance"
    )
    
    # Start trading
    print("\n🚀 Starting paper trading for 2 minutes...\n")
    engine.start_trading(duration=120)
    
    print("\n✅ Trading complete!")