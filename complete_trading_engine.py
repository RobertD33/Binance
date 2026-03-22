
"""Complete Trading Engine Integration"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from data_fetcher import HybridDataFetcher, PriceData
from signal_generator import SignalGenerator, SignalType
from enhanced_risk_manager import EnhancedRiskManager, TradeAction
from paper_trading_engine import PaperTradingEngine
from order_manager import OrderManager
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

class CompleteTradeingEngine:
    """Complete trading engine integrating all modules"""
    
    def __init__(self, symbols=None, interval="5m", historical_days=7, initial_balance=10000.0, exchange="binance", verbose=True):
        if symbols is None:
            symbols = ["USDTUSD"]
        
        self.symbols = symbols
        self.interval = interval
        self.initial_balance = initial_balance
        self.exchange = exchange
        self.verbose = verbose
        
        logger.info("\n" + "="*70)
        logger.info("INITIALIZING COMPLETE TRADING ENGINE")
        logger.info("="*70)
        
        # 1. Data Fetcher
        logger.info("\n1️⃣  Initializing Data Fetcher...")
        self.data_fetcher = HybridDataFetcher(symbols=symbols, interval=interval, historical_days=historical_days)
        logger.info(f"   ✅ Data fetcher ready for {len(symbols)} symbols")
        
        # 2. Signal Generators
        logger.info("\n2️⃣  Initializing Signal Generators...")
        self.signal_generators = {
            symbol: SignalGenerator(symbol=symbol, ma_short_period=20, ma_long_period=50, buy_threshold=-0.002, sell_threshold=0.002, min_confidence=0.5, use_bollinger=True, debug=False)
            for symbol in symbols
        }
        logger.info(f"   ✅ Signal generators ready for {len(symbols)} symbols")
        
        # 3. Risk Manager
        logger.info("\n3️⃣  Initializing Risk Manager...")
        self.risk_manager = EnhancedRiskManager(
            initial_balance=initial_balance,
            max_risk_per_trade=0.02,
            max_drawdown=0.10,
            max_positions=5,
            position_sizing="fixed_percent",
            position_size_percent=0.05,
            min_confidence=0.5
        )
        logger.info(f"   ✅ Risk manager initialized")
        
        # 4. Paper Trading Engine
        logger.info("\n4️⃣  Initializing Paper Trading Engine...")
        self.paper_engine = PaperTradingEngine(initial_balance=initial_balance, exchange=exchange, verbose=verbose)
        logger.info(f"   ✅ Paper trading engine ready")
        
        # 5. Order Manager
        logger.info("\n5️⃣  Initializing Order Manager...")
        self.order_manager = OrderManager(mode="paper", exchange=exchange, paper_trading_engine=self.paper_engine)
        logger.info(f"   ✅ Order manager ready")
        
        self.total_signals = 0
        self.total_trades = 0
        self.total_rejections = 0
        
        logger.info("\n" + "="*70)
        logger.info("✅ ALL MODULES INITIALIZED AND CONNECTED")
        logger.info("="*70)
    
    def process_candle(self, data: PriceData):
        """Process new price candle through entire engine"""
        symbol = data.symbol
        current_price = data.close
        timestamp = data.timestamp
        
        prices = [c.close for c in self.data_fetcher.get_buffer(symbol)]
        
        if not prices or len(prices) < 50:
            return
        
        signal = self.signal_generators[symbol].process_candle(prices, timestamp)
        
        if not signal or signal.signal_type == SignalType.HOLD:
            return
        
        self.total_signals += 1
        
        allowed, reason, action = self.risk_manager.check_position_control(symbol=symbol, signal_type=signal.signal_type.value, signal_price=current_price)
        
        if not allowed:
            logger.warning(f"❌ Position control: {reason}")
            self.total_rejections += 1
            return
        
        validation = self.risk_manager.validate_trade(symbol=symbol, signal_type=signal.signal_type.value, signal_price=current_price, signal_confidence=signal.confidence, stop_loss_pct=0.005, take_profit_pct=0.003, existing_position_size=0.0)
        
        if not validation.is_valid:
            logger.warning(f"❌ Risk validation: {validation.reason}")
            self.total_rejections += 1
            return
        
        self.total_trades += 1
        trade_size = validation.trade_size
        
        if signal.signal_type == SignalType.BUY:
            emoji = "🟢"
            order_type = "MARKET"
        else:
            emoji = "🔴"
            order_type = "MARKET"
        
        logger.info(f"\n{emoji} TRADE SIGNAL APPROVED")
        logger.info(f"   Symbol: {symbol}")
        logger.info(f"   Type: {signal.signal_type.value}")
        logger.info(f"   Position Size: ${trade_size.position_size:.2f}")
        
        trade_id = self.order_manager.execute_trade(symbol=symbol, side=signal.signal_type.value, quantity=trade_size.contracts, order_type=order_type, price=current_price if order_type == "LIMIT" else None)
        
        self.order_manager.process_fills({symbol: current_price})
        self.paper_engine.update_positions_market_value({symbol: current_price})
    
    def start_trading(self, duration: int = 120):
        """Start paper trading"""
        logger.info("\n" + "="*70)
        logger.info("🚀 STARTING PAPER TRADING")
        logger.info("="*70)
        
        self.data_fetcher.add_callback(self.process_candle)
        self.data_fetcher.start()
        
        logger.info("✅ Trading started - listening for signals...\n")
        
        try:
            import time
            start_time = time.time()
            while time.time() - start_time < duration:
                time.sleep(1)
                if int(time.time() - start_time) % 30 == 0:
                    self.print_status()
        except KeyboardInterrupt:
            logger.info("\n⏹️  Trading interrupted by user")
        finally:
            self.data_fetcher.stop()
            self.print_final_summary()
    
    def print_status(self):
        """Print current trading status"""
        stats = self.order_manager.get_trade_stats()
        print("\n" + "-"*70)
        print("📊 LIVE TRADING STATUS")
        print("-"*70)
        print(f"Balance: ${stats['balance']:.2f}")
        print(f"P&L: ${stats['total_pnl']:+.2f} ({stats['total_return_pct']:+.2f}%)")
        print(f"Trades: {stats['trades']} | Wins: {stats['winning_trades']} | Win Rate: {stats['win_rate']:.1f}%")
        print(f"Open Positions: {stats['open_positions']}")
    
    def print_final_summary(self):
        """Print final summary"""
        logger.info("\n" + "="*70)
        logger.info("📊 FINAL TRADING SUMMARY")
        logger.info("="*70)
        
        logger.info(f"\nSignal Generation:")
        logger.info(f"  Total Signals: {self.total_signals}")
        logger.info(f"  Trades Executed: {self.total_trades}")
        logger.info(f"  Trades Rejected: {self.total_rejections}")
        
        stats = self.order_manager.get_trade_stats()
        
        logger.info(f"\nPerformance:")
        logger.info(f"  Starting Balance: ${self.initial_balance:.2f}")
        logger.info(f"  Current Balance: ${stats['balance']:.2f}")
        logger.info(f"  Total P&L: ${stats['total_pnl']:+.2f}")
        logger.info(f"  Return: {stats['total_return_pct']:+.2f}%")
        
        logger.info(f"\nTrade Statistics:")
        logger.info(f"  Total Trades: {stats['trades']}")
        logger.info(f"  Winning Trades: {stats['winning_trades']}")
        logger.info(f"  Losing Trades: {stats['losing_trades']}")
        logger.info(f"  Win Rate: {stats['win_rate']:.1f}%")
        
        self.order_manager.print_status()



