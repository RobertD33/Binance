"""
Data Fetcher Module
Real-time cryptocurrency price data collection from Binance WebSocket

Features:
- WebSocket connection for real-time data
- Multiple symbol support
- Data buffering and aggregation
- Error handling and reconnection
- Performance optimized
"""

import asyncio
import json
import logging
import time
import os
from datetime import datetime
from typing import Dict, List, Callable, Optional
from dataclasses import dataclass, asdict
from queue import Queue
from threading import Thread, Lock
import websocket

# Create logs directory if it doesn't exist
os.makedirs('logs', exist_ok=True)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/data_fetcher.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


@dataclass
class PriceData:
    """Data structure for price information"""
    symbol: str
    timestamp: float  # Unix timestamp (ms)
    open: float
    high: float
    low: float
    close: float
    volume: float
    
    def to_dict(self):
        return asdict(self)


class BinanceWebSocketFetcher:
    """
    Real-time data fetcher using Binance WebSocket
    
    Advantages:
    ✓ Real-time data (no delay)
    ✓ Low bandwidth usage
    ✓ 24/7 continuous data
    ✓ Reliable connection
    ✓ Simple integration
    
    Disadvantages:
    ✗ Only recent candles (no historical)
    ✗ Need REST API for historical data
    """
    
    # Binance WebSocket endpoints
    TESTNET_BASE = "wss://stream.binancefutures.com/ws"
    MAINNET_BASE = "wss://stream.binance.com:9443/ws"
    
    # Supported intervals
    INTERVALS = ["1m", "5m", "15m", "30m", "1h", "4h", "1d"]
    
    def __init__(
        self,
        symbols: List[str],
        interval: str = "1m",
        testnet: bool = False,
        buffer_size: int = 1000
    ):
        """
        Initialize Binance WebSocket fetcher
        
        Args:
            symbols: List of symbols (e.g., ["USDTUSD", "USDCUSDT"])
            interval: Kline interval (1m, 5m, 15m, 30m, 1h, 4h, 1d)
            testnet: Use testnet (default: mainnet)
            buffer_size: Max buffered candles per symbol
        """
        
        if interval not in self.INTERVALS:
            raise ValueError(f"Invalid interval. Must be one of {self.INTERVALS}")
        
        self.symbols = [s.lower() for s in symbols]
        self.interval = interval
        self.testnet = testnet
        self.buffer_size = buffer_size
        
        # Data storage
        self.data: Dict[str, List[PriceData]] = {s: [] for s in self.symbols}
        self.latest: Dict[str, Optional[PriceData]] = {s: None for s in self.symbols}
        self.lock = Lock()
        
        # Connection management
        self.ws = None
        self.connected = False
        self.reconnect_attempts = 0
        self.max_reconnect_attempts = 5
        self.reconnect_delay = 5  # seconds
        
        # Callbacks
        self.callbacks: List[Callable[[PriceData], None]] = []
        
        logger.info(f"Initialized BinanceWebSocketFetcher for {len(symbols)} symbols")
    
    def add_callback(self, callback: Callable[[PriceData], None]):
        """Add callback function to be called on new data"""
        self.callbacks.append(callback)
        logger.info(f"Added callback: {callback.__name__}")
    
    def _get_stream_url(self) -> str:
        """Generate WebSocket stream URL"""
        base = self.TESTNET_BASE if self.testnet else self.MAINNET_BASE
        streams = "/".join([f"{s}@kline_{self.interval}" for s in self.symbols])
        return f"{base}/{streams}"
    
    def connect(self):
        """Establish WebSocket connection"""
        try:
            url = self._get_stream_url()
            logger.info(f"Connecting to Binance WebSocket: {url[:50]}...")
            
            self.ws = websocket.WebSocketApp(
                url,
                on_message=self._on_message,
                on_error=self._on_error,
                on_close=self._on_close,
                on_open=self._on_open
            )
            
            # Run in separate thread
            self.ws_thread = Thread(target=self.ws.run_forever, daemon=True)
            self.ws_thread.start()
            
            # Wait for connection
            timeout = 10
            start = time.time()
            while not self.connected and (time.time() - start) < timeout:
                time.sleep(0.1)
            
            if not self.connected:
                raise ConnectionError("Failed to connect within timeout")
            
            logger.info("Successfully connected to Binance WebSocket")
            self.reconnect_attempts = 0
            
        except Exception as e:
            logger.error(f"Connection failed: {e}")
            self._handle_reconnect()
    
    def _on_open(self, ws):
        """Called when WebSocket opens"""
        self.connected = True
        logger.info("WebSocket connection opened")
    
    def _on_message(self, ws, message: str):
        """Process incoming WebSocket message"""
        try:
            data = json.loads(message)
            
            # Extract kline data
            kline = data.get('k', {})
            symbol = data.get('s', '').lower()
            
            # Only process closed candles
            if not kline.get('x'):  # x = is this kline closed?
                return
            
            # Create PriceData object
            price_data = PriceData(
                symbol=symbol,
                timestamp=float(kline['T']),
                open=float(kline['o']),
                high=float(kline['h']),
                low=float(kline['l']),
                close=float(kline['c']),
                volume=float(kline['v'])
            )
            
            # Store data
            with self.lock:
                self.latest[symbol] = price_data
                self.data[symbol].append(price_data)
                
                # Keep buffer size limited
                if len(self.data[symbol]) > self.buffer_size:
                    self.data[symbol].pop(0)
            
            # Call callbacks
            for callback in self.callbacks:
                try:
                    callback(price_data)
                except Exception as e:
                    logger.error(f"Callback error: {e}")
            
            logger.debug(f"Received {symbol}: close={price_data.close}")
            
        except Exception as e:
            logger.error(f"Message processing error: {e}")
    
    def _on_error(self, ws, error):
        """Handle WebSocket errors"""
        logger.error(f"WebSocket error: {error}")
        self._handle_reconnect()
    
    def _on_close(self, ws, close_status_code, close_msg):
        """Called when WebSocket closes"""
        self.connected = False
        logger.warning(f"WebSocket closed: {close_msg}")
        self._handle_reconnect()
    
    def _handle_reconnect(self):
        """Handle reconnection logic"""
        if self.reconnect_attempts < self.max_reconnect_attempts:
            self.reconnect_attempts += 1
            wait_time = self.reconnect_delay * self.reconnect_attempts
            logger.info(f"Reconnecting in {wait_time}s (attempt {self.reconnect_attempts})")
            time.sleep(wait_time)
            self.connect()
        else:
            logger.error("Max reconnection attempts reached")
    
    def get_latest(self, symbol: str) -> Optional[PriceData]:
        """Get latest price data for symbol"""
        with self.lock:
            return self.latest.get(symbol.lower())
    
    def get_buffer(self, symbol: str, limit: Optional[int] = None) -> List[PriceData]:
        """Get buffered candles for symbol"""
        with self.lock:
            data = self.data.get(symbol.lower(), [])
            if limit:
                return data[-limit:]
            return data.copy()
    
    def disconnect(self):
        """Disconnect from WebSocket"""
        if self.ws:
            self.ws.close()
            self.connected = False
            logger.info("WebSocket disconnected")
            
class RestAPIFetcher:
    """
    Historical data fetcher using REST API (Binance or Polygon)
    
    Use for:
    - Backtesting
    - Initial data loading
    - Filling gaps
    """
    
    def __init__(self, api_source: str = "binance"):
        """
        Initialize REST API fetcher
        
        Args:
            api_source: 'binance' or 'polygon'
        """
        self.api_source = api_source
        self.session = None
    
    def fetch_historical(
        self,
        symbol: str,
        interval: str,
        start_date: str,
        end_date: str
    ) -> List[PriceData]:
        """
        Fetch historical data
        
        Args:
            symbol: Trading pair
            interval: Time interval
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
        
        Returns:
            List of PriceData objects
        """
        
        if self.api_source == "binance":
            return self._fetch_binance(symbol, interval, start_date, end_date)
        elif self.api_source == "polygon":
            return self._fetch_polygon(symbol, interval, start_date, end_date)
        else:
            raise ValueError(f"Unknown API source: {self.api_source}")
    
    def _fetch_binance(
        self,
        symbol: str,
        interval: str,
        start_date: str,
        end_date: str
    ) -> List[PriceData]:
        """Fetch from Binance REST API"""
        try:
            from binance.client import Client
            client = Client("", "")  # No keys needed for public data
            
            logger.info(f"Fetching {symbol} from Binance ({start_date} to {end_date})")
            
            klines = client.get_historical_klines(
                symbol=symbol,
                interval=interval,
                start_str=start_date,
                end_str=end_date
            )
            
            result = []
            for kline in klines:
                result.append(PriceData(
                    symbol=symbol,
                    timestamp=float(kline[0]),
                    open=float(kline[1]),
                    high=float(kline[2]),
                    low=float(kline[3]),
                    close=float(kline[4]),
                    volume=float(kline[7])
                ))
            
            logger.info(f"Fetched {len(result)} candles from Binance")
            return result
            
        except Exception as e:
            logger.error(f"Binance fetch error: {e}")
            return []
    
    def _fetch_polygon(
        self,
        symbol: str,
        interval: str,
        start_date: str,
        end_date: str
    ) -> List[PriceData]:
        """Fetch from Polygon.io REST API"""
        try:
            from massive import RESTClient
            client = RESTClient("YOUR_API_KEY")
            
            # Convert interval format for Polygon
            multiplier = 1
            timespan = interval
            
            logger.info(f"Fetching {symbol} from Polygon ({start_date} to {end_date})")
            
            aggs = []
            for a in client.list_aggs(
                symbol,
                multiplier,
                timespan,
                start_date,
                end_date,
                adjusted="true",
                sort="asc",
                limit=50000,
            ):
                aggs.append(a)
            
            result = []
            for agg in aggs:
                result.append(PriceData(
                    symbol=symbol,
                    timestamp=float(agg.timestamp),
                    open=agg.open,
                    high=agg.high,
                    low=agg.low,
                    close=agg.close,
                    volume=agg.volume if hasattr(agg, 'volume') else 0
                ))
            
            logger.info(f"Fetched {len(result)} candles from Polygon")
            return result
            
        except Exception as e:
            logger.error(f"Polygon fetch error: {e}")
            return []


class HybridDataFetcher:
    """
    Combines WebSocket (real-time) and REST API (historical) for complete data
    """
    
    def __init__(
        self,
        symbols: List[str],
        interval: str = "1m",
        historical_days: int = 30
    ):
        """
        Initialize hybrid fetcher
        
        Args:
            symbols: List of symbols
            interval: Time interval
            historical_days: Days of historical data to load
        """
        self.symbols = symbols
        self.interval = interval
        self.historical_days = historical_days
        
        self.ws_fetcher = BinanceWebSocketFetcher(symbols, interval)
        self.rest_fetcher = RestAPIFetcher("binance")
        
        logger.info("Initialized HybridDataFetcher")
    
    def start(self):
        """Start real-time data collection"""
        logger.info("Starting hybrid data fetcher...")
        
        # Load historical data
        self._load_historical_data()
        
        # Start WebSocket
        self.ws_fetcher.connect()
        
        logger.info("Hybrid data fetcher started")
    
    def _load_historical_data(self):
        """Load historical data for backfill"""
        from datetime import datetime, timedelta
        
        end_date = datetime.now().strftime("%Y-%m-%d")
        start_date = (datetime.now() - timedelta(days=self.historical_days)).strftime("%Y-%m-%d")
        
        for symbol in self.symbols:
            logger.info(f"Loading historical data for {symbol}...")
            data = self.rest_fetcher.fetch_historical(
                symbol,
                self.interval,
                start_date,
                end_date
            )
            logger.info(f"Loaded {len(data)} historical candles for {symbol}")
    
    def get_latest(self, symbol: str) -> Optional[PriceData]:
        """Get latest price"""
        return self.ws_fetcher.get_latest(symbol)
    
    def get_buffer(self, symbol: str, limit: int = 100) -> List[PriceData]:
        """Get recent candles"""
        return self.ws_fetcher.get_buffer(symbol, limit)
    
    def add_callback(self, callback: Callable[[PriceData], None]):
        """Add callback for new candles"""
        self.ws_fetcher.add_callback(callback)
    
    def stop(self):
        """Stop data collection"""
        self.ws_fetcher.disconnect()
        logger.info("Data fetcher stopped")

#==========================================
# EXAMPLE USAGE
# ==========================================

if __name__ == "__main__":
    
    # Example 1: WebSocket only (real-time)
    print("="*60)
    print("Example 1: WebSocket Real-Time Data")
    print("="*60)
    
    def on_new_candle(data: PriceData):
        print(f"{data.symbol}: Close={data.close}, Volume={data.volume}")
    
    ws_fetcher = BinanceWebSocketFetcher(
        symbols=["USDTUSD", "USDCUSDT"],
        interval="5m"
    )
    ws_fetcher.add_callback(on_new_candle)
    
    try:
        ws_fetcher.connect()
        
        # Run for 30 seconds
        import time
        time.sleep(30)
        
        # Get latest data
        for symbol in ["usdtusd", "usdcusdt"]:
            latest = ws_fetcher.get_latest(symbol)
            if latest:
                print(f"\nLatest {symbol}: {latest.close}")
        
        # Get buffer
        buffer = ws_fetcher.get_buffer("usdtusd", limit=5)
        print(f"\nLast 5 candles for USDTUSD:")
        for candle in buffer:
            print(f"  {datetime.fromtimestamp(candle.timestamp/1000)}: {candle.close}")
    
    finally:
        ws_fetcher.disconnect()
    
    # Example 2: Hybrid (historical + real-time)
    print("\n" + "="*60)
    print("Example 2: Hybrid Data Fetcher")
    print("="*60)
    
    hybrid = HybridDataFetcher(
        symbols=["USDTUSD"],
        interval="1h",
        historical_days=7
    )
    hybrid.add_callback(on_new_candle)
    
    try:
        hybrid.start()
        time.sleep(10)
    finally:
        hybrid.stop()

"""
Signal Generator Module
Generates BUY/SELL signals based on mean reversion strategy

Features:
- Moving averages (SMA)
- Bollinger Bands
- Standard deviation
- Multiple timeframe support
- Customizable thresholds
- Signal filtering
"""

import logging
import numpy as np
from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass
from datetime import datetime
from enum import Enum

logger = logging.getLogger(__name__)


class SignalType(Enum):
    """Signal types"""
    BUY = "BUY"
    SELL = "SELL"
    HOLD = "HOLD"
    NONE = "NONE"


@dataclass
class Signal:
    """Trading signal"""
    symbol: str
    signal_type: SignalType
    price: float
    timestamp: float
    confidence: float  # 0.0 to 1.0
    reason: str
    
    # Indicator values at time of signal
    ma_short: Optional[float] = None
    ma_long: Optional[float] = None
    bollinger_upper: Optional[float] = None
    bollinger_lower: Optional[float] = None
    bollinger_middle: Optional[float] = None
    std_dev: Optional[float] = None
    deviation_pct: Optional[float] = None
    
    def to_dict(self):
        return {
            'symbol': self.symbol,
            'signal': self.signal_type.value,
            'price': self.price,
            'timestamp': self.timestamp,
            'confidence': self.confidence,
            'reason': self.reason,
            'ma_short': self.ma_short,
            'ma_long': self.ma_long,
            'bollinger_upper': self.bollinger_upper,
            'bollinger_lower': self.bollinger_lower,
            'bollinger_middle': self.bollinger_middle,
            'std_dev': self.std_dev,
            'deviation_pct': self.deviation_pct
        }


@dataclass
class Indicators:
    """Calculated indicators"""
    symbol: str
    timestamp: float
    close: float
    
    # Moving Averages
    sma_short: Optional[float] = None  # Fast MA (e.g., 20)
    sma_long: Optional[float] = None   # Slow MA (e.g., 50)
    
    # Bollinger Bands
    bb_upper: Optional[float] = None
    bb_middle: Optional[float] = None
    bb_lower: Optional[float] = None
    
    # Volatility
    std_dev: Optional[float] = None
    
    # Mean Reversion
    deviation_from_sma: Optional[float] = None  # (close - sma) / sma
    z_score: Optional[float] = None  # Standard deviations from mean
    
    def to_dict(self):
        return {
            'symbol': self.symbol,
            'timestamp': self.timestamp,
            'close': self.close,
            'sma_short': self.sma_short,
            'sma_long': self.sma_long,
            'bb_upper': self.bb_upper,
            'bb_middle': self.bb_middle,
            'bb_lower': self.bb_lower,
            'std_dev': self.std_dev,
            'deviation': self.deviation_from_sma,
            'z_score': self.z_score
        }


class SignalGenerator:
    """
    Generates trading signals based on mean reversion strategy
    
    Strategy: Price tends to revert to mean
    - Buy when price drops below mean (oversold)
    - Sell when price rises above mean (overbought)
    """
    
    def __init__(
        self,
        symbol: str,
        ma_short_period: int = 20,
        ma_long_period: int = 50,
        bollinger_period: int = 20,
        bollinger_std: float = 2.0,
        buy_threshold: float = -0.002,  # -0.2% below MA
        sell_threshold: float = 0.002,   # +0.2% above MA
        min_confidence: float = 0.5,
        use_bollinger: bool = True,
        debug: bool = False
    ):
        """
        Initialize Signal Generator
        
        Args:
            symbol: Trading pair (e.g., "USDTUSD")
            ma_short_period: Short MA period (default 20)
            ma_long_period: Long MA period (default 50)
            bollinger_period: Bollinger Band period
            bollinger_std: Bollinger Band standard deviations
            buy_threshold: Price deviation % to trigger BUY
            sell_threshold: Price deviation % to trigger SELL
            min_confidence: Minimum confidence (0-1) to generate signal
            use_bollinger: Use Bollinger Bands for signals
            debug: Enable debug logging
        """
        
        self.symbol = symbol
        self.ma_short_period = ma_short_period
        self.ma_long_period = ma_long_period
        self.bollinger_period = bollinger_period
        self.bollinger_std = bollinger_std
        self.buy_threshold = buy_threshold
        self.sell_threshold = sell_threshold
        self.min_confidence = min_confidence
        self.use_bollinger = use_bollinger
        self.debug = debug
        
        # Indicator history
        self.indicators_history: List[Indicators] = []
        self.signals_history: List[Signal] = []
        
        logger.info(f"Initialized SignalGenerator for {symbol}")
        logger.info(f"  MA periods: {ma_short_period}/{ma_long_period}")
        logger.info(f"  Bollinger: {bollinger_period}x{bollinger_std}σ")
        logger.info(f"  Thresholds: BUY={buy_threshold*100:.2f}%, SELL={sell_threshold*100:.2f}%")
    
    def calculate_indicators(self, prices: List[float], timestamp: float) -> Indicators:
        """
        Calculate indicators for given prices
        
        Args:
            prices: List of close prices (most recent last)
            timestamp: Current timestamp
        
        Returns:
            Indicators object with calculated values
        """
        
        if not prices or len(prices) < 2:
            logger.warning(f"Not enough price data: {len(prices)}")
            return Indicators(
                symbol=self.symbol,
                timestamp=timestamp,
                close=prices[-1] if prices else 0.0
            )
        
        current_price = prices[-1]
        
        # Calculate Simple Moving Averages
        sma_short = self._calculate_sma(prices, self.ma_short_period) if len(prices) >= self.ma_short_period else None
        sma_long = self._calculate_sma(prices, self.ma_long_period) if len(prices) >= self.ma_long_period else None
        
        # Use short MA as reference (mean)
        reference_price = sma_short if sma_short else np.mean(prices)
        
        # Calculate Bollinger Bands
        bb_middle = self._calculate_sma(prices, self.bollinger_period)
        std_dev = self._calculate_std_dev(prices, self.bollinger_period)
        bb_upper = bb_middle + (std_dev * self.bollinger_std)
        bb_lower = bb_middle - (std_dev * self.bollinger_std)
        
        # Calculate deviation from mean
        deviation = (current_price - reference_price) / reference_price if reference_price > 0 else 0
        
        # Calculate Z-score (standard deviations from mean)
        z_score = (current_price - reference_price) / std_dev if std_dev > 0 else 0
        
        indicators = Indicators(
            symbol=self.symbol,
            timestamp=timestamp,
            close=current_price,
            sma_short=sma_short,
            sma_long=sma_long,
            bb_upper=bb_upper,
            bb_middle=bb_middle,
            bb_lower=bb_lower,
            std_dev=std_dev,
            deviation_from_sma=deviation,
            z_score=z_score
        )
        
        # Store in history
        self.indicators_history.append(indicators)
        
        if self.debug:
            logger.debug(f"{self.symbol} indicators:")
            logger.debug(f"  Price: {current_price:.8f}")
            logger.debug(f"  SMA({self.ma_short_period}): {sma_short:.8f}")
            logger.debug(f"  Deviation: {deviation*100:.4f}%")
            logger.debug(f"  BB: {bb_lower:.8f} - {bb_middle:.8f} - {bb_upper:.8f}")
        
        return indicators
    
    def generate_signal(self, indicators: Indicators) -> Optional[Signal]:
        """
        Generate trading signal based on indicators
        
        Args:
            indicators: Calculated indicators
        
        Returns:
            Signal object or None if no signal
        """
        
        current_price = indicators.close
        deviation = indicators.deviation_from_sma
        z_score = indicators.z_score
        
        signal_type = SignalType.NONE
        confidence = 0.0
        reason = ""
        
        # ==========================================
        # BUY SIGNAL LOGIC (Mean Reversion Down)
        # ==========================================
        # Buy when price drops below average
        
        if deviation < self.buy_threshold:  # Price below threshold
            signal_type = SignalType.BUY
            
            # Calculate confidence based on how far below
            deviation_strength = abs(deviation) / abs(self.buy_threshold)
            
            # Bonus confidence from Bollinger Bands
            bb_strength = 0.0
            if self.use_bollinger and indicators.std_dev:
                if current_price < indicators.bb_lower:
                    bb_strength = 0.3  # Strong oversold signal
                elif current_price < indicators.bb_middle:
                    bb_strength = 0.15  # Mild oversold
            
            # Total confidence
            confidence = min(0.95, 0.5 + deviation_strength * 0.4 + bb_strength)
            
            deviation_pct = (deviation * 100) if deviation else 0
            reason = f"Mean Reversion BUY: Price {deviation_pct:.3f}% below MA"
            if self.use_bollinger and indicators.bb_lower and current_price < indicators.bb_lower:
                reason += " | Below Lower Bollinger Band (Strong)"
            elif self.use_bollinger and indicators.bb_middle and current_price < indicators.bb_middle:
                reason += " | Below Middle Bollinger Band (Mild)"
        
        # ==========================================
        # SELL SIGNAL LOGIC (Mean Reversion Up)
        # ==========================================
        # Sell when price rises above average
        
        elif deviation > self.sell_threshold:  # Price above threshold
            signal_type = SignalType.SELL
            
            # Calculate confidence based on how far above
            deviation_strength = abs(deviation) / abs(self.sell_threshold)
            
            # Bonus confidence from Bollinger Bands
            bb_strength = 0.0
            if self.use_bollinger and indicators.std_dev:
                if current_price > indicators.bb_upper:
                    bb_strength = 0.3  # Strong overbought signal
                elif current_price > indicators.bb_middle:
                    bb_strength = 0.15  # Mild overbought
            
            # Total confidence
            confidence = min(0.95, 0.5 + deviation_strength * 0.4 + bb_strength)
            
            deviation_pct = (deviation * 100) if deviation else 0
            reason = f"Mean Reversion SELL: Price {deviation_pct:.3f}% above MA"
            if self.use_bollinger and indicators.bb_upper and current_price > indicators.bb_upper:
                reason += " | Above Upper Bollinger Band (Strong)"
            elif self.use_bollinger and indicators.bb_middle and current_price > indicators.bb_middle:
                reason += " | Above Middle Bollinger Band (Mild)"
        
        # No signal
        else:
            signal_type = SignalType.HOLD
            confidence = 0.0
            deviation_pct = (deviation * 100) if deviation else 0
            reason = f"No signal: Deviation {deviation_pct:.3f}% within thresholds"
        
        # Create signal object
        signal = Signal(
            symbol=self.symbol,
            signal_type=signal_type,
            price=current_price,
            timestamp=indicators.timestamp,
            confidence=confidence,
            reason=reason,
            ma_short=indicators.sma_short,
            ma_long=indicators.sma_long,
            bollinger_upper=indicators.bb_upper,
            bollinger_lower=indicators.bb_lower,
            bollinger_middle=indicators.bb_middle,
            std_dev=indicators.std_dev,
            deviation_pct=deviation
        )
        
        # Store in history
        if signal_type != SignalType.NONE:
            self.signals_history.append(signal)
            logger.info(f"{self.symbol} Signal: {signal.signal_type.value} @ {current_price:.8f} (confidence: {confidence:.2%})")
            logger.info(f"  {reason}")
        
        return signal
    
    def process_candle(self, prices: List[float], timestamp: float) -> Optional[Signal]:
        """
        Process new candle and generate signal
        
        Args:
            prices: List of close prices (most recent last)
            timestamp: Current timestamp
        
        Returns:
            Signal object or None
        """
        
        # Need minimum data
        if not prices or len(prices) < self.ma_long_period:
            return None
        
        # Calculate indicators
        indicators = self.calculate_indicators(prices, timestamp)
        
        # Generate signal only if we have all indicators
        if (indicators.sma_short is not None and 
            indicators.sma_long is not None and
            indicators.std_dev is not None):
            signal = self.generate_signal(indicators)
            
            # Only return signal if confidence exceeds minimum
            if signal and signal.confidence >= self.min_confidence:
                return signal
        
        return None
    
    def get_indicators_history(self, limit: Optional[int] = None) -> List[Indicators]:
        """Get indicator history"""
        if limit:
            return self.indicators_history[-limit:]
        return self.indicators_history.copy()
    
    def get_signals_history(self, limit: Optional[int] = None) -> List[Signal]:
        """Get signal history"""
        if limit:
            return self.signals_history[-limit:]
        return self.signals_history.copy()
    
    @staticmethod
    def _calculate_sma(prices: List[float], period: int) -> float:
        """Calculate Simple Moving Average"""
        if len(prices) < period:
            return np.mean(prices)
        return float(np.mean(prices[-period:]))
    
    @staticmethod
    def _calculate_std_dev(prices: List[float], period: int) -> float:
        """Calculate Standard Deviation"""
        if len(prices) < period:
            return float(np.std(prices))
        return float(np.std(prices[-period:]))


class MultiSymbolSignalGenerator:
    """
    Manages signal generation for multiple symbols
    """
    
    def __init__(self, symbols: List[str], **generator_kwargs):
        """
        Initialize multi-symbol generator
        
        Args:
            symbols: List of symbols to track
            **generator_kwargs: Arguments to pass to SignalGenerator
        """
        
        self.symbols = symbols
        self.generators: Dict[str, SignalGenerator] = {}
        
        for symbol in symbols:
            self.generators[symbol] = SignalGenerator(symbol, **generator_kwargs)
        
        logger.info(f"Initialized MultiSymbolSignalGenerator for {len(symbols)} symbols")
    
    def process_candle(self, symbol: str, prices: List[float], timestamp: float) -> Optional[Signal]:
        """Process candle for specific symbol"""
        if symbol.lower() not in self.generators:
            logger.warning(f"Unknown symbol: {symbol}")
            return None
        
        return self.generators[symbol.lower()].process_candle(prices, timestamp)
    
    def get_all_signals(self, limit: Optional[int] = None) -> Dict[str, List[Signal]]:
        """Get signal history for all symbols"""
        return {
            symbol: gen.get_signals_history(limit)
            for symbol, gen in self.generators.items()
        }
    
    def get_all_indicators(self, limit: Optional[int] = None) -> Dict[str, List[Indicators]]:
        """Get indicator history for all symbols"""
        return {
            symbol: gen.get_indicators_history(limit)
            for symbol, gen in self.generators.items()
        }


# ==========================================
# EXAMPLE USAGE
# ==========================================

if __name__ == "__main__":
    
    import random
    
    print("\n" + "="*60)
    print("SIGNAL GENERATOR EXAMPLE")
    print("="*60)
    
    # Create generator
    generator = SignalGenerator(
        symbol="USDTUSD",
        ma_short_period=20,
        ma_long_period=50,
        buy_threshold=-0.002,  # -0.2% below MA
        sell_threshold=0.002,   # +0.2% above MA
        min_confidence=0.5,
        use_bollinger=True,
        debug=True
    )
    
    # Simulate price data (random walk around 1.0)
    print("\nSimulating 100 price movements (1-minute candles)...")
    print("Building price history...\n")
    
    prices = [1.0]
    signal_count = 0
    
    for i in range(100):
        # Random walk - create price movements
        change = random.gauss(0, 0.0008)  # Increased volatility
        new_price = prices[-1] + change
        new_price = max(0.998, min(1.002, new_price))  # Keep near 1.0
        prices.append(new_price)
        
        # Process candle
        timestamp = 1000 + (i * 60)  # Simulate 1-minute candles
        signal = generator.process_candle(prices, timestamp)
        
        # Display signals
        if signal and signal.signal_type != SignalType.HOLD:
            signal_count += 1
            emoji = "🟢" if signal.signal_type == SignalType.BUY else "🔴"
            print(f"{emoji} [{i:3d}] {signal.signal_type.value:4s} @ {signal.price:.8f} (confidence: {signal.confidence:.1%})")
            print(f"        {signal.reason}")
        
        # Show progress
        if (i + 1) % 25 == 0:
            print(f"  ... processed {i+1} candles ...")
    
    print(f"\nSimulation completed!")
    
    # Print summary
    print("\n" + "="*60)
    print("SUMMARY")
    print("="*60)
    
    signals = generator.get_signals_history()
    print(f"Total signals generated: {len(signals)}")
    
    buy_signals = [s for s in signals if s.signal_type == SignalType.BUY]
    sell_signals = [s for s in signals if s.signal_type == SignalType.SELL]
    
    print(f"Buy signals: {len(buy_signals)}")
    print(f"Sell signals: {len(sell_signals)}")
    
    if buy_signals:
        print(f"\nFirst buy signal:")
        s = buy_signals[0]
        print(f"  Price: {s.price:.8f}")
        print(f"  Confidence: {s.confidence:.2%}")
        print(f"  Reason: {s.reason}")
    
    if sell_signals:
        print(f"\nFirst sell signal:")
        s = sell_signals[0]
        print(f"  Price: {s.price:.8f}")
        print(f"  Confidence: {s.confidence:.2%}")
        print(f"  Reason: {s.reason}")

"""
Enhanced Risk Manager Module
Includes position control and reconnection recovery

Features:
- Control existing positions (prevent over-trading same symbol)
- Reconnection recovery (detect orphaned positions)
- Position merging logic
- Position scaling (add to existing positions)
- Orphaned position alerts
"""

import logging
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass, field
from enum import Enum
from datetime import datetime

logger = logging.getLogger(__name__)


class TradeAction(Enum):
    """What action to take with existing position"""
    NEW_TRADE = "new_trade"              # No existing position
    ADD_TO_POSITION = "add_to_position"  # Add more to existing
    REDUCE_POSITION = "reduce_position"  # Close part of position
    CLOSE_POSITION = "close_position"    # Close entire position
    REVERSE_POSITION = "reverse_position" # Close and open opposite


@dataclass
class PositionInfo:
    """Information about open position"""
    symbol: str
    entry_price: float
    current_price: float
    size: float                          # Position size (USD)
    contracts: float                     # Number of contracts/coins
    entry_time: float                    # Unix timestamp
    unrealized_pnl: float = 0.0
    status: str = "open"                 # open, closed, partial
    
    def to_dict(self):
        return {
            'symbol': self.symbol,
            'entry_price': self.entry_price,
            'current_price': self.current_price,
            'size': self.size,
            'contracts': self.contracts,
            'entry_time': self.entry_time,
            'unrealized_pnl': self.unrealized_pnl,
            'status': self.status
        }


class PositionManager:
    """
    Manages existing positions and prevents over-trading
    """
    
    def __init__(self):
        """Initialize position manager"""
        self.open_positions: Dict[str, PositionInfo] = {}
        self.closed_positions: List[PositionInfo] = []
        self.position_history: List[PositionInfo] = []
    
    def add_position(
        self,
        symbol: str,
        entry_price: float,
        size: float,
        contracts: float,
        entry_time: float = None
    ) -> PositionInfo:
        """Add new position"""
        if entry_time is None:
            entry_time = datetime.now().timestamp()
        
        position = PositionInfo(
            symbol=symbol,
            entry_price=entry_price,
            current_price=entry_price,
            size=size,
            contracts=contracts,
            entry_time=entry_time
        )
        
        self.open_positions[symbol] = position
        self.position_history.append(position)
        
        logger.info(f"Position added: {symbol} @ {entry_price:.8f} (${size:.2f})")
        return position
    
    def update_position(
        self,
        symbol: str,
        current_price: float,
        additional_size: float = 0.0,
        additional_contracts: float = 0.0
    ) -> Optional[PositionInfo]:
        """Update existing position"""
        if symbol not in self.open_positions:
            logger.warning(f"Position not found: {symbol}")
            return None
        
        position = self.open_positions[symbol]
        position.current_price = current_price
        
        # Calculate unrealized P&L
        position.unrealized_pnl = (current_price - position.entry_price) * position.contracts
        
        # Add to position (pyramiding)
        if additional_size > 0:
            position.size += additional_size
            position.contracts += additional_contracts
            logger.info(f"Position updated: {symbol} now ${position.size:.2f}")
        
        return position
    
    def close_position(
        self,
        symbol: str,
        exit_price: float = None,
        exit_time: float = None
    ) -> Optional[PositionInfo]:
        """Close position"""
        if symbol not in self.open_positions:
            logger.warning(f"Position not found: {symbol}")
            return None
        
        position = self.open_positions[symbol]
        position.status = "closed"
        
        if exit_price:
            position.current_price = exit_price
        
        if exit_time:
            position.entry_time = exit_time  # Reuse field for exit time
        
        # Move to closed
        self.closed_positions.append(position)
        del self.open_positions[symbol]
        
        logger.info(f"Position closed: {symbol} with P&L ${position.unrealized_pnl:.2f}")
        return position
    
    def get_position(self, symbol: str) -> Optional[PositionInfo]:
        """Get position details"""
        return self.open_positions.get(symbol)
    
    def has_open_position(self, symbol: str) -> bool:
        """Check if position exists"""
        return symbol in self.open_positions
    
    def get_all_open_positions(self) -> Dict[str, PositionInfo]:
        """Get all open positions"""
        return self.open_positions.copy()
    
    def get_total_exposure(self) -> float:
        """Get total capital at risk"""
        return sum(p.size for p in self.open_positions.values())
    
    def sync_from_exchange(
        self,
        exchange_positions: Dict[str, Dict]
    ) -> List[str]:
        """
        Sync positions from exchange after reconnection
        
        Args:
            exchange_positions: Positions from exchange
            {
                'USDTUSD': {'size': 500, 'entry_price': 0.9985, 'current_price': 1.0001},
                ...
            }
        
        Returns:
            List of orphaned positions found
        """
        
        orphaned = []
        
        logger.info("🔄 Syncing positions with exchange...")
        
        for symbol, exchange_pos in exchange_positions.items():
            if symbol not in self.open_positions:
                # Found orphaned position!
                orphaned.append(symbol)
                logger.warning(f"⚠️  ORPHANED POSITION FOUND: {symbol}")
                logger.warning(f"   Size: ${exchange_pos['size']:.2f}")
                logger.warning(f"   Entry: {exchange_pos['entry_price']:.8f}")
                logger.warning(f"   Current: {exchange_pos['current_price']:.8f}")
                
                # Add it to our tracking
                self.add_position(
                    symbol=symbol,
                    entry_price=exchange_pos['entry_price'],
                    size=exchange_pos['size'],
                    contracts=exchange_pos['size'] / exchange_pos['entry_price']
                )
        
        # Check for positions we think we have but exchange doesn't
        for symbol in list(self.open_positions.keys()):
            if symbol not in exchange_positions:
                logger.warning(f"⚠️  MISMATCH: We think we have {symbol} but exchange doesn't!")
                # Could be closed, need manual intervention
        
        if orphaned:
            logger.error(f"🚨 Found {len(orphaned)} orphaned positions during reconnection!")
            logger.error("   Action: Review and manually close or acknowledge them")
        else:
            logger.info("✅ All positions synced correctly")
        
        return orphaned


class EnhancedRiskManager:
    """
    Enhanced Risk Manager with position control and reconnection recovery
    """
    
    def __init__(
        self,
        initial_balance: float = 10000.0,
        max_risk_per_trade: float = 0.02,
        max_daily_loss: float = 0.05,
        max_drawdown: float = 0.10,
        max_positions: int = 5,
        max_exposure_percent: float = 0.50,
        max_position_add_percent: float = 0.50,  # Max to add to existing
        position_sizing: str = "fixed_percent",
        min_confidence: float = 0.5,
        position_size_percent: float = 0.02,
        risk_reward_ratio_min: float = 1.0,
        debug: bool = False
    ):
        """
        Initialize Enhanced Risk Manager
        
        Args:
            max_position_add_percent: Max to add to existing position (50% of original)
        """
        
        self.initial_balance = initial_balance
        self.current_balance = initial_balance
        self.peak_balance = initial_balance
        self.max_risk_per_trade = max_risk_per_trade
        self.max_daily_loss = max_daily_loss
        self.max_drawdown = max_drawdown
        self.max_positions = max_positions
        self.max_exposure_percent = max_exposure_percent
        self.max_position_add_percent = max_position_add_percent
        self.position_sizing = position_sizing
        self.min_confidence = min_confidence
        self.position_size_percent = position_size_percent
        self.risk_reward_ratio_min = risk_reward_ratio_min
        self.debug = debug
        
        # Position management
        self.position_manager = PositionManager()
        self.orphaned_positions: List[str] = []
        
        # Reconnection tracking
        self.last_sync_time: Optional[float] = None
        self.reconnection_count: int = 0
        
        logger.info(f"Initialized EnhancedRiskManager with position control")
        logger.info(f"  Max position add: {max_position_add_percent*100:.0f}%")
    
    def reconnect(self) -> Dict:
        """
        Called when system reconnects (after disconnect)
        
        Returns:
            Reconnection status and any issues found
        """
        
        self.reconnection_count += 1
        self.last_sync_time = datetime.now().timestamp()
        
        logger.warning(f"🔌 System reconnected (attempt #{self.reconnection_count})")
        logger.warning(f"   Checking for orphaned positions...")
        
        return {
            'reconnection_count': self.reconnection_count,
            'last_sync_time': self.last_sync_time,
            'orphaned_positions': self.orphaned_positions,
            'open_positions': self.position_manager.get_all_open_positions()
        }
    
    def sync_exchange_positions(self, exchange_positions: Dict[str, Dict]) -> List[str]:
        """
        Sync with exchange after reconnection
        
        Args:
            exchange_positions: Current positions from exchange
        
        Returns:
            List of orphaned positions
        """
        
        orphaned = self.position_manager.sync_from_exchange(exchange_positions)
        self.orphaned_positions = orphaned
        
        return orphaned
    
    def check_position_control(
        self,
        symbol: str,
        signal_type: str,
        signal_price: float
    ) -> Tuple[bool, str, Optional[TradeAction]]:
        """
        Check if we should allow a trade given existing positions
        
        Args:
            symbol: Trading pair
            signal_type: BUY or SELL
            signal_price: Current price
        
        Returns:
            (is_allowed, reason, action)
        """
        
        existing_position = self.position_manager.get_position(symbol)
        
        if not existing_position:
            # No existing position - allow new trade
            return True, "No existing position", TradeAction.NEW_TRADE
        
        # We have an existing position
        logger.info(f"ℹ️  Existing position in {symbol}: ${existing_position.size:.2f}")
        
        # ==========================================
        # LOGIC: What to do with existing position
        # ==========================================
        
        if signal_type.upper() == "BUY":
            if existing_position.unrealized_pnl >= 0:
                # We're in profit - allow adding
                max_add = existing_position.size * self.max_position_add_percent
                
                if max_add > 0:
                    return True, f"Can add up to ${max_add:.2f} to position", TradeAction.ADD_TO_POSITION
                else:
                    return False, f"Can't add more (max {self.max_position_add_percent*100:.0f}% reached)", TradeAction.ADD_TO_POSITION
            else:
                # We're in loss - don't add
                return False, f"Position in loss (${existing_position.unrealized_pnl:.2f}). Close first or wait for recovery", TradeAction.ADD_TO_POSITION
        
        elif signal_type.upper() == "SELL":
            # Selling when we have a position = close it
            return True, f"Close existing position (${existing_position.size:.2f})", TradeAction.CLOSE_POSITION
        
        return False, "Unknown signal type", None
    
    def get_position_info(self, symbol: str) -> Optional[Dict]:
        """Get detailed position information"""
        position = self.position_manager.get_position(symbol)
        if not position:
            return None
        
        return {
            'symbol': position.symbol,
            'size': position.size,
            'entry_price': position.entry_price,
            'current_price': position.current_price,
            'unrealized_pnl': position.unrealized_pnl,
            'pnl_percent': (position.unrealized_pnl / position.size * 100) if position.size > 0 else 0,
            'status': position.status
        }
    
    def print_position_summary(self):
        """Print all open positions"""
        positions = self.position_manager.get_all_open_positions()
        
        if not positions:
            print("\n📭 No open positions")
            return
        
        print("\n" + "="*60)
        print("OPEN POSITIONS")
        print("="*60)
        
        total_pnl = 0
        for symbol, pos in positions.items():
            print(f"\n{symbol}:")
            print(f"  Size: ${pos.size:.2f}")
            print(f"  Entry: {pos.entry_price:.8f}")
            print(f"  Current: {pos.current_price:.8f}")
            print(f"  P&L: ${pos.unrealized_pnl:+.2f} ({pos.unrealized_pnl/pos.size*100:+.2f}%)")
            total_pnl += pos.unrealized_pnl
        
        print(f"\nTotal Open P&L: ${total_pnl:+.2f}")
        print(f"Total Exposure: ${self.position_manager.get_total_exposure():.2f}")


# ==========================================
# EXAMPLE USAGE
# ==========================================

if __name__ == "__main__":
    
    print("\n" + "="*70)
    print("ENHANCED RISK MANAGER - POSITION CONTROL EXAMPLE")
    print("="*70)
    
    # Create manager
    rm = EnhancedRiskManager(
        initial_balance=10000.0,
        max_position_add_percent=0.50
    )
    
    # Example 1: New trade (no position)
    print("\n" + "="*70)
    print("Example 1: New Trade - No Existing Position")
    print("="*70)
    
    allowed, reason, action = rm.check_position_control(
        symbol="USDTUSD",
        signal_type="BUY",
        signal_price=0.9985
    )
    
    print(f"Allowed: {allowed}")
    print(f"Reason: {reason}")
    print(f"Action: {action}")
    
    # Add a position
    rm.position_manager.add_position(
        symbol="USDTUSD",
        entry_price=0.9985,
        size=500.0,
        contracts=500.5
    )
    
    # Example 2: Add to profitable position
    print("\n" + "="*70)
    print("Example 2: Add to Profitable Position")
    print("="*70)
    
    # Update to profitable
    rm.position_manager.update_position(
        symbol="USDTUSD",
        current_price=1.0005
    )
    
    allowed, reason, action = rm.check_position_control(
        symbol="USDTUSD",
        signal_type="BUY",
        signal_price=1.0005
    )
    
    print(f"Allowed: {allowed}")
    print(f"Reason: {reason}")
    print(f"Action: {action}")
    
    # Example 3: Try to add to losing position
    print("\n" + "="*70)
    print("Example 3: Try to Add to Losing Position (REJECTED)")
    print("="*70)
    
    # Update to losing
    rm.position_manager.update_position(
        symbol="USDTUSD",
        current_price=0.9975
    )
    
    allowed, reason, action = rm.check_position_control(
        symbol="USDTUSD",
        signal_type="BUY",
        signal_price=0.9975
    )
    
    print(f"Allowed: {allowed}")
    print(f"Reason: {reason}")
    print(f"Action: {action}")
    
    # Example 4: Close position with SELL signal
    print("\n" + "="*70)
    print("Example 4: Close Position with SELL Signal")
    print("="*70)
    
    allowed, reason, action = rm.check_position_control(
        symbol="USDTUSD",
        signal_type="SELL",
        signal_price=0.9975
    )
    
    print(f"Allowed: {allowed}")
    print(f"Reason: {reason}")
    print(f"Action: {action}")
    
    # Example 5: Reconnection with orphaned position
    print("\n" + "="*70)
    print("Example 5: Reconnection Recovery")
    print("="*70)
    
    # System reconnects
    reconnection_info = rm.reconnect()
    print(f"Reconnection #{reconnection_info['reconnection_count']}")
    
    # Sync with exchange - simulate orphaned position
    exchange_positions = {
        'USDCUSDT': {
            'size': 250.0,
            'entry_price': 1.0005,
            'current_price': 1.0010
        }
    }
    
    orphaned = rm.sync_exchange_positions(exchange_positions)
    print(f"\nOrphaned positions found: {orphaned}")
    
    # Print summary
    print("\n" + "="*70)
    print("Position Summary")
    print("="*70)
    
    rm.print_position_summary()
    
    
"""
Paper Trading Simulator
Simulates order execution and market conditions for backtesting

Features:
- Limit and market order simulation
- Realistic fill simulation based on spread
- Order status tracking
- P&L calculation
- Trade history
- Multiple exchange support (Binance, Kraken, Coinbase simulation)
"""

import logging
import time
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass, field
from enum import Enum
from datetime import datetime
import random

logger = logging.getLogger(__name__)


class OrderStatus(Enum):
    """Order status"""
    PENDING = "pending"        # Waiting to be filled
    PARTIALLY_FILLED = "partially_filled"
    FILLED = "filled"
    CANCELLED = "cancelled"
    REJECTED = "rejected"
    EXPIRED = "expired"


class OrderType(Enum):
    """Order type"""
    LIMIT = "limit"
    MARKET = "market"


class OrderSide(Enum):
    """Order side"""
    BUY = "buy"
    SELL = "sell"


@dataclass
class PaperOrder:
    """Paper trading order"""
    order_id: str
    symbol: str
    side: OrderSide
    order_type: OrderType
    quantity: float           # Number of coins
    price: float              # Order price
    status: OrderStatus = OrderStatus.PENDING
    filled_quantity: float = 0.0
    filled_price: float = 0.0  # Average fill price
    timestamp: float = field(default_factory=lambda: time.time())
    fill_time: Optional[float] = None
    fees: float = 0.0
    
    @property
    def fill_percentage(self) -> float:
        """Fill percentage"""
        return (self.filled_quantity / self.quantity * 100) if self.quantity > 0 else 0
    
    @property
    def is_filled(self) -> bool:
        """Check if fully filled"""
        return self.status in [OrderStatus.FILLED, OrderStatus.PARTIALLY_FILLED]
    
    def to_dict(self):
        return {
            'order_id': self.order_id,
            'symbol': self.symbol,
            'side': self.side.value,
            'type': self.order_type.value,
            'quantity': self.quantity,
            'price': self.price,
            'status': self.status.value,
            'filled_quantity': self.filled_quantity,
            'filled_price': self.filled_price,
            'fill_percentage': self.fill_percentage,
            'timestamp': self.timestamp,
            'fill_time': self.fill_time,
            'fees': self.fees
        }


@dataclass
class Fill:
    """Order fill record"""
    order_id: str
    symbol: str
    side: OrderSide
    quantity: float
    price: float
    timestamp: float
    fees: float = 0.0
    
    def to_dict(self):
        return {
            'order_id': self.order_id,
            'symbol': self.symbol,
            'side': self.side.value,
            'quantity': self.quantity,
            'price': self.price,
            'timestamp': self.timestamp,
            'fees': self.fees
        }


class FillSimulator:
    """
    Simulates order fills based on market conditions
    
    Scenarios:
    - Market orders: fill immediately at market price + slippage
    - Limit orders: fill when price touches limit, with some probability
    - Partial fills: realistic partial execution
    - Rejected orders: market conditions prevent fill
    """
    
    def __init__(
        self,
        maker_fee: float = 0.001,      # 0.1% for limit orders
        taker_fee: float = 0.0015,     # 0.15% for market orders
        slippage: float = 0.0001,      # 0.01% slippage on market orders
        fill_probability: float = 0.8   # 80% chance limit order fills
    ):
        """
        Initialize fill simulator
        
        Args:
            maker_fee: Fee for limit orders (maker)
            taker_fee: Fee for market orders (taker)
            slippage: Slippage on market orders
            fill_probability: Probability limit order gets filled
        """
        
        self.maker_fee = maker_fee
        self.taker_fee = taker_fee
        self.slippage = slippage
        self.fill_probability = fill_probability
    
    def simulate_fill(
        self,
        order: PaperOrder,
        current_market_price: float,
        spread: float = 0.0001  # Bid-ask spread
    ) -> Optional[Fill]:
        """
        Simulate order fill
        
        Args:
            order: Order to fill
            current_market_price: Current market price
            spread: Bid-ask spread
        
        Returns:
            Fill object if filled, None if not
        """
        
        if order.status in [OrderStatus.FILLED, OrderStatus.CANCELLED]:
            return None  # Already processed
        
        # ==========================================
        # MARKET ORDER - Fill immediately
        # ==========================================
        if order.order_type == OrderType.MARKET:
            
            if order.side == OrderSide.BUY:
                # Buy at ask price (above market)
                fill_price = current_market_price * (1 + spread + self.slippage)
                fee = order.quantity * fill_price * self.taker_fee
            else:
                # Sell at bid price (below market)
                fill_price = current_market_price * (1 - spread - self.slippage)
                fee = order.quantity * fill_price * self.taker_fee
            
            # Fill immediately
            order.filled_quantity = order.quantity
            order.filled_price = fill_price
            order.fees = fee
            order.status = OrderStatus.FILLED
            order.fill_time = time.time()
            
            return Fill(
                order_id=order.order_id,
                symbol=order.symbol,
                side=order.side,
                quantity=order.quantity,
                price=fill_price,
                timestamp=order.fill_time,
                fees=fee
            )
        
        # ==========================================
        # LIMIT ORDER - Fill if conditions met
        # ==========================================
        elif order.order_type == OrderType.LIMIT:
            
            can_fill = False
            
            if order.side == OrderSide.BUY:
                # Buy limit: price must be at or below order price
                can_fill = current_market_price <= order.price
                fill_price = min(order.price, current_market_price)
            else:
                # Sell limit: price must be at or above order price
                can_fill = current_market_price >= order.price
                fill_price = max(order.price, current_market_price)
            
            # Check if fill conditions met
            if can_fill:
                # Random chance of fill (realistic)
                if random.random() < self.fill_probability:
                    fee = order.quantity * fill_price * self.maker_fee
                    
                    # Could be partial fill (realistic)
                    fill_qty = order.quantity
                    if random.random() < 0.1:  # 10% chance partial fill
                        fill_qty = order.quantity * random.uniform(0.5, 0.9)
                    
                    order.filled_quantity = fill_qty
                    order.filled_price = fill_price
                    order.fees = fee
                    
                    if fill_qty == order.quantity:
                        order.status = OrderStatus.FILLED
                    else:
                        order.status = OrderStatus.PARTIALLY_FILLED
                    
                    order.fill_time = time.time()
                    
                    return Fill(
                        order_id=order.order_id,
                        symbol=order.symbol,
                        side=order.side,
                        quantity=fill_qty,
                        price=fill_price,
                        timestamp=order.fill_time,
                        fees=fee
                    )
        
        return None


class PaperTradingEngine:
    """
    Paper trading engine - simulates trading without real money
    
    Features:
    - Place limit and market orders
    - Simulate order fills
    - Track P&L
    - Monitor positions
    - Generate trade reports
    """
    
    def __init__(
        self,
        initial_balance: float = 10000.0,
        exchange: str = "binance",
        verbose: bool = True
    ):
        """
        Initialize paper trading engine
        
        Args:
            initial_balance: Starting capital
            exchange: Exchange name (binance, kraken, coinbase)
            verbose: Print status messages
        """
        
        self.initial_balance = initial_balance
        self.current_balance = initial_balance
        self.exchange = exchange
        self.verbose = verbose
        
        # Orders
        self.orders: Dict[str, PaperOrder] = {}
        self.next_order_id = 1
        
        # Fills
        self.fills: List[Fill] = []
        
        # Positions
        self.positions: Dict[str, Dict] = {}  # {symbol: {quantity, avg_price, value}}
        
        # Fill simulator
        self.fill_simulator = FillSimulator()
        
        # P&L tracking
        self.closed_pnl: float = 0.0
        self.open_pnl: float = 0.0
        
        logger.info(f"Initialized PaperTradingEngine on {exchange}")
        logger.info(f"  Initial balance: ${initial_balance:.2f}")
    
    def place_order(
        self,
        symbol: str,
        side: str,  # "BUY" or "SELL"
        order_type: str,  # "LIMIT" or "MARKET"
        quantity: float,
        price: Optional[float] = None
    ) -> PaperOrder:
        """
        Place an order
        
        Args:
            symbol: Trading pair (e.g., "USDTUSD")
            side: "BUY" or "SELL"
            order_type: "LIMIT" or "MARKET"
            quantity: Number of coins
            price: Price for limit orders
        
        Returns:
            PaperOrder object
        """
        
        order_id = f"{self.exchange}_{self.next_order_id}"
        self.next_order_id += 1
        
        order = PaperOrder(
            order_id=order_id,
            symbol=symbol,
            side=OrderSide[side.upper()],
            order_type=OrderType[order_type.upper()],
            quantity=quantity,
            price=price or 0.0
        )
        
        self.orders[order_id] = order
        
        if self.verbose:
            logger.info(f"📋 Order placed: {side} {quantity:.4f} {symbol} @ {price if price else 'MARKET'}")
            logger.info(f"   Order ID: {order_id}")
        
        return order
    
    def process_order(
        self,
        order_id: str,
        current_market_price: float
    ) -> Optional[Fill]:
        """
        Process order fill at current market price
        
        Args:
            order_id: Order to process
            current_market_price: Current market price
        
        Returns:
            Fill object if filled
        """
        
        if order_id not in self.orders:
            logger.warning(f"Order not found: {order_id}")
            return None
        
        order = self.orders[order_id]
        
        # Simulate fill
        fill = self.fill_simulator.simulate_fill(
            order=order,
            current_market_price=current_market_price
        )
        
        if fill:
            # Record fill
            self.fills.append(fill)
            
            # Update position
            self._update_position(fill)
            
            # Update balance
            self._update_balance(fill)
            
            status_emoji = "✅" if order.status == OrderStatus.FILLED else "⚠️"
            if self.verbose:
                logger.info(f"{status_emoji} Order {order.status.value}: {fill.quantity:.4f} {order.symbol} @ {fill.price:.8f}")
                logger.info(f"   Fees: ${fill.fees:.4f}")
                logger.info(f"   Balance: ${self.current_balance:.2f}")
            
            return fill
        
        return None
    
    def _update_position(self, fill: Fill):
        """Update position after fill"""
        
        symbol = fill.symbol
        
        if symbol not in self.positions:
            self.positions[symbol] = {
                'quantity': 0,
                'avg_price': 0,
                'entry_cost': 0,
                'status': 'open'
            }
        
        pos = self.positions[symbol]
        
        if fill.side == OrderSide.BUY:
            # Add to position
            total_cost = pos['entry_cost'] + (fill.quantity * fill.price)
            total_qty = pos['quantity'] + fill.quantity
            pos['avg_price'] = total_cost / total_qty if total_qty > 0 else fill.price
            pos['quantity'] = total_qty
            pos['entry_cost'] = total_cost
        
        else:  # SELL
            # Close position
            if pos['quantity'] > 0:
                # Calculate P&L
                pnl = (fill.price - pos['avg_price']) * fill.quantity
                self.closed_pnl += pnl
                
                # Reduce position
                pos['quantity'] -= fill.quantity
                
                if pos['quantity'] == 0:
                    pos['status'] = 'closed'
                    if self.verbose:
                        logger.info(f"💰 Position closed: {symbol} | P&L: ${pnl:+.4f}")
    
    def _update_balance(self, fill: Fill):
        """Update account balance after fill"""
        
        if fill.side == OrderSide.BUY:
            cost = (fill.quantity * fill.price) + fill.fees
            self.current_balance -= cost
        else:
            proceeds = (fill.quantity * fill.price) - fill.fees
            self.current_balance += proceeds
    
    def update_positions_market_value(self, current_prices: Dict[str, float]):
        """
        Update position market values and P&L
        
        Args:
            current_prices: {symbol: current_price}
        """
        
        self.open_pnl = 0.0
        
        for symbol, pos in self.positions.items():
            if pos['quantity'] > 0 and symbol in current_prices:
                current_price = current_prices[symbol]
                market_value = pos['quantity'] * current_price
                pnl = market_value - pos['entry_cost']
                self.open_pnl += pnl
    
    def get_account_summary(self) -> Dict:
        """Get account summary"""
        
        total_pnl = self.closed_pnl + self.open_pnl
        
        return {
            'balance': self.current_balance,
            'closed_pnl': self.closed_pnl,
            'open_pnl': self.open_pnl,
            'total_pnl': total_pnl,
            'total_return_pct': (total_pnl / self.initial_balance * 100) if self.initial_balance > 0 else 0,
            'positions': len(self.positions),
            'orders': len(self.orders),
            'fills': len(self.fills)
        }
    
    def print_summary(self):
        """Print account summary"""
        
        summary = self.get_account_summary()
        
        print("\n" + "="*70)
        print("PAPER TRADING - ACCOUNT SUMMARY")
        print("="*70)
        print(f"\nBalance:")
        print(f"  Current: ${summary['balance']:.2f}")
        print(f"  Initial: ${self.initial_balance:.2f}")
        
        print(f"\nP&L:")
        print(f"  Closed: ${summary['closed_pnl']:+.2f}")
        print(f"  Open: ${summary['open_pnl']:+.2f}")
        print(f"  Total: ${summary['total_pnl']:+.2f}")
        print(f"  Return: {summary['total_return_pct']:+.2f}%")
        
        print(f"\nActivity:")
        print(f"  Orders: {summary['orders']}")
        print(f"  Fills: {summary['fills']}")
        print(f"  Open Positions: {summary['positions']}")
        
        if self.positions:
            print(f"\nPositions:")
            for symbol, pos in self.positions.items():
                if pos['quantity'] > 0:
                    print(f"  {symbol}: {pos['quantity']:.4f} @ {pos['avg_price']:.8f}")
    
    def print_trade_history(self, limit: int = 10):
        """Print recent trades"""
        
        print("\n" + "="*70)
        print(f"RECENT FILLS (last {min(limit, len(self.fills))})")
        print("="*70)
        
        for fill in self.fills[-limit:]:
            timestamp = datetime.fromtimestamp(fill.timestamp).strftime("%Y-%m-%d %H:%M:%S")
            print(f"\n{timestamp} - {fill.side.value.upper()} {fill.symbol}")
            print(f"  Quantity: {fill.quantity:.4f}")
            print(f"  Price: {fill.price:.8f}")
            print(f"  Cost: ${fill.quantity * fill.price:.4f}")
            print(f"  Fees: ${fill.fees:.4f}")


# ==========================================
# EXAMPLE USAGE
# ==========================================

if __name__ == "__main__":
    
    print("\n" + "="*70)
    print("PAPER TRADING ENGINE - EXAMPLE")
    print("="*70)
    
    # Create paper trading engine
    engine = PaperTradingEngine(
        initial_balance=10000.0,
        exchange="binance",
        verbose=True
    )
    
    print("\n" + "-"*70)
    print("Example 1: Buy with market order")
    print("-"*70)
    
    # Place market buy order
    order1 = engine.place_order(
        symbol="USDTUSD",
        side="BUY",
        order_type="MARKET",
        quantity=500.0
    )
    
    # Process fill at current market price
    fill1 = engine.process_order(order1.order_id, current_market_price=0.9985)
    
    print("\n" + "-"*70)
    print("Example 2: Sell with limit order")
    print("-"*70)
    
    # Place limit sell order
    order2 = engine.place_order(
        symbol="USDTUSD",
        side="SELL",
        order_type="LIMIT",
        quantity=250.0,
        price=1.0005
    )
    
    # Process fill - should fill since price is above limit
    fill2 = engine.process_order(order2.order_id, current_market_price=1.0010)
    
    print("\n" + "-"*70)
    print("Example 3: Price doesn't reach limit")
    print("-"*70)
    
    # Place limit sell order
    order3 = engine.place_order(
        symbol="USDTUSD",
        side="SELL",
        order_type="LIMIT",
        quantity=100.0,
        price=1.0020
    )
    
    # Price doesn't reach limit - no fill
    fill3 = engine.process_order(order3.order_id, current_market_price=1.0008)
    
    # Update positions at current market price
    engine.update_positions_market_value({
        'USDTUSD': 1.0008
    })
    
    # Print summaries
    engine.print_summary()
    engine.print_trade_history()    
    
"""
Order Manager Module
Manages trade execution, order tracking, and P&L monitoring

Features:
- Place limit and market orders
- Track order status
- Monitor fills
- Calculate P&L
- Multiple exchange support
- Real vs paper trading mode
"""

import logging
import time
from typing import Dict, List, Optional
from dataclasses import dataclass, field
from enum import Enum
from datetime import datetime

logger = logging.getLogger(__name__)


class TradingMode(Enum):
    """Trading mode"""
    PAPER = "paper"      # Simulate trading
    LIVE = "live"        # Real trading


@dataclass
class ExecutedTrade:
    """Record of executed trade"""
    trade_id: str
    symbol: str
    side: str             # BUY or SELL
    quantity: float
    entry_price: float
    exit_price: Optional[float] = None
    exit_time: Optional[float] = None
    pnl: Optional[float] = None
    pnl_percent: Optional[float] = None
    status: str = "open"  # open or closed
    fees: float = 0.0
    
    def to_dict(self):
        return {
            'trade_id': self.trade_id,
            'symbol': self.symbol,
            'side': self.side,
            'quantity': self.quantity,
            'entry_price': self.entry_price,
            'exit_price': self.exit_price,
            'pnl': self.pnl,
            'pnl_percent': self.pnl_percent,
            'status': self.status,
            'fees': self.fees
        }


class OrderManager:
    """
    Manages order execution and trade tracking
    """
    
    def __init__(
        self,
        mode: str = "paper",
        exchange: str = "binance",
        paper_trading_engine=None,
        api_key: Optional[str] = None,
        api_secret: Optional[str] = None
    ):
        """
        Initialize Order Manager
        
        Args:
            mode: "paper" or "live"
            exchange: "binance", "kraken", "coinbase"
            paper_trading_engine: Paper trading engine (for paper mode)
            api_key: Exchange API key (for live mode)
            api_secret: Exchange API secret (for live mode)
        """
        
        self.mode = TradingMode[mode.upper()]
        self.exchange = exchange
        self.paper_engine = paper_trading_engine
        
        # API credentials (for live trading)
        self.api_key = api_key
        self.api_secret = api_secret
        
        # Trade tracking
        self.trades: Dict[str, ExecutedTrade] = {}
        self.next_trade_id = 1
        
        # Order tracking
        self.active_orders: Dict[str, Dict] = {}
        
        # Stats
        self.total_trades = 0
        self.winning_trades = 0
        self.losing_trades = 0
        self.total_pnl = 0.0
        
        logger.info(f"Initialized OrderManager")
        logger.info(f"  Mode: {self.mode.value}")
        logger.info(f"  Exchange: {exchange}")
    
    def execute_trade(
        self,
        symbol: str,
        side: str,
        quantity: float,
        order_type: str = "LIMIT",
        price: Optional[float] = None
    ) -> Optional[str]:
        """
        Execute a trade
        
        Args:
            symbol: Trading pair
            side: "BUY" or "SELL"
            quantity: Number of coins
            order_type: "LIMIT" or "MARKET"
            price: Price for limit orders
        
        Returns:
            Trade ID if successful, None otherwise
        """
        
        if self.mode == TradingMode.PAPER:
            return self._execute_paper_trade(symbol, side, quantity, order_type, price)
        else:
            return self._execute_live_trade(symbol, side, quantity, order_type, price)
    
    def _execute_paper_trade(
        self,
        symbol: str,
        side: str,
        quantity: float,
        order_type: str,
        price: Optional[float]
    ) -> Optional[str]:
        """Execute paper trade"""
        
        if not self.paper_engine:
            logger.error("Paper trading engine not configured")
            return None
        
        # Place order in paper trading engine
        order = self.paper_engine.place_order(
            symbol=symbol,
            side=side,
            order_type=order_type,
            quantity=quantity,
            price=price
        )
        
        # Track active order
        self.active_orders[order.order_id] = {
            'symbol': symbol,
            'side': side,
            'quantity': quantity,
            'order_id': order.order_id,
            'status': 'pending'
        }
        
        logger.info(f"📋 Trade executed (paper): {side} {quantity} {symbol}")
        
        return order.order_id
    
    def _execute_live_trade(
        self,
        symbol: str,
        side: str,
        quantity: float,
        order_type: str,
        price: Optional[float]
    ) -> Optional[str]:
        """
        Execute live trade on exchange
        
        For now, just logs - real implementation would use exchange API
        """
        
        logger.warning(f"⚠️  LIVE TRADING NOT YET IMPLEMENTED")
        logger.warning(f"   Would execute: {side} {quantity} {symbol} @ {price if price else 'MARKET'}")
        logger.warning(f"   Exchange: {self.exchange}")
        
        # TODO: Implement with exchange API
        # - Binance: python-binance
        # - Kraken: krakenex
        # - Coinbase: cbpro
        
        return None
    
    def process_fills(self, current_prices: Dict[str, float]):
        """
        Process order fills at current market prices
        
        Args:
            current_prices: {symbol: current_price}
        """
        
        if self.mode == TradingMode.PAPER:
            self._process_paper_fills(current_prices)
        else:
            self._process_live_fills(current_prices)
    
    def _process_paper_fills(self, current_prices: Dict[str, float]):
        """Process paper trading fills"""
        
        if not self.paper_engine:
            return
        
        # Get all pending orders
        for order_id, order_info in list(self.active_orders.items()):
            symbol = order_info['symbol']
            
            if symbol in current_prices:
                # Try to fill order
                fill = self.paper_engine.process_order(
                    order_id=order_id,
                    current_market_price=current_prices[symbol]
                )
                
                if fill:
                    order_info['status'] = 'filled'
    
    def _process_live_fills(self, current_prices: Dict[str, float]):
        """Process live trading fills"""
        
        # TODO: Check with exchange API for actual fills
        pass
    
    def close_position(
        self,
        symbol: str,
        quantity: Optional[float] = None,
        order_type: str = "MARKET",
        price: Optional[float] = None
    ) -> Optional[str]:
        """
        Close a position
        
        Args:
            symbol: Trading pair
            quantity: Amount to close (None = close all)
            order_type: "LIMIT" or "MARKET"
            price: Price for limit orders
        
        Returns:
            Trade ID if successful
        """
        
        # Find open position
        pos = self.paper_engine.positions.get(symbol) if self.paper_engine else None
        
        if not pos or pos['quantity'] <= 0:
            logger.warning(f"No open position in {symbol}")
            return None
        
        # Close full or partial position
        close_qty = quantity if quantity else pos['quantity']
        close_qty = min(close_qty, pos['quantity'])
        
        logger.info(f"📊 Closing position: {symbol} {close_qty:.4f}")
        
        # Execute close order
        trade_id = self.execute_trade(
            symbol=symbol,
            side="SELL",
            quantity=close_qty,
            order_type=order_type,
            price=price
        )
        
        return trade_id
    
    def get_trade_stats(self) -> Dict:
        """Get trading statistics"""
        
        if self.paper_engine:
            summary = self.paper_engine.get_account_summary()
        else:
            summary = {
                'balance': 0,
                'closed_pnl': 0,
                'open_pnl': 0,
                'total_pnl': 0,
                'total_return_pct': 0,
                'positions': 0,
                'orders': 0,
                'fills': len(self.trades)
            }
        
        return {
            'trades': len(self.trades),
            'winning_trades': sum(1 for t in self.trades.values() if t.pnl and t.pnl > 0),
            'losing_trades': sum(1 for t in self.trades.values() if t.pnl and t.pnl < 0),
            'win_rate': (sum(1 for t in self.trades.values() if t.pnl and t.pnl > 0) / len(self.trades) * 100) if self.trades else 0,
            'total_pnl': summary['total_pnl'],
            'total_return_pct': summary['total_return_pct'],
            'balance': summary['balance'],
            'open_positions': summary['positions']
        }
    
    def print_status(self):
        """Print trading status"""
        
        print("\n" + "="*70)
        print("ORDER MANAGER - TRADING STATUS")
        print("="*70)
        print(f"\nMode: {self.mode.value.upper()}")
        print(f"Exchange: {self.exchange}")
        
        stats = self.get_trade_stats()
        
        print(f"\nTrade Statistics:")
        print(f"  Total Trades: {stats['trades']}")
        print(f"  Wins: {stats['winning_trades']}")
        print(f"  Losses: {stats['losing_trades']}")
        print(f"  Win Rate: {stats['win_rate']:.1f}%")
        
        print(f"\nP&L:")
        print(f"  Total P&L: ${stats['total_pnl']:+.2f}")
        print(f"  Return: {stats['total_return_pct']:+.2f}%")
        print(f"  Balance: ${stats['balance']:.2f}")
        
        print(f"\nPositions:")
        print(f"  Open: {stats['open_positions']}")
        
        if self.paper_engine:
            self.paper_engine.print_summary()


# ==========================================
# EXAMPLE USAGE
# ==========================================

if __name__ == "__main__":
    
    from complete_trading_engine import CompleteTradeingEngine
    
    print("\n" + "="*70)
    print("🚀 STARTING REAL TRADING ENGINE")
    print("="*70)
    
    engine = CompleteTradeingEngine(
        symbols=["USDTUSD"],
        interval="1h",
        historical_days=7,
        initial_balance=1000000.0,
        exchange="binance",
        verbose=True
    )
    
    print("\n✅ Trading engine initialized")
    print("💰 Account: $1,000,000")
    print("📊 Symbol: USDTUSD")
    print("⏰ Interval: 1 hour")
    print("\n🟢 Starting trading...\n")
    
    try:
        engine.start_trading(duration=86400*365)
    except KeyboardInterrupt:
        print("\n\n⏹️ Trading stopped by user")
        engine.print_final_summary()
    
    # Create order manager
    om = OrderManager(
        mode="paper",
        exchange="binance",
        paper_trading_engine=paper_engine
    )
    
    # Example 1: Market buy
    print("\n" + "-"*70)
    print("Example 1: Market Buy")
    print("-"*70)
    
    trade_id = om.execute_trade(
        symbol="USDTUSD",
        side="BUY",
        quantity=500.0,
        order_type="MARKET"
    )
    
    # Process fill at current market price
    om.process_fills({'USDTUSD': 0.9985})
    
    # Example 2: Limit sell
    print("\n" + "-"*70)
    print("Example 2: Limit Sell")
    print("-"*70)
    
    trade_id = om.execute_trade(
        symbol="USDTUSD",
        side="SELL",
        quantity=250.0,
        order_type="LIMIT",
        price=1.0010
    )
    
    # Price reaches limit - fill
    om.process_fills({'USDTUSD': 1.0015})
    
    # Example 3: Close position
    print("\n" + "-"*70)
    print("Example 3: Close Position")
    print("-"*70)
    
    om.close_position(
        symbol="USDTUSD",
        order_type="MARKET"
    )
    
    # Process final fill
    om.process_fills({'USDTUSD': 1.0008})
    
    # Print status
    om.print_status()   