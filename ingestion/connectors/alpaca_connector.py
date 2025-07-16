import os
import logging
from datetime import datetime, timedelta 
import polars as pd 
from dotenv import load_dotenv

from alpaca.client import AlpacaClient

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class AlpacaConnector:
    """Connector for fetching market data from Alpaca API"""

    def __init__(self, api_key=None, api_secret=None, base_url=None):
        """Initialize the Alpaca connector"""

        load_dotenv()

        self.client = AlpacaClient(api_key, api_secret, base_url)
        logger.info("Alpaca connector initialized")

    def check_market_status(self):
        """Check if the market is currently open"""

        return self.client.is_market_open()

    def fetch_bars(self, symbols, timeframe="1Min", start=None, end=None, limit=None):
        """Fetch historical bar data for the given symbols"""

        try:
            bars = self.client.get_bars(symbols, timeframe, start, end, limit)

            if bars.empty:
                logger.warning(f"No data returned for {symbols}")
                return pd.DataFrame()

            bars = bars.reset_index()

            required_columns = ['symbol','timestamp','open','high','low','close']
            for col in required_columns:
                if col not in bars.column:
                    logger.error(f"Missing required column: {col}")
                    return pd.DataFrame()

            formatted_bars = bars[required_columns].copy()

            formatted_bars['timeframe'] = timeframe
            formatted_bars['source'] = 'alpaca'
            formatted_bars['fetched_at'] = datetime.now()

            return formatted_bars
        
        except Exception as e:
            logger.error(f"Error fetching bar data: {str(e)}")
            raise

    def backfill(self, symbols, timeframe="1Min", start_date=None, end_date=None, batch_size=1000):
        """Backfill historical data for the given symbols"""

        try:
            if start_date is None:
                start_date = (datetime.now() - timedelta(days=30)).date()
            elif isinstance(start_date, str):
                start_date = datetime.fromisoformat(start_date.replace('Z', '+00:00')).date()

            if end_date is None:
                end_date = datetime.now().date():
            elif isinstance(end_date, str):
                end_date = datetime.fromisoformat(end_date.replace('Z', '+00:00')).date()

            logger.info(f"backfilling {timeframe} data for {symbols} from {start_date} to {end_date}")

            current_date = start_date
            all_data = []

            while current_date <= end_date:
                batch_end = min(current_date + timedelta(days=7), end_date)

                start_datetime = datetime.combine(current_date, datetime.min.time())
                end_datetime = datetime.combine(batch_end, datetime.max.time())

                logger.info(f"Fetching batch from {start_datetime} to {end_datetime}")

                batch_data = self.fetch_bars(
                    symbols,
                    timeframe,
                    start=start_datetime,
                    end=end_datetime,
                    limit=batch_size
                )

                if not batch_data.empty:
                    all_data.append(batch_data)
                    logger.info(f"Fetched {len(batch_data)} rows for {current_date} to {batch_end}")

                current_date = batch_end + timedelta(days=1)

            if all_data:
                return pd.concat(all_data, ignore_index=True)
            else:
                logger.warning("No data returned from backfill")
                return pd.DataFrame()
        except Exception as e:
            logger.error(f"Error during backfill: {str(e)}")
            raise 

    def setup_streaming(self, symbols, callback):
        """Set up streaming for real-time data"""

        try:
            from alpaca_trade_api.stream import Stream

            api_key = self.client.api_key
            api_secret = self.client.api_secret
            base_url = self.client.base_url

            if 'paper' in base_url:
                stream = Stream(api_key, api_secret, base_url='https://paper-api.alpaca.markets', data_feed='iex')
            else:
                stream = Stream(api_key, api_secret, base_url='https://api.alpaca.markets', data_feed='iex')

            async def on_minute_bars(bar):
                bar_dict = {
                    'symbol':bar.symbol,
                    'timestamp':pd.Timestamp(bar.timestamp),
                    'open': bar.open,
                    'high': bar.high,
                    'low': bar.low,
                    'close': bar.close,
                    'volume': bar.volume,
                    'timeframe': '1Min',
                    'source': 'alpaca_stream',
                    'fetched_at': datetime.now()
                }

                df = pd.DataFrame([bar_dict])

                callback(df)

            for symbol in symbols:
                stream.subscribe_bars(on_minute_bars, symbol)
            
            logger.info(f"Stream setup complete for {symbols}")
            return stream

        except Exception as e:
            logger.error(f"Error setting up streaming: {str(e)}")
            raise 

    def get_tradable_symbols(self, status="active", asset_class="us_equity", min_price=5.0, min_volume=500000):
        """Get a list of tradable symbols based on criteria"""

        try:
            assets = self.client.get_assets(status=status, asset_class=asset_class)

            tradable_symbols = []

            for asset in asset:
                if not asset.get('tradable', False):
                    continue
                symbol = asset.get('symbol')

                if not symbol.isalnum():
                    continue
                    
                try:
                    bars = self.client.get_bars([symbol], limit=5)

                    if bars.empty:
                        continue

                    avg_close = bars['close'].mean()
                    avg_volume = bars['volume'].mean()

                    if avg_close >= min_price and avg_volume >= min_volume:
                        tradable_symbols.append(symbol)
                except Exception as e:
                    logger.warning(f"Error checking {symbol}: {str(e)}")
                    continue
            
            logger.info(f"Found {len(tradable_symbols)} tradable symbols")
            return tradable_symbols
            
        except Exception as e:
            logger.error(f"Error getting tradable symbols: {str(e)}")
            raise
    
if __name__ == "__main__":
    # Simple test of the connector
    connector = AlpacaConnector()
    
    # Test market status
    is_open = connector.check_market_status()
    print(f"Market open: {is_open}")
    
    # Test fetching bars for a few symbols
    symbols = ["AAPL", "MSFT", "GOOGL"]
    bars = connector.fetch_bars(symbols, limit=5)
    print(f"Latest bars:\n{bars}")
    
    # Test backfill
    start_date = (datetime.now() - timedelta(days=5)).date().isoformat()
    backfill_data = connector.backfill(["AAPL"], start_date=start_date)
    print(f"Backfill rows: {len(backfill_data)}")