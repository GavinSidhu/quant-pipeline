#from csv import QUOTE_STRINGS
import os 
import logging
from datetime import datetime, timedelta 
import polars as pl 
from alpaca_trade_api.rest import REST 
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class AlpacaClient:
    """Client for interacting with Alpaca API"""

    def __init__(self, api_key=None, api_secret=None, base_url=None):
        """Initialize Alpaca client"""


        load_dotenv()

        self.api_key = api_key or os.environ.get("ALPACA_API_KEY")
        self.api_secret = api_secret or os.environ.get("ALPACA_API_SECRET")
        self.base_url = base_url or os.environ.get("ALPACA_BASE_URL")

        if not self.api_key or not self.api_secret or not self.base_url:
            raise ValueError("Alpaca API Credentials messing")

        self.api = REST(self.api_key, self.api_secret, self.base_url)
        logger.info("Alpaca client initialized with base URL: %s", self.base_url) 


    def get_account(self):
        """Retrieve account information"""

        try:
            account = self.api.get_account()
            return {
                "id": account.id,
                "status": account.status,
                "equity": float(account.equity),
                "cash": float(account.cash),
                "buying_power": float(account.buying_power),
                "portfolio_value": float(account.portfolio_value), 
                "created_at": account.created_at,
                "currency": account.currency 
            }
        except Exception as e:
            logger.error("Error fetching account information: %s", str(e))
            raise

    def get_bars(self, symbols, timeframe="1Min", start=None, end=None, limit=None):
        """Fetch historical bar data for given symbols and timeframe"""

        try:
        # Default to last 24 hours if not specified
            if start is None:
                start = (datetime.now() - timedelta(days=1))
            elif isinstance(start, str):
                start = datetime.fromisoformat(start.replace('Z', '+00:00'))
            
            if end is None:
                end = datetime.now()
            elif isinstance(end, str):
                end = datetime.fromisoformat(end.replace('Z', '+00:00'))
        
        # Format timestamps in RFC3339 format without microseconds
            start_str = start.strftime('%Y-%m-%dT%H:%M:%SZ')
            end_str = end.strftime('%Y-%m-%dT%H:%M:%SZ')
            
            logger.info(f"Fetching {timeframe} bars for {symbols} from {start_str} to {end_str}")
        
        # Get bars data
            bars = self.api.get_bars(
                symbols,
                timeframe,
                start=start_str,
                end=end_str,
                limit=limit
            ).df

            if bars.empty:
                logger.warning(f"No data returned for {symbols} from {start} to {end}")
                return pl.DataFrame()

            return bars 

        except Exception as e:
            logger.error(f"Error fetching bar data: {str(e)}")
            raise 

    def get_latest_quotes(self, symbols):
        """Get latest qutoes for given symbols"""

        try:
            quotes = {}

            for symbol in symbols:
                quote = self.api.get_latest_quote(symbol)
                quotes[symbol] = {
                    "bid_price": float(quote.bp),
                    "bid_size": int(quote.bs),
                    "ask_price": float(quote.ap),
                    "ask_size": int(quote.as_),
                    "timestamp": quote.t
                }
                
            return quotes 
        except Exception as e:
            logger.error(f"Error fetching quotes: {str(e)}")
            raise 

    def is_market_open(self):
        """Checks if market is currently open"""

        try:
            clock = self.api.get_clock()
            return clock.is_open
        except Exception as e:
            logger.error(f"Error checking market status: {str(e)}")
            raise       

    def get_assets(self, status="active", asset_class="us_equity"):
        """Get a list of assets"""
        try:
            assets = self.api.list_assets(status=status, asset_class=asset_class)
            return [
                {
                    "id": asset.id,
                    "symbol": asset.symbol,
                    "name": asset.name,
                    "exchange": asset.exchange,
                    "tradable": asset.tradable
                }
                for asset in assets
            ]
        except Exception as e:
            logger.error(f"Error fetching assets: {str(e)}")
            raise

    def get_calendar(self, start=None, end=None):
        """Get the trading calendar between start and end dates"""
        try:
            # Default to today if not specified
            if start is None:
                start = datetime.now().date().isoformat()
            if end is None:
                end = datetime.now().date().isoformat()
            
            calendar = self.api.get_calendar(start=start, end=end)
            return [
                {
                    "date": day.date.isoformat(), 
                    "open": day.open.isoformat(),
                    "close": day.close.isoformat()
                }
                for day in calendar
            ]
        except Exception as e:
            logger.error(f"Error fetching calendar: {str(e)}")
            raise  

if __name__ == "__main__":
    # Simple test of the client
    client = AlpacaClient()
    
    # Test account info
    account = client.get_account()
    print(f"Account: {account}")
    
    # Test market status
    is_open = client.is_market_open()
    print(f"Market open: {is_open}")
    
    # Test getting bars for a few symbols
    symbols = ["AAPL", "MSFT", "GOOGL"]
    bars = client.get_bars(symbols, limit=5)
    print(f"Latest bars:\n{bars}")                                                                                                                                                                                                                                                                                                            