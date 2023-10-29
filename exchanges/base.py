from abc import ABC, abstractmethod
from datetime import datetime


class Exchange(ABC):
    def __init__(self, api_key, api_secret, subaccount: str = None) -> None:
        self.api_key = api_key
        self.api_secret = api_secret
        self.subaccount = subaccount

    ###########
    # ACCOUNT #
    ###########
    @abstractmethod
    def get_api_key_information(self):
        pass

    @abstractmethod
    def is_spot_trading_enabled(self):
        pass

    @abstractmethod
    def get_balance_assets(self):
        pass

    @abstractmethod
    def get_balance_in_usd(self):
        pass

    @abstractmethod
    def get_symbol_qty(self, symbol: str):
        pass

    @abstractmethod
    def get_symbol_amount_in_usd(self, symbol: str):
        pass

    ###########
    # MARKETS #
    ###########
    @abstractmethod
    def get_tickers(self):
        pass

    @abstractmethod
    def get_ohlcv(
        self,
        market: str,
        timeframe: str = "1d",
        start_date: datetime = None,
        end_date: datetime = None,
        limit: int = None,
    ):
        pass

    @abstractmethod
    def get_ticker_price(self, symbol: str):
        pass

    @abstractmethod
    def get_ticker_precision(self, market: str):
        pass

    @abstractmethod
    def get_ticker_min_notional(self, market: str):
        pass

    #########
    # ORDER #
    #########
    @abstractmethod
    def place_order(
        self, market: str, side: str, qty: float, order_type: str, price: float = None
    ):
        pass

    @abstractmethod
    def get_open_orders(self, market: str = None):
        pass

    @abstractmethod
    def cancel_order(self, market: str, order_id: str):
        pass
