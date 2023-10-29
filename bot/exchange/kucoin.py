import re
import time
import warnings
from datetime import datetime

import pandas as pd
from kucoin.client import Market, Trade, User

from bot.exchange.base import Exchange
from bot.utils import camelcase_to_snakecase, date_to_unix, unix_to_dt


TF_MAP = {
    "1m": "1min",
    "3m": "3min",
    "5m": "5min",
    "15m": "15min",
    "30m": "30min",
    "1h": "1hour",
    "2h": "2hour",
    "4h": "4hour",
    "6h": "6hour",
    "12h": "12hour",
    "1d": "1day",
    "1w": "1week",
}


def convert_market_name(market: str) -> str:
    """Convert ETHUSDT to ETH-USDT, BTCUSDT to BTC-USDT etc."""
    if re.match("\w+USDT", market, flags=re.IGNORECASE):
        return market.replace("USDT", "-USDT")
    return market


class Kucoin(Exchange):
    # STABLECOINS = ['USDT', 'USDC', "DAI", "BUSD", "TUSD", "FRAX", "USDD", "USDP", "GUSD", "USDJ"]

    def __init__(
        self,
        api_key: str,
        api_secret: str,
        api_passphrase: str,
        subaccount: str,
        main_api_key: str,
        main_api_secret: str,
        main_api_passphrase: str,
    ):
        self.market = Market(url="https://api.kucoin.com")
        self.trade = Trade(api_key, api_secret, api_passphrase)
        self.user = User(api_secret, api_secret, api_passphrase)
        self.user_main = User(main_api_key, main_api_secret, main_api_passphrase)
        self.api_key = api_key
        self.subaccount = subaccount

    # def server_time(self):
    #     return self.client.time()['serverTime']

    # def exchange_info(self):
    #     """Return exchange info"""
    #     return self.client.exchange_info()

    #################################
    ############ ACCOUNT ############
    #################################
    def get_api_key_information(self) -> dict:
        """Return API KEY information such as createTime, enableReading, enableWithdrawals etc.
        NEEDS to use the MAIN account to retrieve subaccount's api info."""
        apis_list = self.user_main.get_sub_account_api_list(sub_name=self.subaccount)
        if not apis_list:
            raise ValueError(f"No API KEY found in subaccount {self.subaccount}")
        ls = [x for x in apis_list if x["apiKey"] == self.api_key]
        if not ls:
            raise ValueError(
                f"Subaccount API KEY provided does not exist in subaccount {self.subaccount}."
            )
        return ls[0]

    def _get_sub_info(self) -> str:
        """Returns info about subaccounts.
        Info is: 'userId', 'uid', 'subName', 'type', 'remarks', 'access'"""
        sub_list = self.user_main.get_sub_user()
        if not sub_list:
            raise ValueError(f"No subaccounts found.")
        ls = [x for x in sub_list if x["subName"] == self.subaccount]
        if not ls:
            raise ValueError(f"No subaccount with the name {self.subaccount} found.")
        return ls[0]

    def is_spot_trading_enabled(self) -> bool:
        """Tell whether SPOT trading is active for this API KEY"""
        api_key_info = self.get_api_key_information()
        return "Spot" in api_key_info["permission"]

    def get_balance_assets(self) -> pd.DataFrame:
        """Return balance assets (total quantity & free) for positive balances only"""
        sub_uid = self._get_sub_info()["userId"]
        r = self.user_main.get_sub_account(sub_uid)
        assets = pd.DataFrame(r["tradeAccounts"])
        assets = assets.astype({"balance": float, "available": float})
        assets.rename(
            columns={"currency": "symbol", "balance": "qty", "available": "free"},
            inplace=True,
        )
        return assets[["symbol", "qty", "free"]]

    def get_balance_in_usd(self) -> float:
        """Return total balance worth in USD"""
        assets = self.get_balance_assets()
        assets["price"] = [
            self.get_ticker_price(x + "USDT") if x != "USDT" else 1
            for x in assets["symbol"]
        ]
        assets["amount"] = assets["qty"] * assets["price"]
        return assets["amount"].sum()

    def get_symbol_qty(self, symbol: str) -> float:
        """Return the quantity of a given symbol in wallet"""
        if re.match("\w+-USDT", symbol, flags=re.IGNORECASE):
            raise ValueError(
                f"{symbol} looks like a market, not a symbol, try removing the USDT part."
            )
        if (symbol + "USDT" not in self.get_tickers()) and (
            symbol not in ["USDT", "USD"]
        ):
            raise ValueError(
                f"{symbol} is not a valid symbol or is not listed on Kucoin."
            )
        assets = self.get_balance_assets()
        if symbol not in assets["symbol"].to_list():
            return 0
        return float(assets.loc[assets["symbol"] == symbol, "free"])

    def get_symbol_amount_in_usd(self, symbol: str) -> float:
        """Return amount in USD of a given symbol"""
        if symbol in ["USDT"]:
            return self.get_symbol_qty(symbol)

        return self.get_symbol_qty(symbol) * self.get_ticker_price(
            market=symbol + "USDT"
        )

    #################################
    ############ MARKETS ############
    #################################
    def get_tickers(self):
        """Return listed (active) symbols on Kucoin (with status = TRADING)"""
        r = self.market.get_symbol_list()
        return [x["symbol"].replace("-", "") for x in r if (x["enableTrading"] == True)]

    def get_ohlcv(
        self,
        market: str,
        timeframe: str = "1d",
        start_date: datetime = None,
        end_date: datetime = None,
        limit: int = None,
    ) -> pd.DataFrame:
        """Get open, high, low, close, volume data. Max ? bars. Returns 100 bars if no date range is provided."""
        if limit is not None:
            warnings.warn(
                "`limit` argument is not working by now. It has no effect.",
                category=UserWarning,
            )
        if bool(start_date) != bool(end_date):
            raise ValueError(
                f"Either both or none of start_date and end_date argument should be provided."
            )
        # init kline data
        kline_data = dict(
            symbol=convert_market_name(market),
            kline_type=TF_MAP[timeframe],
        )
        if start_date and end_date:
            kline_data.update(
                {
                    "startAt": date_to_unix(start_date, unit="s"),
                    "endAt": end_date + pd.Timedelta(days=1),
                }
            )
        r = self.market.get_kline(**kline_data)
        df = pd.DataFrame(r)
        df = df[[0, 1, 2, 3, 4, 5]]
        df.columns = ["date", "open", "high", "low", "close", "volume"]
        df["date"] = pd.to_datetime(df["date"], utc=True, unit="s")
        df = df.astype(
            {
                "open": float,
                "high": float,
                "low": float,
                "close": float,
                "volume": float,
            }
        )
        df_r = df[::-1].reset_index(drop=True)
        return df_r

    def get_ticker_price(self, market: str) -> float:
        """Return price (in USDT) of a market"""
        if not re.match("\w+-?USDT", market, flags=re.IGNORECASE):
            raise ValueError(f"'{market}' is not a valid market.")
        price = self.market.get_ticker(convert_market_name(market))
        return float(price["price"])

    def get_ticker_precision(self, market: str) -> float:
        """Precision of a ticker, ex: "0.0001" means one can buy 1.0001 COIN A at best precision
        Specifying a market to Kucoin API does not work, so I filter the correct market myself.
        """
        symbols = self.market.get_symbol_list()
        ls = [
            x["baseIncrement"]
            for x in symbols
            if x["symbol"] == convert_market_name(market)
        ]
        if len(ls) > 0:
            return float(ls[0])
        raise ValueError(f"Can't find a precision for this market {market}")

    def get_ticker_min_notional(self, market: str) -> float:
        """Minimum amount (in USD) for which a coin can be bought"""
        symbols = self.market.get_symbol_list()
        ls = [
            x["quoteMinSize"]
            for x in symbols
            if x["symbol"] == convert_market_name(market)
        ]
        if len(ls) > 0:
            return float(ls[0])
        raise ValueError(f"Can't find a precision for this market {market}")

    #########
    # ORDER #
    #########
    # def place_test_order(self, market: str, side: str, qty: float, order_type='MARKET', price: float = None):
    #     order = self.client.new_order_test(symbol=market, side=side, type=order_type, quantity=qty, price=price)
    #     return order

    def _get_order_details(self, order_id: str) -> dict:
        """Get order details if exists."""
        r = self.trade.get_order_details(orderId=order_id)
        return r

    def _format_order_details(self, order: dict) -> dict:
        """Format order info so it matches database columns"""
        order_f = {}
        order_f["exchange_order_id"] = order["id"]
        order_f["datetime"] = unix_to_dt(int(order["createdAt"]))
        order_f["market"] = order["symbol"].replace("-", "")
        order_f["side"] = order["side"].upper()
        order_f["type"] = order["type"].upper()
        order_f["price"] = self.get_ticker_price(order["symbol"])
        order_f["qty"] = float(order["dealSize"])
        order_f["quote_quantity"] = float(order["dealFunds"])
        order_f["fee"] = float(order["fee"])
        order_f["original_data"] = order
        return order_f

    def place_order(
        self, market: str, side: str, qty: float, order_type="MARKET", **kwargs
    ) -> dict:
        """Place order"""
        if order_type != "MARKET":
            raise ValueError(f"Only MARKET order type is supported so far.")

        order = self.trade.create_market_order(
            symbol=convert_market_name(market),
            side=side,
            size=qty,
        )
        time.sleep(5)
        order = self._get_order_details(order_id=order["orderId"])
        order_formatted = self._format_order_details(order)
        return order_formatted

    def get_open_orders(self, market: str = None) -> dict:
        data = {"status": "active"}
        if market:
            data["symbol"] = convert_market_name(market)
        r = self.trade.get_order_list(**data)
        return r["items"]

    def cancel_order(self, order_id: str) -> dict:
        """Cancel open order"""
        return self.trade.cancel_order(orderId=order_id)
