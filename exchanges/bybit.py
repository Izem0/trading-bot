import re
import time
import warnings
from datetime import datetime

import numpy as np
import pandas as pd
import backoff
from pybit.unified_trading import HTTP

from exchanges.base import Exchange
from bot.utils import (
    camelcase_to_snakecase,
    date_to_unix,
    truncate_float,
    unix_to_dt,
    validate_market,
)


TF_MAP = {
    "1m": 1,
    "3m": 3,
    "5m": 5,
    "15m": 15,
    "30m": 30,
    "1h": 60,
    "2h": 120,
    "4h": 240,
    "6h": 360,
    "12h": 720,
    "1d": "D",
    "1w": "W",
    "1M": "M",
}


class Bybit(Exchange):
    STABLECOINS = ["USDT", "USDC", "DAI", "BUSD", "USDD"]

    def __init__(self, api_key: str, api_secret: str):
        self.client = HTTP(testnet=False, api_key=api_key, api_secret=api_secret)

    ###########
    # ACCOUNT #
    ###########
    def modify_api_key_info(self, **kwargs):
        return self.client.modify_sub_api_key(**kwargs)

    def get_api_key_information(self) -> dict:
        """Return API KEY information such as permissions, ips authorized, vipLevel etc."""
        r = self.client.get_api_key_information()
        return r["result"]

    def is_spot_trading_enabled(self) -> bool:
        """Tell whether SPOT trading is active for this API KEY"""
        return "SpotTrade" in self.get_api_key_information()["permissions"]["Spot"]

    def get_balance_assets(self) -> pd.DataFrame:
        """Return balance assets (total quantity & free) for positive balances only"""
        r = self.client.get_wallet_balance(accountType="UNIFIED")
        assets = pd.DataFrame(r["result"]["list"][0]["coin"])
        assets.columns = [camelcase_to_snakecase(col) for col in assets.columns]
        assets = assets[["coin", "wallet_balance", "available_to_withdraw"]]
        assets.rename(
            columns={
                "coin": "symbol",
                "wallet_balance": "qty",
                "available_to_withdraw": "free",
            },
            inplace=True,
        )
        assets = assets.astype({"qty": float, "free": float})
        return assets

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
        if re.match(r"\w+-?USDT", symbol, flags=re.IGNORECASE):
            raise ValueError(
                f"{symbol} looks like a market, not a symbol, try removing the USDT part."
            )
        if (symbol + "USDT" not in self.get_tickers()) and (
            symbol not in ["USDT", "USD"]
        ):
            raise ValueError(
                f"{symbol} is not a valid symbol or is not listed on Bybit."
            )
        assets = self.get_balance_assets()
        if symbol not in assets["symbol"].to_list():
            return 0
        return float(assets.loc[assets["symbol"] == symbol, "free"])

    def get_symbol_amount_in_usd(self, symbol: str):
        """Return amount in USD of a given symbol"""
        if symbol in ["USDT"]:
            return self.get_symbol_qty(symbol)
        return self.get_symbol_qty(symbol=symbol) * self.get_ticker_price(
            market=symbol + "USDT"
        )

    ###########
    # MARKETS #
    ###########
    def get_tickers(self):
        """Return listed symbols on Bybit (symbols returned by the API are all active, no need to filter)"""
        r = self.client.get_tickers(category="spot")
        return [x["symbol"] for x in r["result"]["list"]]

    def get_ohlcv(
        self,
        market: str,
        timeframe: str = "1d",
        start_date: datetime = None,
        end_date: datetime = None,
        limit: int = 200,
    ):
        """Get open, high, low, close, volume data. Max 200 bars."""
        if not validate_market(market):
            raise ValueError(f"'{market}' is not a valid market.")
        r = self.client.get_kline(
            category="spot",
            symbol=market,
            interval=TF_MAP[timeframe],
            start=date_to_unix(start_date),
            end=date_to_unix(end_date),
            limit=limit,
        )
        if not r["result"]["list"]:
            raise Exception(
                f"No data returned by Bybit API. Consider changing the date range."
            )

        ohlcv = pd.DataFrame(r["result"]["list"])
        ohlcv = ohlcv[[0, 1, 2, 3, 4, 5]]
        ohlcv.columns = ["date", "open", "high", "low", "close", "volume"]
        ohlcv["date"] = pd.to_datetime(ohlcv["date"], utc=True, unit="ms")
        ohlcv = ohlcv.astype(
            {
                "open": float,
                "high": float,
                "low": float,
                "close": float,
                "volume": float,
            }
        )
        ohlcv = ohlcv[::-1].reset_index(drop=True)
        if start_date is not None or end_date is not None:
            if (start_date != ohlcv["date"].iloc[0]) or (
                end_date != ohlcv["date"].iloc[-1]
            ):
                warnings.warn(
                    f"Start date and end date from returned data do not match `start_date` ({start_date}) and `end_date` {end_date} arguments."
                    f"Consider requesting 200 bars maximum.",
                    category=UserWarning,
                )
        return ohlcv

    def get_ticker_price(self, market: str) -> float:
        """Return price (in USDT) of a market"""
        if not validate_market(market):
            raise ValueError(f"'{market}' is not a valid market.")
        r = self.client.get_tickers(category="spot", symbol=market)
        return float(r["result"]["list"][0]["lastPrice"])

    def get_ticker_precision(self, market: str) -> float:
        """Precision of a ticker, ex: "0.0001" means one can buy 1.0001 COIN A at best precision"""
        if not validate_market(market):
            raise ValueError(f"'{market}' is not a valid market.")
        r = self.client.get_instruments_info(category="spot", symbol=market)
        return float(r["result"]["list"][0]["lotSizeFilter"]["basePrecision"])

    def get_ticker_min_notional(self, market: str) -> float:
        """Minimum amount (in USD) for which a coin can be bought"""
        if not validate_market(market):
            raise ValueError(f"'{market}' is not a valid market.")
        r = self.client.get_instruments_info(category="spot", symbol=market)
        return float(r["result"]["list"][0]["lotSizeFilter"]["minOrderAmt"])

    #########
    # ORDER #
    #########
    def _get_order_history(self, **kwargs) -> dict:
        """Query order history."""
        orders = []
        next_page_cursor = ""
        while True:
            r = self.client.get_order_history(
                category="spot", cursor=next_page_cursor, **kwargs
            )
            orders.extend(r["result"]["list"])
            next_page_cursor = r["result"]["nextPageCursor"]
            if not next_page_cursor:
                break
        return orders

    def _get_order_details(self, order_id: str, **kwargs) -> dict:
        """Get order details if exists."""
        r = self.client.get_order_history(category="spot", orderId=order_id, **kwargs)
        ls = [x for x in r["result"]["list"] if x.get("orderId") == order_id]
        if not ls:
            return {}
        return ls[0]

    def _format_order_details(self, order: dict) -> dict:
        """Format order info so it matches database columns"""
        order_f = {}
        order_f["exchange_order_id"] = order.get("orderId")
        created_time = order.get("createdTime")
        if not created_time:
            order_f["datetime"] = None
        else:
            order_f["datetime"] = unix_to_dt(int(order.get("createdTime")))
        order_f["market"] = order.get("symbol")
        order_f["side"] = order.get("side").upper()
        order_f["type"] = order.get("orderType").upper()
        order_f["price"] = float(order.get("avgPrice"))
        order_f["qty"] = float(order.get("cumExecQty"))
        order_f["quote_quantity"] = float(order.get("cumExecValue"))
        order_f["fee"] = float(order.get("cumExecFee"))
        if order_f["side"] == "BUY" and order_f["type"] == "MARKET":
            order_f["fee"] *= order_f["price"]
        order_f["original_data"] = order
        return order_f

    def place_order(
        self,
        market: str,
        side: str,
        qty: float,
        order_type: str = "MARKET",
        price: float = None,
        time_in_force: str = None,
    ) -> dict:
        """Place order"""
        if price is None:
            price = ""

        if order_type.upper() != "MARKET":
            time_in_force = "GTC"

        buy_market_order = (side.upper() == "BUY") and (order_type.upper() == "MARKET")
        # For Spot Market Buy order, qty should be quote curreny amount
        if buy_market_order:
            raw_qty = qty * self.get_ticker_price(market=market)
            qty = truncate_float(raw_qty, precision=0.01)

        r = self.client.place_order(
            category="spot",
            symbol=market,
            side=side,
            orderType=order_type,
            qty=qty,
            price=price,
            timeInForce=time_in_force,
        )
        time.sleep(15)
        order_id = r["result"].get("orderId")
        if not order_id:
            raise ValueError(
                f"Can't get the order id of the successfully placed order. \n{r=}"
            )
        order = self._get_order_details(order_id=order_id)
        order_formatted = self._format_order_details(order)
        return order_formatted

    def get_open_orders(self, market: str = None) -> pd.DataFrame:
        r = self.client.get_open_orders(category="spot", symbol=market)
        return r["result"]["list"]

    def cancel_order(self, order_id: str) -> dict:
        """Cancel open order"""
        order = self._get_order_details(order_id=order_id)
        r = self.client.cancel_order(
            category="spot", orderId=order_id, symbol=order["symbol"]
        )
        return r["result"]

    def _get_internal_transfer_records(self, **kwargs):
        r = self.client.get_internal_transfer_records(**kwargs)
        if not r["result"]["list"]:
            return pd.DataFrame()
        transfers = pd.DataFrame(r["result"]["list"])
        transfers["timestamp"] = pd.to_datetime(
            transfers["timestamp"], unit="ms", utc=True
        )
        transfers = transfers.astype({"amount": float})
        transfers.sort_values("timestamp", ignore_index=True, inplace=True)
        return transfers

    def _get_universal_transfer_records(self, **kwargs):
        r = self.client.get_universal_transfer_records(**kwargs)
        if not r["result"]["list"]:
            return pd.DataFrame()
        transfers = pd.DataFrame(r["result"]["list"])
        transfers["timestamp"] = pd.to_datetime(
            transfers["timestamp"], unit="ms", utc=True
        )
        transfers = transfers.astype(
            {"amount": float, "fromMemberId": int, "toMemberId": int}
        )
        transfers.sort_values("timestamp", ignore_index=True, inplace=True)
        return transfers

    def get_net_transfers(self, **kwargs):
        """Use methods _get_universal_transfer_records and _get_internal_transfer_records to get
        transfers (in and out) of the UNIFIED trading sub-account"""
        # get internal & universal transfers
        internal = self._get_internal_transfer_records(**kwargs)
        universal = self._get_universal_transfer_records(**kwargs)

        # adapt internal data to match universal data
        user_id = self.get_api_key_information().get("userID")
        internal["fromMemberId"] = user_id
        internal["toMemberId"] = user_id

        # join the two dataframes
        transfers = pd.concat([internal, universal], ignore_index=True)
        # compute net amounts
        transfers["net_amount"] = np.where(
            (transfers["toMemberId"] == user_id)
            & (transfers["toAccountType"] == "UNIFIED"),
            transfers["amount"],
            np.nan,
        )
        transfers["net_amount"] = np.where(
            (transfers["fromMemberId"] == user_id)
            & (transfers["fromAccountType"] == "UNIFIED"),
            -transfers["amount"],
            transfers["net_amount"],
        )
        # clean a bit
        transfers.sort_values(["timestamp"], ignore_index=True, inplace=True)
        transfers = transfers[["timestamp", "status", "coin", "net_amount"]]
        transfers.rename(columns={"timestamp": "date"}, inplace=True)
        return transfers
