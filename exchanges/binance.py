from datetime import datetime
import re

import pandas as pd
from binance.spot import Spot

from exchanges.base import Exchange
from bot.utils import camelcase_to_snakecase, date_to_unix, validate_market


class Binance(Exchange):
    STABLECOINS = [
        "USDT",
        "USDC",
        "DAI",
        "BUSD",
        "TUSD",
        "FRAX",
        "USDD",
        "USDP",
        "GUSD",
        "USDJ",
    ]

    def __init__(self, api_key: str, api_secret: str):
        self.client = Spot(api_key, api_secret)

    # def server_time(self):
    #     return self.client.time()['serverTime']

    # def exchange_info(self):
    #     """Return exchange info"""
    #     return self.client.exchange_info()

    ###########
    # ACCOUNT #
    ###########
    def get_api_key_information(self) -> dict:
        """Return API KEY information such as createTime, enableReading, enableWithdrawals etc."""
        return self.client.api_key_permissions()

    def is_spot_trading_enabled(self) -> bool:
        """Tell whether SPOT trading is active for this API KEY"""
        permissions = self.client.api_key_permissions()
        return permissions["enableSpotAndMarginTrading"]

    def get_balance_assets(self) -> pd.DataFrame:
        """Return balance assets (total quantity & free) for positive balances only"""
        r = self.client.user_asset()
        assets = pd.DataFrame(r)
        assets = assets.astype({"free": float, "locked": float})
        assets["qty"] = assets["free"] + assets["locked"]
        assets = assets[["asset", "qty", "free"]]
        assets.rename(columns={"asset": "symbol"}, inplace=True)
        assets.reset_index(drop=True, inplace=True)
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
                f"{symbol} is not a valid symbol or is not listed on Binance."
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

    ###########
    # MARKETS #
    ###########
    def get_tickers(self):
        """Return listed (active) symbols on Binance (with status = TRADING)"""
        r = self.client.exchange_info()
        return [x["symbol"] for x in r["symbols"] if x["status"] == "TRADING"]

    def get_ohlcv(
        self,
        market: str,
        timeframe: str = "1d",
        start_date: datetime = None,
        end_date: datetime = None,
        limit: int = 200,
    ) -> pd.DataFrame:
        """Get open, high, low, close, volume data. Max 1000 bars."""
        if not validate_market(market):
            raise ValueError(f"'{market}' is not a valid market.")
        r = self.client.klines(
            symbol=market,
            interval=timeframe,
            startTime=date_to_unix(start_date),
            endTime=date_to_unix(end_date),
            limit=limit,
        )
        df = pd.DataFrame(r)
        df = df[[0, 1, 2, 3, 4, 5]]
        df.columns = ["date", "open", "high", "low", "close", "volume"]
        df["date"] = pd.to_datetime(df["date"], utc=True, unit="ms")
        df = df.astype(
            {
                "open": float,
                "high": float,
                "low": float,
                "close": float,
                "volume": float,
            }
        )
        return df

    def get_ticker_price(self, market: str) -> float:
        """Return price (in USDT) of a market"""
        if not validate_market(market):
            raise ValueError(f"'{market}' is not a valid market.")
        price = self.client.ticker_price(market)
        return float(price["price"])

    def get_ticker_precision(self, market: str) -> float:
        """Precision of a ticker, ex: "0.0001" means one can buy 1.0001 COIN A at best precision"""
        if not validate_market(market):
            raise ValueError(f"'{market}' is not a valid market.")
        info = self.client.exchange_info(market)
        filters = info["symbols"][0]["filters"]
        new_filters = {item["filterType"]: item for item in filters}
        return float(new_filters["LOT_SIZE"]["stepSize"])

    def get_ticker_min_notional(self, market: str) -> float:
        """Minimum amount (in USD) for which a coin can be bought"""
        if not validate_market(market):
            raise ValueError(f"'{market}' is not a valid market.")
        filters = self.client.exchange_info(market)["symbols"][0]["filters"]
        new_filters = {item["filterType"]: item for item in filters}
        return float(new_filters["NOTIONAL"]["minNotional"])

    #########
    # ORDER #
    #########
    # def place_test_order(self, market: str, side: str, qty: float, order_type='MARKET', price: float = None):
    #     order = self.client.new_order_test(symbol=market, side=side, type=order_type, quantity=qty, price=price)
    #     return order

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
        if order_type != "MARKET":
            time_in_force = "GTC"

        r = self.client.new_order(
            symbol=market,
            side=side,
            type=order_type,
            quantity=qty,
            price=price,
            timeInForce=time_in_force,
        )
        order = {}
        order["exchange_order_id"] = r["orderId"]
        order["datetime"] = pd.Timestamp.fromtimestamp(
            int(str(r["transactTime"])[:-3]), tz="utc"
        )
        order["market"] = r["symbol"]
        order["side"] = r["side"]
        order["type"] = r["type"]
        order["quantity"] = r["executedQty"]
        order["quote_quantity"] = r["cummulativeQuoteQty"]
        return order

    def get_open_orders(self, market: str = None) -> dict:
        r = self.client.get_open_orders(symbol=market)
        return r

    def cancel_order(self, market: str, order_id: str) -> dict:
        """Cancel open order"""
        return self.client.cancel_order(symbol=market, orderId=order_id)

    # def get_account_snapshots(
    #         self,
    #         type: str = 'SPOT',
    #         start_date: datetime = None,
    #         end_date: datetime = None,
    #         limit: int = 30
    #     ) -> pd.DataFrame:
    #     snap = self.client.account_snapshot(
    #         type=type,
    #         startTime=utils.date_to_unix(start_date),
    #         endTime=utils.date_to_unix(end_date),
    #         limit=limit
    #     )
    #     snap = pd.json_normalize(snap['snapshotVos'])
    #     if snap.shape[0] == 0:
    #         return pd.DataFrame()
    #     snap['updateTime'] = pd.to_datetime(snap['updateTime'], utc=True, unit='ms')
    #     snap = snap.astype({'data.totalAssetOfBtc': float})
    #     snap.rename(columns={
    #         'updateTime': 'date',
    #         'data.totalAssetOfBtc': 'balance_btc',
    #         'data.balances': 'balances'
    #         }, inplace=True)
    #     return snap

    # def get_fiat_order_history(
    #         self,
    #         trasaction_type: int,
    #         start_date: datetime = None,
    #         end_date: datetime = None,
    #         page: int = 1,
    #         rows: int = 500
    #     ) -> pd.DataFrame:
    #     """
    #     Get Fiat Deposit/Withdraw History (USER_DATA).

    #     transactionType (int) 0-deposit, 1-withdraw
    #     beginTime (int, optional)
    #     endTime (int, optional)
    #     page (int, optional) default 1
    #     rows (int, optional) default 100, max 500

    #     https://binance-docs.github.io/apidocs/spot/en/#get-fiat-deposit-withdraw-history-user_data
    #     """
    #     history = self.client.fiat_order_history(
    #         transactionType=trasaction_type,
    #         beginTime=utils.date_to_unix(start_date),
    #         endTime=utils.date_to_unix(end_date),
    #         page=page,
    #         rows=rows
    #     )
    #     df = pd.DataFrame(history['data'])
    #     if df.shape[0] == 0:
    #         return pd.DataFrame(columns=[
    #             'transaction_type',
    #             'order_no',
    #             'fiat_currency',
    #             'indicated_amount',
    #             'amount',
    #             'total_fee',
    #             'method',
    #             'status',
    #             'create_time',
    #             'update_time'
    #         ])
    #     df['transaction_type'] = trasaction_type
    #     df = df.astype({'indicatedAmount': float, 'amount': float, 'totalFee': float})
    #     df.rename(utils.camelcase_to_snakecase, axis=1, inplace=True)
    #     df['create_time'] = pd.to_datetime(df['create_time'], unit='ms', utc=True)
    #     df['update_time'] = pd.to_datetime(df['update_time'], unit='ms', utc=True)
    #     df = df[::-1].reset_index(drop=True)
    #     return df

    # def get_transfer_history(
    #         self,
    #         type: str,
    #         start_date: datetime = None,
    #         end_date: datetime = None,
    #         current: int = 1,
    #         size: int = 100,
    #         from_symbol: str = None,
    #         to_symbol: str = None,
    #     ) -> pd.DataFrame:
    #     """Query User Universal Transfer History"""

    #     transfers = self.client.user_universal_transfer_history(
    #         type=type,
    #         startTime=utils.date_to_unix(start_date),
    #         endTime=utils.date_to_unix(end_date),
    #         current=current,
    #         size=size,
    #         fromSymbol=from_symbol,
    #         toSymbol=to_symbol,
    #     )

    #     if transfers['total'] == 0:
    #         return pd.DataFrame(columns=['datetime', 'asset', 'amount', 'type', 'status', 'tran_id'])

    #     df = pd.DataFrame(transfers['rows'])
    #     # format df
    #     df.rename(utils.camelcase_to_snakecase, axis=1, inplace=True)
    #     df.rename(columns={'timestamp': 'datetime'}, inplace=True)
    #     df = df.astype({'amount': float, 'tran_id': np.int64})
    #     df['datetime'] = pd.to_datetime(df['datetime'], unit='ms', utc=True)
    #     df = df[::-1].reset_index(drop=True)
    #     return df
