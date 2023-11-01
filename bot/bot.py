import pandas as pd
from json2html import json2html
from sqlalchemy import create_engine, types

import strategies
import exchanges
from bot.utils import send_email, truncate_float


class TradingBot:
    def __init__(
        self, user_id, email, exchange_name, credentials: dict, limit, engine
    ) -> None:
        self.user_id = user_id
        self.email = email
        self.exchange_name = exchange_name
        self.credentials = credentials
        self.limit = limit
        self.exchange = getattr(exchanges, exchange_name)(**credentials)
        self.engine = engine

    def check_api_active(self):
        return self.exchange.is_spot_trading_enabled()

    def get_balance(self) -> float:
        return self.excjange.get_balance_in_usd()

    def send_api_permissions(self):
        permissions = self.exchange.get_api_key_information()
        send_email(
            subject=f"API is not active for {self.email=}.",
            html=json2html.convert(permissions),
        )

    def check_sufficient_balance(self, min_balance: int):
        balance = self.exchange.get_balance_in_usd()
        return balance >= min_balance

    def send_insufficient_balance_error(self) -> None:
        balance = self.exchange.get_balance_in_usd()
        send_email(
            subject=f"{self.exchange_name} - {self.email}'s balance is too low ({balance=})."
        )

    def get_portfolio_configs(self) -> pd.DataFrame:
        # get markets to trade for this exchange and this user
        portfolio_configs = pd.read_sql(
            f"""
            select u.id as user_id
            , pc.id as portfolio_config_id
            , m.name
            , m.base
            , m.quote
            , weight
            , s.name as strategy
            , active
            from portfolio_configs pc
            join portfolios p on pc.portfolio_id = p.id 
            join account_connections ac on p.account_connection_id = ac.id 
            join users u on ac.user_id = u.id 
            join markets m on pc.market_id = m.id 
            join exchanges e on m.exchange_id = e.id
            join strategies s on pc.strategy_id = s.id
            where active = true and u.id = {self.user_id} and e.name = '{self.exchange_name}';
        """,
            con=self.engine,
        )
        return portfolio_configs

    def get_signal(self, strategy, market) -> float | int:
        ohlcv = self.exchange.get_ohlcv(market).set_index("date")
        full_signal = getattr(strategies, strategy)(ohlcv)
        return full_signal[-1]

    def get_portfolio_worth(self, pf_assets):
        return sum(
            [self.exchange.get_symbol_amount_in_usd(symbol) for symbol in pf_assets]
        )

    def get_relative_size(
        self, base_asset: str, pf_assets: list[str], limit=999999
    ) -> float:
        portfolio_worth = self.get_portfolio_worth(pf_assets)
        symbol_worth = self.exchange.get_symbol_amount_in_usd(base_asset)
        relative_size = symbol_worth / min(portfolio_worth, limit)
        return relative_size

    def get_quantity_to_buy(self, base, market, pf_assets, weight, signal, limit=99999):
        portfolio_worth = self.get_portfolio_worth(pf_assets)
        target_qty = (
            min(limit, portfolio_worth) * weight * signal
        ) / self.exchange.get_ticker_price(market)
        current_qty = self.exchange.get_symbol_qty(base)
        raw_qty_to_buy = target_qty - current_qty
        precision = self.exchange.get_ticker_precision(market)
        qty_to_buy_trunc = truncate_float(raw_qty_to_buy, precision=precision)
        return qty_to_buy_trunc

    def get_quantity_to_sell(self, base, market, pf_assets, weight, signal, limit=99999):
        portfolio_worth = self.get_portfolio_worth(pf_assets)
        target_qty = (
            min(limit, portfolio_worth) * weight * signal
        ) / self.exchange.get_ticker_price(market)
        current_qty = self.exchange.get_symbol_qty(symbol=base)
        raw_qty_to_sell = current_qty - target_qty
        precision = self.exchange.get_ticker_precision(market)
        qty_to_sell_trunc = truncate_float(raw_qty_to_sell, precision=precision)
        return qty_to_sell_trunc

    def get_symbol_qty(self, symbol="USDT"):
        return self.exchange.get_symbol_qty(symbol=symbol)

    def get_ticker_price(self, market):
        return self.exchange.get_ticker_price(market)

    def place_order(self, market, side, qty):
        order = self.exchange.place_order(market, side=side, qty=qty)
        return order

    def write_order_to_db(self, order):
        order_df = pd.DataFrame(
            columns=[
                "portfolio_config_id",
                "exchange",
                "exchange_order_id",
                "datetime",
                "market",
                "side",
                "signal",
                "type",
                "quantity",
                "quote_quantity",
                "fee",
                "balance",
                "price",
                "original_data",
            ],
            data=[
                [
                    order["portfolio_config_id"],
                    self.exchange_name,
                    order["exchange_order_id"],
                    order["datetime"],
                    order["market"],
                    order["side"],
                    order["signal"],
                    order["type"],
                    order["qty"],
                    order["quote_quantity"],
                    order["fee"],
                    self.exchange.get_balance_in_usd(),
                    order["price"],
                    order["original_data"],
                ]
            ],
        )
        order_df.to_sql(
            "orders",
            self.engine,
            if_exists="append",
            index=False,
            dtype={"original_data": types.JSON},
        )
