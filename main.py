import os
import math
import json
from concurrent.futures import ThreadPoolExecutor

import pandas as pd
from sqlalchemy import create_engine
from infisical import InfisicalClient
from json2html import json2html
from dotenv import load_dotenv

from bot.bot import TradingBot
from bot.utils import setup_logger, send_email, decrypt_data

load_dotenv()


# load secret token
infisical = InfisicalClient(token=os.getenv("INFISICAL_TOKEN"))
# load all env variables
infisical.get_all_secrets(attach_to_os_environ=True)

# constants
LOG = setup_logger()
DB_URL = os.getenv("TRADING_BOT_DB")
ENCRYPTION_KEY = os.getenv("ENCRYPTION_KEY")
ENGINE = create_engine(DB_URL.replace("postgres://", "postgresql://"))
MIN_BALANCE = 50
DEBUG = os.getenv("DEBUG")


def run_bot(user_id, email, exchange_name, credentials, limit, engine):
    bot = TradingBot(user_id, email, exchange_name, credentials, limit, engine)

    if not bot.check_api_active():
        LOG.warning(f"{exchange_name} - {email=} API is not active.")
        # send api permissions by email
        bot.send_api_permissions()
        return

    balance = bot.check_sufficient_balance(min_balance=50)
    if not balance < MIN_BALANCE:
        send_email(
            subject=f"{exchange_name} - {email}'s balance is too low.",
            body=f"{balance=:.2f}",
        )
        return

    portfolio_configs = bot.get_portfolio_configs()

    for _, row in portfolio_configs.iterrows():
        # get pf config data
        portfolio_config_id = row["portfolio_config_id"]
        base = row["base"]
        quote = row["quote"]
        market = base + quote
        weight = row["weight"]
        pf_assets = portfolio_configs["base"].to_list() + ["USDT"]
        strategy = row["strategy"]

        # get signal
        signal = bot.get_signal(strategy=strategy, market=market)

        # get relative size of that asset in portfolio
        rel_size = bot.get_relative_size(base_asset=base, pf_assets=pf_assets)

        if rel_size > 1:
            LOG.warning(
                f"{exchange_name} Wrong relative size for {email=} ({rel_size=:.2f}!"
            )
            send_email(
                subject=f"{exchange_name} - Wrong relative size for {email=}!",
                body=f"{rel_size=:.2f}!",
            )
            continue

        # compare position with signal
        if math.isclose(a=rel_size, b=signal * weight, abs_tol=0.1):
            LOG.info(
                f"{email=} - {exchange_name=} - {market=} - {signal * weight=} ~= {rel_size=:.2f} -> DO NOTHING!"
            )
            continue

        # init order
        order = None

        ############
        ### BUY ###
        ############
        if signal * weight > rel_size:
            LOG.info(
                f"{email=} - {exchange_name} - {market=} - {signal=} * {weight=} = {signal * weight} > {rel_size=:.2f} -> BUY"
            )

            if DEBUG:
                LOG.info(f"{DEBUG=}, not executing BUY order.")
                continue

            # get quantity to buy
            qty_to_buy_trunc = bot.get_quantity_to_buy(
                base=base,
                market=market,
                pf_assets=pf_assets,
                weight=weight,
                signal=signal,
            )

            # check if user has enough quote asset (USDT) to buy this qty
            free_usdt = bot.get_symbol_qty(symbol="USDT")
            qty_to_buy_usd = qty_to_buy_trunc * bot.get_ticker_price(market)
            if free_usdt < qty_to_buy_usd:
                send_email(
                    subject=f"{email=} - Can't BUY {qty_to_buy_usd:.2f}$ worth of {market}, "
                    f"user has only {free_usdt:.2f} USDT in account."
                )
                continue

            # try placing order
            try:
                order = bot.place_order(market, side="BUY", qty=qty_to_buy_trunc)
                LOG.info("Order successfully placed!")
            except Exception as e:
                send_email(
                    subject=f"Error placing BUY order for {market} for user {email}",
                    body=json2html.convert(e),
                )
                LOG.info(f"Error placing BUY order for {market} for user {email}. {e=}")

        ############
        ### SELL ###
        ############
        if rel_size > signal * weight:
            LOG.info(
                f"{email=} - {exchange_name} - {market=} - {signal=} * {weight=} = {signal * weight} > {rel_size=:.2f} -> SELL"
            )

            if DEBUG:
                LOG.info(f"{DEBUG=}, not executing SELL order.")
                continue

            # get qty to sell
            qty_to_sell_trunc = bot.get_quantity_to_sell(
                base=base,
                market=market,
                pf_assets=pf_assets,
                weight=weight,
                signal=signal,
            )

            # try placing sell order
            try:
                order = bot.place_order(market, side="SELL", qty=qty_to_sell_trunc)
                LOG.info("Order successfully placed!")
            except Exception as e:
                send_email(
                    subject=f"Error placing SELL order for user {email}",
                    body=json2html.convert(e),
                )
                LOG.info(
                    f"Problem with SELL order for {market} for user {email} (error: {e})"
                )

        if order:
            # add info to original order
            order["portfolio_config_id"] = portfolio_config_id
            order["signal"] = signal

            # add order to DB
            bot.write_order_to_db(order)
            LOG.info("Order added to database!")

            # send email
            order_data = order["original_data"]
            order.pop("original_data")
            order.update({"original_data": order_data})
            subject = f"{order['side']} {order['type']} order executed for {market} on {exchange_name} at {order['datetime']:%Y-%m-%d %H:%M:%S} UTC"
            send_email(receipient=email, subject=subject, html=json2html.convert(order))
            LOG.info(f"Mail sent to {email}!")


def main():
    # get users
    users_data = pd.read_sql(
        """
        select u.id as user_id
        , email
        , credentials
        , e.name as exchange_name
        , active
        from portfolios p
        join account_connections ac on p.account_connection_id = ac.id
        join users u on ac.user_id = u.id
        join exchanges e on ac.exchange_id = e.id
        where active = true;
    """,
        con=ENGINE,
    )

    if DEBUG:
        for _, row in users_data.iterrows():
            run_bot(
                user_id=row["user_id"],
                email=row["email"],
                credentials=json.loads(
                    decrypt_data(ENCRYPTION_KEY, row["credentials"])
                ),
                exchange_name=row["exchange_name"],
                limit=99999,
                engine=ENGINE,
            )
    else:
        with ThreadPoolExecutor() as executor:
            for _, row in users_data.iterrows():
                executor.submit(
                    run_bot,
                    user_id=row["user_id"],
                    email=row["email"],
                    credentials=json.loads(
                        decrypt_data(ENCRYPTION_KEY, row["credentials"])
                    ),
                    exchange_name=row["exchange_name"],
                    limit=99999,
                    engine=ENGINE,
                )


if __name__ == "__main__":
    main()
