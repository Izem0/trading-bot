"""Get balance data (to run every day at 00:00:05)"""

import os
import json
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, types
from dotenv import load_dotenv

import exchanges
from bot.utils import setup_logger, decrypt_data

load_dotenv()


# CONSTANTS
BASE_DIR = Path(__file__).parent
LOG_DIR = BASE_DIR / "logs"
LOG_DIR.mkdir(exist_ok=True)
LOG = setup_logger(
    logger_name="routines",
    log_config_file=BASE_DIR / "logging.yaml",
    log_file=LOG_DIR / "trading_bot.log",
)
DB_URL = os.getenv("TRADING_BOT_DB")
ENCRYPTION_KEY = os.getenv("ENCRYPTION_KEY")
DEBUG = os.getenv("DEBUG")


class Database:
    def __init__(self, url) -> None:
        self.engine = create_engine(url)

    def query(self, query) -> pd.DataFrame:
        return pd.read_sql(query, con=self.engine)

    def post(self, df, table, index=False, if_exists="append", **kwargs) -> int:
        """Post a dataframe"""
        return df.to_sql(table, self.engine, index=index, if_exists=if_exists, **kwargs)


def main():
    LOG.info("Script is running")

    # init db connection
    db = Database(url=DB_URL.replace("postgres://", "postgresql://"))

    # get users
    connections = db.query(
        """
        select email, ac.id as account_connection_id, credentials, name as exchange
        from account_connections ac
        join users u on ac.user_id = u.id
        join exchanges e on ac.exchange_id = e.id
        join portfolios p2 on ac.id = p2.account_connection_id
        where p2.active = true;
        """
    )

    for _, row in connections.iterrows():
        LOG.info(f"Fetching balance for {row['email']} on {row['exchange']}")

        creds = json.loads(decrypt_data(ENCRYPTION_KEY, row["credentials"]))
        exchange = getattr(exchanges, row["exchange"])(**creds)

        # get account snapshots
        balance_usd = exchange.get_balance_in_usd()
        balance_assets = exchange.get_balance_assets()

        # create dataframe to insert
        balance_history = pd.DataFrame(
            {
                "account_connection_id": [row["account_connection_id"]],
                "datetime": [pd.Timestamp.now(tz="utc")],
                "balance_usd": [balance_usd],
                "assets": [balance_assets.to_dict("records")],
            }
        )

        if DEBUG:
            LOG.debug("Debug mode, do not add balance to DB.")
            continue

        # post data to DB
        r = db.post(balance_history, "balance_history", dtype={"assets": types.JSON})

        if r == 0:
            LOG.info("No data added to balance")
            # send_email(subject='Error adding data to balance')
        else:
            LOG.info("Balance data added to database")


if __name__ == "__main__":
    main()
