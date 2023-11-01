"""Get balance data (to run every day at 00:00:05)"""

import os
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, types
from dotenv import load_dotenv
from infisical import InfisicalClient

import exchanges
from bot.utils import setup_logger

load_dotenv()


# load secret token
infisical = InfisicalClient(token=os.getenv("INFISICAL_TOKEN"))
# load all env variables
infisical.get_all_secrets(attach_to_os_environ=True)

# CONSTANTS
BASE_DIR = Path(__file__).parent
LOG = setup_logger(filename=BASE_DIR / "logs/trading_bot.log")
DB_URL = os.getenv("TRADING_BOT_DB")


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
        join exchanges e on ac.exchange_id = e.id;
    """
    )

    for _, row in connections.iterrows():
        LOG.info(f"Fetching balance for {row['email']} on {row['exchange']}")

        exchange = getattr(exchanges, row["exchange"])(
            **row["credentials"]
        )

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

        # post data to DB
        r = db.post(balance_history, "balance_history", dtype={"assets": types.JSON})

        if r == 0:
            LOG.info("No data added to balance")
            # send_email(subject='Error adding data to balance')
        else:
            LOG.info("Balance data added to database")


if __name__ == "__main__":
    main()
