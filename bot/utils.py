import os
import smtplib
import math
import re
import logging
import logging.config
import yaml
import warnings
from email.message import EmailMessage
from datetime import datetime, timezone

import requests
import pandas as pd
from cryptography.fernet import Fernet


def truncate_float(raw_qty: float, precision: float) -> float:
    """Truncate float with respect to precision.
    ex. 25.36546 with precision=0.01 -> 25.36"""
    if precision == 1:
        return math.floor(raw_qty)

    decimal_places = len(str(precision).split(".")[-1])
    qty = round(raw_qty, decimal_places)
    if qty > raw_qty:
        qty = round(
            float(round(raw_qty, decimal_places) - float(f"10e-{decimal_places + 1}")),
            decimal_places,
        )
    return qty


def date_to_unix(date: datetime, tzinfo=timezone.utc, unit="ms"):
    unit_map = {"s": 1, "ms": 1e3, "ns": 1e6}
    if unit not in unit_map.keys():
        raise ValueError(
            f"Wrong unit {unit}. Unit should be one of {list(unit_map.keys())}"
        )

    if date is None:
        return None
    return int(date.replace(tzinfo=tzinfo).timestamp() * unit_map[unit])


def unix_to_dt(timestamp: int, tz=timezone.utc, unit="ms"):
    unit_map = {"s": 1, "ms": 1e3, "ns": 1e6}
    if unit not in unit_map.keys():
        raise ValueError(
            f"Wrong unit {unit}. Unit should be one of {list(unit_map.keys())}"
        )
    if timestamp is None:
        return None
    return datetime.fromtimestamp(timestamp / unit_map[unit], tz=tz)


def send_email(
    sender: str = "izem.mangione@gmail.com",
    receipient: str = "izem.mangione@gmail.com",
    subject: str = "",
    body: str = "",
    html: str = None,
):
    msg = EmailMessage()

    # generic email headers
    msg["From"] = sender
    msg["To"] = receipient
    msg["Subject"] = subject

    # set the body of the mail
    msg.set_content(body)

    if html:
        msg.add_alternative(html, subtype="html")

    # send it using smtplib
    email_address = os.getenv("GMAIL_ADDRESS")
    email_password = os.getenv("GMAIL_PASSWORD")

    with smtplib.SMTP_SSL("smtp.gmail.com", 0) as smtp:
        smtp.login(email_address, email_password)
        smtp.send_message(msg)


def setup_logger(
    logger_name: str, log_config_file: str, log_file: str = "file1.log"
) -> logging.Logger:
    if not log_config_file:
        raise ValueError("Please provide a log configuration file path.")

    with open(log_config_file, "r") as f:
        config = yaml.safe_load(f.read())

        # set the filename for the RotatingFileHandler
        config["handlers"]["file"]["filename"] = log_file

        # apply logging config to logging
        logging.config.dictConfig(config)

        if logger_name not in config["loggers"]:
            warnings.warn(
                "Beware! The logger name you provided does not match any logger defined in the logging config file. "
                f"({list(config['loggers'].keys())}). Using the root logger."
            )
            logger_name = "root"

        return logging.getLogger(logger_name)


def camelcase_to_snakecase(string: str) -> str:
    return re.sub(r"(?<!^)(?=[A-Z])", "_", string).lower()


def validate_market(market: str) -> bool:
    return re.match("^\w+USDT$", market, flags=re.IGNORECASE)


def encrypt_data(encryption_key: str, data: str) -> str:
    f = Fernet(encryption_key.encode())
    encrypted_data = f.encrypt(data.encode())
    return encrypted_data.decode()


def decrypt_data(encryption_key: str, data: str) -> str:
    f = Fernet(encryption_key.encode())
    decrypted_data = f.decrypt(data.encode())
    return decrypted_data.decode()


def get_binance_ohlcv(
    market: str,
    timeframe: str = "1d",
    start_date: datetime = None,
    end_date: datetime = None,
    limit: int = None,
) -> pd.DataFrame:
    params = {
        "symbol": market,
        "interval": timeframe,
        "startTime": date_to_unix(start_date),
        "endTime": date_to_unix(end_date),
        "limit": limit,
    }
    r = requests.get("https://api.binance.com/api/v3/klines", params=params)
    df = pd.DataFrame(r.json())
    df = df[[0, 1, 2, 3, 4, 5]]
    df.columns = ["date", "open", "high", "low", "close", "volume"]
    df["date"] = pd.to_datetime(df["date"], utc=True, unit="ms")
    df = df.astype(
        {"open": float, "high": float, "low": float, "close": float, "volume": float}
    )
    return df
