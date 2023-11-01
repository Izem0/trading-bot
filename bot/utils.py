import os
import smtplib
import math
import re
import logging
import sys
from email.message import EmailMessage
from datetime import datetime, timezone

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
    name: str = __name__,
    datefmt: str = "%Y-%m-%d %H:%M:%S%z",
    handlers: list = None,
    level="INFO",
):
    """Generate a logger"""
    _nameToLevel = {
        "CRITICAL": logging.CRITICAL,
        "FATAL": logging.FATAL,
        "ERROR": logging.ERROR,
        "WARNING": logging.WARNING,
        "INFO": logging.INFO,
        "DEBUG": logging.DEBUG,
    }

    if not handlers:
        handlers = [logging.StreamHandler(sys.stdout)]  # print to console

    logging.basicConfig(
        format="%(asctime)s | %(filename)s:%(lineno)d | %(levelname)s | %(message)s",
        datefmt=datefmt,
        level=_nameToLevel.get(level),
        handlers=handlers,
    )

    logger = logging.getLogger(name)

    return logger


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
