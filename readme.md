# Trading bot

## Description
This trading bot is designed to automate crypto trading. I actually use that program.

Here is how it works: I have a database where I put some configurations details needed to execute a trade: which exchange to use, exchange API KEY, market(s) to trade, market(s) weights (ex. BTCUSDT weight=60%, ETHUSDT weight=40%) and which trading strategy.

Then every day at midnight UTC, the program pulls market data (namely close price), compute buy/sell signals based on the strategy and executes the trade if a signal is found. A mail is then sent with the order details. Orders are saved in the database. I also have a routine that is triggered every day to get and store the balance of the users in the database.

## Features
* Automated trading based on user-defined portfolio configurations (which markets to trade with which weights ex. BTCUSDT weight=60%, ETHUSDT weight=40%)
* Support for multiple cryptocurrency exchanges (currently Binance, Bybit and Kucoin)
* Integration with various trading strategies (strategies are defined in `strategies.py`, in this repo I use `strategies.py.example` for demonstration purposes, I do not share my personal trading strategies :smiley:)
* Automated email notifications when an order is placed
