"""Console script for cy_procedure."""
import sys
import click as c
from cy_widgets.exchange.provider import *
from cy_components.helpers.formatter import *
from cy_data_access.models.config import *
from .subject.exchange.binance import *


@c.command()
@c.option('--begin', type=str, prompt="Begin date (e.g. 20201122)", required=True)
@c.option('--end', type=str, prompt="End date (e.g. 20201122)", default='20301022', required=False)
@c.option('--asset', type=str, prompt=True, required=True, default='USDT')
def binance_interest(begin, end, asset):
    connect_db_env(db_name=DB_CONFIG)
    # ccxt
    ccxt_config = CCXTConfiguration.configuration_with(CCXTExchangeType.Binance)
    if ccxt_config is None:
        print("ccxt configuration not founded.")
        return
    provider = CCXTProvider(ccxt_config.app_key, ccxt_config.app_secret, ExchangeType.Binance)
    binance_handler = BinanceHandler(provider)
    end_timestamp = DateFormatter.convert_string_to_timestamp(
        end, '%Y%m%d') if end is not None else datetime.now().timestamp()
    interests = binance_handler.lending_interest_history(
        DateFormatter.convert_string_to_timestamp(begin, '%Y%m%d'), end_timestamp, asset)
    total = 0
    print("利息记录")
    for interest in interests[::-1]:
        daily_interest = float(interest['interest'])
        total += daily_interest
        print(DateFormatter.convert_timestamp_to_string(interest['time'], "%Y%m%d"), daily_interest)
    print("Total:", total)
