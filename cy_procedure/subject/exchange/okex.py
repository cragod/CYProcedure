import math
from datetime import datetime
from cy_widgets.exchange.provider import *
from cy_widgets.trader.exchange_trader import *


class OKExHandler:

    def __init__(self, ccxt_provider: CCXTProvider):
        self.__ccxt_provider = ccxt_provider
        self.__fee_percent = 0
        self.__all_swap_instruments = None

    def fetch_all_swap_instruments(self):
        """所有的永续合约"""
        self.__all_swap_instruments = self.__ccxt_provider.ccxt_object_for_fetching.swapGetInstruments()
        return self.__all_swap_instruments

    def fetch_all_delivery_instruments(self):
        """所有的交割合约"""
        return self.__ccxt_provider.ccxt_object_for_fetching.futuresGetInstruments()

    def fetch_swap_instrument_fund_rate(self, instrument_id):
        """获取合约费率"""
        return self.__ccxt_provider.ccxt_object_for_fetching.swapGetInstrumentsInstrumentIdFundingTime({
            "instrument_id": instrument_id
        })

    def handle_spot_buying(self, coin_pair, amount, trader_logger):
        """TODO 现货买入"""
        order = Order(coin_pair, amount, 0)  # Only set base coin amount
        executor = ExchangeOrderExecutorFactory.executor(self.__ccxt_provider, order, trader_logger)
        # 下单
        response = executor.handle_long_order_request()
        if response is None:
            raise ConnectionError("Request buying order failed")
        # Binance 手续费已经扣掉了
        # {'id': '701037299',
        # 'clientOrderId': 'Se8IFlpHyWpsY7OaYkhKC1',
        # 'timestamp': 1597589153872,
        # 'datetime': '2020-08-16T14:45:53.872Z',
        # 'lastTradeTimestamp': None,
        # 'symbol': 'BNB/USDT',
        # 'type': 'limit',
        # 'side': 'buy',
        # 'price': 23.3236,
        # 'amount': 0.47,
        # 'cost': 10.854319,
        # 'average': 23.094295744680853,
        # 'filled': 0.47,
        # 'remaining': 0.0,
        # 'status': 'closed',
        # 'fee': None,
        # 'trades': None}
        price = response['average']
        cost = response['cost']
        filled = response['filled']
        buy_amount = math.floor(filled * (1 - self.__fee_percent) * 1e8) / 1e8  # *1e8 向下取整再 / 1e8
        return {
            'price': price,
            'cost': cost,
            'amount': buy_amount
        }

    def handle_spot_selling(self, coin_pair, amount, trader_logger):
        """TODO: 现货卖出"""
        order = Order(coin_pair, 0, amount, side=OrderSide.SELL)  # Only set trade coin amount
        executor = ExchangeOrderExecutorFactory.executor(self.__ccxt_provider, order, trader_logger)
        # place order
        response = executor.handle_close_order_request()
        if response is None:
            raise ConnectionError("Request selling order failed")
        price = response['average']
        cost = response['cost']
        filled = response['filled']
        return {
            'price': price,
            'cost': cost,
            'amount': filled
        }
