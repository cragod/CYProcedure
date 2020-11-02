import math
from cy_widgets.exchange.provider import *
from cy_widgets.trader.exchange_trader import *


class BinanceHandler:

    def __init__(self, ccxt_provider: CCXTProvider):
        self.__ccxt_provider = ccxt_provider
        self.__lending_products = None
        self.__fee_percent = 0  # 订单里已经把手续费扣掉返回了，不需要计算

    def fetch_all_lending_product(self):
        """所有活期产品"""
        # https://binance-docs.github.io/apidocs/spot/cn/#baa37cb2f9
        # [{
        #    "asset": "BTC",
        #    "avgAnnualInterestRate": "0.00250025",
        #    "canPurchase": true,
        #    "canRedeem": true,
        #    "dailyInterestPerThousand": "0.00685000",
        #    "featured": true,
        #    "minPurchaseAmount": "0.01000000",
        #    "productId": "BTC001",
        #    "purchasedAmount": "16.32467016",
        #    "status": "PURCHASING",
        #    "upLimit": "200.00000000",
        #    "upLimitPerUser": "5.00000000"
        # }, {...}]
        self.__lending_products = self.__ccxt_provider.ccxt_object_for_query.sapi_get_lending_daily_product_list()

    def daily_lending_product(self, coin_name):
        """查找对应币种的活期产品"""
        if self.__lending_products is None:
            self.fetch_all_lending_product()
        return list(filter(lambda x: x['asset'].lower() == coin_name.lower(), self.__lending_products))

    def fetch_balance(self, type="spot"):
        """查询余额
        type: spot/margin/future"""
        return self.__ccxt_provider.ccxt_object_for_query.fetch_balance({'type': type})

    def transfer_margin(self, coin_name, amount, type=1):
        """全仓杠杆账户划转

        Parameters
        ----------
        type : int, optional
            1: in, 2: out
        """
        return self.__ccxt_provider.ccxt_object_for_query.sapi_post_margin_transfer({
            'asset': coin_name,
            'amount': amount,
            'type': type
        })

    def handle_spot_buying(self, coin_pair, amount, trader_logger):
        """现货买入"""
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
        """现货卖出"""
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
