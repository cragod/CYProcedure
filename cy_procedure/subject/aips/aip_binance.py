from .aip_base import *
from ..exchange.binance import *


class BinanceAIP(AIPBase):
    """币安定投"""

    def __init__(self,
                 coin_pair,
                 time_frame,
                 signal_provider,
                 day_of_week,
                 ma_periods,
                 trader_provider,
                 invest_base_amount,
                 recorder,
                 debug=False,
                 fee_percent=0.0015):
        super().__init__(coin_pair, time_frame, signal_provider, day_of_week, ma_periods,
                         trader_provider, invest_base_amount, recorder, debug=debug, fee_percent=0)
        self.__handler = BinanceHandler(trader_provider)

    def _prepare_to_buying(self, invest_amount):
        """下单前准备，失败自行记录:
        self.recorder.record_summary_log('**划转{}失败**'.format(self.coin_pair.base_coin.upper()))
        """
        try:
            self.__handler.transfer_margin(self.coin_pair.base_coin, invest_amount, 2)
        except Exception:
            self.recorder.record_summary_log('**划转{}失败**'.format(self.coin_pair.base_coin.upper()))

    def _place_buying_order(self, invest_amount):
        """下单，成功返回：
        {
        'price': 123,
        'cost': 123,
        'amount': 123
        }"""
        try:
            logger = TraderLogger(self.trader_provicer.display_name, self.coin_pair.formatted(), 'Spot', self.recorder)
            return self.__handler.handle_spot_buying(self.coin_pair, invest_amount, logger)
        except Exception as e:
            self.recorder.record_summary_log("下单失败，" + str(e))

    def _rollback_when_order_failed(self, invest_amount):
        """下单失败后回滚"""
        try:
            self.__handler.transfer_margin(self.coin_pair.base_coin, invest_amount, 1)
        except Exception:
            self.recorder.record_summary_log('**回滚划转{}失败**'.format(self.coin_pair.base_coin.upper()))

    def _finishing_aip(self, remaining_base_coin_amount, order_amount):
        """完成定投后的收尾工作"""
        try:
            self.__handler.transfer_margin(self.coin_pair.base_coin, remaining_base_coin_amount, 1)
            self.__handler.transfer_margin(self.coin_pair.trade_coin, order_amount, 1)
        except Exception:
            self.recorder.record_summary_log('**定投结束后划转失败**')
