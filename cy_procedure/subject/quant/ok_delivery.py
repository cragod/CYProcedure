from .base import *
from ..exchange.okex import *


class OKDeliveryBC(BaseBrickCarrier):
    """OK 单账户实盘执行流程"""

    __symbol_info_columns = ['账户权益', '持仓方向', '持仓量', '持仓收益率', '持仓收益', '持仓均价', '当前价格', '最大杠杆']

    def _did_init(self):
        """初始化结束"""
        self.__ok_handler = OKExHandler(self._ccxt_provider)
        self.__symbol_list = list(map(lambda x: x.coin_pair, self._strategies))
        self.__symbol_info_df = pd.DataFrame(index=self.__symbol_list, columns=self.__symbol_info_columns)  # 转化为dataframe

    def perform_procedure(self):
        # 更新账户信息symbol_info
        self.__symbol_info_df = self.__ok_handler.update_symbol_info(self.__symbol_info_df, self.__symbol_list)
        print('\nsymbol_info:\n', self.__symbol_info_df, '\n')
