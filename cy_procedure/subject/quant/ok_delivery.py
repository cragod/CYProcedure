from multiprocessing.pool import Pool
from .base import *
from ..exchange.okex import *


class OKDeliveryBC(BaseBrickCarrier):
    """OK 单账户实盘执行流程"""

    __symbol_info_columns = ['账户权益', '持仓方向', '持仓量', '持仓收益率', '持仓收益', '持仓均价', '当前价格', '最大杠杆']
    __symbol_info_df = None
    __next_run_time = None

    def _did_init(self):
        """初始化结束，父类调用"""
        self.__ok_handler = OKExHandler(self._ccxt_provider)
        self.__symbol_info_df = pd.DataFrame(index=self._symbol_list, columns=self.__symbol_info_columns)  # 转化为dataframe

    def perform_procedure(self):
        self.__symbol_info_df = self.__ok_handler.update_symbol_info(self.__symbol_info_df, self._symbol_list)
        print('\nsymbol_info:\n', self.__symbol_info_df, '\n')
        while True:
            # TODO: Try.Exception
            # 计算每个策略的下次开始时间
            all_next_time_infos = self._all_next_run_time_infos()
            # 取最小的作为下次开始
            self.__next_run_time = min(all_next_time_infos.values())
            print('策略执行时间:')
            for nt_key in all_next_time_infos:
                print(nt_key, ': ', all_next_time_infos[nt_key])
            print('下次执行时间: ', self.__next_run_time)
            # 取最近的，等等等
            time.sleep(max(0, (self.__next_run_time - datetime.now()).seconds))
            while True:  # 在靠近目标时间时
                if datetime.now() > self.__next_run_time:
                    break
            # 到时间的策略进入流程
            to_run_strategie_ids = [s_id for s_id in all_next_time_infos.keys() if all_next_time_infos[s_id] == self.__next_run_time]
            with Pool(processes=4) as pool:
                pool.map(self.single_strategy_procedure, to_run_strategie_ids)
            # 信息整理保存到log
            # 获取账户信息
            # 进入下次循环

    def single_strategy_procedure(self, strategy_id):
        """并行线程，单个策略流程"""
        try:
            cfg: StrategyCfg = list(filter(lambda x: x.identifier == strategy_id, self._strategy_cfgs))[0]
            strategy: BaseExchangeStrategy = self._strategy_from_cfg(cfg)
            current_time = self.__next_run_time.astimezone()
            # 取K线
            while True:
                candle_df = self._fetch_candle_for_strategy(ContractCoinPair.coin_pair_with(cfg.coin_pair, '-'),
                                                            TimeFrame(cfg.time_interval),
                                                            limit=strategy.candle_count_for_calculating)
                if candle_df is None or candle_df.empty or candle_df[candle_df.candle_begin_time == current_time].shape[0] == 0:
                    if datetime.now() > self.__next_run_time + timedelta(minutes=1):
                        raise ValueError('{} 时间超过3分钟，放弃，返回空数据'.format(cfg.coin_pair))
                    else:
                        print('{} 没有最新数据'.format(cfg.coin_pair))
                        time.sleep(0.5)
                else:
                    break
            # 策略信号 TODO：假信号逻辑
            print('{} 信号'.format(cfg.coin_pair), strategy.calculate_signals(candle_df).iloc[-1].signal)
            # 策略下单
            # TODO 去 ticker 下单逻辑
            # 订单保存
        except Exception as e:
            # TODO 到了这里，就发一个通知，这次执行就算失败了，忽略了。
            print(e)
