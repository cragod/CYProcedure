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
        """主流程"""
        while True:
            # TODO: Try.Exception
            self.__symbol_info_df = self.__ok_handler.update_symbol_info(self.__symbol_info_df, self._symbol_list)
            print('\nsymbol_info:\n', self.__symbol_info_df, '\n')
            # 计算每个策略的下次开始时间
            all_next_time_infos = self._all_next_run_time_infos()
            # 取最小的作为下次开始
            self.__next_run_time = min(all_next_time_infos.values())
            print('策略执行时间:')
            for nt_key in all_next_time_infos:
                print(nt_key, list(filter(lambda x: x.identifier == nt_key, self._strategy_cfgs))[0].coin_pair, ': ', all_next_time_infos[nt_key])
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
            # 本次循环结束
            print('\n', '-' * 20, '本次循环结束，%f秒后进入下一次循环' % 10, '-' * 20, '\n\n')
            time.sleep(10)

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
                # 用来计算信号的，把最新这根删掉
                cal_signal_df = candle_df[candle_df.candle_begin_time < current_time]
                if cal_signal_df is None or cal_signal_df.empty or cal_signal_df.shape[0] == 0:
                    if datetime.now() > self.__next_run_time + timedelta(minutes=1):
                        raise ValueError('{} 时间超过3分钟，放弃，返回空数据'.format(cfg.coin_pair))
                    else:
                        print('{} 没有最新数据'.format(cfg.coin_pair), datetime.now())
                        time.sleep(0.5)
                else:
                    break
            # 策略信号 TODO：假信号逻辑
            signals = self.__calculate_signal(strategy, cal_signal_df, cfg.coin_pair)
            print('{} 信号'.format(cfg.coin_pair), signals)
            # 策略下单
            if signals:
                signal_price = candle_df.iloc[-1].close  # 最后一根K线的收盘价作为信号价
                holding = self.__symbol_info_df.at[cfg.coin_pair, "持仓量"]
                equity = self.__symbol_info_df.at[cfg.coin_pair, "账户权益"]
                leverage = min(float(cfg.leverage), float(self.__symbol_info_df.at[cfg.coin_pair, "最大杠杆"]))
                # 下单逻辑
                order_ids = self.__ok_handler.okex_future_place_order(cfg.coin_pair, signals, signal_price, holding, equity, leverage)
                print('{} 下单记录：\n'.format(cfg.coin_pair), order_ids)
                # 更新订单信息，查看是否完全成交
                time.sleep(self._short_sleep_time)  # 休息一段时间再更新订单信息
                order_infos = self.__ok_handler.update_future_order_info(cfg.coin_pair, order_ids)
                print('更新下单记录：', '\n', order_infos)
            # 订单保存
        except Exception as e:
            # TODO 到了这里，就发一个通知，这次执行就算失败了，忽略了。
            print(e)

    def __calculate_signal(self, strategy: BaseExchangeStrategy, candle_data, coin_pair_str):
        """计算信号，根据持仓情况给出最终信号（止盈止损情况后面加"""

        # 赋值相关数据
        df = candle_data.copy()  # 最新数据
        now_pos = self.__symbol_info_df.at[coin_pair_str, '持仓方向']  # 当前持仓方向
        avg_price = self.__symbol_info_df.at[coin_pair_str, '持仓均价']  # 当前持仓均价（后面用来控制止盈止损

        # 需要计算的目标仓位
        target_pos = None
        symbol_signal = None

        # 根据策略计算出目标交易信号。
        if not df.empty:  # 当原始数据不为空的时候
            target_pos = strategy.calculate_realtime_signals(df, avg_price)

        # 根据目标仓位和实际仓位，计算实际操作，"1": "开多"，"2": "开空"，"3": "平多"， "4": "平空"
        if now_pos == 1 and target_pos == 0:  # 平多
            symbol_signal = [3]
        elif now_pos == -1 and target_pos == 0:  # 平空
            symbol_signal = [4]
        elif now_pos == 0 and target_pos == 1:  # 开多
            symbol_signal = [1]
        elif now_pos == 0 and target_pos == -1:  # 开空
            symbol_signal = [2]
        elif now_pos == 1 and target_pos == -1:  # 平多，开空
            symbol_signal = [3, 2]
        elif now_pos == -1 and target_pos == 1:  # 平空，开多
            symbol_signal = [4, 1]

        self.__symbol_info_df.at[coin_pair_str, '信号时间'] = datetime.now()  # 计算产生信号的时间

        return symbol_signal
