import pytz
import time
from datetime import datetime, timedelta
from multiprocessing.pool import Pool
from cy_components.defines.enums import *
from cy_widgets.exchange.provider import *
from cy_widgets.strategy.neutral import *
from cy_data_access.models.quant import *
from cy_data_access.models.config import *
from cy_data_access.models.market import *
from ...exchange.binance import *
from ....util.helper import ProcedureHelper as ph
from ....util.logger import *


class BinanceSwapNeutral:

    _short_sleep_time = 1
    _long_sleep_time = 10
    _sleep_when_debug = False  # debug 模式下默认不睡

    def __init__(self, bc_cfg: BrickCarrierCfg, wechat_token, log_type, debug=False):
        # 整体配置
        self._debug = debug
        self._bc_cfg = bc_cfg
        self.__wechat_token = wechat_token
        self.__log_type = log_type

        # ccxt 初始化
        ccxt_cfg = CCXTConfiguration.configuration_with_id(bc_cfg.ccxt_cfg_id)
        self._ccxt_provider = CCXTProvider(ccxt_cfg.app_key, ccxt_cfg.app_secret, ExchangeType(ccxt_cfg.e_type), {
            'password': ccxt_cfg.app_pw,
            'defaultType': 'future'
        })
        self.__binance_handler = BinanceHandler(self._ccxt_provider)

        # 取策略, 只支持一个策略
        query = {}
        query["_id"] = {
            u"$in": bc_cfg.strategies
        }
        query["stop"] = {
            u"$ne": True
        }
        self._strategy_cfg = list(StrategyCfg.objects.raw(query))[0]
        strategy_name = self._strategy_cfg.strategy_name
        parameters = self._strategy_cfg.parameters

        self._strategy: NeutralStrategyBase = eval(strategy_name).strategy_with_parameters(parameters)

        # 币对
        self.__symbol_list_with_sep = self.__binance_handler.all_usdt_swap_symbols()
        self.__symbol_list = list(map(lambda x: x.replace("/", ''), self.__symbol_list_with_sep))

    @property
    def _generate_recorder(self):
        return PersistenceRecorder(self.__wechat_token, MessageType.WeChatWork, self.__log_type)

    def __sleep_to_next_run_time(self, next_run_time):
        """等到下一次"""
        # 非 Debug || Debug 模式也等 = 等待
        if not self._debug or self._sleep_when_debug:
            print('下次执行时间: ', next_run_time)
            # 取最近的，等等等
            time.sleep(max(0, (next_run_time - datetime.now()).seconds))
            while True:  # 在靠近目标时间时
                if datetime.now() > next_run_time:
                    break

    def __fetch_all_candle(self, limit, run_time):
        """取所有币的K线"""
        # 创建参数列表
        arg_list = [(CoinPair.coin_pair_with(symbol), TimeFrame('1h'), limit, run_time) for symbol in self.__symbol_list_with_sep]
        # 多进程获取数据
        s_time = time.time()
        with Pool(processes=2) as pl:
            # 利用starmap启用多进程信息
            result = pl.starmap(self.fetch_candle_for_strategy, arg_list)

        df = dict(result)
        df = {x: df[x] for x in df if x is not None}
        print('获取所有币种K线数据完成，花费时间：', time.time() - s_time, len(df))
        return df

    def fetch_candle_for_strategy(self, coin_pair: CoinPair, time_frame: TimeFrame, limit, run_time):
        """取策略需要用的K线"""
        candle_cls = candle_record_class_with_components(self._ccxt_provider.ccxt_object_for_fetching.name, coin_pair, time_frame, '_swap')
        # 取最后的 limit 条
        pipeline = [{
            '$sort': {
                '_id': -1
            }
        }, {
            "$limit": limit
        }, {
            '$sort': {
                '_id': 1
            }
        }]
        df = pd.DataFrame(list(candle_cls.objects.aggregate(*pipeline)))

        # 数据不够，不要了
        symbol = coin_pair.formatted().upper()
        if df is None or df.shape[0] < limit:
            print(f'{symbol} 数据太少')
            return None, None

        # Tidy candle
        ph.tidy_candle_from_database(df)

        # 删除runtime那行的数据，如果有的话
        candle_begin_time = run_time.astimezone(tz=pytz.utc)
        df = df[df['candle_begin_time'] != candle_begin_time]

        # 离现在超过2小时，不要了
        delta = candle_begin_time - df.iloc[-1].candle_begin_time.tz_convert(pytz.utc)
        # TODO: 2h
        if delta.total_seconds() >= 60 * 60 * 4:
            print(f'{symbol} 没有最近的K线, {delta.total_seconds()}')
            return None, None

        df['symbol'] = symbol
        # print('结束获取k线数据：', symbol, datetime.now())

        return symbol, df

    def perform_proc(self):
        while True:
            # =====获取账户信息
            # 获取账户的实际持仓
            symbol_info = self.__binance_handler.update_symbol_info(self.__symbol_list)
            print(symbol_info[symbol_info['当前持仓量'] != 0].T, '\n')
            next_run_time = TimeFrame(self._strategy_cfg.time_interval).next_date_time_point()
            # 睡吧，等开工
            self.__sleep_to_next_run_time(next_run_time)
            # 开工了，取 K 线
            limit = self._strategy.candle_count_4_cal_factor
            candle_df_dict = self.__fetch_all_candle(limit, next_run_time)
            # 太少币种
            if len(candle_df_dict) < 10:
                self._generate_recorder.record_exception(f'可选的币太少了，{len(candle_df_dict)}')
                time.sleep(self._long_sleep_time)
                continue
            select_coin_factor_df = self._strategy.cal_factor_and_select_coins(candle_df_dict, next_run_time)
            print(select_coin_factor_df)
            break
