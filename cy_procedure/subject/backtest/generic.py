from datetime import datetime
from cy_components.helpers.formatter import DateFormatter as dfr, CandleFormatter as cfr
from cy_components.defines.column_names import *
from cy_components.defines.enums import TimeFrame
from cy_widgets.backtest.strategy import *
from cy_widgets.backtest.helper import *
from cy_data_access.models.backtest import *
from cy_data_access.util.convert import *
from multiprocessing.pool import Pool


class GenericBacktestProcedure:
    """简单回测流程"""

    def __init__(self, task_identifier, time_frames, coin_pair, df, strategy_cls, params, position_func, evaluation_func):

        self.__raw_df = df  # 原始 df
        self.__df = df  # 当前回测在用的 df，方便后面加 df 分割和 resample 用
        self.__raw_time_frames = time_frames
        self.__time_frame = ""
        # ---- K线层参数

        self.__strategy_cls = strategy_cls
        self.__params = params
        self.__position_func = position_func
        self.__evaluation_func = evaluation_func
        self.__task_identifier = task_identifier

    def __result_handler(self, context, position_df, evaluated_df, strategy, error_des):
        """单次回测结束"""
        if error_des is not None:
            print('回测失败', error_des)
        else:
            param_identifier = context['param_identifier']
            curve = str(evaluated_df.iloc[-1][COL_EQUITY_CURVE])
            # 简要数据
            print(position_df.shape)
            overview = BacktestOverview(task_identifier=self.__task_identifier,
                                        param_identifier=param_identifier,
                                        equity_curve=curve)
            json_list = convert_df_to_json_list(position_df, COL_CANDLE_BEGIN_TIME)
            # 保存信号表
            backtest_signal_candle_class(overview.signal_collection_name()).bulk_upsert_records(json_list)
            # 保存总览数据
            overview.save()

    def calculation(self, param):
        """单次回测开始"""
        try:
            strategy = self.__strategy_cls(**param)
            param_str = "|".join(["{}:{}".format(key, param[key]) for key in sorted(param.keys())])
            start_date = dfr.convert_local_date_to_string(self.__df.iloc[0][COL_CANDLE_BEGIN_TIME], "%Y%m%d")
            end_date = dfr.convert_local_date_to_string(self.__df.iloc[-1][COL_CANDLE_BEGIN_TIME], "%Y%m%d")
            param_identifier = "{}|{}|{}|{}".format(strategy.name, param_str, self.__time_frame, "{},{}".format(start_date, end_date))

            if len(list(BacktestOverview.objects.raw({'task_identifier': self.__task_identifier,
                                                      'param_identifier': param_identifier}))) > 0:
                print("{} 已存在，跳过".format(param_identifier))
                return None

            context = {
                'param_identifier': param_identifier
            }

            bt = StrategyBacktest(self.__df.copy(), strategy, self.__position_func, self.__evaluation_func, self.__result_handler, context)
            return bt.perform_test()
        except Exception as e:
            print(str(param), str(e))
            return None

    def perform_test_proc(self, processes=2):
        for time_frame in self.__raw_time_frames:
            self.__time_frame = time_frame

            tf = TimeFrame(time_frame)
            self.__df = cfr.resample(self.__raw_df, tf.rule_type)

            # 到这里 self.__df / self.__time_frame 都是策略的了
            with Pool(processes=processes) as pool:
                start_date = datetime.now()
                pool.map(self.calculation, self.__params)
                print(datetime.now() - start_date)
