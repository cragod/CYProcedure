from datetime import datetime
from cy_components.helpers.formatter import DateFormatter as dfr
from cy_components.defines.column_names import *
from cy_widgets.backtest.strategy import *
from cy_widgets.backtest.helper import *
from cy_data_access.models.backtest import *
from multiprocessing.pool import Pool


class SimpleBacktestProcedure:
    """简单回测流程"""

    def __init__(self, coin_pair, df, strategy_cls, params, position_func, evaluation_func):
        self.__df = df
        self.__strategy_cls = strategy_cls
        self.__params = params
        self.__position_func = position_func
        self.__evaluation_func = evaluation_func
        self.__collection_name = '{}_{}_{}'.format(dfr.convert_local_date_to_string(
            datetime.now(), '%Y%m%d%H%M%S'), coin_pair.formatted('_').lower(), str(strategy_cls()).lower())

        self.__start_date = df.iloc[0][COL_CANDLE_BEGIN_TIME]
        self.__end_date = df.iloc[-1][COL_CANDLE_BEGIN_TIME]

    def __result_handler(self, evaluated_df, strategy, error_des):
        if error_des is not None:
            print(error_des)
        else:
            param = StrategyHelper.formatted_identifier(strategy)
            curve = str(evaluated_df.iloc[-1][COL_EQUITY_CURVE])
            print('finished', param, curve)
            summary_collection_cls = backtest_summary_class(self.__collection_name)
            summary_collection_cls(
                parameter=param,
                equity_curve=curve,
                start_date=self.__start_date,
                end_date=self.__end_date
            ).save()

    def calculation(self, param):
        try:
            bt = StrategyBacktest(self.__df.copy(), self.__strategy_cls(
                **param), self.__position_func, self.__evaluation_func, self.__result_handler)
            return bt.perform_test()
        except Exception as e:
            print(str(param), str(e))
            return None

    def perform_test_proc(self, processes=2):
        # self.calculation(self.__params[0])
        with Pool(processes=processes) as pool:
            start_date = datetime.now()
            pool.map(self.calculation, self.__params)
            print(datetime.now() - start_date)
