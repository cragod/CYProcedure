from datetime import datetime
from cy_components.defines.column_names import *
from cy_widgets.backtest.strategy import *
from cy_widgets.backtest.helper import *
from multiprocessing.pool import Pool


class SimpleBacktestProcedure:
    """简单回测流程"""

    def __init__(self, df, strategy_cls, params, position_func, evaluation_func):
        self.__df = df
        self.__strategy_cls = strategy_cls
        self.__params = params
        self.__position_func = position_func
        self.__evaluation_func = evaluation_func

    def __result_handler(self, evaluated_df, strategy, error_des):
        if error_des is not None:
            print(error_des)
            return None
        else:
            rtn = pd.DataFrame()
            rtn.loc[0, 'param'] = StrategyHelper.formatted_identifier(strategy)
            rtn.loc[0, 'equity_curve'] = evaluated_df.iloc[-1][COL_EQUITY_CURVE]
            return rtn

    def calculation(self, param):
        bt = StrategyBacktest(self.__df.copy(), self.__strategy_cls(
            **param), self.__position_func, self.__evaluation_func, self.__result_handler)
        return bt.perform_test()

    def perform_test_proc(self):
        with Pool(processes=2) as pool:
            start_date = datetime.now()
            result_dfs = pool.map(self.calculation, self.__params)
            totally_dfs = pd.concat(result_dfs, ignore_index=True)
            print(datetime.now() - start_date)
            print(totally_dfs)
