import os
import pandas as pd
from datetime import datetime
from cy_components.defines.column_names import *
from ..generic.contract_fetching import *


class HistoricalContractCandle:
    def __init__(self, coin_pair, time_frame, exchange_type, contract_type, start_date='2020-03-03 00:00:00', end_date=None, save_path=None, save_daily=True):
        self.__df = pd.DataFrame()
        self.save_path = save_path
        self.save_daily = save_daily
        # 时间区间
        self.start_date = DateFormatter.convert_string_to_local_date(start_date)
        self.end_date = DateFormatter.convert_string_to_local_date(end_date) if end_date is not None else datetime.now()
        # 抓取使用的配置
        self.provider = CCXTProvider("", "", exchange_type)
        self.config = ContractFetchingConfiguration(
            coin_pair, time_frame, 1, ContractFetchingType.FILL_RECENTLY, contract_type)
        self.fetcher = BaseContractFetcher.dispatched_fetcher(self.provider)

    def run_task(self):
        # 开始抓取
        ContractFetchingProcedure(self.fetcher, self.config, self.__get_earliest_date(),
                                  self.__get_latest_date, self.__save_df).run_task()

    def __get_earliest_date(self):
        # 不往后抓，用不到
        return datetime.now()

    def __get_latest_date(self):
        if self.__df.shape[0] > 0:
            return self.__df[COL_CANDLE_BEGIN_TIME].iloc[-1]
        # 从开始时间，往最近的抓
        return self.start_date

    def __save_df(self, data_df: pd.DataFrame):
        before_count = self.__df.shape[0]

        # 为了去重
        data_df.set_index(COL_CANDLE_BEGIN_TIME, inplace=True)
        if before_count > 0:
            self.__df.set_index(COL_CANDLE_BEGIN_TIME, inplace=True)
        df_res = pd.concat([self.__df, data_df[~data_df.index.isin(self.__df.index)]])
        df_res.update(data_df)

        self.__df = df_res
        # 排序后重置 index
        self.__df.sort_index(inplace=True)
        self.__df.reset_index(inplace=True)

        after_count = self.__df.shape[0]
        # 没有新增
        hasnt_new_candle = before_count == after_count
        is_reach_end_date = self.__df[COL_CANDLE_BEGIN_TIME].iloc[-1] >= self.end_date
        stop = hasnt_new_candle or is_reach_end_date
        if stop:
            self.__df = self.__df[(self.__df[COL_CANDLE_BEGIN_TIME] >= self.start_date)
                                  & (self.__df[COL_CANDLE_BEGIN_TIME] <= self.end_date)]
            self.__df.set_index(COL_CANDLE_BEGIN_TIME, inplace=True)
            print(self.__df)
            if self.save_path is not None:
                self.__save_df_to_csv()
        return not stop

    def __save_df_to_csv(self):
        # (BASE_PATH)/okex/2020-3-17/BTC-USDT-200317.csv
        path = "{}/{}".format(self.save_path, self.provider.ccxt_object_for_fetching.id)
        os.path.join(path)
        if self.save_daily:
            path = path + '/' + DateFormatter.convert_local_date_to_string(self.start_date, '%Y-%m-%d')
        if not os.path.exists(path):
            os.makedirs(path, exist_ok=True)
        path = "{}/{}-{}.csv".format(path, self.config.coin_pair.formatted('-'), self.config.time_frame.value.upper())
        self.__df.to_csv(path)
