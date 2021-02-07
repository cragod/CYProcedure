import os
import time
from datetime import datetime
from cy_components.helpers.formatter import CandleFormatter
from cy_data_access.models.market import *
from cy_data_access.util.convert import convert_df_to_json_list
from ...generic.spot_fetching import *


class DBHistoricalSpotCandle:
    """外部连接数据库，内部只负责写入"""

    def __init__(self, coin_pair, time_frame, exchange_type, start_date='2020-03-03 00:00:00', end_date=None, per_limit=1000):
        # 时间区间
        self.start_date = DateFormatter.convert_string_to_local_date(start_date).astimezone()
        self.end_date = DateFormatter.convert_string_to_local_date(end_date) if end_date is not None else datetime.now()
        self.end_date = self.end_date.astimezone()
        # 抓取使用的配置
        self.provider = CCXTProvider("", "", exchange_type)
        self.config = ExchangeFetchingConfiguration(
            coin_pair, time_frame, 1, ExchangeFetchingType.FILL_RECENTLY, batch_limit=per_limit)
        self.fetcher = ExchangeFetcher(self.provider)
        # table name
        self.candle_cls = candle_record_class_with_components(self.provider.display_name, coin_pair, time_frame)

    def run_task(self, retry=10):
        # 开始抓取
        try:
            procedure = ExchangeFetchingProcedure(self.fetcher, self.config, None, self.__get_latest_date, self.__save_df)
            procedure.run_task()
        except Exception as e:
            print("Fetche Failed", e)
            if retry > 0:
                time.sleep(3)
                self.run_task(retry - 1)

    def __get_latest_date(self):
        print("lastest date: {}".format(self.start_date))
        return self.start_date

    def __save_df(self, df: pd.DataFrame):
        if df.shape[0] == 0:
            return False
        # 最后日期
        self.start_date = df.sort_values(COL_CANDLE_BEGIN_TIME, ascending=False)[COL_CANDLE_BEGIN_TIME].iloc[0]
        # 保存
        json_list = convert_df_to_json_list(df, COL_CANDLE_BEGIN_TIME)
        self.candle_cls.bulk_upsert_records(json_list)
        return self.start_date < self.end_date
