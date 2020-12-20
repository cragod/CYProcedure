import pytz
from multiprocessing.pool import Pool
from cy_data_access.util.convert import *
from cy_data_access.models.market import *
from cy_widgets.fetcher.exchange import *
from cy_components.helpers.formatter import *
from .config_reader import *
from ...generic.spot_fetching import *


class CandleRealtimeCrawler:
    """
    1. 从 ConfigReader 获取需要抓取的相关配置
    2. 检查完整，补齐
    3. 等待到下一分钟开始
    4. 并发开始抓取
    """

    __config_reader: CrawlerConfigReader

    def __init__(self, config_reader):
        self.__config_reader = config_reader

    def __get_configs(self):
        """获取需要抓取的K线配置"""
        return self.__config_reader.configs

    def fetch_kline_and_save(self, config: CrawlerItemConfig):
        """抓取"""
        start_time = datetime.now().astimezone(tz=pytz.utc)
        time_frame = config.time_frame
        coin_pair = config.coin_pair
        while True:
            try:
                if (start_time.replace(second=0) + timedelta(minutes=1) - datetime.now().astimezone(tz=pytz.utc)).seconds < 15:
                    print('{} {} 马上到下一个周期了，不试了'.format(config.coin_pair.formatted(), config.time_frame.value))
                    return
                df = ExchangeFetcher(self.__config_reader.ccxt_provider).fetch_historical_candle_data_by_end_date(coin_pair, time_frame, datetime.now(), 10)
                # 空的
                if df.empty:
                    continue
                delta = start_time - df.iloc[-1].candle_begin_time
                if time_frame.value.endswith('m'):
                    has_last = (delta.seconds % 3600 // 60) < int(time_frame.value[:-1])
                elif time_frame.vlaue.endswith('h'):
                    has_last = (delta.days * 24 + delta.seconds // 3600) < int(time_frame.value[:-1])
                else:
                    print('time_interval不以m或者h结尾，出错，程序exit')
                    exit()
                if not has_last:
                    print('{} {} 获取数据不包含最新的数据，重新获取'.format(config.coin_pair.formatted(), config.time_frame.value))
                    time.sleep(1)
                    continue
                json_list = convert_df_to_json_list(df, COL_CANDLE_BEGIN_TIME)
                candle_record_class_with_components(config.exchange_name, config.coin_pair, config.time_frame).bulk_upsert_records(json_list)
                return
            except Exception as e:
                print('{} {} 出错'.format(config.coin_pair.formatted(), config.time_frame.value))
                print(e)
                time.sleep(5)

    def __dispatch_task(self, configs):
        """分配任务"""
        try:
            pool = Pool(processes=len(configs))
            _ = pool.map(self.fetch_kline_and_save, configs)
        finally:
            pool.close()
            pool.join()

    def run_crawling(self):
        configs = self.__get_configs()
        self.__dispatch_task(configs)
        while True:
            # 等待到下一分钟
            current_time = datetime.now().astimezone(tz=pytz.utc)
            next_time = current_time.replace(second=0) + timedelta(minutes=1)
            time.sleep(max(0, (next_time - current_time).seconds))
            while True:  # 在靠近目标时间时
                if datetime.now().astimezone(tz=pytz.utc) > next_time:
                    break
            print('开始', self.__config_reader.name, datetime.now())
            configs = self.__get_configs()
            self.__dispatch_task(configs)
            print('结束', self.__config_reader.name, datetime.now())