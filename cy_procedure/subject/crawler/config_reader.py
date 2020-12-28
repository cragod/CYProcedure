from abc import ABC, abstractproperty
from cy_widgets.exchange.provider import *
from cy_data_access.models.crawler import *
from cy_components.defines.enums import *
from cy_components.utils.coin_pair import *


class CrawlerItemConfig:
    coin_pair: CoinPair
    time_frame: TimeFrame
    exchange_name: str


class CrawlerConfigReader(ABC):
    """配置读取基类"""

    @abstractproperty
    def name(self):
        return ""

    @abstractproperty
    def configs(self):
        raise NotImplementedError("Subclass")

    @abstractproperty
    def ccxt_provider(self):
        raise NotImplementedError("Subclass")

    def _fetch_configs(self, type: CrawlerType):
        """Fetch + Convert to config"""
        def mapper(item):
            cfg = CrawlerItemConfig()
            cfg.coin_pair = ContractCoinPair.coin_pair_with(item.coin_pair, type.separator)
            cfg.time_frame = TimeFrame(item.time_frame)
            cfg.exchange_name = self.ccxt_provider.display_name
            return cfg
        results = list(CrawlerRealtimeConfig.objects.raw({'exchange_type': type.value, 'active': True}))
        return list(map(mapper, results))


class BinanceDeliveryCrawlerConfigReader(CrawlerConfigReader):
    """币安合约"""

    def __init__(self):
        super().__init__()
        self.__provider = CCXTProvider("", "", ExchangeType.Binance, {
            'defaultType': 'delivery'
        })

    @property
    def name(self):
        return "Binance.Delivery"

    @property
    def configs(self):
        return self._fetch_configs(CrawlerType.BNC_DELIVERY)

    @property
    def ccxt_provider(self):
        return self.__provider


class OKExContractCrawlerConfigReader(CrawlerConfigReader):
    """OK合约"""

    def __init__(self):
        super().__init__()
        self.__provider = CCXTProvider("", "", ExchangeType.Okex)

    @property
    def name(self):
        return "OKEx.Contract"

    @property
    def configs(self):
        return self._fetch_configs(CrawlerType.OK_CONTRACT)

    @property
    def ccxt_provider(self):
        return self.__provider
