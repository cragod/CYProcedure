from abc import ABC, abstractmethod
from cy_widgets.exchange.provider import *
from cy_data_access.models.quant import *
from cy_data_access.models.config import *


class BaseBrickCarrier(ABC):
    """搬砖人基类"""

    def __init__(self, bc_cfg: BrickCarrierCfg):
        # 整体配置
        self._bc_cfg = bc_cfg

        # ccxt 初始化
        ccxt_cfg = CCXTConfiguration.configuration_with_id(bc_cfg.ccxt_cfg_id)
        self._ccxt_provider = CCXTProvider(ccxt_cfg.app_key, ccxt_cfg.app_secret, ExchangeType(ccxt_cfg.e_type), {
            'password': ccxt_cfg.app_pw
        })

        # 取策略
        query = {}
        query["_id"] = {
            u"$in": [
                5.0,
                7.0
            ]
        }
        query["stop"] = {
            u"$ne": True
        }
        self._strategies = list(StrategyCfg.objects.raw(query))
        if self._strategies is None or len(self._strategies) == 0:
            raise ValueError("没有可执行的策略，Exit")

        # 初始化完成
        self._did_init()

    def __str__(self):
        return "{}\n{}\n{}".format(self._bc_cfg, self._ccxt_provider, self._strategies)

    @abstractmethod
    def _did_init(self):
        raise NotImplementedError("Subclass")

    @abstractmethod
    def perform_procedure(self):
        raise NotImplementedError("Subclass")