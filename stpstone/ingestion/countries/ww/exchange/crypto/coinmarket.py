import pandas as pd
from datetime import datetime
from typing import Optional
from sqlalchemy.orm import Session
from logging import Logger
from requests import Response
from stpstone._config.global_slots import YAML_WW_CRYPTO_COINMARKET
from stpstone.utils.cals.handling_dates import DatesBR
from stpstone.utils.connections.netops.proxies.managers.free import YieldFreeProxy
from stpstone.ingestion.abc.requests import ABCRequests


class CoinMarket(ABCRequests):

    def __init__(
        self,
        session: Optional[Session] = None,
        dt_ref:datetime=DatesBR().sub_working_days(DatesBR().curr_date, 1),
        cls_db:Optional[Session]=None,
        logger:Optional[Logger]=None,
        token:Optional[str]=None
    ) -> None:
        self.session = session
        self.dt_ref = dt_ref
        self.cls_db = cls_db
        self.logger = logger
        self.token = token
        super().__init__(
            dict_metadata=YAML_WW_CRYPTO_COINMARKET,
            session=session,
            dt_ref=dt_ref,
            cls_db=cls_db,
            logger=logger,
            token=token
        )

    def req_trt_injection(self, resp_req:Response) -> Optional[pd.DataFrame]:
        list_ser = list()
        json_ = resp_req.json()
        for dict_ in json_['data']:
            list_ser.append({
                'ID': dict_['id'],
                'NAME': dict_['name'],
                'SYMBOL': dict_['symbol'],
                'PRICE': dict_['quote']['USD']['price'],
                'MARKET_CAP': dict_['quote']['USD']['market_cap'],
                'VOLUME': dict_['quote']['USD']['volume_24h'],
                'SLUG': dict_['slug'],
                'TOTAL_SUPPLY': dict_['total_supply'],
                'CMC_RANK': dict_['cmc_rank'],
                'NUM_MARKET_PAIRS': dict_['num_market_pairs'],
                'LAST_UPDATE': dict_['last_updated'],
            })
        return pd.DataFrame(list_ser)
