import pandas as pd
from datetime import datetime
from typing import Optional, List
from sqlalchemy.orm import Session
from logging import Logger
from requests import Response
from time import sleep
from stpstone._config.global_slots import YAML_US_ALPHAVANTAGE
from stpstone.utils.cals.handling_dates import DatesBR
from stpstone.utils.connections.netops.proxies.managers.free import YieldFreeProxy
from stpstone.ingestion.abc.requests import ABCRequests


class AlphaVantageUS(ABCRequests):

    def __init__(
        self,
        session: Optional[Session] = None,
        dt_ref:datetime=DatesBR().sub_working_days(DatesBR().curr_date, 1),
        cls_db:Optional[Session]=None,
        logger:Optional[Logger]=None,
        token:Optional[str]=None,
        list_slugs:Optional[List[str]]=None
    ) -> None:
        self.session = session
        self.dt_ref = dt_ref
        self.cls_db = cls_db
        self.logger = logger
        self.token = token
        self.list_slugs = list_slugs
        super().__init__(
            dict_metadata=YAML_US_ALPHAVANTAGE,
            session=session,
            dt_ref=dt_ref,
            cls_db=cls_db,
            logger=logger,
            token=token,
            list_slugs=list_slugs
        )

    def req_trt_injection(self, resp_req:Response) -> Optional[pd.DataFrame]:
        list_ = list()
        sleep(10)
        json_ = resp_req.json()
        for str_day, dict_ in json_['Time Series (Daily)'].items():
            list_.append({
                'DATE': str_day,
                'TICKER': json_['Meta Data']['2. Symbol'],
                'OPEN': dict_['1. open'],
                'HIGH': dict_['2. high'],
                'LOW': dict_['3. low'],
                'CLOSE': dict_['4. close'],
                'VOLUME': dict_['5. volume'],
            })
        return pd.DataFrame(list_)
