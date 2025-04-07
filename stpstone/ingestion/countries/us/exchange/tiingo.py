import pandas as pd
from datetime import datetime
from typing import Optional, List
from sqlalchemy.orm import Session
from logging import Logger
from requests import Response
from time import sleep
from stpstone._config.global_slots import YAML_US_TIINGO
from stpstone.utils.cals.handling_dates import DatesBR
from stpstone.utils.connections.netops.proxies.managers.free import YieldFreeProxy
from stpstone.ingestion.abc.requests import ABCRequests


class TiingoUS(ABCRequests):

    def __init__(
        self,
        session: Optional[Session] = None,
        dt_inf:datetime=DatesBR().sub_working_days(DatesBR().curr_date, 52),
        dt_sup:datetime=DatesBR().sub_working_days(DatesBR().curr_date, 1),
        cls_db:Optional[Session]=None,
        logger:Optional[Logger]=None,
        token:Optional[str]=None,
        list_slugs:Optional[List[str]]=None
    ) -> None:
        self.session = session
        self.dt_inf = dt_inf
        self.dt_sup = dt_sup
        self.dt_inf_yyyymmdd = dt_inf.strftime('%Y-%m-%d')
        self.dt_sup_yyyymmdd = dt_sup.strftime('%Y-%m-%d')
        self.cls_db = cls_db
        self.logger = logger
        self.token = token,
        self.list_slugs = list_slugs
        super().__init__(
            dict_metadata=YAML_US_TIINGO,
            session=session,
            cls_db=cls_db,
            logger=logger,
            token=token,
            list_slugs=list_slugs
        )

    def req_trt_injection(self, req_resp:Response) -> Optional[pd.DataFrame]:
        sleep(10)
        return None
