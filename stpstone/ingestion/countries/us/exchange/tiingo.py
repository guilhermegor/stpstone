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
        dt_start:datetime=DatesBR().sub_working_days(DatesBR().curr_date, 52),
        dt_end:datetime=DatesBR().sub_working_days(DatesBR().curr_date, 1),
        cls_db:Optional[Session]=None,
        logger:Optional[Logger]=None,
        token:Optional[str]=None,
        list_slugs:Optional[List[str]]=None
    ) -> None:
        self.session = session
        self.dt_start = dt_start
        self.dt_end = dt_end
        self.dt_inf_yyyymmdd = dt_start.strftime('%Y-%m-%d')
        self.dt_sup_yyyymmdd = dt_end.strftime('%Y-%m-%d')
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

    def req_trt_injection(self, resp_req:Response) -> Optional[pd.DataFrame]:
        sleep(10)
        return None
