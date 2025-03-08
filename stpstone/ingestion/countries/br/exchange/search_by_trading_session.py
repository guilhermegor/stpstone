### SEARCH BY TRADING B3 - INGESTION REQUEST ###

# pypi.org libs
import pandas as pd
from datetime import datetime
from typing import Optional, List
from sqlalchemy.orm import Session
from logging import Logger
from requests import Response
from time import sleep
# project modules
from stpstone._config.global_slots import YAML_B3_SEARCH_BY_TRADING_SESSION
from stpstone.utils.cals.handling_dates import DatesBR
from stpstone.utils.connections.netops.session import ReqSession
from stpstone.ingestion.abc.requests import ABCRequests


class SearchByTradingB3(ABCRequests):

    def __init__(
        self,
        session:Optional[ReqSession]=None,
        dt_ref:datetime=DatesBR().sub_working_days(DatesBR().curr_date, 1),
        cls_db:Optional[Session]=None,
        logger:Optional[Logger]=None, 
        token:Optional[str]=None, 
        list_slugs:Optional[List[str]]=None
    ) -> None:
        super().__init__(
            dict_metadata=YAML_B3_SEARCH_BY_TRADING_SESSION,
            session=session,
            dt_ref=dt_ref,
            cls_db=cls_db,
            logger=logger, 
            token=token, 
            list_slugs=list_slugs
        )
        self.session = session
        self.dt_ref = dt_ref
        self.cls_db = cls_db
        self.logger = logger
        self.token = token, 
        self.list_slugs = list_slugs
        self.dt_ref_yymmdd = self.dt_ref.strftime('%y%m%d')
        self.dt_inf_month = DatesBR().dates_inf_sup_month(self.dt_ref)[0]
        self.dt_inf_month_yymmdd = self.dt_inf_month.strftime('%y%m%d')
    
    def req_trt_injection(self, req_resp:Response) -> Optional[pd.DataFrame]:
        return None