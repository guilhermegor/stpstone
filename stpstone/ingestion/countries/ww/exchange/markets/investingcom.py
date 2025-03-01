### INVESTING.COM INGESTION REQUEST ###

# pypi.org libs
import pandas as pd
from datetime import datetime
from typing import Optional, List
from sqlalchemy.orm import Session
from logging import Logger
from requests import Response
from time import sleep
# project modules
from stpstone._config.global_slots import YAML_WW_INVESTINGCOM
from stpstone.utils.cals.handling_dates import DatesBR
from stpstone.utils.connections.netops.session import ReqSession
from stpstone.ingestion.abc.requests import ABCRequests


class InvestingCom(ABCRequests):

    def __init__(
        self,
        session:Optional[ReqSession]=None,
        dt_inf:datetime=DatesBR().sub_working_days(DatesBR().curr_date, 5),
        dt_sup:datetime=DatesBR().sub_working_days(DatesBR().curr_date, 0), 
        str_ticker:str='PETR4',
        cls_db:Optional[Session]=None,
        logger:Optional[Logger]=None, 
        token:Optional[str]=None, 
        list_slugs:Optional[List[str]]=None
    ) -> None:
        super().__init__(
            dict_metadata=YAML_WW_INVESTINGCOM,
            session=session,
            cls_db=cls_db,
            logger=logger, 
            token=token, 
            list_slugs=list_slugs
        )
        self.session = session
        self.dt_inf = dt_inf
        self.dt_sup = dt_sup
        self.cls_db = cls_db
        self.logger = logger
        self.token = token, 
        self.list_slugs = list_slugs
        self.ticker = str_ticker
        self.dt_inf_unix_ts = DatesBR().datetime_to_timestamp(dt_inf)
        self.dt_sup_unix_ts = DatesBR().datetime_to_timestamp(dt_sup)
        self.ticker_id = self.source('ticker_id', bl_fetch=True)['ticker_id'].tolist()[0]
    
    def req_trt_injection(self, req_resp:Response) -> Optional[pd.DataFrame]:
        return None
