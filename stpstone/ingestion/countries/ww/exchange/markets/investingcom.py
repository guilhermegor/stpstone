from datetime import datetime
from logging import Logger
from time import sleep
from typing import List, Optional

import pandas as pd
from requests import Response
from sqlalchemy.orm import Session

from stpstone._config.global_slots import YAML_WW_INVESTINGCOM
from stpstone.ingestion.abc.requests import ABCRequests
from stpstone.utils.calendars.calendar_abc import DatesBR
from stpstone.utils.connections.netops.proxies.managers.free_proxies_manager import YieldFreeProxy


class InvestingCom(ABCRequests):

    def __init__(
        self,
        session: Optional[Session] = None,
        date_start:datetime=DatesBR().sub_working_days(DatesBR().curr_date(), 5),
        date_end:datetime=DatesBR().sub_working_days(DatesBR().curr_date(), 0),
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
        self.date_start = date_start
        self.date_end = date_end
        self.cls_db = cls_db
        self.logger = logger
        self.token = token,
        self.list_slugs = list_slugs
        self.ticker = str_ticker
        self.date_start_unix_ts = DatesBR().datetime_to_unix_timestamp(date_start)
        self.date_end_unix_ts = DatesBR().datetime_to_unix_timestamp(date_end)
        self.ticker_id = self.source('ticker_id', bool_fetch=True)['ticker_id'].tolist()[0]

    def req_trt_injection(self, resp_req:Response) -> Optional[pd.DataFrame]:
        return None
