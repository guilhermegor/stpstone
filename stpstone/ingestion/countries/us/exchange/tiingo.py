from datetime import datetime
from logging import Logger
from time import sleep
from typing import List, Optional

import pandas as pd
from requests import Response
from sqlalchemy.orm import Session

from stpstone._config.global_slots import YAML_US_TIINGO
from stpstone.ingestion.abc.requests import ABCRequests
from stpstone.utils.calendars.calendar_br import DatesBRAnbima
from stpstone.utils.connections.netops.proxies.managers.free_proxies_manager import YieldFreeProxy


class TiingoUS(ABCRequests):

    def __init__(
        self,
        session: Optional[Session] = None,
        date_start:datetime=DatesBRAnbima().sub_working_days(DatesBRAnbima().curr_date(), 52),
        date_end:datetime=DatesBRAnbima().sub_working_days(DatesBRAnbima().curr_date(), 1),
        cls_db:Optional[Session]=None,
        logger:Optional[Logger]=None,
        token:Optional[str]=None,
        list_slugs:Optional[List[str]]=None
    ) -> None:
        self.session = session
        self.date_start = date_start
        self.date_end = date_end
        self.date_start_yyyymmdd = date_start.strftime('%Y-%m-%d')
        self.date_end_yyyymmdd = date_end.strftime('%Y-%m-%d')
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
