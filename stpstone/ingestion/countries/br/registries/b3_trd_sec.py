from datetime import datetime
from logging import Logger
from time import sleep
from typing import List, Optional

import pandas as pd
from requests import Response
from sqlalchemy.orm import Session

from stpstone._config.global_slots import YAML_B3_TRD_SEC
from stpstone.ingestion.abc.requests import ABCRequests
from stpstone.utils.cals.cal_abc import DatesBR
from stpstone.utils.connections.netops.proxies.managers.free_proxies_manager import YieldFreeProxy


class B3TrdSec(ABCRequests):

    def __init__(
        self,
        session: Optional[Session] = None,
        date_ref:datetime=DatesBR().sub_working_days(DatesBR().curr_date(), 1),
        cls_db:Optional[Session]=None,
        logger:Optional[Logger]=None,
        token:Optional[str]=None,
        list_slugs:Optional[List[str]]=None
    ) -> None:
        super().__init__(
            dict_metadata=YAML_B3_TRD_SEC,
            session=session,
            date_ref=date_ref,
            cls_db=cls_db,
            logger=logger,
            token=token,
            list_slugs=list_slugs
        )
        self.session = session
        self.date_ref = date_ref
        self.cls_db = cls_db
        self.logger = logger
        self.list_slugs = list_slugs

    def req_trt_injection(self, resp_req:Response) -> Optional[pd.DataFrame]:
        return None
