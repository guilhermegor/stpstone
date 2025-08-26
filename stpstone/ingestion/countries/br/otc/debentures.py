from datetime import datetime
from logging import Logger
from typing import Optional

import pandas as pd
from requests import Response
from sqlalchemy.orm import Session

from stpstone._config.global_slots import YAML_DEBENTURES
from stpstone.ingestion.abc.requests import ABCRequests
from stpstone.utils.calendars.calendar_abc import DatesBR
from stpstone.utils.connections.netops.proxies.managers.free_proxies_manager import YieldFreeProxy


class DebenturesComBR(ABCRequests):
    """
    Debentures MTM ingestion
    Metadata:
        - https://www.debentures.com.br/exploreosnd/exploreosnd.asp
    Special thanks to Rodrigo Prado (https://github.com/royopa) for helping to develop this class
    """

    def __init__(
        self,
        session: Optional[Session] = None,
        date_start:datetime=DatesBR().sub_working_days(DatesBR().curr_date(), 10),
        date_end:datetime=DatesBR().sub_working_days(DatesBR().curr_date(), 1),
        cls_db:Optional[Session]=None,
        logger:Optional[Logger]=None
    ) -> None:
        self.session = session
        self.date_start = date_start
        self.date_end = date_end
        self.cls_db = cls_db
        self.logger = logger
        self.date_ref = date_end
        self.date_start_yyyymmdd = date_start.strftime('%Y%m%d')
        self.date_end_yyyymmdd = date_end.strftime('%Y%m%d')
        self.date_start_ddmmyyyy = date_start.strftime('%d/%m/%Y')
        self.date_end_ddmmyyyy = date_end.strftime('%d/%m/%Y')
        super().__init__(
            dict_metadata=YAML_DEBENTURES,
            session=session,
            date_ref=date_end,
            cls_db=cls_db,
            logger=logger
        )

    def req_trt_injection(self, resp_req:Response) -> Optional[pd.DataFrame]:
        return None
