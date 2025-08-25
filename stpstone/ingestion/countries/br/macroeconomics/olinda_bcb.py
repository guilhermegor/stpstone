from datetime import datetime
from logging import Logger
from time import sleep
from typing import List, Optional

import pandas as pd
from requests import Response
from sqlalchemy.orm import Session

from stpstone._config.global_slots import YAML_OLINDA_BCB
from stpstone.ingestion.abc.requests import ABCRequests
from stpstone.utils.cals.cal_abc import DatesBR


class OlindaBCB(ABCRequests):

    def __init__(
        self,
        session: Optional[Session] = None,
        date_start: datetime = DatesBR().sub_working_days(DatesBR().curr_date(), 60),
        date_end: datetime = DatesBR().sub_working_days(DatesBR().curr_date(), 1),
        date_ref: datetime = DatesBR().sub_working_days(DatesBR().curr_date(), 1),
        cls_db: Optional[Session] = None,
        logger: Optional[Logger] = None,
        token: Optional[str] = None,
        list_slugs: Optional[List[str]] = None
    ) -> None:
        super().__init__(
            dict_metadata=YAML_OLINDA_BCB,
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
        self.date_start = date_start
        self.date_end = date_end
        self.date_start_repr = date_start.strftime('%m-%d-%Y')
        self.date_end_repr = date_end.strftime('%m-%d-%Y')

    def req_trt_injection(self, resp_req: Response) -> Optional[pd.DataFrame]:
        return pd.DataFrame(resp_req.json()["value"])
