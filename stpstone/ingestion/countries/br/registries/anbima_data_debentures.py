from datetime import datetime
from logging import Logger
from typing import List, Optional

import pandas as pd
from requests import Response, Session as RequestsSession
from sqlalchemy.orm import Session

from stpstone._config.global_slots import YAML_ANBIMA_DATA_DEBENTURES
from stpstone.ingestion.abc.requests import ABCRequests
from stpstone.utils.cals.handling_dates import DatesBR


class AnbimaDataDebentures(ABCRequests):

    def __init__(
        self,
        session: Optional[RequestsSession] = None,
        dt_ref: datetime = DatesBR().sub_working_days(DatesBR().curr_date(), 1),
        cls_db: Optional[Session] = None,
        logger: Optional[Logger] = None,
        token: Optional[str] = None,
        list_slugs: Optional[List[str]] = None,
        str_user_agent: Optional[str] = None,
        int_wait_load_seconds: int = 10,
        bool_headless: bool = False,
        bool_incognito: bool = False
    ) -> None:
        super().__init__(
            dict_metadata=YAML_ANBIMA_DATA_DEBENTURES,
            session=session,
            dt_ref=dt_ref,
            cls_db=cls_db,
            logger=logger,
            token=token,
            list_slugs=list_slugs,
            str_user_agent=str_user_agent,
            int_wait_load_seconds=int_wait_load_seconds,
            bool_headlesbool_headless,
            bool_incognitbool_incognito
        )
        self.session = session
        self.dt_ref = dt_ref
        self.cls_db = cls_db
        self.logger = logger
        self.list_slugs = list_slugs
        self.str_user_agent = str_user_agent
        self.int_wait_load_seconds = int_wait_load_seconds
        self.bool_headless = bool_headless
        self.bool_incognito = bool_incognito

    def req_trt_injection(self, resp_req: Response) -> Optional[pd.DataFrame]:
        return None
