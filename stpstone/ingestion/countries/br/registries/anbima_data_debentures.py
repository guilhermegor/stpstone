import pandas as pd
from datetime import datetime
from typing import Optional, List
from sqlalchemy.orm import Session
from logging import Logger
from requests import Response
from requests import Session as RequestsSession
from stpstone._config.global_slots import YAML_ANBIMA_DATA_DEBENTURES
from stpstone.utils.cals.handling_dates import DatesBR
from stpstone.ingestion.abc.requests import ABCRequests


class AnbimaDataDebentures(ABCRequests):

    def __init__(
        self,
        session: Optional[RequestsSession] = None,
        dt_ref: datetime = DatesBR().sub_working_days(DatesBR().curr_date, 1),
        cls_db: Optional[Session] = None,
        logger: Optional[Logger] = None,
        token: Optional[str] = None,
        list_slugs: Optional[List[str]] = None,
        str_user_agent: Optional[str] = None,
        int_wait_load_seconds: int = 10,
        bl_headless: bool = False,
        bl_incognito: bool = False
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
            bl_headless=bl_headless,
            bl_incognito=bl_incognito
        )
        self.session = session
        self.dt_ref = dt_ref
        self.cls_db = cls_db
        self.logger = logger
        self.list_slugs = list_slugs
        self.str_user_agent = str_user_agent
        self.int_wait_load_seconds = int_wait_load_seconds
        self.bl_headless = bl_headless
        self.bl_incognito = bl_incognito

    def req_trt_injection(self, resp_req: Response) -> Optional[pd.DataFrame]:
        return None
