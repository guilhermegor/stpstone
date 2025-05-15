import pandas as pd
from datetime import datetime
from typing import Optional, List
from sqlalchemy.orm import Session
from logging import Logger
from requests import Response
from time import sleep
from stpstone._config.global_slots import YAML_SGS_BCB
from stpstone.utils.cals.handling_dates import DatesBR
from stpstone.ingestion.abc.requests import ABCRequests


class SGSBCB(ABCRequests):

    def __init__(
        self,
        session: Optional[Session] = None,
        dt_start: datetime = DatesBR().sub_working_days(DatesBR().curr_date, 60),
        dt_end: datetime = DatesBR().sub_working_days(DatesBR().curr_date, 1),
        dt_ref: datetime = DatesBR().sub_working_days(DatesBR().curr_date, 1),
        cls_db: Optional[Session] = None,
        logger: Optional[Logger] = None,
        token: Optional[str] = None,
        list_slugs: Optional[List[str]] = None
    ) -> None:
        super().__init__(
            dict_metadata=YAML_SGS_BCB,
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
        self.list_slugs = list_slugs
        self.dt_start = dt_start
        self.dt_end = dt_end
        self.dt_start_repr = dt_start.strftime('%d/%m/%Y')
        self.dt_end_repr = dt_end.strftime('%d/%m/%Y')

    def req_trt_injection(self, resp_req: Response) -> Optional[pd.DataFrame]:
        json_ = resp_req.json()
        int_url_slug = int(resp_req.url.split("/bcdata.sgs.")[-1].split("/")[0])
        df_ = pd.DataFrame(json_)
        df_.columns = [x.upper() for x in df_.columns]
        df_["NOME"] = "IGPM" if int_url_slug == 189 else \
            "SELIC_DAILY" if int_url_slug == 11 else \
            "SELIC_TARGET" if int_url_slug == 432 else None
        return df_
