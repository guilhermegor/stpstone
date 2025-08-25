from datetime import datetime
from logging import Logger
from time import sleep
from typing import List, Optional

import pandas as pd
from requests import Response
from sqlalchemy.orm import Session

from stpstone._config.global_slots import YAML_SGS_BCB
from stpstone.ingestion.abc.requests import ABCRequests
from stpstone.utils.cals.cal_abc import DatesBR


class SGSBCB(ABCRequests):

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
            dict_metadata=YAML_SGS_BCB,
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
        self.date_start_repr = date_start.strftime('%d/%m/%Y')
        self.date_end_repr = date_end.strftime('%d/%m/%Y')

    def req_trt_injection(self, resp_req: Response) -> Optional[pd.DataFrame]:
        json_ = resp_req.json()
        int_url_slug = int(resp_req.url.split("/bcdata.sgs.")[-1].split("/")[0])
        df_ = pd.DataFrame(json_)
        df_.columns = [x.upper() for x in df_.columns]
        df_["NOME"] = "IGPM" if int_url_slug == 189 else \
            "SELIC_DAILY" if int_url_slug == 11 else \
            "SELIC_TARGET" if int_url_slug == 432 else None
        return df_
