from datetime import datetime
from logging import Logger
from time import sleep
from typing import List, Optional

import pandas as pd
from requests import Response
from sqlalchemy.orm import Session

from stpstone._config.global_slots import YAML_INVESTINGCOM_BR
from stpstone.ingestion.abc.requests import ABCRequests
from stpstone.utils.calendars.calendar_br import DatesBRAnbima


class InvestingComBR(ABCRequests):

    def __init__(
        self,
        session: Optional[Session] = None,
        date_ref: datetime = DatesBRAnbima().sub_working_days(DatesBRAnbima().curr_date(), 1),
        cls_db: Optional[Session] = None,
        logger: Optional[Logger] = None,
        token: Optional[str] = None,
        list_slugs: Optional[List[str]] = None
    ) -> None:
        super().__init__(
            dict_metadata=YAML_INVESTINGCOM_BR,
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

    def req_trt_injection(self, resp_req: Response) -> Optional[pd.DataFrame]:
        json_ = resp_req.json()["attr"]
        int_convert_miliseconds_seconds = 1000
        json_ = [
            {
                "DATETIME": DatesBRAnbima().unix_timestamp_to_datetime(
                    int(int(dict_['timestamp']) / int_convert_miliseconds_seconds)),
                "ACTUAL_STATE": str(dict_['actual_state']),
                "ACTUAL": float(dict_['actual']),
                "FORECAST": dict_['forecast'],
                "REVISED": dict_['revised'],
            }
            for dict_ in json_
        ]
        return pd.DataFrame(json_)
