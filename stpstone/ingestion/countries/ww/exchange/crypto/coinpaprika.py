import pandas as pd
from datetime import datetime
from typing import Optional
from sqlalchemy.orm import Session
from logging import Logger
from requests import Response
from stpstone._config.global_slots import YAML_WW_CRYPTO_COINPAPRIKA
from stpstone.utils.cals.handling_dates import DatesBR
from stpstone.utils.connections.netops.sessions.proxy_scrape import ReqSession
from stpstone.ingestion.abc.requests import ABCRequests


class CoinPaprika(ABCRequests):

    def __init__(
        self,
        session:Optional[ReqSession]=None,
        dt_ref:datetime=DatesBR().sub_working_days(DatesBR().curr_date, 1),
        cls_db:Optional[Session]=None,
        logger:Optional[Logger]=None
    ) -> None:
        self.session = session
        self.dt_ref = dt_ref
        self.cls_db = cls_db
        self.logger = logger
        super().__init__(
            dict_metadata=YAML_WW_CRYPTO_COINPAPRIKA,
            session=session,
            dt_ref=dt_ref,
            cls_db=cls_db,
            logger=logger
        )

    def req_trt_injection(self, req_resp:Response) -> Optional[pd.DataFrame]:
        return None
