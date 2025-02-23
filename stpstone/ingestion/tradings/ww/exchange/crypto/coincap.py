### CRYPTO - COINCAP INGESTION REQUEST ###

# pypi.org libs
import pandas as pd
from datetime import datetime
from typing import Optional
from sqlalchemy.orm import Session
from logging import Logger
from requests import Response
# project modules
from stpstone._config.global_slots import YAML_WW_CRYPTO_COINCAP
from stpstone.utils.cals.handling_dates import DatesBR
from stpstone.utils.connections.netops.session import ReqSession
from stpstone.ingestion.abc.requests import ABCRequests


class CoinCap(ABCRequests):

    def __init__(
        self,
        session:Optional[ReqSession]=None,
        dt_ref:datetime=DatesBR().sub_working_days(DatesBR().curr_date, 1),
        cls_db:Optional[Session]=None,
        logger:Optional[Logger]=None, 
        token:Optional[str]=None
    ) -> None:
        self.session = session
        self.dt_ref = dt_ref
        self.cls_db = cls_db
        self.logger = logger
        self.token = token
        super().__init__(
            dict_metadata=YAML_WW_CRYPTO_COINCAP,
            session=session,
            dt_ref=dt_ref,
            cls_db=cls_db,
            logger=logger, 
            token=token
        )
    
    def req_trt_injection(self, req_resp:Response) -> Optional[pd.DataFrame]:
        json_ = req_resp.json()
        return pd.DataFrame([json_['data']])
