### DEBENTURES MTM INGESTION ###

# pypi.org libs
import pandas as pd
from datetime import datetime
from typing import Optional
from sqlalchemy.orm import Session
from logging import Logger
from requests import Response
# project modules
from stpstone._config.global_slots import YAML_DEBENTURES
from stpstone.utils.cals.handling_dates import DatesBR
from stpstone.utils.connections.netops.session import ReqSession
from stpstone.ingestion.abc.requests import ABCRequests


class DebenturesComBR(ABCRequests):
    """
    Debentures MTM ingestion
    Metadata:
        - https://www.debentures.com.br/exploreosnd/exploreosnd.asp
    Special thanks to Rodrigo Prado (https://github.com/royopa) for helping to develop this class
    """

    def __init__(
        self,
        session:Optional[ReqSession]=None,
        dt_beg:datetime=DatesBR().sub_working_days(DatesBR().curr_date, 10),
        dt_end:datetime=DatesBR().sub_working_days(DatesBR().curr_date, 1),
        cls_db:Optional[Session]=None,
        logger:Optional[Logger]=None
    ) -> None:
        self.session = session
        self.dt_beg = dt_beg
        self.dt_end = dt_end
        self.cls_db = cls_db
        self.logger = logger
        self.dt_ref = dt_end
        self.dt_beg_yyyymmdd = dt_beg.strftime('%Y%m%d')
        self.dt_end_yyyymmdd = dt_end.strftime('%Y%m%d')
        self.dt_beg_ddmmyyyy = dt_beg.strftime('%d/%m/%Y')
        self.dt_end_ddmmyyyy = dt_end.strftime('%d/%m/%Y')
        super().__init__(
            dict_metadata=YAML_DEBENTURES,
            session=session,
            dt_ref=dt_end,
            cls_db=cls_db,
            logger=logger
        )
    
    def req_trt_injection(self, req_resp:Response) -> Optional[pd.DataFrame]:
        return None
