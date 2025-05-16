import pandas as pd
from datetime import datetime
from typing import Optional
from sqlalchemy.orm import Session
from logging import Logger
from requests import Response
from stpstone._config.global_slots import YAML_DEBENTURES
from stpstone.utils.cals.handling_dates import DatesBR
from stpstone.utils.connections.netops.proxies.managers.free import YieldFreeProxy
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
        session: Optional[Session] = None,
        dt_start:datetime=DatesBR().sub_working_days(DatesBR().curr_date, 10),
        dt_end:datetime=DatesBR().sub_working_days(DatesBR().curr_date, 1),
        cls_db:Optional[Session]=None,
        logger:Optional[Logger]=None
    ) -> None:
        self.session = session
        self.dt_start = dt_start
        self.dt_end = dt_end
        self.cls_db = cls_db
        self.logger = logger
        self.dt_ref = dt_end
        self.dt_inf_yyyymmdd = dt_start.strftime('%Y%m%d')
        self.dt_sup_yyyymmdd = dt_end.strftime('%Y%m%d')
        self.dt_inf_ddmmyyyy = dt_start.strftime('%d/%m/%Y')
        self.dt_sup_ddmmyyyy = dt_end.strftime('%d/%m/%Y')
        super().__init__(
            dict_metadata=YAML_DEBENTURES,
            session=session,
            dt_ref=dt_end,
            cls_db=cls_db,
            logger=logger
        )

    def req_trt_injection(self, resp_req:Response) -> Optional[pd.DataFrame]:
        return None
