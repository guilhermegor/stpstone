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
        dt_inf:datetime=DatesBR().sub_working_days(DatesBR().curr_date, 10),
        dt_sup:datetime=DatesBR().sub_working_days(DatesBR().curr_date, 1),
        cls_db:Optional[Session]=None,
        logger:Optional[Logger]=None
    ) -> None:
        self.session = session
        self.dt_inf = dt_inf
        self.dt_sup = dt_sup
        self.cls_db = cls_db
        self.logger = logger
        self.dt_ref = dt_sup
        self.dt_inf_yyyymmdd = dt_inf.strftime('%Y%m%d')
        self.dt_sup_yyyymmdd = dt_sup.strftime('%Y%m%d')
        self.dt_inf_ddmmyyyy = dt_inf.strftime('%d/%m/%Y')
        self.dt_sup_ddmmyyyy = dt_sup.strftime('%d/%m/%Y')
        super().__init__(
            dict_metadata=YAML_DEBENTURES,
            session=session,
            dt_ref=dt_sup,
            cls_db=cls_db,
            logger=logger
        )

    def req_trt_injection(self, req_resp:Response) -> Optional[pd.DataFrame]:
        return None
