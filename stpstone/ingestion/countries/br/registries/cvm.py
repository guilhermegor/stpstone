import pandas as pd
from requests import Response
from sqlalchemy.orm import Session
from datetime import datetime
from logging import Logger
from time import sleep
from typing import List, Optional
from stpstone._config.global_slots import YAML_BR_CVM_REGISTRIES
from stpstone.ingestion.abc.requests import ABCRequests
from stpstone.utils.cals.handling_dates import DatesBR
from stpstone.utils.connections.netops.proxies.managers.free import YieldFreeProxy


class CVMRegistries(ABCRequests):

    def __init__(
        self,
        session: Optional[Session] = None,
        dt_ref: datetime = DatesBR().sub_working_days(DatesBR().curr_date, 1),
        cls_db: Optional[Session] = None,
        logger: Optional[Logger] = None,
        token: Optional[str] = None,
        list_slugs: Optional[List[str]] = None,
    ) -> None:
        super().__init__(
            dict_metadata=YAML_BR_CVM_REGISTRIES,
            session=session,
            dt_ref=dt_ref,
            cls_db=cls_db,
            logger=logger,
            token=token,
            list_slugs=list_slugs,
        )
        self.session = session
        self.dt_ref = dt_ref
        self.cls_db = cls_db
        self.logger = logger
        self.token = token
        self.list_slugs = list_slugs
        self.month_ref = self.dt_ref.strftime("%Y%m")
        self.month_ref = DatesBR().add_months(self.dt_ref, -1).strftime("%Y%m")
        self.year_ref = DatesBR().year_number(self.dt_ref)

    def req_trt_injection(self, resp_req: Response) -> Optional[pd.DataFrame]:
        return None
