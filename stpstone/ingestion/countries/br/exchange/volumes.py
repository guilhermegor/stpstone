from datetime import datetime
from logging import Logger
from typing import Optional

import pandas as pd
from requests import Response
from sqlalchemy.orm import Dict, Session

from stpstone._config.global_slots import YAML_B3_UP2DATA_VOLUMES_TRD
from stpstone.ingestion.abc.requests import ABCRequests
from stpstone.utils.calendars.calendar_abc import DatesBR
from stpstone.utils.connections.netops.proxies.managers.free_proxies_manager import YieldFreeProxy


class ExchVolumesTrdBR(ABCRequests):

    def __init__(
        self,
        session: Optional[Session] = None,
        date_ref:datetime=DatesBR().sub_working_days(DatesBR().curr_date(), 1),
        dict_headers:Optional[Dict[str, str]]=None,
        dict_payload:Optional[Dict[str, str]]=None,
        cls_db:Optional[Session]=None,
        logger:Optional[Logger]=None
    ) -> None:
        self.token = self.access_token
        super().__init__(
            dict_metadata=YAML_B3_UP2DATA_VOLUMES_TRD,
            session=session,
            date_ref=date_ref,
            dict_headers=dict_headers,
            dict_payload=dict_payload,
            cls_db=cls_db,
            logger=logger
        )

    def trt_injection(self, resp_req:Response) -> Optional[pd.DataFrame]:
        return None
