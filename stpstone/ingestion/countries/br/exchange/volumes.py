import pandas as pd
from datetime import datetime
from typing import Optional
from sqlalchemy.orm import Session, Dict
from logging import Logger
from requests import Response
from stpstone._config.global_slots import YAML_B3_UP2DATA_VOLUMES_TRD
from stpstone.utils.cals.handling_dates import DatesBR
from stpstone.utils.connections.netops.sessions.proxy_scrape import ReqSession
from stpstone.ingestion.abc.requests import ABCRequests


class ExchVolumesTrdBR(ABCRequests):

    def __init__(
        self,
        session:Optional[ReqSession]=None,
        dt_ref:datetime=DatesBR().sub_working_days(DatesBR().curr_date, 1),
        dict_headers:Optional[Dict[str, str]]=None,
        dict_payload:Optional[Dict[str, str]]=None,
        cls_db:Optional[Session]=None,
        logger:Optional[Logger]=None
    ) -> None:
        self.token = self.access_token
        super().__init__(
            dict_metadata=YAML_B3_UP2DATA_VOLUMES_TRD,
            session=session,
            dt_ref=dt_ref,
            dict_headers=dict_headers,
            dict_payload=dict_payload,
            cls_db=cls_db,
            logger=logger
        )

    def trt_injection(self, req_resp:Response) -> Optional[pd.DataFrame]:
        return None
