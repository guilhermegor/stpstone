import pandas as pd
from datetime import datetime
from typing import Optional, List
from sqlalchemy.orm import Session
from logging import Logger
from requests import Response
from time import sleep
from stpstone._config.global_slots import YAML_B3_FINANCIAL_INDICATORS
from stpstone.utils.cals.handling_dates import DatesBR
from stpstone.ingestion.abc.requests import ABCRequests


class B3FinancialIndicators(ABCRequests):

    def __init__(
        self,
        session: Optional[Session] = None,
        dt_ref: datetime = DatesBR().sub_working_days(DatesBR().curr_date, 1),
        cls_db: Optional[Session] = None,
        logger: Optional[Logger] = None,
        token: Optional[str] = None,
        list_slugs: Optional[List[str]] = None
    ) -> None:
        super().__init__(
            dict_metadata=YAML_B3_FINANCIAL_INDICATORS,
            session=session,
            dt_ref=dt_ref,
            cls_db=cls_db,
            logger=logger,
            token=token,
            list_slugs=list_slugs
        )
        self.session = session
        self.dt_ref = dt_ref
        self.cls_db = cls_db
        self.logger = logger
        self.list_slugs = list_slugs

    def req_trt_injection(self, resp_req: Response) -> Optional[pd.DataFrame]:
        list_ser = list()
        for dict_ in resp_req.json():
            list_ser.append({
                "SECURITY_ID": dict_["securityIdentificationCode"], 
                "DESCRIPTION": dict_["description"],
                "GROUP_DESCRIPTION": dict_["groupDescription"], 
                "VALUE": float(dict_["value"].replace(",", ".")) if dict_["value"] is not None \
                    and len(dict_["value"]) > 0 else 0.0,
                "RATE": float(dict_["rate"].replace(",", ".")) if dict_["rate"] is not None \
                    and len(dict_["rate"]) > 0 else 0.0,
                "LAST_UPDATE": dict_["lastUpdate"]
            })
        return pd.DataFrame(list_ser)
