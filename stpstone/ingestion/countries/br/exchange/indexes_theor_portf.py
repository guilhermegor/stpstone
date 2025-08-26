from datetime import datetime
from logging import Logger
from time import sleep
from typing import List, Optional

import pandas as pd
from requests import Response
from sqlalchemy.orm import Session

from stpstone._config.global_slots import YAML_B3_INDEXES_THEOR_PORTF
from stpstone.ingestion.abc.requests import ABCRequests
from stpstone.utils.calendars.calendar_br import DatesBRAnbima
from stpstone.utils.connections.netops.proxies.managers.free_proxies_manager import YieldFreeProxy
from stpstone.utils.parsers.dicts import HandlingDicts


class IndexesTheorPortfB3(ABCRequests):

    def __init__(
        self,
        session: Optional[Session] = None,
        date_ref:datetime=DatesBRAnbima().sub_working_days(DatesBRAnbima().curr_date(), 1),
        cls_db:Optional[Session]=None,
        logger:Optional[Logger]=None,
        token:Optional[str]=None,
        list_slugs:Optional[List[str]]=None
    ) -> None:
        super().__init__(
            dict_metadata=YAML_B3_INDEXES_THEOR_PORTF,
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
        self.token = token,
        self.list_slugs = list_slugs

    def req_trt_injection(self, resp_req:Response) -> Optional[pd.DataFrame]:
        json_  = resp_req.json()
        df_ = pd.DataFrame(
            HandlingDicts().add_key_value_to_dicts(
                json_['results'],
                key='date',
                value=json_['header']['date']
            )
        )
        for col_ in df_.columns:
            df_[col_] = [x if x is None else x.replace('.', '') for x in df_[col_]]
            df_[col_] = [x if x is None else x.replace(",", ".") for x in df_[col_]]
        return df_
