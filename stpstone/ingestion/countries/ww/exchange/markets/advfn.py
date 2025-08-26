from datetime import datetime
from logging import Logger
import re
from time import sleep
from typing import List, Optional

import pandas as pd
from requests import Response
from sqlalchemy.orm import Session

from stpstone._config.global_slots import YAML_WW_ADVFN
from stpstone.ingestion.abc.requests import ABCRequests
from stpstone.utils.calendars.calendar_br import DatesBRAnbima
from stpstone.utils.connections.netops.proxies.managers.free_proxies_manager import YieldFreeProxy
from stpstone.utils.parsers.dicts import HandlingDicts


class ADVFNWW(ABCRequests):

    def __init__(
        self,
        session: Optional[Session] = None,
        date_start:datetime=DatesBRAnbima().sub_working_days(DatesBRAnbima().curr_date(), 5),
        date_end:datetime=DatesBRAnbima().sub_working_days(DatesBRAnbima().curr_date(), 0),
        str_market:str='BOV',
        str_ticker:str='PETR4',
        cls_db:Optional[Session]=None,
        logger:Optional[Logger]=None,
        token:Optional[str]=None,
        list_slugs:Optional[List[str]]=None
    ) -> None:
        super().__init__(
            dict_metadata=YAML_WW_ADVFN,
            session=session,
            cls_db=cls_db,
            logger=logger,
            token=token,
            list_slugs=list_slugs
        )
        self.session = session
        self.date_start = date_start
        self.date_end = date_end
        self.cls_db = cls_db
        self.logger = logger
        self.token = token,
        self.list_slugs = list_slugs
        self.market = str_market
        self.ticker = str_ticker
        self.date_start_unix_ts = DatesBRAnbima().datetime_to_unix_timestamp(date_start)
        self.date_end_unix_ts = DatesBRAnbima().datetime_to_unix_timestamp(date_end)

    def req_trt_injection(self, resp_req:Response) -> Optional[pd.DataFrame]:
        re_pattern = r'\^([^ ]+)'
        json_ = resp_req.json()
        re_match = re.search(re_pattern, json_['result']['symbol'])
        if re_match is not None:
            ticker = re_match.group(1)
        else:
            ticker = json_['result']['symbol']
        list_ser = HandlingDicts().add_key_value_to_dicts(
            json_['result']['rows'],
            key='ticker',
            value=ticker
        )
        return pd.DataFrame(list_ser)
