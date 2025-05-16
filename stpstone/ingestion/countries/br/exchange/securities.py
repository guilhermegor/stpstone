import pandas as pd
from datetime import datetime
from typing import Optional, Dict, Any, List
from sqlalchemy.orm import Session
from logging import Logger
from requests import Response
from stpstone._config.global_slots import YAML_B3_UP2DATA_REGISTRIES
from stpstone.utils.cals.handling_dates import DatesBR
from stpstone.utils.connections.netops.proxies.managers.free import YieldFreeProxy
from stpstone.ingestion.abc.requests import ABCRequests
from stpstone.utils.parsers.str import StrHandler
from stpstone.utils.parsers.lists import ListHandler


class ExchRegBR(ABCRequests):

    def __init__(
        self,
        dt_ref:datetime=DatesBR().sub_working_days(DatesBR().curr_date, 1),
        session: Optional[Session] = None,
        cls_db:Optional[Session]=None,
        logger:Optional[Logger]=None
    ) -> None:
        self.token = self.access_token
        super().__init__(
            dict_metadata=YAML_B3_UP2DATA_REGISTRIES,
            session=session,
            dt_ref=dt_ref,
            dict_headers=None,
            dict_payload=None,
            cls_db=cls_db,
            logger=logger
        )

    def req_trt_injection(self, resp_req:Response) -> Optional[pd.DataFrame]:
        # setting variables
        list_ser = list()
        # dealing with response
        str_req_resp = resp_req.text
        # cleaning response
        list_req_resp = str_req_resp.split('\n')
        list_headers = list_req_resp[1].split(';')
        list_data = list_req_resp[2:]
        for row in list_data:
            if len(row) == 0: continue
            list_row = row.split(';')
            list_ser.append(dict(zip(list_headers, list_row)))
        # dataframe from serialized list
        df_ = pd.DataFrame(list_ser)
        # changing columns case, from cammel to snake
        df_.columns = [StrHandler().convert_case(x, 'pascal', 'constant') for x in df_.columns]
        # return dataframe
        return df_
