import pandas as pd
from datetime import datetime
from typing import Optional, List, Any, Dict, Tuple
from sqlalchemy.orm import Session
from logging import Logger
from requests import Response
from time import sleep
from bs4 import BeautifulSoup
from math import nan
from stpstone._config.global_slots import YAML_ANBIMA_FORECASTS
from stpstone.utils.cals.handling_dates import DatesBR
from stpstone.ingestion.abc.requests import ABCRequests
from stpstone.utils.parsers.html import HtmlHandler
from stpstone.utils.parsers.folders import DirFilesManagement
from stpstone.utils.parsers.dicts import HandlingDicts
from stpstone.utils.parsers.str import StrHandler
from stpstone.utils.loggs.create_logs import CreateLog


class AnbimaForecasts(ABCRequests):

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
            dict_metadata=YAML_ANBIMA_FORECASTS,
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
        self.token = token
        self.list_slugs = list_slugs

    def td_parser(self, root: BeautifulSoup, source: str) -> Dict[str, List[Any]]:
        list_data = list()
        for i_tb, bs_table in enumerate(root.find_all('table')):
            if source.lower() == 'mp1':
                if (i_tb + 1) % 3 != 1: continue
            elif source.lower() == 'mp2':
                if (i_tb + 1)  % 3 != 2: continue
            elif source.lower() == 'ltm':
                if (i_tb + 1)  % 3 != 0: continue
            else:
                raise ValueError("Invalid source")
            if i_tb // 3 == 0:
                str_pmi = 'IGPM'
            elif i_tb // 3 == 1:
                str_pmi = 'IPCA'
            else:
                raise ValueError("Invalid index")
            for i_tr, bs_tr in enumerate(bs_table.find_all('tr')):
                if i_tr >= 2:
                    list_ = [
                        x.get_text().replace("-", "9999.99") if len(x.get_text()) == 1
                        else x.get_text().replace(",", ".")
                        for x in bs_tr.find_all('td')
                    ]
                    if (source.lower() == "ltm") and (len(list_) == 3):
                        list_.insert(0, nan)
                        list_.insert(len(list_), nan)
                    list_.extend([str_pmi, source.upper()])
                    list_data.extend(list_)
        return list_data

    def table_(self, list_data: List[Any], source: str) -> pd.DataFrame:
        list_cols = list(YAML_ANBIMA_FORECASTS[source]["dtypes"].keys())
        list_ser = HandlingDicts().pair_headers_with_data(list_cols, list_data)
        df_ = pd.DataFrame(list_ser)
        df_["PROJECAO"] = df_["PROJECAO"].astype(float) / 100.0
        if "VALOR_EFETIVO" in list(df_.columns):
            df_["VALOR_EFETIVO"] = df_["VALOR_EFETIVO"].astype(float) / 100.0
        return df_

    def req_trt_injection(self, resp_req: Response) -> Optional[pd.DataFrame]:
        root = HtmlHandler().bs_parser(resp_req)
        source = self.get_query_params(resp_req.url, "source")
        list_data = self.td_parser(root, source)
        return self.table_(list_data, source)

