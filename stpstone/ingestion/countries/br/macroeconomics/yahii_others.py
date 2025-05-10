import pandas as pd
from datetime import datetime
from typing import Optional, List, Any, Dict
from sqlalchemy.orm import Session
from logging import Logger
from requests import Response
from time import sleep
from stpstone._config.global_slots import YAML_YAHII_OHTERS
from stpstone.utils.cals.handling_dates import DatesBR
from stpstone.ingestion.abc.requests import ABCRequests
from stpstone.utils.parsers.html import HtmlHandler
from stpstone.utils.parsers.folders import DirFilesManagement
from stpstone.utils.parsers.dicts import HandlingDicts
from stpstone.utils.parsers.str import StrHandler
from stpstone.utils.loggs.create_logs import CreateLog

pd.set_option("display.max_rows", None)

class YahiiOthersBR(ABCRequests):

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
            dict_metadata=YAML_YAHII_OHTERS,
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

    def td_th_parser(self, list_td: List[Any], list_headers: List[str]) -> List[Dict[str, Any]]:
        for i, td in enumerate(list_td):
            if "DECRETO" not in td and "$" in td:
                list_td[i] = float(td.replace("NCz$ ", "").replace("NCr$ ", "")\
                    .replace("CR$", "").replace("R$", "").replace("Cr$", "").replace("NCz$", "")\
                    .replace("$000", "").replace(".", "")\
                    .replace(",", ".").replace("Cz$ ", "").strip())
            elif "DECRETO" not in td and len(td) >= 8 and "." in td:
                list_td[i] = td.replace(" (Abono)", "").replace(" (URV) – (Real)", "")\
                    .replace(" (Cruzeiro Real)", "").replace(" (Cruzeiro)", "")\
                    .replace(" (Cruzado)", "").replace(" (Cruzeiro Novo)", "")\
                    .replace(" (Cruzado Novo)", "").replace(" (Réis)", "")
        list_ser = HandlingDicts().pair_headers_with_data(
            list_headers,
            list_td
        )
        return list_ser

    def req_trt_injection(self, req_resp: Response) -> Optional[pd.DataFrame]:
        root = HtmlHandler().lxml_parser(req_resp)
        source = self.get_query_params(req_resp.url, "source").lower()
        list_td = [
            x.text.strip() for x in HtmlHandler().lxml_xpath(
                root, YAML_YAHII_OHTERS[source]["xpaths"]["list_td"]
            )
            if x.text is not None
        ]
        list_headers = list(YAML_YAHII_OHTERS[source]["dtypes"].keys())
        # deal with data/headers specificity for the project
        list_ser = self.td_th_parser(list_td, list_headers)
        df_ = pd.DataFrame(list_ser)
        df_ = df_[df_["DATA"] != "REVOGADA"]
        df_["DATA"] = [DatesBR().str_date_to_datetime(d, "DD.MM.YY") for d in df_["DATA"]]
        df_ = df_[(df_["DATA"] >= DatesBR().build_date(2000, 1, 1)) & (df_["DATA"] <= self.dt_ref)]
        return df_