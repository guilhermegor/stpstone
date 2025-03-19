import pandas as pd
from datetime import datetime
from typing import Optional, List, Any, Tuple
from sqlalchemy.orm import Session
from logging import Logger
from requests import Response
from time import sleep
from bs4 import BeautifulSoup
from stpstone._config.global_slots import YAML_WW_GR
from stpstone.utils.cals.handling_dates import DatesBR
from stpstone.utils.connections.netops.session import ReqSession
from stpstone.ingestion.abc.requests import ABCRequests
from stpstone.utils.parsers.html import HtmlHndler
from stpstone.utils.parsers.folders import DirFilesManagement
from stpstone.utils.parsers.dicts import HandlingDicts
from stpstone.utils.loggs.create_logs import CreateLog
from stpstone.utils.parsers.str import StrHandler
from stpstone.utils.parsers.numbers import NumHandler
from stpstone.utils.parsers.html import HtmlHndler


class GlobalRates(ABCRequests):

    def __init__(
        self,
        session: Optional[ReqSession] = None,
        dt_ref: datetime = DatesBR().sub_working_days(DatesBR().curr_date, 1),
        cls_db: Optional[Session] = None,
        logger: Optional[Logger] = None,
        token: Optional[str] = None,
        list_slugs: Optional[List[str]] = None
    ) -> None:
        super().__init__(
            dict_metadata=YAML_WW_GR,
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

    def repeat_list_n_times(self, list_: List[Any], n: int) -> List[Any]:
        return list_ * n

    def repeat_elements_in_sequence(self, list_: List[Any], n: int) -> List[Any]:
        return [element for element in list_ for _ in range(n)]

    def get_td_th_raw(self, bs_table: BeautifulSoup) -> pd.DataFrame:
        list_outlooks_raw = list()
        list_th_raw = [
            th.get_text().replace("\n", "").replace("/", "_").strip().upper()
            for th in bs_table.find_all("th")
        ]
        list_td_raw = [
            float(td.get_text().replace("\n", "").replace("%", "").strip()) / 100.0
            if NumHandler().is_numeric(td.get_text().replace("\n", "").replace("%", "").strip())
            else td.get_text().replace("\n", "").replace("%", "").strip()
            for td in bs_table.find_all("td") if len(td.get_text().replace("\n", "")\
                                                     .replace("%", "").replace("-", "0").strip()) > 0
        ]
        try:
            for div in bs_table.find_all("div", class_="table-normal text-end"):
                try:
                    str_outlook = StrHandler().replace_all(
                        div.find("i")["class"][1], {
                            "fa-circle-down": "DOWNWARD",
                            "fa-circle-up": "UPWARD"
                        }
                    )
                    list_outlooks_raw.append(str_outlook)
                except (AttributeError, TypeError):
                    continue
            int_col_dir = list_th_raw.index("DIRECTION")
            int_rng_upper = len(list_td_raw) // (len(list_th_raw) - 1)
            for i in range(int_rng_upper):
                list_td_raw.insert(3 + (int_col_dir + 3) * i, list_outlooks_raw[i])
        except ValueError:
            pass
        return list_th_raw, list_td_raw, list_outlooks_raw

    def td_th_parser(self, bs_table: BeautifulSoup, str_source: str) -> pd.DataFrame:
        list_th_raw, list_td_raw, list_outlooks_raw = self.get_td_th_raw(bs_table)
        list_th_dts = [x for x in list_th_raw if len(x) > 0]
        list_td_rts_names = [x for x in list_td_raw if isinstance(x, str)]
        list_td_rts_values = [x for x in list_td_raw if NumHandler().is_number(x) == True]
        list_rts_names = self.repeat_elements_in_sequence(list_td_rts_names, len(list_th_dts))
        list_rt_dts = self.repeat_list_n_times(list_th_dts, len(list_td_rts_names))
        if str_source in ["ester"]:
            list_rt_dts = list_td_rts_names.copy()
            list_rts_names = [f"{str_source}".upper()] * len(list_td_rts_values)
        return pd.DataFrame({
            "DATE": list_rt_dts,
            "RATE_NAME": list_rts_names,
            "RATE_VALUE": list_td_rts_values
        })

    def req_trt_injection(self, req_resp: Response) -> Optional[pd.DataFrame]:
        bs_html = HtmlHndler().bs_parser(req_resp)
        str_source = StrHandler().get_url_query(req_resp.url, bl_include_fragment=True)["source"]
        bs_table = bs_html.find("table")
        return self.td_th_parser(bs_table, str_source)
