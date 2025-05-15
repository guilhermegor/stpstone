import pandas as pd
from datetime import datetime
from logging import Logger
from time import sleep
from typing import Any, List, Optional, Tuple
from requests import Response
from sqlalchemy.orm import Session
from stpstone._config.global_slots import YAML_B3_FUTURES_CLOSING_ADJ
from stpstone.ingestion.abc.requests import ABCRequests
from stpstone.utils.cals.handling_dates import DatesBR
from stpstone.utils.connections.netops.proxies.managers.free import YieldFreeProxy
from stpstone.utils.loggs.create_logs import CreateLog
from stpstone.utils.parsers.dicts import HandlingDicts
from stpstone.utils.parsers.folders import DirFilesManagement
from stpstone.utils.parsers.html import HtmlHandler
from stpstone.utils.parsers.str import StrHandler


class FuturesClosingAdjB3(ABCRequests):

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
            dict_metadata=YAML_B3_FUTURES_CLOSING_ADJ,
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
        self.repr_dt_ref = self.dt_ref.strftime("%d/%m/%Y").replace("/", "%2F")

    def td_th_parser(
        self, resp_req: Response, list_th: List[Any]
    ) -> Tuple[List[Any], int, Optional[int]]:
        list_headers = list_th.copy()
        int_init_td = 0
        int_end_td = None
        # for using this workaround, please pass a dummy variable to the url, within the YAML file,
        #   like https://example.com/app/#source=dummy_1&bl_debug=True
        return list_headers, int_init_td, int_end_td

    def req_trt_injection(self, resp_req: Response) -> Optional[pd.DataFrame]:
        bl_debug = (
            True
            if StrHandler().match_string_like(resp_req.url, "*bl_debug=True*") == True
            else False
        )
        root = HtmlHandler().lxml_parser(resp_req)
        # export html tree to data folder, if is user's will
        if bl_debug == True:
            path_project = DirFilesManagement().find_project_root(
                marker="pyproject.toml"
            )
            HtmlHandler().html_tree(root, file_path=rf"{path_project}/data/test.html")
        list_th = [
            x.text.strip().replace("R$)", "BRL")
            for x in HtmlHandler().lxml_xpath(
                root,
                YAML_B3_FUTURES_CLOSING_ADJ["futures_closing_adj"]["xpaths"]["list_th"],
            )
        ]
        list_td = [
            (
                ""
                if x.text is None
                else x.text.replace("\xa0", "")
                .replace(".", "")
                .replace(",", ".")
                .strip()
            )
            for x in HtmlHandler().lxml_xpath(
                root,
                YAML_B3_FUTURES_CLOSING_ADJ["futures_closing_adj"]["xpaths"]["list_td"],
            )
        ]
        # deal with data/headers specificity for the project
        list_headers, int_init_td, int_end_td = self.td_th_parser(resp_req, list_th)
        if bl_debug == True:
            print(list_headers)
            print(f"LEN LIST HEADERS: {len(list_headers)}")
            print(list_td[int_init_td:int_end_td])
            print(f"LEN LIST TD: {len(list_td[int_init_td:int_end_td])}")
        list_ser = HandlingDicts().pair_headers_with_data(
            list_headers, list_td[int_init_td:int_end_td]
        )
        return pd.DataFrame(list_ser)
