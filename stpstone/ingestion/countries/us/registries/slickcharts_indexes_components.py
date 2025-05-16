import pandas as pd
from datetime import datetime
from typing import Optional, List, Any, Tuple, Dict, Union
from lxml.html import HtmlElement
from sqlalchemy.orm import Session
from logging import Logger
from requests import Response
from time import sleep
from numbers import Number
from stpstone._config.global_slots import YAML_US_SLICKCHARTS_INDEXES_COMPONENTS
from stpstone.utils.cals.handling_dates import DatesBR
from stpstone.ingestion.abc.requests import ABCRequests
from stpstone.utils.parsers.html import HtmlHandler
from stpstone.utils.parsers.folders import DirFilesManagement
from stpstone.utils.parsers.dicts import HandlingDicts
from stpstone.utils.parsers.str import StrHandler
from stpstone.utils.parsers.numbers import NumHandler


class SlickChartsIndexesComponents(ABCRequests):

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
            dict_metadata=YAML_US_SLICKCHARTS_INDEXES_COMPONENTS,
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

    def td_th_parser(self, root: HtmlElement, resp_req: Response) -> pd.DataFrame:
        list_ser = []
        source = self.get_query_params(resp_req.url, "source")
        for i in range(1, 600):
            try:
                el_tr = HtmlHandler().lxml_xpath(
                    root,
                    YAML_US_SLICKCHARTS_INDEXES_COMPONENTS[source]["xpaths"]["list_tr"].format(i))[0]
            except IndexError:
                break
            list_ser.append({
                "NUM_COMPANY": HtmlHandler().lxml_xpath(el_tr, "./td[1]")[0].text.strip(),
                "NAME_COMPANY": HtmlHandler().lxml_xpath(el_tr, "./td[2]/a")[0].text.strip(),
                "TICKER": HtmlHandler().lxml_xpath(el_tr, "./td[3]/a")[0].text.strip(),
                "WEIGHT": float(HtmlHandler().lxml_xpath(el_tr, "./td[4]")[0].text.strip()\
                    .replace("%", ""))/100.0,
            })
        return pd.DataFrame(list_ser)

    def req_trt_injection(self, resp_req: Response) -> Optional[pd.DataFrame]:
        bl_debug = True if StrHandler().match_string_like(
            resp_req.url, "*bl_debug=True*") == True else False
        root = HtmlHandler().lxml_parser(resp_req)
        # export html tree to data folder, if is user's will
        if bl_debug == True:
            path_project = DirFilesManagement().find_project_root(marker="pyproject.toml")
            HtmlHandler().html_tree(root, file_path=rf"{path_project}/data/test.html")
        return pd.DataFrame(self.td_th_parser(root, resp_req))
