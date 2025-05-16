import pandas as pd
from datetime import datetime
from typing import Optional, List, Any, Tuple
from sqlalchemy.orm import Session
from logging import Logger
from requests import Response
from time import sleep
from selenium.webdriver.remote.webdriver import WebDriver
from stpstone._config.global_slots import YAML_WW_TRADING_ECON
from stpstone.utils.cals.handling_dates import DatesBR
from stpstone.ingestion.abc.requests import ABCRequests
from stpstone.utils.parsers.html import HtmlHandler
from stpstone.utils.parsers.folders import DirFilesManagement
from stpstone.utils.parsers.dicts import HandlingDicts
from stpstone.utils.parsers.str import StrHandler
from stpstone.utils.loggs.create_logs import CreateLog
from stpstone.utils.webdriver_tools.selenium_wd import SeleniumWD


class TradingEconWW(ABCRequests):

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
            dict_metadata=YAML_WW_TRADING_ECON,
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

    def list_web_elements(self, cls_selenium_wd: SeleniumWD, web_driver: WebDriver, xpath_: str) \
        -> List[Any]:
        """
        Get list of web elements from xpath

        Args:
            cls_selenium_wd (SeleniumWD): selenium web web_driver
            web_driver (WebDriver): selenium web web_driver
            xpath (str): xpath to find elements

        Returns:
            list: list of contents of web elements
        """
        list_els = cls_selenium_wd.find_elements(web_driver, xpath_)
        list_els = [x.text for x in list_els]
        return list_els

    def req_trt_injection(self, resp_req: Response) -> Optional[pd.DataFrame]:
        source = self.get_query_params(resp_req.url, "source")
        try:
            cls_selenium = SeleniumWD(resp_req.url, bl_headless=True, bl_incognito=True)
            web_driver = cls_selenium.get_web_driver
            list_th = list(YAML_WW_TRADING_ECON[source]["dtypes"].keys())
            list_td = self.list_web_elements(cls_selenium, web_driver, 
                                             YAML_WW_TRADING_ECON[source]["xpaths"]["list_td"])
        finally:
            web_driver.quit()
        list_ser = HandlingDicts().pair_headers_with_data(list_th, list_td)
        return pd.DataFrame(list_ser)
