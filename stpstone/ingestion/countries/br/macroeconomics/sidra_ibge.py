import pandas as pd
from datetime import datetime
from typing import Optional, List, Any, Tuple
from sqlalchemy.orm import Session
from logging import Logger
from requests import Response
from time import sleep
from lxml import html
from selenium.webdriver.remote.webdriver import WebDriver
from stpstone._config.global_slots import YAML_SIDRA_IBGE
from stpstone.utils.cals.handling_dates import DatesBR
from stpstone.ingestion.abc.requests import ABCRequests
from stpstone.utils.parsers.folders import DirFilesManagement
from stpstone.utils.parsers.dicts import HandlingDicts
from stpstone.utils.parsers.str import StrHandler
from stpstone.utils.loggs.create_logs import CreateLog
from stpstone.utils.webdriver_tools.selenium_wd import SeleniumWD


class SidraIBGE(ABCRequests):

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
            dict_metadata=YAML_SIDRA_IBGE,
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

    def td_th_parser(self, cls_selenium_wd: SeleniumWD, web_driver: html.HtmlElement) -> Tuple[List[Any], int, Optional[int]]:
        i = 1
        list_ser = list()
        while True:
            list_pool_name = self.list_web_elements(
                cls_selenium_wd, 
                web_driver, 
                YAML_SIDRA_IBGE["pools_releases_dates"]["xpaths"]["iter_pool_name"].format(i)
            )
            if list_pool_name == None or list_pool_name == [] or len(list_pool_name) == 0:
                break
            list_pool_release_dt_new = self.list_web_elements(
                cls_selenium_wd, 
                web_driver, 
                YAML_SIDRA_IBGE["pools_releases_dates"]["xpaths"][
                    "iter_pool_release_dt_new"].format(i)
            )
            list_ser.append({
                "POOL_NAME": list_pool_name[0].split("\n")[0],
                "DT_POOL_RELEASE_NEW": list_pool_release_dt_new[0].split("\n")[0],
                "REFERENCED_PERIOD": list_pool_release_dt_new[0].split("\n")[1]
            })
            i += 1
        return pd.DataFrame(list_ser)

    def req_trt_injection(self, req_resp: Response) -> Optional[pd.DataFrame]:
        cls_selenium = SeleniumWD(req_resp.url, bl_headless=True, bl_incognito=True)
        web_driver = cls_selenium.get_web_driver
        return self.td_th_parser(cls_selenium, web_driver)