import pandas as pd
from datetime import datetime
from typing import Optional, List, Any, Tuple
from sqlalchemy.orm import Session
from logging import Logger
from requests import Response
from time import sleep
from lxml.html import HtmlElement
from selenium.webdriver.remote.webdriver import WebDriver
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException
from selenium.common.exceptions import TimeoutException
from stpstone._config.global_slots import YAML_US_ETFDB_VETTAFI
from stpstone.utils.cals.handling_dates import DatesBR
from stpstone.ingestion.abc.requests import ABCRequests
from stpstone.utils.parsers.folders import DirFilesManagement
from stpstone.utils.parsers.dicts import HandlingDicts
from stpstone.utils.parsers.str import StrHandler
from stpstone.utils.loggs.create_logs import CreateLog


class EtfDBVettaFi(ABCRequests):

    def __init__(
        self,
        session: Optional[Session] = None,
        dt_ref: datetime = DatesBR().sub_working_days(DatesBR().curr_date, 1),
        cls_db: Optional[Session] = None,
        logger: Optional[Logger] = None,
        token: Optional[str] = None,
        list_slugs: Optional[List[str]] = None,
        int_wait_load_seconds: int = 10,
        bl_headless: bool = False,
        bl_incognito: bool = False
    ) -> None:
        super().__init__(
            dict_metadata=YAML_US_ETFDB_VETTAFI,
            session=session,
            dt_ref=dt_ref,
            cls_db=cls_db,
            logger=logger,
            token=token,
            list_slugs=list_slugs,
            int_wait_load_seconds=int_wait_load_seconds,
            bl_headless=bl_headless,
            bl_incognito=bl_incognito
        )
        self.session = session
        self.dt_ref = dt_ref
        self.cls_db = cls_db
        self.logger = logger
        self.token = token
        self.list_slugs = list_slugs

    def td_th_parser(self, root: HtmlElement) -> pd.DataFrame:
        list_ser = list()
        for i in range(1, 50):
            try:
                el_tr = root.find_element(getattr(By, "XPATH"),
                    YAML_US_ETFDB_VETTAFI["reits"]["xpaths"]["list_tr"].format(i)
                )
                list_ser.append({
                    "SYMBOL": el_tr.find_element(getattr(By, "XPATH"), "./td[1]/a").text.strip(),
                    "HOLDING": el_tr.find_element(getattr(By, "XPATH"), "./td[2]").text.strip(),
                    "WEIGHT": float(el_tr.find_element(getattr(By, "XPATH"), "./td[3]").text.strip()\
                        .replace("%", "")) / 100.0
                })
            except (NoSuchElementException, TimeoutException):
                break
        if len(list_ser) == 0:
            return [{"SYMBOL": "ERROR", "HOLDING": "ERROR", "WEIGHT": 0.0}]
        return list_ser

    def req_trt_injection(self, web_driver: WebDriver) -> Optional[pd.DataFrame]:
        list_ser = self.td_th_parser(web_driver)
        return pd.DataFrame(list_ser)
