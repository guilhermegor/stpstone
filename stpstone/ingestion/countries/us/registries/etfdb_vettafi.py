from datetime import datetime
from logging import Logger
from time import sleep
from typing import Any, List, Optional, Tuple

from lxml.html import HtmlElement
import pandas as pd
from requests import Response
from selenium.common.exceptions import NoSuchElementException, TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.remote.webdriver import WebDriver
from sqlalchemy.orm import Session

from stpstone._config.global_slots import YAML_US_ETFDB_VETTAFI
from stpstone.ingestion.abc.requests import ABCRequests
from stpstone.utils.calendars.calendar_br import DatesBRAnbima
from stpstone.utils.loggs.create_logs import CreateLog
from stpstone.utils.parsers.dicts import HandlingDicts
from stpstone.utils.parsers.folders import DirFilesManagement
from stpstone.utils.parsers.str import StrHandler


class EtfDBVettaFi(ABCRequests):

    def __init__(
        self,
        session: Optional[Session] = None,
        date_ref: datetime = DatesBRAnbima().sub_working_days(DatesBRAnbima().curr_date(), 1),
        cls_db: Optional[Session] = None,
        logger: Optional[Logger] = None,
        token: Optional[str] = None,
        list_slugs: Optional[List[str]] = None,
        int_wait_load_seconds: int = 10,
        bool_headless: bool = False,
        bool_incognito: bool = False
    ) -> None:
        super().__init__(
            dict_metadata=YAML_US_ETFDB_VETTAFI,
            session=session,
            date_ref=date_ref,
            cls_db=cls_db,
            logger=logger,
            token=token,
            list_slugs=list_slugs,
            int_wait_load_seconds=int_wait_load_seconds,
            bool_headlesbool_headless,
            bool_incognitbl_incognito
        )
        self.session = session
        self.date_ref = date_ref
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
