import pandas as pd
from datetime import datetime
from typing import Optional, List, Any, Tuple, Dict
from sqlalchemy.orm import Session
from logging import Logger
from requests import Response
from time import sleep
from selenium.webdriver.remote.webdriver import WebDriver
from stpstone._config.global_slots import YAML_YAHII
from stpstone.utils.cals.handling_dates import DatesBR
from stpstone.ingestion.abc.requests import ABCRequests
from stpstone.utils.parsers.html import HtmlHandler
from stpstone.utils.parsers.folders import DirFilesManagement
from stpstone.utils.parsers.dicts import HandlingDicts
from stpstone.utils.parsers.str import StrHandler
from stpstone.utils.parsers.numbers import NumHandler
from stpstone.utils.parsers.lists import ListHandler
from stpstone.utils.webdriver_tools.selenium_wd import SeleniumWD


class YahiiBRMacro(ABCRequests):

    MONTHS_COMBINED = [
        # english (3-letter)
        "JAN", "FEB", "MAR", "APR", "MAY", "JUN",
        "JUL", "AUG", "SEP", "OCT", "NOV", "DEC",
        # english (full)
        "JANUARY", "FEBRUARY", "MARCH", "APRIL", "MAY", "JUNE",
        "JULY", "AUGUST", "SEPTEMBER", "OCTOBER", "NOVEMBER", "DECEMBER",
        # portuguese (3-letter)
        "JAN", "FEV", "MAR", "ABR", "MAI", "JUN",
        "JUL", "AGO", "SET", "OUT", "NOV", "DEZ",
        # portuguese (full)
        "JANEIRO", "FEVEREIRO", "MARÇO", "ABRIL", "MAIO", "JUNHO",
        "JULHO", "AGOSTO", "SETEMBRO", "OUTUBRO", "NOVEMBRO", "DEZEMBRO"
    ]

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
            dict_metadata=YAML_YAHII,
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

    def td_value(self, str_value: str) -> float:
        """
        Get value from string

        Args:
            str_value (str): string to convert

        Returns:
            float: converted value
        """
        if "%" in str_value:
            return float(str_value.replace("(-)", "").replace("%", "").replace(",", ".")) / 100.0
        elif str_value in ["", "A/M", " "]:
            return ""
        elif NumHandler().is_numeric(str_value) == True:
            return float(str_value)
        else:
            self.create_log.log_message(
                self.logger, 
                f"{str_value} of type {type(str_value)}, which cannot be handled by the method, "
                + f"please check", 
                "warning"
            )
            return ""

    def td_th_parser(self, source: str, list_th_td: List[Any]) -> List[Dict[str, Any]]:
        list_td_months = list()
        list_td_years = list()
        list_td_values = list()
        i_m = 0
        list_ser = list()
        for td in list_th_td:
            if (td in self.MONTHS_COMBINED and "/" not in td) or "ACUMULADO" in td:
                list_td_months.append(td)
            elif len(td) == 4 and NumHandler().is_numeric(td) == True:
                list_td_years.append(int(td))
            else:
                list_td_values.append(self.td_value(td))
        list_td_months = ListHandler().remove_duplicates(list_td_months)
        if source not in ["cetip"]:
            list_td_values = [x for x in list_td_values if x not in ["", " "]]
        # print(f"list_td_months: {list_td_months}")
        # print(f"list_td_years: {list_td_years}")
        # print(f"list_td_values: {list_td_values}")
        for i_y, int_year in enumerate(list_td_years):
            if source in ["cetip"]:
                i_m = 0
            for str_month in list_td_months:
                try:
                    if source not in ["cetip"]:
                        list_ser.append({
                            "YEAR": int_year,
                            "MONTH": str_month,
                            "VALUE": list_td_values[i_m],
                            "ECONOMIC_INDICATOR": source.upper()
                        })
                    else:
                        list_ser.append({
                            "YEAR": int_year,
                            "MONTH": str_month,
                            "VALUE": list_td_values[2 + i_y + 9 * i_m],
                            "ECONOMIC_INDICATOR": "CDI" if source == "cetip" else source.upper()
                        })
                    i_m += 1
                except IndexError:
                    break
        # print(f"list_ser: {list_ser}")
        # raise Exception("BREAKPOINT")
        return list_ser
        
    def list_web_elements(self, cls_selenium_wd: SeleniumWD, driver: WebDriver, xpath_: str) -> List[Any]:
        """
        Get list of web elements from xpath

        Args:
            cls_selenium_wd (SeleniumWD): selenium web driver
            driver (WebDriver): selenium web driver
            xpath (str): xpath to find elements

        Returns:
            list: list of contents of web elements
        """
        list_els = cls_selenium_wd.find_elements(driver, xpath_)
        list_els = [x.text for x in list_els]
        return list_els

    def req_trt_injection(self, req_resp: Response) -> Optional[pd.DataFrame]:
        try:
            source = self.get_query_params(req_resp.url, "source")
            cls_selenium_wd = SeleniumWD(req_resp.url, bl_headless=True, bl_incognito=True)
            driver = cls_selenium_wd.get_web_driver
            list_th_td = self.list_web_elements(
                cls_selenium_wd, driver, YAML_YAHII["pmi_exchange_rates"]['xpaths']['list_th_td'])
            list_ser = self.td_th_parser(source, list_th_td)
        finally:
            driver.quit()
        return pd.DataFrame(list_ser)

