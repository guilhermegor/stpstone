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
from stpstone.utils.cals.handling_dates import DatesBR


pd.set_option("display.max_rows", None)

class YahiiBRMacro(ABCRequests):

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
        if str_value == "A/M":
            return "A/M"
        elif ("%" in str_value) and ("*" not in str_value):
            return float(str_value.replace("(-)", "-").replace("%", "").replace(".", "")\
                         .replace(",", ".").replace(" ", "")) / 100.0
        elif "(-)" in str_value:
            return float(str_value.replace("(-)", "-").replace("%", "").replace(".", "")\
                         .replace(",", "."))
        elif str_value in ["", " "]:
            return ""
        elif NumHandler().is_numeric(str_value.replace(",", ".")) == True:
            return float(str_value.replace(",", "."))
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
        list_ser = list()
        # populating list_td_months, list_td_years and list_td_values
        for td in list_th_td:
            if (td in YAML_YAHII["pmi_rf_rates"]["list_months_combined"] 
                and "/" not in td) \
                or "ACUMULADO" in td \
                or "ACUM" in td:
                list_td_months.append(td)
            elif len(td) == 4 and NumHandler().is_numeric(td) == True:
                try:
                    list_td_years.append(int(td))
                except ValueError:
                    list_td_values.append(self.td_value(td.replace(".", ",")))
            else:
                list_td_values.append(self.td_value(td))
        # cleaning list_td_months and list_td_years, regarding the source
        if source in ["igpm"]: 
            list_td_values.remove("A/M")
        elif source in ["ipcae"]:
            list_td_years = list_td_years[2:]
            list_td_months = ["JAN", "FEV", "MAR", "ACUM TRIM", "ABR", "MAI", "JUN", "ACUM TRIM", 
                              "JUL", "AGO", "SET", "ACUM TRIM", "OUT", "NOV", "DEZ", "ACUM TRIM", 
                              "ACUMULADO NO ANO"]
        elif source in ["ivar"]:
            list_td_years = list_td_years[1:]
            list_td_months = [x for x in list_td_months if x not in ["JAN", "FEV", "MAR", "ABR", 
                                                                     "MAI", "JUN", "JUL", "AGO", 
                                                                     "SET", "OUT", "NOV", "DEZ"]]
        elif source in ["iiebr"]:
            list_td_months = ListHandler().remove_duplicates(list_td_months)
        elif source in ["poupanca", "tjlp"]:
            list_td_months = ListHandler().remove_duplicates(list_td_months)
            list_td_years = ListHandler().remove_duplicates(list_td_years)
            list_td_years = [x for x in list_td_years \
                             if x <= DatesBR().year_number(DatesBR().curr_date) + 1 and x >= 1995]
            list_td_years.sort()
        print(f"list_td_months: {list_td_months}")
        print(f"list_td_years: {list_td_years}")
        print(f"list_td_values: {[(i, x) for i, x in enumerate(list_td_values)]}")
        # populating list_ser with year, month and values
        for i_y, int_year in enumerate(list_td_years):
            if DatesBR().year_number(DatesBR().curr_date) < int_year: break
            i_m = 0
            if ("int_rows_tbl" in YAML_YAHII["pmi_rf_rates"]["dict_fw_td_th"][source]) \
                and ("int_year_min" in YAML_YAHII["pmi_rf_rates"]["dict_fw_td_th"][source]) \
                and ("int_cols_tbl" in YAML_YAHII["pmi_rf_rates"]["dict_fw_td_th"][source]) \
                and ("int_step_tbl_begin" in YAML_YAHII["pmi_rf_rates"]["dict_fw_td_th"][source]):
                int_num_tbl = (int_year - YAML_YAHII["pmi_rf_rates"]["dict_fw_td_th"][
                    source]["int_year_min"]) // YAML_YAHII["pmi_rf_rates"]["dict_fw_td_th"][
                        source]["int_cols_tbl"]
                int_start_tbl = YAML_YAHII["pmi_rf_rates"]["dict_fw_td_th"][source]["int_start"] \
                    + int_num_tbl * YAML_YAHII["pmi_rf_rates"]["dict_fw_td_th"][source][
                        "int_step_tbl_begin"]
            else:
                int_num_tbl = 0
                int_start_tbl = YAML_YAHII["pmi_rf_rates"]["dict_fw_td_th"][source]["int_start"]
            for str_month in list_td_months:
                int_pos = int_start_tbl \
                    + YAML_YAHII["pmi_rf_rates"]["dict_fw_td_th"][source]["int_y"] * i_y \
                    + YAML_YAHII["pmi_rf_rates"]["dict_fw_td_th"][source]["int_m"] * i_m \
                    if "ACUMULADO" not in str_month else \
                    YAML_YAHII["pmi_rf_rates"]["dict_fw_td_th"][source]["int_start_acc"] \
                        + YAML_YAHII["pmi_rf_rates"]["dict_fw_td_th"][source]["int_y"] * i_y
                try:
                    list_ser.append({
                        "INT_START": YAML_YAHII["pmi_rf_rates"]["dict_fw_td_th"][source][
                            "int_start"],
                        "INT_Y": YAML_YAHII["pmi_rf_rates"]["dict_fw_td_th"][source]["int_y"],
                        "INT_M": YAML_YAHII["pmi_rf_rates"]["dict_fw_td_th"][source]["int_m"],
                        "INT_ROWS_TBL": YAML_YAHII["pmi_rf_rates"]["dict_fw_td_th"][source][
                            "int_rows_tbl"],
                        "INT_COLS_TBL": YAML_YAHII["pmi_rf_rates"]["dict_fw_td_th"][source][
                            "int_cols_tbl"],
                        "INT_STEP_TBL_BEGIN": YAML_YAHII["pmi_rf_rates"]["dict_fw_td_th"][source][
                            "int_step_tbl_begin"],
                        "INT_YEAR_MIN": YAML_YAHII["pmi_rf_rates"]["dict_fw_td_th"][source][
                            "int_year_min"],
                        "INT_START_ACC": YAML_YAHII["pmi_rf_rates"]["dict_fw_td_th"][source][
                            "int_start_acc"],
                        "INT_NUM_TBL": int_num_tbl,
                        "I_Y": i_y,
                        "I_M": i_m,
                        "INT_START_TBL": int_start_tbl,
                        "INDEX_TD": int_pos,
                        "YEAR": int_year,
                        "MONTH": str_month,
                        "VALUE": list_td_values[int_pos],
                        "ECONOMIC_INDICATOR": "CDI" if source == "cetip" else source.upper()
                    })
                    i_m += 1
                except IndexError:
                    break
        return list_ser
        
    def list_web_elements(self, cls_selenium_wd: SeleniumWD, driver: WebDriver, xpath_: str) \
        -> List[Any]:
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
            source = self.get_query_params(req_resp.url, "source").lower()
            cls_selenium_wd = SeleniumWD(req_resp.url, bl_headless=True, bl_incognito=True)
            driver = cls_selenium_wd.get_web_driver
            list_th_td = self.list_web_elements(
                cls_selenium_wd, driver, YAML_YAHII["pmi_rf_rates"]['xpaths']['list_th_td'])
            list_ser = self.td_th_parser(source, list_th_td)
        finally:
            driver.quit()
        return pd.DataFrame(list_ser)

