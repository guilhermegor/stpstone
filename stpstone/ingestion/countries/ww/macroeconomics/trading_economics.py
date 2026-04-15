"""Trading Economics US macroeconomic indicators ingestion via Selenium."""

from datetime import date
from logging import Logger
from typing import Any, Optional, Union

import pandas as pd
from playwright.sync_api import Page as PlaywrightPage
from requests import Response, Session
from selenium.webdriver.remote.webdriver import WebDriver as SeleniumWebDriver

from stpstone.ingestion.abc.ingestion_abc import (
    ABCIngestionOperations,
    ContentParser,
    CoreIngestion,
)
from stpstone.utils.calendars.calendar_abc import DatesCurrent
from stpstone.utils.calendars.calendar_br import DatesBRAnbima
from stpstone.utils.loggs.create_logs import CreateLog
from stpstone.utils.parsers.dicts import HandlingDicts
from stpstone.utils.parsers.folders import DirFilesManagement
from stpstone.utils.webdriver_tools.selenium_wd import SeleniumWD


class _TradingEconBase(ABCIngestionOperations):
    """Base class with shared Selenium scraping logic for all Trading Economics sources."""

    _BASE_HOST = "https://tradingeconomics.com/"
    _PATH: str = ""
    _TABLE_NAME: str = ""
    _DTYPES: dict[str, Any] = {}
    _XPATH: str = ""

    def __init__(
        self,
        date_ref: Optional[date] = None,
        logger: Optional[Logger] = None,
        cls_db: Optional[Session] = None,
    ) -> None:
        """Initialize the base ingestion class.

        Parameters
        ----------
        date_ref : Optional[date]
            The date of reference, by default None.
        logger : Optional[Logger]
            The logger, by default None.
        cls_db : Optional[Session]
            The database session, by default None.

        Returns
        -------
        None
        """
        super().__init__(cls_db=cls_db)
        CoreIngestion.__init__(self)
        ContentParser.__init__(self)

        self.logger = logger
        self.cls_db = cls_db
        self.cls_dir_files_management = DirFilesManagement()
        self.cls_dates_current = DatesCurrent()
        self.cls_create_log = CreateLog()
        self.cls_dates_br = DatesBRAnbima()
        self.date_ref = date_ref or \
            self.cls_dates_br.add_working_days(self.cls_dates_current.curr_date(), -1)
        self.url = f"{self._BASE_HOST}{self._PATH}"

    def run(
        self,
        timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (12.0, 21.0),
        bool_verify: bool = False,
        bool_insert_or_ignore: bool = False,
        str_table_name: Optional[str] = None,
    ) -> Optional[pd.DataFrame]:
        """Run the ingestion process.

        If the database session is provided, the data is inserted into the database.
        Otherwise, the transformed DataFrame is returned.

        Parameters
        ----------
        timeout : Optional[Union[int, float, tuple[float, float], tuple[int, int]]], optional
            Unused; kept for interface consistency, by default (12.0, 21.0).
        bool_verify : bool
            Unused; kept for interface consistency, by default False.
        bool_insert_or_ignore : bool
            Whether to insert or ignore the data, by default False.
        str_table_name : Optional[str]
            The name of the table, by default the class-level _TABLE_NAME.

        Returns
        -------
        Optional[pd.DataFrame]
            The transformed DataFrame.
        """
        str_table_name = str_table_name or self._TABLE_NAME
        web_driver = self.get_response()
        df_ = self.transform_data(resp_req=web_driver)
        df_ = self.standardize_dataframe(
            df_=df_,
            date_ref=self.date_ref,
            dict_dtypes=self._DTYPES,
            cols_to_case="upper_constant",
            str_fmt_dt="YYYY-MM-DD",
            url=self.url,
        )
        if self.cls_db:
            self.insert_table_db(
                cls_db=self.cls_db,
                str_table_name=str_table_name,
                df_=df_,
                bool_insert_or_ignore=bool_insert_or_ignore,
            )
        else:
            return df_

    def get_response(
        self,
        timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (12.0, 21.0),
        bool_verify: bool = False,
    ) -> Union[Response, PlaywrightPage, SeleniumWebDriver]:
        """Launch a headless Selenium browser and navigate to the source URL.

        Parameters
        ----------
        timeout : Optional[Union[int, float, tuple[float, float], tuple[int, int]]], optional
            Unused; kept for interface consistency, by default (12.0, 21.0).
        bool_verify : bool
            Unused; kept for interface consistency, by default False.

        Returns
        -------
        Union[Response, PlaywrightPage, SeleniumWebDriver]
            A Selenium WebDriver instance positioned at the source URL.
        """
        self._cls_selenium = SeleniumWD(self.url, bool_headless=True, bool_incognito=True)
        return self._cls_selenium.get_web_driver()

    def parse_raw_file(
        self,
        resp_req: Union[Response, PlaywrightPage, SeleniumWebDriver],
    ) -> Union[Response, PlaywrightPage, SeleniumWebDriver]:
        """Pass through the WebDriver for scraping in transform_data.

        Parameters
        ----------
        resp_req : Union[Response, PlaywrightPage, SeleniumWebDriver]
            The Selenium WebDriver.

        Returns
        -------
        Union[Response, PlaywrightPage, SeleniumWebDriver]
            The same WebDriver, unchanged.
        """
        return resp_req

    def transform_data(
        self,
        resp_req: Union[Response, PlaywrightPage, SeleniumWebDriver],
    ) -> pd.DataFrame:
        """Scrape the target XPath and build a DataFrame.

        Parameters
        ----------
        resp_req : Union[Response, PlaywrightPage, SeleniumWebDriver]
            A Selenium WebDriver positioned at the source URL.

        Returns
        -------
        pd.DataFrame
            The scraped DataFrame.
        """
        try:
            list_td = self._list_web_elements(self._cls_selenium, resp_req, self._XPATH)
            list_ser = HandlingDicts().pair_headers_with_data(
                list(self._DTYPES.keys()), list_td
            )
            return pd.DataFrame(list_ser)
        finally:
            resp_req.quit()

    @staticmethod
    def _list_web_elements(
        cls_selenium_wd: SeleniumWD,
        web_driver: SeleniumWebDriver,
        xpath_: str,
    ) -> list[str]:
        """Collect text content of all elements matching an XPath.

        Parameters
        ----------
        cls_selenium_wd : SeleniumWD
            The Selenium helper wrapper.
        web_driver : SeleniumWebDriver
            The active Selenium WebDriver session.
        xpath_ : str
            XPath expression to locate the target elements.

        Returns
        -------
        list[str]
            Text content of each matched element.
        """
        return [el.text for el in cls_selenium_wd.find_elements(web_driver, xpath_)]


class TradingEconNonFarmPayrollForecasts(_TradingEconBase):
    """Trading Economics US Non-Farm Payroll forecasts ingestion class."""

    _PATH = "united-states/non-farm-payrolls"
    _TABLE_NAME = "us_trading_economics_non_farm_payroll_forecasts"
    _XPATH = "//table[@id='calendar']/tbody/tr/td"
    _DTYPES = {
        "CALENDAR": "date",
        "GMT": "category",
        "N_A": "category",
        "REFERENCE": str,
        "ACTUAL": str,
        "PREVIOUS": str,
        "CONSENSUS": str,
        "TE_FORECAST": str,
    }


class TradingEconNonFarmPayrollComponents(_TradingEconBase):
    """Trading Economics US Non-Farm Payroll components breakdown ingestion class."""

    _PATH = "united-states/non-farm-payrolls"
    _TABLE_NAME = "us_trading_economics_non_farm_payroll_components"
    _XPATH = (
        "//div[@id='ctl00_ContentPlaceHolder1_ctl00_ctl02_PanelComponents']"
        "//table[@class='table table-hover']//td"
    )
    _DTYPES = {
        "COMPONENTS": str,
        "LAST": float,
        "PREVIOUS": float,
        "UNIT": str,
        "REFERENCE": str,
    }


class TradingEconNonFarmPayrollRelated(_TradingEconBase):
    """Trading Economics US Non-Farm Payroll related indicators ingestion class."""

    _PATH = "united-states/non-farm-payrolls"
    _TABLE_NAME = "us_trading_economics_non_farm_payroll_related"
    _XPATH = (
        "//div[@id='ctl00_ContentPlaceHolder1_ctl00_ctl02_PanelPeers']//table//td"
    )
    _DTYPES = {
        "COMPONENTS": str,
        "LAST": float,
        "PREVIOUS": float,
        "UNIT": str,
        "REFERENCE": str,
    }


class TradingEconNonFarmPayrollStats(_TradingEconBase):
    """Trading Economics US Non-Farm Payroll historical statistics ingestion class."""

    _PATH = "united-states/non-farm-payrolls"
    _TABLE_NAME = "us_trading_economics_non_farm_payroll_stats"
    _XPATH = (
        "//div[@id='ctl00_ContentPlaceHolder1_ctl00_ctl03_Panel1']//table//td"
    )
    _DTYPES = {
        "N_A": "category",
        "ACTUAL": float,
        "PREVIOUS": float,
        "HIGHEST": float,
        "LOWEST": float,
        "DATES": str,
        "UNIT": str,
        "FREQUENCY": str,
        "N_A_2": "category",
    }


class TradingEconWagesUsa(_TradingEconBase):
    """Trading Economics US wages data ingestion class."""

    _PATH = "united-states/wages"
    _TABLE_NAME = "us_trading_economics_wages"
    _XPATH = "//table[@class='table table-hover']//td"
    _DTYPES = {
        "RELATED": str,
        "LAST": float,
        "PREVIOUS": float,
        "UNIT": str,
        "REFERENCE": str,
    }
