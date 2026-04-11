"""ETF DB VettaFi ETF holdings ingestion."""

import contextlib
from datetime import date
from logging import Logger
from typing import Optional, Union

import backoff
import pandas as pd
from playwright.sync_api import Page as PlaywrightPage
import requests
from requests import Response, Session
from selenium.common.exceptions import NoSuchElementException, TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.remote.webdriver import WebDriver as SeleniumWebDriver

from stpstone.ingestion.abc.ingestion_abc import (
    ABCIngestionOperations,
    ContentParser,
    CoreIngestion,
)
from stpstone.utils.calendars.calendar_abc import DatesCurrent
from stpstone.utils.calendars.calendar_br import DatesBRAnbima
from stpstone.utils.loggs.create_logs import CreateLog
from stpstone.utils.parsers.folders import DirFilesManagement
from stpstone.utils.webdriver_tools.selenium_wd import SeleniumWD


_BASE_HOST = "https://etfdb.com/"
_APP_TEMPLATE = "etf/{slug}/#holdings"
_XPATH_WAIT_UNTIL_LOADED = "//*[@id='etf-holdings']/thead/tr/th[1]/div[1]/span[1]"
_XPATH_ROW_TEMPLATE = "//table[@id='etf-holdings']/tbody/tr[{i}]"

_DEFAULT_SLUGS: list[str] = ["VNQ", "IYR", "SCHH", "XLRE", "REM"]


class EtfDBVettaFi(ABCIngestionOperations):
    """ETF DB VettaFi REIT holdings ingestion class."""

    def __init__(
        self,
        date_ref: Optional[date] = None,
        logger: Optional[Logger] = None,
        cls_db: Optional[Session] = None,
        list_slugs: Optional[list[str]] = None,
        int_wait_load_seconds: int = 10,
        bool_headless: bool = False,
        bool_incognito: bool = False,
    ) -> None:
        """Initialize the ingestion class.

        Parameters
        ----------
        date_ref : Optional[date], optional
            The date of reference, by default None.
        logger : Optional[Logger], optional
            The logger, by default None.
        cls_db : Optional[Session], optional
            The database session, by default None.
        list_slugs : Optional[list[str]], optional
            ETF ticker slugs to scrape, by default None (uses the canonical REIT list).
        int_wait_load_seconds : int, optional
            Seconds to wait for the browser to load each page, by default 10.
        bool_headless : bool, optional
            Whether to run Chrome in headless mode, by default False.
        bool_incognito : bool, optional
            Whether to run Chrome in incognito mode, by default False.

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
        self.list_slugs = list_slugs if list_slugs is not None else list(_DEFAULT_SLUGS)
        self.int_wait_load_seconds = int_wait_load_seconds
        self.bool_headless = bool_headless
        self.bool_incognito = bool_incognito
        self.url = _BASE_HOST

    def run(
        self,
        timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (12.0, 21.0),
        bool_verify: bool = False,
        bool_insert_or_ignore: bool = False,
        str_table_name: str = "us_etfdb_vettafi_reits",
    ) -> Optional[pd.DataFrame]:
        """Run the ingestion process for all configured slugs.

        Iterates over ``list_slugs``, opens a Selenium browser for each ETF
        holdings page, scrapes the holdings table, concatenates all results,
        standardises types, and either persists to the database (when
        ``cls_db`` is set) or returns the combined DataFrame.

        Parameters
        ----------
        timeout : Optional[Union[int, float, tuple[float, float], tuple[int, int]]], optional
            The timeout, by default (12.0, 21.0).
        bool_verify : bool, optional
            Whether to verify the SSL certificate, by default False.
        bool_insert_or_ignore : bool, optional
            Whether to insert or ignore the data, by default False.
        str_table_name : str, optional
            The name of the table, by default "us_etfdb_vettafi_reits".

        Returns
        -------
        Optional[pd.DataFrame]
            The transformed DataFrame when no database session is configured,
            otherwise None.
        """
        if not self.list_slugs:
            return None

        list_frames: list[pd.DataFrame] = []

        for slug in self.list_slugs:
            slug_url = f"{_BASE_HOST}{_APP_TEMPLATE.format(slug=slug)}"
            self.cls_create_log.log_message(
                logger=self.logger,
                message=f"EtfDBVettaFi: fetching slug '{slug}' at {slug_url}",
                log_level="info",
            )
            web_driver = self.get_response(slug_url=slug_url)
            df_slug = self.transform_data(web_driver=web_driver)
            if df_slug is not None and not df_slug.empty:
                df_slug["SLUG"] = slug
                list_frames.append(df_slug)
            with contextlib.suppress(Exception):
                web_driver.quit()

        if not list_frames:
            return None

        df_ = pd.concat(list_frames, ignore_index=True)
        df_ = self.standardize_dataframe(
            df_=df_,
            date_ref=self.date_ref,
            dict_dtypes={
                "SLUG": str,
                "SYMBOL": str,
                "HOLDING": str,
                "WEIGHT": float,
            },
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
            return None

        return df_

    @backoff.on_exception(backoff.expo, requests.exceptions.HTTPError, max_time=60)
    def get_response(
        self,
        slug_url: str,
        timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (12.0, 21.0),
        bool_verify: bool = False,
    ) -> Union[Response, PlaywrightPage, SeleniumWebDriver]:
        """Open a Selenium browser session for a single ETF holdings page.

        Parameters
        ----------
        slug_url : str
            Fully-qualified URL for the ETF holdings page to scrape.
        timeout : Optional[Union[int, float, tuple[float, float], tuple[int, int]]], optional
            The timeout, by default (12.0, 21.0).
        bool_verify : bool, optional
            Verify the SSL certificate, by default False.

        Returns
        -------
        Union[Response, PlaywrightPage, SeleniumWebDriver]
            A configured Selenium WebDriver already pointing at the page.
        """
        cls_selenium = SeleniumWD(
            url=slug_url,
            int_wait_load_seconds=self.int_wait_load_seconds,
            int_delay_seconds=self.int_wait_load_seconds,
            bool_headless=self.bool_headless,
            bool_incognito=self.bool_incognito,
            logger=self.logger,
        )
        cls_selenium.wait_until_el_loaded(_XPATH_WAIT_UNTIL_LOADED)
        return cls_selenium.web_driver

    def parse_raw_file(
        self,
        resp_req: Union[Response, PlaywrightPage, SeleniumWebDriver],
    ) -> SeleniumWebDriver:
        """Pass through the WebDriver as the raw file handle.

        ETF DB pages are scraped directly from the live DOM; there is no
        intermediate file to decode.  This method satisfies the
        ``ABCIngestion`` contract and returns the driver unchanged so that
        ``transform_data`` can query elements directly.

        Parameters
        ----------
        resp_req : Union[Response, PlaywrightPage, SeleniumWebDriver]
            The Selenium WebDriver returned by ``get_response``.

        Returns
        -------
        SeleniumWebDriver
            The same WebDriver, passed through unchanged.
        """
        return resp_req

    def transform_data(
        self,
        web_driver: Union[Response, PlaywrightPage, SeleniumWebDriver],
    ) -> pd.DataFrame:
        """Extract the holdings table rows and return a tidy DataFrame.

        Delegates row extraction to ``_td_th_parser``.

        Parameters
        ----------
        web_driver : Union[Response, PlaywrightPage, SeleniumWebDriver]
            A Selenium WebDriver pointed at an ETF holdings page.

        Returns
        -------
        pd.DataFrame
            DataFrame with columns SYMBOL, HOLDING, WEIGHT.
        """
        list_rows = self._td_th_parser(web_driver)
        return pd.DataFrame(list_rows)

    def _td_th_parser(
        self,
        web_driver: Union[Response, PlaywrightPage, SeleniumWebDriver],
    ) -> list[dict[str, Union[str, float]]]:
        """Scrape individual holdings rows from the ETF holdings table.

        Iterates over table rows using the row-indexed XPath until a
        ``NoSuchElementException`` or ``TimeoutException`` signals the end
        of the table.  Returns a sentinel row when no data is found.

        Parameters
        ----------
        web_driver : Union[Response, PlaywrightPage, SeleniumWebDriver]
            A Selenium WebDriver pointed at an ETF holdings page.

        Returns
        -------
        list[dict[str, Union[str, float]]]
            A list of row dicts with keys SYMBOL, HOLDING, WEIGHT.
        """
        list_rows: list[dict[str, Union[str, float]]] = []

        for i in range(1, 50):
            try:
                el_tr = web_driver.find_element(
                    By.XPATH,
                    _XPATH_ROW_TEMPLATE.format(i=i),
                )
                list_rows.append({
                    "SYMBOL": el_tr.find_element(By.XPATH, "./td[1]/a").text.strip(),
                    "HOLDING": el_tr.find_element(By.XPATH, "./td[2]").text.strip(),
                    "WEIGHT": float(
                        el_tr.find_element(By.XPATH, "./td[3]").text.strip().replace("%", "")
                    ) / 100.0,
                })
            except (NoSuchElementException, TimeoutException):
                break

        if not list_rows:
            self.cls_create_log.log_message(
                logger=self.logger,
                message="EtfDBVettaFi: no holdings rows found; returning sentinel record.",
                log_level="warning",
            )
            return [{"SYMBOL": "ERROR", "HOLDING": "ERROR", "WEIGHT": 0.0}]

        return list_rows
