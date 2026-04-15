"""Anbima IGPM Core base class (private)."""

from datetime import date
from logging import Logger
from math import nan
from typing import Optional, Union

import pandas as pd
from requests import Session

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
from stpstone.utils.parsers.html import HtmlHandler
from stpstone.utils.parsers.lists import ListHandler
from stpstone.utils.webdriver_tools.playwright_wd import PlaywrightScraper


class AnbimaIGPMCore(ABCIngestionOperations):
    """Anbima IGPM forecasts private base class."""

    def __init__(
        self,
        date_ref: Optional[date] = None,
        logger: Optional[Logger] = None,
        cls_db: Optional[Session] = None,
        url: str = "FILL_ME",
    ) -> None:
        """Initialize the ingestion class.

        Parameters
        ----------
        date_ref : Optional[date]
            The date of reference, by default None.
        logger : Optional[Logger]
            The logger, by default None.
        cls_db : Optional[Session]
            The database session, by default None.
        url : str
            The URL, by default "FILL_ME".

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
        self.cls_html_handler = HtmlHandler()
        self.cls_dict_handler = HandlingDicts()
        self.cls_list_handler = ListHandler()
        self.date_ref = date_ref or \
            self.cls_dates_br.add_working_days(self.cls_dates_current.curr_date(), -1)
        self.url = url

    def run(
        self,
        dict_dtypes: dict[str, Union[str, int, float]],
        timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (12.0, 21.0),
        bool_verify: bool = True,
        bool_insert_or_ignore: bool = False,
        str_table_name: str = "<COUNTRY>_<SOURCE>_<TABLE_NAME>",
    ) -> Optional[pd.DataFrame]:
        """Run the ingestion process.

        If the database session is provided, the data is inserted into the database.
        Otherwise, the transformed DataFrame is returned.

        Parameters
        ----------
        dict_dtypes : dict[str, Union[str, int, float]]
            The dictionary of data types.
        timeout : Optional[Union[int, float, tuple[float, float], tuple[int, int]]], optional
            The timeout, by default (12.0, 21.0).
        bool_verify : bool
            Whether to verify the SSL certificate, by default True.
        bool_insert_or_ignore : bool
            Whether to insert or ignore the data, by default False.
        str_table_name : str
            The name of the table, by default "<COUNTRY>_<SOURCE>_<TABLE_NAME>".

        Returns
        -------
        Optional[pd.DataFrame]
            The transformed DataFrame.
        """
        scraper_playwright = self.parse_raw_file()
        df_ = self.transform_data(scraper_playwright=scraper_playwright)
        df_ = self.standardize_dataframe(
            df_=df_,
            date_ref=self.date_ref,
            dict_dtypes=dict_dtypes,
            str_fmt_dt="DD/MM/YY",
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

    def parse_raw_file(self) -> PlaywrightScraper:
        """Parse the raw file content.

        Returns
        -------
        PlaywrightScraper
            The parsed content.
        """
        return PlaywrightScraper(bool_headless=True, int_default_timeout=5_000)

    def get_response(
        self,
        timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (12.0, 21.0),
        bool_verify: bool = True,
    ) -> None:
        """Return a list of response objects.

        Parameters
        ----------
        timeout : Optional[Union[int, float, tuple[float, float], tuple[int, int]]], optional
            The timeout, by default (12.0, 21.0).
        bool_verify : bool
            Verify the SSL certificate, by default True.

        Returns
        -------
        None
        """
        pass

    def transform_data(
        self,
        xpath_current_month_forecasts: str,
        list_th: list[str],
        scraper_playwright: PlaywrightScraper,
    ) -> pd.DataFrame:
        """Transform a list of response objects into a DataFrame.

        Parameters
        ----------
        xpath_current_month_forecasts : str
            The XPath to the current month forecasts.
        list_th : list[str]
            The list of headers.
        scraper_playwright : PlaywrightScraper
            The PlaywrightScraper.

        Returns
        -------
        pd.DataFrame
            The transformed DataFrame.

        Raises
        ------
        RuntimeError
            If the navigation to the URL fails.
        """
        with scraper_playwright.launch():
            if not scraper_playwright.navigate(self.url):
                raise RuntimeError(f"Failed to navigate to URL: {self.url}")
            list_td = scraper_playwright.get_elements(
                selector=xpath_current_month_forecasts,
                timeout=10_000,
            )
            list_td = [
                nan if len(str(dict_["text"])) == 1
                else str(dict_["text"]).replace(",", ".")
                for dict_ in list_td
            ]

        list_ser = self.cls_dict_handler.pair_headers_with_data(list_th, list_td)
        return pd.DataFrame(list_ser)
