"""Anbima Data Core."""

from datetime import date
from logging import Logger
import re
from time import sleep
from typing import Optional, Union

import backoff
import pandas as pd
import requests
from requests import Session

from stpstone.ingestion.abc.ingestion_abc import (
    ABCIngestionOperations,
    ContentParser,
    CoreIngestion,
)
from stpstone.utils.cache.cache_manager import CacheManager
from stpstone.utils.calendars.calendar_abc import DatesCurrent
from stpstone.utils.calendars.calendar_br import DatesBRAnbima
from stpstone.utils.loggs.create_logs import CreateLog
from stpstone.utils.parsers.folders import DirFilesManagement
from stpstone.utils.parsers.lists import ListHandler
from stpstone.utils.parsers.str import StrHandler
from stpstone.utils.webdriver_tools.playwright_wd import PlaywrightScraper


class AnbimaDataCore(ABCIngestionOperations):
    """Anbima Data Core - Instruments Available.
    
    Notes
    -----
    [1] bool_headless: Run browser in headless mode must be False because otherwise it is 
    triggered https://data.anbima.com.br/robo, which blocks the access.
    """
    
    def __init__(
        self, 
        fstr_url_iter: str,
        int_pg_start: Optional[int] = None,
        int_pg_end: Optional[int] = None,
        date_ref: Optional[date] = None, 
        logger: Optional[Logger] = None,
        cls_db: Optional[Session] = None,
        int_default_timeout_miliseconds: int = 30_000,
        int_wait_next_request_seconds: int = 10,
        bool_headless: bool = False,
        bool_minimized_window: bool = False,
        bool_persist_cache: bool = True,
        bool_reuse_cache: bool = True,
        int_days_cache_expiration: int = 1,
        int_cache_ttl_days: int = 7,
        path_cache_dir: Optional[str] = None,
    ) -> None:
        """Initialize the ingestion class.
        
        Parameters
        ----------
        fstr_url_iter : str
            The URL to iterate over.
        int_pg_start : Optional[int], optional
            The start page number, by default None.
        int_pg_end : Optional[int], optional
            The end page number, by default None.
        date_ref : Optional[date], optional
            The date of reference, by default None.
        logger : Optional[Logger], optional
            The logger, by default None.
        cls_db : Optional[Session], optional
            The database session, by default None.
        int_default_timeout_miliseconds : int, optional
            The default timeout in milliseconds, by default 30_000
        int_wait_next_request_seconds : int, optional
            The number of seconds to wait between requests, by default 10
        bool_headless : bool, optional
            If True, run browser in headless mode, by default False
        bool_minimized_window : bool, optional
            If True, run browser in minimized window, by default False
        
        Returns
        -------
        None
        """
        super().__init__(cls_db=cls_db)
        CoreIngestion.__init__(self)
        ContentParser.__init__(self)

        self.logger = logger
        self.cls_db = cls_db
        self.int_wait_next_request_seconds = int_wait_next_request_seconds

        self.cls_cache_manager = CacheManager(
            bool_persist_cache=bool_persist_cache,
            bool_reuse_cache=bool_reuse_cache,
            int_days_cache_expiration=int_days_cache_expiration,
            int_cache_ttl_days=int_cache_ttl_days,
            path_cache_dir=path_cache_dir,
            logger=logger,
        )

        self.cls_dir_files_management = DirFilesManagement()
        self.cls_dates_current = DatesCurrent()
        self.cls_create_log = CreateLog()
        self.cls_dates_br = DatesBRAnbima()
        self.cls_list_handler = ListHandler()
        self.cls_str_handler = StrHandler()
        self.cls_playwright_scraper = PlaywrightScraper(
            bool_headless=bool_headless,
            int_default_timeout=int_default_timeout_miliseconds, 
            bool_incognito=True,
            bool_minimized_window=bool_minimized_window,
        )
        self.date_ref = date_ref or \
            self.cls_dates_br.add_working_days(self.cls_dates_current.curr_date(), -1)
        self.fstr_url_iter = fstr_url_iter
        self.int_pg_start = int_pg_start or 1
        self.int_pg_end = int_pg_end or self.get_number_pages()

    def run(
        self,
        dict_dtypes: dict[str, Union[str, int, float]],
        str_table_name: str,
        timeout: int = 30_000,
        str_fmt_dt: str = "DD/MM/YYYY",
        bool_insert_or_ignore: bool = False,
    ) -> Optional[pd.DataFrame]:
        """Run the ingestion process.
        
        If the database session is provided, the data is inserted into the database.
        Otherwise, the transformed DataFrame is returned.

        Parameters
        ----------
        timeout : Optional[Union[int, float, tuple[float, float], tuple[int, int]]], optional
            The timeout, by default (12.0, 21.0)
        bool_verify : bool, optional
            Whether to verify the SSL certificate, by default True
        bool_insert_or_ignore : bool, optional
            Whether to insert or ignore the data, by default False
        str_table_name : str, optional
            The name of the table, by default "br_anbima_data_debentures_available"
        bool_insert_or_ignore : bool, optional
            Whether to insert or ignore the data, by default False

        Returns
        -------
        Optional[pd.DataFrame]
            The transformed DataFrame.
        """
        list_ser: list[dict[str, Union[str, int, float]]] = []

        for i_pg in range(self.int_pg_start, self.int_pg_end + 1):
            url = self.fstr_url_iter.format(i_pg)
            self.cls_create_log.log_message(
                self.logger, 
                f"Processing page {i_pg} of {self.int_pg_end}; URL: {url}", 
                "info"
            )
            list_ = self.get_response(url=url, timeout=timeout)
            df_ = self.transform_data(
                dict_dtypes=dict_dtypes,
                list_instruments_infos=list_,
                url=url,
                int_pg=i_pg
            )
            df_ = self.standardize_dataframe(
                df_=df_, 
                date_ref=self.date_ref,
                dict_dtypes=dict_dtypes, 
                str_fmt_dt=str_fmt_dt,
                url=self.fstr_url_iter,
            )
            if self.cls_db:
                self.insert_table_db(
                    cls_db=self.cls_db, 
                    str_table_name=str_table_name, 
                    df_=df_, 
                    bool_insert_or_ignore=bool_insert_or_ignore
                )
            else:
                list_ser.extend(df_.to_dict("records"))
            sleep(self.int_wait_next_request_seconds)
        
        if not self.cls_db:
            return pd.DataFrame(list_ser)

    @backoff.on_exception(
        backoff.expo, 
        requests.exceptions.HTTPError, 
        max_time=60
    )
    def get_response(
        self, 
        url: str,
        timeout: int = 30_000
    ) -> list[list[Union[str, float, int]]]:
        """Return a list of response objects.

        Parameters
        ----------
        url : str
            The URL.
        timeout : Optional[Union[int, float, tuple[float, float], tuple[int, int]]], optional
            The timeout, by default (12.0, 21.0)
        
        Returns
        -------
        tuple[Union[str, int, float]]
            A list of tuples.
        """

        with self.cls_playwright_scraper.launch():
            if not self.cls_playwright_scraper.navigate(url, timeout=timeout):
                raise RuntimeError(f"Failed to navigate to URL: {self.fstr_url_iter}")
            self._trigger_data_infos_button()
            return self.parse_raw_file()

    def _trigger_data_infos_button(
        self, 
        json_steps: list[dict[str, Union[str, int, float]]], 
        target_content_selectors: Optional[list[str]] = None,
    ) -> None:
        """Trigger the data infos button.

        Parameters
        ----------
        json_steps : list[dict[str, Union[str, int, float]]]
            The JSON steps.
        
        Returns
        -------
        None
        """
        self.cls_playwright_scraper.trigger_strategies(
            json_steps=json_steps, 
            target_content_selectors=target_content_selectors
        )

    def parse_raw_file(
        self, 
        list_tups_xpaths: list[tuple[str, str]]
    ) -> list[list[Union[str, float, int]]]:
        """Parse the raw file content.
        
        Parameters
        ----------
        list_tups_xpaths : list[tuple[str, str]]
            The list of tuples.
        
        Returns
        -------
        list[dict[str, Union[str, int, float]]]
            The parsed JSON content.
        """
        list_lists: list[list[Union[str, float, int]]] = []

        for _, xpath in list_tups_xpaths:
            list_lists.append(self._get_elements(xpath=xpath))

        dict_ = dict(zip([key for key, _ in list_tups_xpaths], list_lists))
        self.cls_list_handler.validate_lists_same_length(dict_lists_data=dict_)
        
        return list_lists
    
    def _get_elements(self, xpath: str) -> list[Union[str, float, int]]:
        """Get elements from the page.
        
        Parameters
        ----------
        xpath : str
            The xpath to find elements.
        
        Returns
        -------
        list[Union[str, float, int]]
            The list of elements.
        """
        list_ = self.cls_playwright_scraper.get_elements(xpath)
        return [item['text'] for item in list_ if item.get('text')]

    @CacheManager.cache_value(key=lambda self: self._key_cache_file())
    def get_number_pages(self, timeout: int = 30_000) -> int:
        """Get the number of pages.
        
        Parameters
        ----------
        timeout : Optional[Union[int, float, tuple[float, float], tuple[int, int]]], optional
            The timeout, by default (12.0, 21.0)
        
        Returns
        -------
        int
            The number of pages.
        """
        with self.cls_playwright_scraper.launch():
            if not self.cls_playwright_scraper.navigate(self.fstr_url_iter.format(1), 
                                                        timeout=timeout):
                return 1
            
            xpath_pages_number: str = '//*[@id="pagination"]/div[3]/span/span'
            element = self.cls_playwright_scraper.get_element(xpath_pages_number, timeout=timeout)
            if element and element.get("text"):
                try:
                    text = element["text"].strip()
                    numbers = re.findall(r"\d+", text)
                    if numbers:
                        return int(numbers[-1])
                except (ValueError, AttributeError):
                    pass
            return 1
        
    def _key_cache_file(self) -> None:
        """Return the cache key.
        
        Returns
        -------
        str
            The cache key.
        """
        str_class_name = self.__class__.__name__
        return self.cls_str_handler.convert_case(str_class_name, "pascal", "lower_constant")
    
    def transform_data(
        self, 
        dict_dtypes: dict[str, Union[str, int, float]],
        list_instruments_infos: list[list[Union[str, float, int]]], 
        url: str, 
        int_pg: int,
    ) -> pd.DataFrame:
        """Transform a list of response objects into a DataFrame.
        
        Parameters
        ----------
        dict_dtypes : dict[str, Union[str, int, float]]
            The dictionary of data types.
        list_instruments_infos : list[list[Union[str, float, int]]]
            The tuple of instruments infos.
        url : str
            The URL.
        int_pg : int
            The page number.
        
        Returns
        -------
        pd.DataFrame
            The transformed DataFrame.
        """
        list_keys = dict_dtypes.keys()
        df_ =  pd.DataFrame(dict(zip(list_keys, list_instruments_infos)))
        df_["URL"] = url
        df_["PAGE"] = int_pg
        return df_