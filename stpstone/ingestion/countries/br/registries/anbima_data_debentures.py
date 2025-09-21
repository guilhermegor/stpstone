"""Implementation of ingestion instance."""

from datetime import date
from logging import Logger
from time import sleep
from typing import Optional, Union

import backoff
import pandas as pd
from playwright.sync_api import Page as PlaywrightPage
import requests
from requests import Session
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
from stpstone.utils.webdriver_tools.playwright_wd import PlaywrightScraper


class AnbimaDataDebenturesAvailable(ABCIngestionOperations):
    """Anbima Data debentures available."""
    
    def __init__(
        self, 
        int_pg_start: int,
        int_pg_end: Optional[int] = 1_000,
        date_ref: Optional[date] = None, 
        logger: Optional[Logger] = None,
        cls_db: Optional[Session] = None,
        int_default_timeout_miliseconds: int = 30_000,
        int_wait_next_request_seconds: int = 10,
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
        
        Returns
        -------
        None
        """
        super().__init__(cls_db=cls_db)
        CoreIngestion.__init__(self)
        ContentParser.__init__(self)

        self.int_pg_start = int_pg_start
        self.int_pg_end = int_pg_end
        self.logger = logger
        self.cls_db = cls_db
        self.int_wait_next_request_seconds = int_wait_next_request_seconds
        self.cls_dir_files_management = DirFilesManagement()
        self.cls_dates_current = DatesCurrent()
        self.cls_create_log = CreateLog()
        self.cls_dates_br = DatesBRAnbima()
        self.cls_playwright_scraper = PlaywrightScraper(
            bool_headless=True,
            int_default_timeout=int_default_timeout_miliseconds, 
            bool_incognito=True,
        )
        self.date_ref = date_ref or \
            self.cls_dates_br.add_working_days(self.cls_dates_current.curr_date(), -1)
        self.url = "https://data.anbima.com.br/busca/debentures?size=100&page={}"
    
    def run(
        self,
        timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (12.0, 21.0),
        bool_insert_or_ignore: bool = False, 
        str_table_name: str = "br_anbima_data_debentures_available"
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

        Returns
        -------
        Optional[pd.DataFrame]
            The transformed DataFrame.
        """
        list_ser: list[dict[str, Union[str, int, float]]] = []

        for i_pg in range(self.int_pg_start, self.int_pg_end + 1):
            url = self.url.format(i_pg)
            tup_ = self.get_response(url=url, timeout=timeout)
            df_ = self.transform_data(
                tup_debentures_infos=tup_,
                url=url,
                int_pg=i_pg
            )
            list_ser.extend(df_.to_dict("records"))
            sleep(self.int_wait_next_request_seconds)
        
        df_ = pd.DataFrame(list_ser)
        df_ = self.standardize_dataframe(
            df_=df_, 
            date_ref=self.date_ref,
            dict_dtypes={
                "CODE": str,
                "ISSUER": str,
                "YIELD": float,
                "MATURITY_DATE": "date",
                "DURATION": float,
                "SECTOR": str,
                "ISSUE_DATE": "date",
                "FACE_VALUE": float,
                "INDICATIVE_PRICE": float,
                "URL": str, 
                "PAGE": int,
            }, 
            str_fmt_dt="DD/MM/YYYY",
            url=self.url,
        )
        if self.cls_db:
            self.insert_table_db(
                cls_db=self.cls_db, 
                str_table_name=str_table_name, 
                df_=df_, 
                bool_insert_or_ignore=bool_insert_or_ignore
            )
        else:
            return df_

    @backoff.on_exception(
        backoff.expo, 
        requests.exceptions.HTTPError, 
        max_time=60
    )
    def get_response(
        self, 
        url: str,
        timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (12.0, 21.0)
    ) -> tuple[list[Union[str, float, int]], ...]:
        """Return a list of response objects.

        Parameters
        ----------
        timeout : Optional[Union[int, float, tuple[float, float], tuple[int, int]]], optional
            The timeout, by default (12.0, 21.0)
        
        Returns
        -------
        tuple[Union[str, int, float]]
            A list of tuples.
        """

        with self.cls_playwright_scraper.launch():
            if not self.cls_playwright_scraper.navigate(url, timeout=timeout):
                raise RuntimeError(f"Failed to navigate to URL: {self.url}")
            self._trigger_data_infos_button()
            return self.parse_raw_file()

    def _trigger_data_infos_button(self) -> None:
        """Trigger the data infos button.
        
        Returns
        -------
        None
        """
        self.cls_playwright_scraper.trigger_strategies(
            json_steps=[
                {
                    "type": "setViewport",
                    "width": 1643,
                    "height": 959,
                    "deviceScaleFactor": 1,
                    "isMobile": "false",
                    "hasTouch": "false",
                    "isLandscape": "false"
                },
                {
                    "type": "navigate",
                    "url": "https://data.anbima.com.br/busca/debentures?view=precos&page=0&q=&size=100",
                    "assertedEvents": [
                        {
                            "type": "navigation",
                            "url": "https://data.anbima.com.br/busca/debentures?view=precos&page=0&q=&size=100",
                            "title": "Resultado de busca em debêntures | ANBIMA Data"
                        }
                    ]
                },
                {
                    "type": "click",
                    "target": "main",
                    "selectors": [
                        [
                            "aria/Características"
                        ],
                        [
                            "div.BuscaDebentures_busca-tools--switch__UX80q button.anbima-ui-button--secondary"
                        ],
                        [
                            "xpath///*[@id=\"__next\"]/main/div/div/div/div/div[1]/div[2]/div[2]/div/button[2]"
                        ],
                        [
                            "pierce/div.BuscaDebentures_busca-tools--switch__UX80q button.anbima-ui-button--secondary"
                        ]
                    ],
                    "offsetY": 18,
                    "offsetX": 6.78125
                }
            ]
        )

    def parse_raw_file(self) -> tuple[list[Union[str, float, int]], ...]:
        """Parse the raw file content.
        
        Returns
        -------
        tuple[list[Union[str, float, int]], ...]
            The parsed content.
        """
        xpath_codes: str = '//*[@id="item-nome-0"]//text()'
        xpath_issuers: str = '//*[@id="debentures-item-emissor-0"]/dd//text()'
        xpath_yield: str = '//*[@id="debentures-item-remuneracao-0"]/dd//text()'
        xpath_maturity_date: str = '//*[@id="debentures-item-data-vencimento-0"]/dd//text()'
        xpath_duration: str = '//*[@id="debentures-item-duration-0"]/dd//text()'
        xpath_sector: str = '//*[@id="debentures-item-setor-0"]/dd//text()'
        xpath_issue_date: str = '//*[@id="debentures-item-setor-0"]/dd//text()'
        xpath_face_value: str = '//*[@id="debentures-item-pu-par0"]/dd//text()'
        xpath_indicative_price: str = '//*[@id="debentures-item-pu-indicativo-0"]/dd/span//text()'

        list_codes = self.cls_playwright_scraper.get_elements(xpath_codes)
        list_issuers = self.cls_playwright_scraper.get_elements(xpath_issuers)
        list_yield = self.cls_playwright_scraper.get_elements(xpath_yield)
        list_maturity_date = self.cls_playwright_scraper.get_elements(xpath_maturity_date)
        list_duration = self.cls_playwright_scraper.get_elements(xpath_duration)
        list_sector = self.cls_playwright_scraper.get_elements(xpath_sector)
        list_issue_date = self.cls_playwright_scraper.get_elements(xpath_issue_date)
        list_face_value = self.cls_playwright_scraper.get_elements(xpath_face_value)
        list_indicative_price = self.cls_playwright_scraper.get_elements(xpath_indicative_price)
        
        return list_codes, list_issuers, list_yield, list_maturity_date, list_duration, \
            list_sector, list_issue_date, list_face_value, list_indicative_price
    
    def transform_data(
        self, 
        tup_debentures_infos: list[tuple[Union[str, int, float]]], 
        url: str, 
        int_pg: int,
    ) -> pd.DataFrame:
        """Transform a list of response objects into a DataFrame.
        
        Parameters
        ----------
        file : StringIO
            The parsed content.
        
        Returns
        -------
        pd.DataFrame
            The transformed DataFrame.
        """
        df_ =  pd.DataFrame({
            "CODE": tup_debentures_infos[0], 
            "ISSUER": tup_debentures_infos[1],
            "YIELD": tup_debentures_infos[2],
            "MATURITY_DATE": tup_debentures_infos[3],
            "DURATION": tup_debentures_infos[4],
            "SECTOR": tup_debentures_infos[5],
            "ISSUE_DATE": tup_debentures_infos[6],
            "FACE_VALUE": tup_debentures_infos[7],
            "INDICATIVE_PRICE": tup_debentures_infos[8]
        })
        df_["URL"] = url
        df_["PAGE"] = int_pg
        return df_
