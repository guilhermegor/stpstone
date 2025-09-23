"""Anbima Data Debentures."""

from datetime import date
from logging import Logger
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
from stpstone.ingestion.countries.br.registries.anbima_data_core import (
    AnbimaDataCore,
)
from stpstone.utils.calendars.calendar_abc import DatesCurrent
from stpstone.utils.calendars.calendar_br import DatesBRAnbima
from stpstone.utils.loggs.create_logs import CreateLog
from stpstone.utils.parsers.folders import DirFilesManagement
from stpstone.utils.parsers.lists import ListHandler
from stpstone.utils.parsers.str import StrHandler
from stpstone.utils.webdriver_tools.playwright_wd import PlaywrightScraper


class AnbimaDataDebenturesAvailable(AnbimaDataCore):
    """Anbima Data debentures available.
    
    Notes
    -----
    [1] Metadata: https://data.anbima.com.br/busca/debentures
    [2] bool_headless: Run browser in headless mode must be False because otherwise it is 
    triggered https://data.anbima.com.br/robo, which blocks the access.
    """
    
    def __init__(
        self,
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
        int_pg_start : Optional[int], optional
            The start page number, by default None
        int_pg_end : Optional[int], optional
            The end page number, by default None
        date_ref : Optional[date], optional
            The date of reference, by default None
        logger : Optional[Logger], optional
            The logger to use, by default None
        cls_db : Optional[Session], optional
            The database session, by default None
        int_default_timeout_miliseconds : int, optional
            The default timeout, by default 30_000
        int_wait_next_request_seconds : int, optional
            The wait time between requests, by default 10
        bool_headless : bool, optional
            Run browser in headless mode, by default False
        bool_minimized_window : bool, optional
            Run browser in minimized window, by default False
        bool_persist_cache : bool, optional
            Persist cache, by default True
        bool_reuse_cache : bool, optional
            Reuse cache, by default True
        int_days_cache_expiration : int, optional
            Days cache expiration, by default 1
        int_cache_ttl_days : int, optional
            Cache TTL days, by default 7
        path_cache_dir : Optional[str], optional
            Path to the cache directory, by default None
        
        Returns
        -------
        None
        """
        super().__init__(
            fstr_url_iter="https://data.anbima.com.br/busca/debentures?size=100&page={}",
            int_pg_start=int_pg_start,
            int_pg_end=int_pg_end,
            date_ref=date_ref,
            logger=logger,
            cls_db=cls_db,
            int_default_timeout_miliseconds=int_default_timeout_miliseconds,
            int_wait_next_request_seconds=int_wait_next_request_seconds,
            bool_headless=bool_headless,
            bool_minimized_window=bool_minimized_window,
            bool_persist_cache=bool_persist_cache,
            bool_reuse_cache=bool_reuse_cache,
            int_days_cache_expiration=int_days_cache_expiration,
            int_cache_ttl_days=int_cache_ttl_days,
            path_cache_dir=path_cache_dir,
        )

    def run(
        self,
        str_table_name: str = "br_anbima_data_debentures_available",
        timeout: int = 30_000,
        str_fmt_dt: str = "DD/MM/YYYY",
        bool_insert_or_ignore: bool = False, 
    ) -> Optional[pd.DataFrame]:
        """Run the ingestion process.
        
        If the database session is provided, the data is inserted into the database.
        Otherwise, the transformed DataFrame is returned.

        Parameters
        ----------
        str_table_name : str, optional
            The name of the table, by default "br_anbima_data_debentures_available"
        timeout : Optional[Union[int, float, tuple[float, float], tuple[int, int]]], optional
            The timeout, by default (12.0, 21.0)
        str_fmt_dt : str, optional
            The format of the date, by default "DD/MM/YYYY"
        bool_insert_or_ignore : bool, optional
            Whether to insert or ignore the data, by default False

        Returns
        -------
        Optional[pd.DataFrame]
            The transformed DataFrame.
        """
        return super().run(
            dict_dtypes={
                "CODE": str,
                "ISSUER": str,
                "YIELD": str,
                "MATURITY_DATE": "date",
                "DURATION": str,
                "SECTOR": str,
                "ISSUE_DATE": "date",
                "FACE_VALUE": str,
                "INDICATIVE_PRICE": str,
                "URL": str, 
                "PAGE": int,
            },
            str_table_name=str_table_name,
            timeout=timeout,
            str_fmt_dt=str_fmt_dt,
            bool_insert_or_ignore=bool_insert_or_ignore,
        )

    def _trigger_data_infos_button(
        self, 
        json_steps: list[dict[str, Union[str, int, float]]] = [
            {
                "type": "setViewport",
                "width": 1905,
                "height": 1080,
                "deviceScaleFactor": 1,
                "isMobile": "false",
                "hasTouch": "false",
                "isLandscape": "false", 
                "description": "Set viewport",
            },
            {
                "type": "navigate",
                "url": "https://data.anbima.com.br/busca/debentures?size=100&page=2",
                "assertedEvents": [
                    {
                        "type": "navigation",
                        "url": "https://data.anbima.com.br/busca/debentures?size=100&page=2",
                        "title": "Resultado de busca em debêntures | ANBIMA Data"
                    }
                ], 
                "description": "Navigate to URL",
            },
            {
                "type": "click",
                "target": "main",
                "selectors": [
                    [
                        "aria/Características",
                        "aria/[role=\"generic\"]"
                    ],
                    [
                        "div.BuscaDebentures_busca-tools--switch__UX80q button.anbima-ui-button--primary > span"
                    ],
                    [
                        "xpath///*[@id=\"__next\"]/main/div/div/div/div/div[1]/div[2]/div[2]/div/button[2]/span"
                    ],
                    [
                        "pierce/div.BuscaDebentures_busca-tools--switch__UX80q button.anbima-ui-button--primary > span"
                    ],
                    [
                        "text/Características"
                    ]
                ],
                "offsetY": 3,
                "offsetX": 37.78125, 
                "description": "Click on caracteristicas button",
            }
        ],
        target_content_selectors: Optional[list[str]] = [
            '//*[@id="item-nome-0"]',
            '//*[@id="debentures-item-emissor-0"]',
            '//div[contains(@class, "debentures-item")]', 
            '//*[@id="__next"]/main/div/div/div/div/div[1]/div[2]/div[2]/div/button[2]/span',
        ]
    ) -> None:
        """Trigger the data infos button.
        
        Parameters
        ----------
        json_steps : list[dict[str, Union[str, int, float]]]
            The JSON steps.
        
        target_content_selectors : Optional[list[str]], optional
            The target content selectors.
        
        Returns
        -------
        None
        """
        super()._trigger_data_infos_button(
            json_steps=json_steps, target_content_selectors=target_content_selectors)

    def parse_raw_file(
        self, 
        list_xpaths: list[tuple[str, str]] = [
            ("xpath_codes", '//a[contains(@id, "item-title")]'),
            ("xpath_issuers", '//*[contains(@id, "debentures-item-emissor-")]/dd'),
            ("xpath_yield", '//*[contains(@id, "debentures-item-remuneracao-")]/dd'),
            ("xpath_maturity_date", '//*[contains(@id, "debentures-item-data-vencimento-")]/dd'),
            ("xpath_duration", '//*[contains(@id, "debentures-item-duration-")]/dd'),
            ("xpath_sector", '//*[contains(@id, "debentures-item-setor-")]/dd'),
            ("xpath_issue_date", '//*[contains(@id, "debentures-item-data-emissao-")]/dd'),
            ("xpath_face_value", '//*[contains(@id, "debentures-item-pu-par")]/dd'),
            ("xpath_indicative_price", '//*[contains(@id, "debentures-item-pu-indicativo-")]/dd'),
        ]
    ) -> pd.DataFrame:
        """Parse the raw file.
        
        Parameters
        ----------
        list_xpaths : list[tuple[str, str]], optional
            The list of xpaths.
        
        Returns
        -------
        pd.DataFrame
            The parsed DataFrame.
        """
        return super().parse_raw_file(list_xpaths)


class AnbimaDataDebenturesInfos(AnbimaDataCore):
    """Anbima Data debentures available.
    
    Notes
    -----
    [1] Metadata: https://data.anbima.com.br/busca/debentures
    [2] bool_headless: Run browser in headless mode must be False because otherwise it is 
    triggered https://data.anbima.com.br/robo, which blocks the access.
    """
    
    def __init__(
        self,
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
        int_pg_start : Optional[int], optional
            The start page number, by default None
        int_pg_end : Optional[int], optional
            The end page number, by default None
        date_ref : Optional[date], optional
            The date of reference, by default None
        logger : Optional[Logger], optional
            The logger to use, by default None
        cls_db : Optional[Session], optional
            The database session, by default None
        int_default_timeout_miliseconds : int, optional
            The default timeout, by default 30_000
        int_wait_next_request_seconds : int, optional
            The wait time between requests, by default 10
        bool_headless : bool, optional
            Run browser in headless mode, by default False
        bool_minimized_window : bool, optional
            Run browser in minimized window, by default False
        bool_persist_cache : bool, optional
            Persist cache, by default True
        bool_reuse_cache : bool, optional
            Reuse cache, by default True
        int_days_cache_expiration : int, optional
            Days cache expiration, by default 1
        int_cache_ttl_days : int, optional
            Cache TTL days, by default 7
        path_cache_dir : Optional[str], optional
            Path to the cache directory, by default None
        
        Returns
        -------
        None
        """
        super().__init__(
            fstr_url_iter="https://data.anbima.com.br/busca/debentures?size=100&page={}",
            int_pg_start=int_pg_start,
            int_pg_end=int_pg_end,
            date_ref=date_ref,
            logger=logger,
            cls_db=cls_db,
            int_default_timeout_miliseconds=int_default_timeout_miliseconds,
            int_wait_next_request_seconds=int_wait_next_request_seconds,
            bool_headless=bool_headless,
            bool_minimized_window=bool_minimized_window,
            bool_persist_cache=bool_persist_cache,
            bool_reuse_cache=bool_reuse_cache,
            int_days_cache_expiration=int_days_cache_expiration,
            int_cache_ttl_days=int_cache_ttl_days,
            path_cache_dir=path_cache_dir,
        )

    def run(
        self,
        str_table_name: str = "br_anbima_data_debentures_infos",
        timeout: int = 30_000,
        str_fmt_dt: str = "DD/MM/YYYY",
        bool_insert_or_ignore: bool = False, 
    ) -> Optional[pd.DataFrame]:
        """Run the ingestion process.
        
        If the database session is provided, the data is inserted into the database.
        Otherwise, the transformed DataFrame is returned.

        Parameters
        ----------
        str_table_name : str, optional
            The name of the table, by default "br_anbima_data_debentures_infos"
        timeout : Optional[Union[int, float, tuple[float, float], tuple[int, int]]], optional
            The timeout, by default (12.0, 21.0)
        str_fmt_dt : str, optional
            The format of the date, by default "DD/MM/YYYY"
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
                dict_dtypes={
                    "NUMBER_SERIES": str,
                    "YIELD": str,
                    "START_DATE_ACCRUAL": str, 
                    "UNIT_DAYS_YEAR": str, 
                    "SERIES_ISSUED_AMOUNT_QUANTITY": str,
                    "SERIES_ISSUED_AMONUT_NOTIONAL": str, 
                    "PAR_VALUE_ISSUANCE_VNE": str,
                    "PAR_VALUE_VNA": str, 
                    "QTY_FREE_FLOAT_B3": str,
                    "NOTIONAL_FREE_FLOAT_B3": str,
                    "DATE_ISSUANCE": "date", 
                    "DATE_MATURITY": "date",
                    "NEXT_RESET_DATE": str, 
                    "TERM_ISSUE": str, 
                    "TIME_TO_MATURITY": str,
                    "IS_EARLY_REDEMPTION": str, 
                    "ISIN": str, 
                    "DATE_NEXT_SCHEDULED_EVENT": str, 
                    "IS_LAW_12_431": str, 
                    "LEGAL_ARTICLE": str,
                    "NUMBER_ISSUANCE": str, 
                    "ISSUER": str, 
                    "SECTOR": str, 
                    "EIN_NUMBER_CNPJ": str, 
                    "ISSUANCE_NOTIONAL": str, 
                    "ISSUANCE_QUANTITY": str, 
                    "ISSUANCE_FREE_FLOAT_B3": str,
                    "LEAD_COORDINATOR": str, 
                    "COORDINATORS": str,
                    "TRUSTEE": str,
                    "LEAD_BANK": str,
                    "WARRANTY": str,
                },
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

    def _trigger_data_infos_button(
        self, 
        json_steps: list[dict[str, Union[str, int, float]]] = [
            {
                "type": "setViewport",
                "width": 1643,
                "height": 959,
                "deviceScaleFactor": 1,
                "isMobile": "false",
                "hasTouch": "false",
                "isLandscape": "false", 
                "description": "Set viewport",
            },
            {
                "type": "navigate",
                "url": "https://data.anbima.com.br/debentures/AFOZ11/caracteristicas",
                "assertedEvents": [
                    {
                        "type": "navigation",
                        "url": "https://data.anbima.com.br/debentures/AFOZ11/caracteristicas",
                        "title": "AFOZ11 | ANBIMA Data"
                    }
                ], 
                "description": "Navigate to URL",
            },
            {
                "type": "click",
                "target": "main",
                "selectors": [
                    [
                        "aria/CARACTERÍSTICAS"
                    ],
                    [
                        "div.detalhes-ativo__tabs li:nth-of-type(1) > a"
                    ],
                    [
                        "xpath///*[@id=\"1\"]"
                    ],
                    [
                        "pierce/div.detalhes-ativo__tabs li:nth-of-type(1) > a"
                    ],
                    [
                        "text/características"
                    ]
                ],
                "offsetY": 28,
                "offsetX": 69.171875, 
                "description": "Click on Caracateristicas button",
            }
        ],
        target_content_selectors: Optional[list[str]] = [
            '//li[@class="anbima-ui-nav__list__item "]/a[@id="1"]',
        ]
    ) -> None:
        """Trigger the data infos button.
        
        Parameters
        ----------
        json_steps : list[dict[str, Union[str, int, float]]]
            The JSON steps.
        
        target_content_selectors : Optional[list[str]], optional
            The target content selectors.
        
        Returns
        -------
        None
        """
        super()._trigger_data_infos_button(
            json_steps=json_steps, target_content_selectors=target_content_selectors)

    def parse_raw_file(
        self, 
        list_xpaths: list[tuple[str, str]] = [
            ("xpath_yield", '//*[@id="output__container--remuneracao"]/div/span'),
        ]
    ) -> pd.DataFrame:
        """Parse the raw file.
        
        Parameters
        ----------
        list_xpaths : list[tuple[str, str]], optional
            The list of xpaths.
        
        Returns
        -------
        pd.DataFrame
            The parsed DataFrame.
        """
        return super().parse_raw_file(list_xpaths)
