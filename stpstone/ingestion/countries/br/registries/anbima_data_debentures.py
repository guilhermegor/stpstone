"""Anbima Data Debentures."""

from datetime import date
from logging import Logger
from typing import Optional, Union

import pandas as pd
from requests import Session

from stpstone.ingestion.countries.br.registries.anbima_data_core import (
    AnbimaDataCoreInstrumentsAvailable,
)


class AnbimaDataDebenturesAvailable(AnbimaDataCoreInstrumentsAvailable):
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