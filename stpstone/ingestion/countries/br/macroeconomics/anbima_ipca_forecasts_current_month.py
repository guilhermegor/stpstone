"""Anbima IPCA Forecasts for the Current Month ingestion module."""

from logging import Logger
from typing import Optional, Union

import pandas as pd
from requests import Session

from stpstone.ingestion.abc.ingestion_abc import ABCIngestionOperations
from stpstone.ingestion.countries.br.macroeconomics._anbima_ipca_core import AnbimaIPCACore
from stpstone.utils.webdriver_tools.playwright_wd import PlaywrightScraper


class AnbimaIPCAForecastsCurrentMonth(AnbimaIPCACore, ABCIngestionOperations):
    """Anbima IPCA Forecasts for the Current Month."""

    def __init__(
        self,
        logger: Optional[Logger] = None,
        cls_db: Optional[Session] = None,
        url: str = "https://www.anbima.com.br/pt_br/informar/estatisticas/precos-e-indices/projecao-de-inflacao-gp-m.htm",
    ) -> None:
        """Initialize the Anbima IPCA Forecasts for the Current Month.

        Parameters
        ----------
        logger : Optional[Logger]
            The logger, by default None.
        cls_db : Optional[Session]
            The database session, by default None.
        url : str
            The URL, by default the ANBIMA IPCA projection page.

        Returns
        -------
        None
        """
        super().__init__(logger=logger, cls_db=cls_db, url=url)

    def run(
        self,
        timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (12.0, 21.0),
        bool_verify: bool = True,
        bool_insert_or_ignore: bool = False,
        str_table_name: str = "br_anbima_ipca_forecasts_current_month",
    ) -> Optional[pd.DataFrame]:
        """Run the Anbima IPCA Forecasts for the Current Month.

        Parameters
        ----------
        timeout : Optional[Union[int, float, tuple[float, float], tuple[int, int]]]
            The timeout, by default (12.0, 21.0).
        bool_verify : bool
            Verify the SSL certificate, by default True.
        bool_insert_or_ignore : bool
            Insert or ignore the data, by default False.
        str_table_name : str
            The table name, by default "br_anbima_ipca_forecasts_current_month".

        Returns
        -------
        Optional[pd.DataFrame]
            The DataFrame.
        """
        return super().run(
            dict_dtypes={
                "MES_COLETA": str,
                "DATA": "date",
                "PROJECAO_PCT": float,
                "DATA_VALIDADE": "date",
            },
            timeout=timeout,
            bool_verify=bool_verify,
            bool_insert_or_ignore=bool_insert_or_ignore,
            str_table_name=str_table_name,
        )

    def transform_data(self, scraper_playwright: PlaywrightScraper) -> pd.DataFrame:
        """Transform a list of response objects into a DataFrame.

        Parameters
        ----------
        scraper_playwright : PlaywrightScraper
            The PlaywrightScraper.

        Returns
        -------
        pd.DataFrame
            The transformed DataFrame.
        """
        return super().transform_data(
            xpath_current_month_forecasts="""
            //*[@id="profile"]/div/div[1]/table/tbody/tr[position()>1]/td
            """,
            list_th=["MES_COLETA", "DATA", "PROJECAO_PCT", "DATA_VALIDADE"],
            scraper_playwright=scraper_playwright,
        )
