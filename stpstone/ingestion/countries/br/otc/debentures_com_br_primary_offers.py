"""Debentures.com.br Primary Offers scraping.

This module provides a class for ingesting primary offer volume data from debentures.com.br.

Notes
-----
[1] Metadata: https://www.debentures.com.br/exploreosnd/consultaadados/volume/volumeporperiodo_f.asp
[2] Special thanks to Rodrigo Prado (https://github.com/royopa) for helping to develop this class.
"""

from datetime import date
from io import StringIO
from logging import Logger
from typing import Optional, Union

import backoff
import pandas as pd
from playwright.sync_api import Page as PlaywrightPage
import requests
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
from stpstone.utils.parsers.folders import DirFilesManagement


class DebenturesComBrPrimaryOffers(ABCIngestionOperations):
    """Debentures.com.br Primary Offers.

    Notes
    -----
    [1] Metadata: https://www.debentures.com.br/exploreosnd/consultaadados/volume/volumeporperiodo_f.asp
    """

    def __init__(
        self,
        date_start: Optional[date] = None,
        date_end: Optional[date] = None,
        logger: Optional[Logger] = None,
        cls_db: Optional[Session] = None,
    ) -> None:
        """Initialize the ingestion class.

        Parameters
        ----------
        date_start : Optional[date]
            The start date, by default None.
        date_end : Optional[date]
            The end date, by default None.
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
        self.date_start = date_start or \
            self.cls_dates_br.add_working_days(self.cls_dates_current.curr_date(), -5)
        self.date_end = date_end or \
            self.cls_dates_br.add_working_days(self.cls_dates_current.curr_date(), -1)
        self.url = "https://www.debentures.com.br/exploreosnd/consultaadados/volume/volumeporperiodo_e.asp?op_exc=False&emissao=0&dt_ini={}%2F{}%2F{}&dt_fim={}%2F{}%2F{}&ICVM=&moeda=1&Submit3.x=22&Submit3.y=13"

    def run(
        self,
        timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (12.0, 21.0),
        bool_verify: bool = True,
        bool_insert_or_ignore: bool = False,
        str_table_name: str = "br_debentures_com_br_primary_offers",
    ) -> Optional[pd.DataFrame]:
        """Run the ingestion process.

        If the database session is provided, the data is inserted into the database.
        Otherwise, the transformed DataFrame is returned.

        Parameters
        ----------
        timeout : Optional[Union[int, float, tuple[float, float], tuple[int, int]]], optional
            The timeout, by default (12.0, 21.0).
        bool_verify : bool
            Whether to verify the SSL certificate, by default True.
        bool_insert_or_ignore : bool
            Whether to insert or ignore the data, by default False.
        str_table_name : str
            The name of the table, by default 'br_debentures_com_br_primary_offers'.

        Returns
        -------
        Optional[pd.DataFrame]
            The transformed DataFrame.
        """
        resp_req = self.get_response(timeout=timeout, bool_verify=bool_verify)
        file = self.parse_raw_file(resp_req)
        df_ = self.transform_data(file=file)
        df_ = self.standardize_dataframe(
            df_=df_,
            date_ref=self.date_end,
            dict_dtypes={
                "CODIGO_ATIVO": str,
                "EMISSOR": str,
                "SITUACAO": str,
                "DATA_EMISSAO": "date",
                "DATA_REGISTRO_SND": "date",
                "DATA_REGISTRO_CVM": "date",
                "VOLUME_MOEDA_EPOCA": float,
            },
            str_fmt_dt="DD/MM/YYYY",
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

    @backoff.on_exception(
        backoff.expo,
        requests.exceptions.HTTPError,
        max_time=60,
    )
    def get_response(
        self,
        timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (12.0, 21.0),
        bool_verify: bool = True,
    ) -> Union[Response, PlaywrightPage, SeleniumWebDriver]:
        """Return a list of response objects.

        Parameters
        ----------
        timeout : Optional[Union[int, float, tuple[float, float], tuple[int, int]]], optional
            The timeout, by default (12.0, 21.0).
        bool_verify : bool
            Verify the SSL certificate, by default True.

        Returns
        -------
        Union[Response, PlaywrightPage, SeleniumWebDriver]
            A list of response objects.
        """
        url = self.url.format(
            self.date_start.strftime("%d"),
            self.date_start.strftime("%m"),
            self.date_start.strftime("%Y"),
            self.date_end.strftime("%d"),
            self.date_end.strftime("%m"),
            self.date_end.strftime("%Y"),
        )
        resp_req = requests.get(url, timeout=timeout, verify=bool_verify)
        resp_req.raise_for_status()
        return resp_req

    def parse_raw_file(
        self,
        resp_req: Union[Response, PlaywrightPage, SeleniumWebDriver],
    ) -> StringIO:
        """Parse the raw file content.

        Parameters
        ----------
        resp_req : Union[Response, PlaywrightPage, SeleniumWebDriver]
            The response object.

        Returns
        -------
        StringIO
            The parsed content.
        """
        return self.get_file(resp_req=resp_req)

    def transform_data(
        self,
        file: StringIO,
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
        return pd.read_csv(
            file,
            sep="\t",
            skiprows=3,
            names=[
                "CODIGO_ATIVO",
                "EMISSOR",
                "SITUACAO",
                "DATA_EMISSAO",
                "DATA_REGISTRO_SND",
                "DATA_REGISTRO_CVM",
                "VOLUME_MOEDA_EPOCA",
            ],
            header=None,
            decimal=",",
            thousands=".",
            encoding="latin-1",
            na_values=["-", "-  ", " ", "ND"],
            skipfooter=4,
            engine="python",
        )
