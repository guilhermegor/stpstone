"""Debentures.com.br OTC PVs scraping.

This module provides a class for ingesting OTC secondary market prices from debentures.com.br.

Notes
-----
[1] Metadata: https://www.debentures.com.br/exploreosnd/consultaadados/mercadosecundario/precosdenegociacao_f.asp?op_exc=Nada
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


class DebenturesComBrOTCPVs(ABCIngestionOperations):
    """Debentures.com.br OTC PVs.

    Notes
    -----
    [1] Metadata: https://www.debentures.com.br/exploreosnd/consultaadados/mercadosecundario/precosdenegociacao_f.asp?op_exc=Nada
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
        self.url = "https://www.debentures.com.br/exploreosnd/consultaadados/mercadosecundario/precosdenegociacao_e.asp?op_exc=Nada&emissor=&isin=&ativo=&dt_ini={}&dt_fim={}"

    def run(
        self,
        timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (12.0, 21.0),
        bool_verify: bool = True,
        bool_insert_or_ignore: bool = False,
        str_table_name: str = "br_debentures_com_br_otc_pvs",
    ) -> Optional[pd.DataFrame]:
        """Run the ingestion process.

        If the database session is provided, the data is inserted into the database.
        Otherwise, the transformed DataFrame is returned.

        Parameters
        ----------
        timeout : Optional[Union[int, float, tuple[float, float], tuple[int, int]]]
            The timeout, by default (12.0, 21.0).
        bool_verify : bool
            Whether to verify the SSL certificate, by default True.
        bool_insert_or_ignore : bool
            Whether to insert or ignore the data, by default False.
        str_table_name : str
            The name of the table, by default 'br_debentures_com_br_otc_pvs'.

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
                "DATA": "date",
                "EMISSOR": str,
                "CODIGO_DO_ATIVO": str,
                "ISIN": str,
                "QUANTIDADE": int,
                "NUMERO_DE_NEGOCIOS": int,
                "PU_MINIMO": float,
                "PU_MEDIO": float,
                "PU_MAXIMO": float,
                "PCT_PU_DA_CURVA": float,
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
        timeout : Optional[Union[int, float, tuple[float, float], tuple[int, int]]]
            The timeout, by default (12.0, 21.0).
        bool_verify : bool
            Verify the SSL certificate, by default True.

        Returns
        -------
        Union[Response, PlaywrightPage, SeleniumWebDriver]
            A list of response objects.
        """
        url = self.url.format(
            self.date_start.strftime("%Y%m%d"),
            self.date_end.strftime("%Y%m%d"),
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
                "DATA",
                "EMISSOR",
                "CODIGO_DO_ATIVO",
                "ISIN",
                "QUANTIDADE",
                "NUMERO_DE_NEGOCIOS",
                "PU_MINIMO",
                "PU_MEDIO",
                "PU_MAXIMO",
                "PCT_PU_DA_CURVA",
            ],
            header=None,
            decimal=",",
            thousands=".",
            encoding="latin-1",
            na_values=["-", "-  ", " ", "ND"],
            skipfooter=4,
            engine="python",
        )
