"""Implementation of ingestion instance."""

from datetime import date
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


class BCBOlindaAnnualMarketExpectations(ABCIngestionOperations):
    """BCB Olinda Annual Market Expectations."""

    def __init__(
        self,
        date_ref: Optional[date] = None,
        logger: Optional[Logger] = None,
        cls_db: Optional[Session] = None,
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
        self.url = (
            "https://olinda.bcb.gov.br/olinda/servico/Expectativas/versao/v1/odata/"
            "ExpectativasMercadoAnuais?$top=100000&$orderby=Data%20desc&$format=json"
            "&$select=Indicador,IndicadorDetalhe,Data,DataReferencia,Media,Mediana,"
            "Minimo,Maximo,numeroRespondentes"
        )

    def run(
        self,
        timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (12.0, 21.0),
        bool_verify: bool = True,
        bool_insert_or_ignore: bool = False,
        str_table_name: str = "br_bcb_olinda_annual_market_expectations",
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
            The name of the table, by default "br_bcb_olinda_annual_market_expectations".

        Returns
        -------
        Optional[pd.DataFrame]
            The transformed DataFrame.
        """
        resp_req = self.get_response(timeout=timeout, bool_verify=bool_verify)
        json_ = self.parse_raw_file(resp_req)
        df_ = self.transform_data(json_=json_)
        df_ = self.standardize_dataframe(
            df_=df_,
            date_ref=self.date_ref,
            dict_dtypes={
                "INDICADOR": str,
                "INDICADOR_DETALHE": str,
                "DATA": "date",
                "DATA_REFERENCIA": int,
                "MEDIA": float,
                "MEDIANA": float,
                "MINIMO": float,
                "MAXIMO": float,
                "NUMERO_RESPONDENTES": int,
            },
            str_fmt_dt="YYYY-MM-DD",
            url=self.url,
            cols_from_case="camel",
            cols_to_case="upper_constant",
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
        resp_req = requests.get(self.url, timeout=timeout, verify=bool_verify)
        resp_req.raise_for_status()
        return resp_req

    def parse_raw_file(
        self,
        resp_req: Union[Response, PlaywrightPage, SeleniumWebDriver],
    ) -> list[dict[str, Union[str, int, float]]]:
        """Parse the raw file content.

        Parameters
        ----------
        resp_req : Union[Response, PlaywrightPage, SeleniumWebDriver]
            The response object.

        Returns
        -------
        list[dict[str, Union[str, int, float]]]
            The parsed content.
        """
        return resp_req.json()["value"]

    def transform_data(
        self,
        json_: list[dict[str, Union[str, int, float]]],
    ) -> pd.DataFrame:
        """Transform a list of response objects into a DataFrame.

        Parameters
        ----------
        json_ : list[dict[str, Union[str, int, float]]]
            The parsed content.

        Returns
        -------
        pd.DataFrame
            The transformed DataFrame.
        """
        return pd.DataFrame(json_)
