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
from stpstone.ingestion.countries.br.macroeconomics.bcb_olinda_currencies import (
    BCBOlindaCurrencies,
)
from stpstone.utils.calendars.calendar_abc import DatesCurrent
from stpstone.utils.calendars.calendar_br import DatesBRAnbima
from stpstone.utils.loggs.create_logs import CreateLog
from stpstone.utils.parsers.folders import DirFilesManagement


class BCBOlindaCurrenciesTS(ABCIngestionOperations):
    """BCB Olinda Currencies TS."""

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
            self.cls_dates_br.add_working_days(self.cls_dates_current.curr_date(), -30)
        self.date_end = date_end or \
            self.cls_dates_br.add_working_days(self.cls_dates_current.curr_date(), -1)
        self.fstr_url = (
            "https://olinda.bcb.gov.br/olinda/servico/PTAX/versao/v1/odata/"
            "CotacaoMoedaPeriodo(moeda=@moeda,dataInicial=@dataInicial,dataFinalCotacao="
            "@dataFinalCotacao)?@moeda='{}'&@dataInicial='{}'&@dataFinalCotacao='{}'&$top="
            "100&$format=json&$select=paridadeCompra,paridadeVenda,cotacaoCompra,"
            "cotacaoVenda,dataHoraCotacao,tipoBoletim"
        )

    def run(
        self,
        timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (12.0, 21.0),
        bool_verify: bool = True,
        bool_insert_or_ignore: bool = False,
        str_table_name: str = "br_bcb_olinda_currencies_ts",
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
            The name of the table, by default "br_bcb_olinda_currencies_ts".

        Returns
        -------
        Optional[pd.DataFrame]
            The transformed DataFrame.
        """
        list_ser: list[dict[str, Union[str, int, float]]] = []
        list_currencies: list[str] = self._currencies()

        for currency in list_currencies:
            url = self.fstr_url.format(
                currency,
                self.date_start.strftime("%m-%d-%Y"),
                self.date_end.strftime("%m-%d-%Y"),
            )
            resp_req = self.get_response(url=url, timeout=timeout, bool_verify=bool_verify)
            json_ = self.parse_raw_file(resp_req)
            df_ = self.transform_data(json_=json_)
            df_ = self.standardize_dataframe(
                df_=df_,
                date_ref=self.date_end,
                dict_dtypes={
                    "PARIDADE_COMPRA": float,
                    "PARIDADE_VENDA": float,
                    "COTACAO_COMPRA": float,
                    "COTACAO_VENDA": float,
                    "DATA_HORA_COTACAO": str,
                    "TIPO_BOLETIM": str,
                },
                str_fmt_dt="YYYY-MM-DD",
                url=url,
                cols_from_case="camel",
                cols_to_case="upper_constant",
            )
            df_["MOEDA"] = currency
            list_ser.extend(df_.to_dict(orient="records"))
        df_ = pd.DataFrame(list_ser)

        if self.cls_db:
            self.insert_table_db(
                cls_db=self.cls_db,
                str_table_name=str_table_name,
                df_=df_,
                bool_insert_or_ignore=bool_insert_or_ignore,
            )
        else:
            return df_

    def _currencies(
        self,
        timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (12.0, 21.0),
        bool_verify: bool = True,
    ) -> list[str]:
        """Return a list of currencies.

        Parameters
        ----------
        timeout : Optional[Union[int, float, tuple[float, float], tuple[int, int]]], optional
            The timeout, by default (12.0, 21.0).
        bool_verify : bool
            Verify the SSL certificate, by default True.

        Returns
        -------
        list[str]
            The list of currencies.
        """
        cls_bcb_olinda_currencies = BCBOlindaCurrencies(
            date_ref=self.date_end,
            logger=self.logger,
            cls_db=self.cls_db,
        )
        df_ = cls_bcb_olinda_currencies.run(timeout, bool_verify=bool_verify)
        return df_["SIMBOLO"].unique().tolist()

    @backoff.on_exception(
        backoff.expo,
        requests.exceptions.HTTPError,
        max_time=60,
    )
    def get_response(
        self,
        url: str,
        timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (12.0, 21.0),
        bool_verify: bool = True,
    ) -> Union[Response, PlaywrightPage, SeleniumWebDriver]:
        """Return a list of response objects.

        Parameters
        ----------
        url : str
            The URL.
        timeout : Optional[Union[int, float, tuple[float, float], tuple[int, int]]], optional
            The timeout, by default (12.0, 21.0).
        bool_verify : bool
            Verify the SSL certificate, by default True.

        Returns
        -------
        Union[Response, PlaywrightPage, SeleniumWebDriver]
            A list of response objects.
        """
        dict_headers = {
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",  # noqa E501: line too long
            "accept-language": "en-US,en;q=0.9,pt;q=0.8,es;q=0.7",
            "cache-control": "max-age=0",
            "priority": "u=0, i",
            "sec-ch-ua": '"Not)A;Brand";v="8", "Chromium";v="138", "Google Chrome";v="138"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"Linux"',
            "sec-fetch-dest": "document",
            "sec-fetch-mode": "navigate",
            "sec-fetch-site": "none",
            "sec-fetch-user": "?1",
            "upgrade-insecure-requests": "1",
            "user-agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36",  # noqa E501: line too long
            "Cookie": "JSESSIONID=00000a8R4vabgSlpTeY6TtlE9A7:1cn7m3fq4; BIGipServer~was_p_as3~was_p~pool_was_443_p=4275048876.47873.0000; TS013694c2=012e4f88b34db898ff3986b27881eb78507825f32ff9acda736e1d6e492db32cd815bbeba23e52ee205bf738fdf0e2fff3d326bee9; BIGipServer~was_p_as3~was_p~pool_was_443_p=4275048876.47873.0000; JSESSIONID=0000uL1SngKW858i5EZcQ-uqotJ:1dof89mke; TS013694c2=012e4f88b30d5bc0d002ffe4c3fb3bb998df95f3ce7fc8e3af1d8e0eb6f28d70ded3a4aad1a7c33208ad781942617da86b243e1824",  # noqa E501: line too long
        }
        resp_req = requests.get(url, timeout=timeout, verify=bool_verify,
                                headers=dict_headers)
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
