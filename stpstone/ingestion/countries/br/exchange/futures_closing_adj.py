"""B3 Futures Closing Adjustments Ingestion Class."""

from datetime import date
from io import StringIO
from logging import Logger
from typing import Optional, Union

import backoff
from lxml.html import HtmlElement
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
from stpstone.utils.parsers.dicts import HandlingDicts
from stpstone.utils.parsers.folders import DirFilesManagement
from stpstone.utils.parsers.html import HtmlHandler


class B3FuturesClosingAdj(ABCIngestionOperations):
    """B3 Futures Closing Adjustments Ingestion Class."""
    
    def __init__(
        self, 
        date_ref: Optional[date] = None, 
        logger: Optional[Logger] = None,
        cls_db: Optional[Session] = None,
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

        self.logger = logger
        self.cls_db = cls_db
        self.cls_dir_files_management = DirFilesManagement()
        self.cls_dates_current = DatesCurrent()
        self.cls_create_log = CreateLog()
        self.cls_dates_br = DatesBRAnbima()
        self.cls_html_handler = HtmlHandler()
        self.cls_dict_handler = HandlingDicts()
        self.date_ref = date_ref or \
            self.cls_dates_br.add_working_days(self.cls_dates_current.curr_date(), -1)
        self.url = "https://www2.bmf.com.br/pages/portal/bmfbovespa/lumis/" \
            + "lum-ajustes-do-pregao-ptBR.asp?dData1={}".format(
                self.date_ref.strftime("%d/%m/%Y").replace("/", "%2F")
            )

    @backoff.on_exception(
        backoff.expo, 
        requests.exceptions.HTTPError, 
        max_time=60
    )
    def get_response(
        self, 
        timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (12.0, 21.0), 
        bool_verify: bool = True
    ) -> Union[Response, PlaywrightPage, SeleniumWebDriver]:
        """Return a list of response objects.

        Parameters
        ----------
        timeout : Optional[Union[int, float, tuple[float, float], tuple[int, int]]], optional
            The timeout, by default (12.0, 21.0)
        bool_verify : bool, optional
            Verify the SSL certificate, by default True
        
        Returns
        -------
        Union[Response, PlaywrightPage, SeleniumWebDriver]
            A list of response objects.
        """
        resp_req = requests.get(
            self.url, 
            timeout=timeout, 
            verify=bool_verify, 
            headers={
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7', # noqa ANN401: typing.Any not allowed
                'Accept-Language': 'en-US,en;q=0.9,pt;q=0.8,es;q=0.7',
                'Cache-Control': 'max-age=0',
                'Connection': 'keep-alive',
                'Content-Type': 'application/x-www-form-urlencoded',
                'Origin': 'https://www2.bmf.com.br',
                'Referer': 'https://www2.bmf.com.br/pages/portal/bmfbovespa/lumis/lum-ajustes-do-pregao-ptBR.asp', # noqa ANN401: typing.Any not allowed
                'Sec-Fetch-Dest': 'iframe',
                'Sec-Fetch-Mode': 'navigate',
                'Sec-Fetch-Site': 'same-origin',
                'Sec-Fetch-Storage-Access': 'active',
                'Sec-Fetch-User': '?1',
                'Upgrade-Insecure-Requests': '1',
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36', # noqa ANN401: typing.Any not allowed
                'sec-ch-ua': '"Chromium";v="134", "Not:A-Brand";v="24", "Google Chrome";v="134"',
                'sec-ch-ua-mobile': '?0',
                'sec-ch-ua-platform': '"Windows"',
                'Cookie': 'TS01ccf8f5=016e3b076f73e3ff43b9565404d03194f5ab74b53c86213c8b1b408be78d1e07d8f0830993a0549c0cc628fdfa26bf846921991d40; dtCookie=v_4_srv_33_sn_60B97404651F24E80F2D122331536B19_perc_100000_ol_0_mul_1_app-3Ae44446475f923f8e_1_rcs-3Acss_0; ASPSESSIONIDSWRCQSST=FKMJAJLANNIAJJDFOGOFKEHB; TS01871345=011d592ce193cf318748faa8dd8821e52cf3b2181909c54e849764f202b6a7b0b7af23fdcc6034eebc45e59d4969d7dfc5ffebb7c3' # noqa ANN401: typing.Any not allowed
            }
        )
        resp_req.raise_for_status()
        return resp_req
    
    def parse_raw_file(
        self, 
        resp_req: Union[Response, PlaywrightPage, SeleniumWebDriver]
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
        return self.cls_html_handler.lxml_parser(resp_req=resp_req)
    
    def transform_data(
        self, 
        html_root: HtmlElement
    ) -> pd.DataFrame:
        """Transform a list of response objects into a DataFrame.
        
        Parameters
        ----------
        html_root : HtmlElement
            The HTML root element
        
        Returns
        -------
        pd.DataFrame
            The transformed DataFrame.
        """
        xpath_td: str = '//*[@id="tblDadosAjustes"]/tbody/tr/td'
        list_th: list[str] = [
            "MERCADORIA", 
            "VENCIMENTO", 
            "PRECO_DE_AJUSTE_ANTERIOR",
            "PRECO_DE_AJUSTE_ATUAL",
            "VARIACAO", 
            "VALOR_DO_AJUSTE_POR_CONTRATO_BRL",
        ]

        list_td: list[Union[str, float]] = [
            "" if x.text is None
            else x.text.replace("\xa0", "")
                .replace(".", "")
                .replace(",", ".")
                .strip()
            for x in self.cls_html_handler.lxml_xpath(html_root, xpath_td)
        ]

        list_ser = self.cls_dict_handler.pair_headers_with_data(
            list_th, 
            list_td
        )

        return pd.DataFrame(list_ser)
    
    def run(
        self,
        timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (12.0, 21.0),
        bool_verify: bool = True,
        bool_insert_or_ignore: bool = False, 
        str_table_name: str = "br_b3_futures_closing_adjustments"
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
            The name of the table, by default "br_b3_futures_closing_adjustments"

        Returns
        -------
        Optional[pd.DataFrame]
            The transformed DataFrame.
        """
        resp_req = self.get_response(timeout=timeout, bool_verify=bool_verify)
        html_root = self.parse_raw_file(resp_req)
        df_ = self.transform_data(html_root=html_root)
        df_ = self.standardize_dataframe(
            df_=df_, 
            date_ref=self.date_ref,
            dict_dtypes={
                "MERCADORIA": str,
                "VENCIMENTO": "category",
                "PRECO_DE_AJUSTE_ANTERIOR": float,
                "PRECO_DE_AJUSTE_ATUAL": float,
                "VARIACAO": float,
                "VALOR_DO_AJUSTE_POR_CONTRATO_BRL": float
            }, 
            str_fmt_dt="YYYY-MM-DD",
            url=self.url,
            dict_fillna_strt={
                "MERCADORIA": "ffill",
            }
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