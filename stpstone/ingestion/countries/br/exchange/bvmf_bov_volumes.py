"""BVMF BOV Negotiation Volumes.

This module provides an implementation of the BVMF BOV Negotiation Volumes ingestion class.
It inherits from the ABCIngestionOperations class and implements the run method.
"""

from datetime import date
from logging import Logger
from time import sleep
from typing import Optional, Union

import backoff
import bs4
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
from stpstone.utils.parsers.numbers import NumHandler
from stpstone.utils.parsers.str import StrHandler


class BVMFVBOVTradingVolumes(ABCIngestionOperations):
    """BVMF BOV Negotiation Volumes."""
    
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
        self.cls_str_handler = StrHandler()
        self.cls_num_handler = NumHandler()
        self.cls_dict_handler = HandlingDicts()
        self.date_ref = date_ref or \
            self.cls_dates_br.add_working_days(self.cls_dates_current.curr_date(), -7)
        self.url = "https://bvmf.bmfbovespa.com.br/sig/FormConsultaNegociacoes.asp?" \
            + "strTipoResumo=RES_NEGOCIACOES&strSocEmissora=B3SA&" \
            + "strDtReferencia={}".format(self.date_ref.strftime("%m/%Y")) \
            + "&strIdioma=P&intCodNivel=1&intCodCtrl=100"

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

        Raises
        ------
        ValueError
            If the response is empty.
        """
        bool_parsed_tables: bool = False
        int_max_retries: int = 5
        i : int = 0
        
        while not bool_parsed_tables:
            resp_req = requests.get(self.url, timeout=timeout, verify=bool_verify)
            resp_req.raise_for_status()
            html_root = self.parse_raw_file(resp_req=resp_req)
            len_tables = len(html_root.find_all("table"))
            if len_tables > 0:
                bool_parsed_tables = True
            elif i < int_max_retries:
                sleep(5)
                i += 1
            else:
                raise ValueError(
                    f"Failed to get response after {int_max_retries} attempts."
                )
        
        return resp_req
    
    def parse_raw_file(
        self, 
        resp_req: Union[Response, PlaywrightPage, SeleniumWebDriver]
    ) -> bs4.BeautifulSoup:
        """Parse the raw file content.
        
        Parameters
        ----------
        resp_req : Union[Response, PlaywrightPage, SeleniumWebDriver]
            The response object.
        
        Returns
        -------
        bs4.BeautifulSoup
            The parsed content.
        """
        return self.cls_html_handler.bs_parser(resp_req=resp_req)
    
    def transform_data(
        self, 
        html_root: bs4.BeautifulSoup
    ) -> pd.DataFrame:
        """Transform a list of response objects into a DataFrame.
        
        Parameters
        ----------
        html_root : bs4.BeautifulSoup
            The parsed content.
        
        Returns
        -------
        pd.DataFrame
            The transformed DataFrame.
        """
        list_th: list[str] = [
            "MERCADO", 
            "NEGOCIACOES", 
            "VOLUME_BRL", 
            "NEGOCIACOES_12M", 
            "VOLUME_BRL_12M",
        ]
        list_td: list[Union[str, float]] = []
        html_table = html_root.find_all("table")[11]
        for i, tr in enumerate(html_table.find_all("tr")):
            if i >= 2:
                list_td.extend([
                    self._td_parser(el)
                    for el in tr.find_all("td")
                ])

        list_ser = self.cls_dict_handler.pair_headers_with_data(
            list_headers=list_th, 
            list_data=list_td
        )
        return pd.DataFrame(list_ser)
    
    def _td_parser(self, el: bs4.element.Tag) -> str:
        """Table data parser.
        
        Parameters
        ----------
        el : bs4.element.Tag
            The element to parse.
        
        Returns
        -------
        str
            The parsed element.
        """
        return float(el.get_text()
                    .strip()
                    .replace(".", "")
                    .replace(",", ".")) \
            if self.cls_num_handler.is_numeric(
                self.cls_str_handler.remove_diacritics(el.get_text())
                    .strip()
                    .replace(".", "")
                    .replace(",", ".")
            ) else \
            self.cls_str_handler.remove_diacritics(el.get_text()) \
                .strip()\
                .replace(" de ", " ")\
                .replace(" do ", " ")\
                .replace(" a ", " ")\
                .replace(" e ", " ")\
                .replace(" - ", " ")\
                .replace("-", " ")\
                .replace(" / ", " ")\
                .replace(" ", "_")\
                .replace(".", "")\
                .replace(",", ".")\
                .replace("(", "")\
                .replace(")", "")\
                .replace("/", "")\
                .upper()

    def run(
        self,
        timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (12.0, 21.0),
        bool_verify: bool = True,
        bool_insert_or_ignore: bool = False, 
        str_table_name: str = "br_b3_volumes"
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
            The name of the table, by default "br_anbima_br_treasuries"

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
                "MERCADO": "str",
                "NEGOCIACOES": "int",
                "VOLUME_BRL": "float",
                "NEGOCIACOES_12M": "int",
                "VOLUME_BRL_12M": "float",
            }, 
            str_fmt_dt="YYYY-MM-DD",
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