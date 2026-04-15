"""IBGE Disclosure Economic Indicators."""

from datetime import date
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
from stpstone.utils.parsers.folders import DirFilesManagement
from stpstone.utils.parsers.html import HtmlHandler


class IBGEDisclosureEconomicIndicators(ABCIngestionOperations):
    """IBGE Disclosure Economic Indicators."""
    
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
        self.cls_html_handler = HtmlHandler()
        self.date_ref = date_ref or \
            self.cls_dates_br.add_working_days(self.cls_dates_current.curr_date(), -1)
        self.url = "https://www.ibge.gov.br/calendario-indicadores-novoportal.html"
    
    def run(
        self,
        timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (12.0, 21.0),
        bool_verify: bool = True,
        bool_insert_or_ignore: bool = False, 
        str_table_name: str = "br_ibge_disclosure_economic_indicators"
    ) -> Optional[pd.DataFrame]:
        """Run the ingestion process.
        
        If the database session is provided, the data is inserted into the database.
        Otherwise, the transformed DataFrame is returned.

        Parameters
        ----------
        timeout : Optional[Union[int, float, tuple[float, float], tuple[int, int]]], optional
            The timeout, by default (12.0, 21.0)
        bool_verify : bool
            Whether to verify the SSL certificate, by default True
        bool_insert_or_ignore : bool
            Whether to insert or ignore the data, by default False
        str_table_name : str
            The name of the table, by default "br_ibge_disclosure_economic_indicators"

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
                "DT_NEXT_RELEASE": "date",
                "REFERENCED_PERIOD": str, 
                "POOL_NAME": str,
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
        timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (12.0, 21.0), 
        bool_verify: bool = True
    ) -> Union[Response, PlaywrightPage, SeleniumWebDriver]:
        """Return a list of response objects.

        Parameters
        ----------
        timeout : Optional[Union[int, float, tuple[float, float], tuple[int, int]]], optional
            The timeout, by default (12.0, 21.0)
        bool_verify : bool
            Verify the SSL certificate, by default True
        
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
        resp_req: Union[Response, PlaywrightPage, SeleniumWebDriver]
    ) -> HtmlElement:
        """Parse the raw file content.
        
        Parameters
        ----------
        resp_req : Union[Response, PlaywrightPage, SeleniumWebDriver]
            The response object.
        
        Returns
        -------
        HtmlElement
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
            The parsed content.
        
        Returns
        -------
        pd.DataFrame
            The transformed DataFrame.
        """
        xpath_next_release: str = '//section[@class=" pure-u-1  pure-u-md-3-4 "]/div[@class="pure-u-1"]//span[@class="data-destaque"]/text()' # noqa E501: line too long
        xpath_pool_reference_period: str = '//section[@class=" pure-u-1  pure-u-md-3-4 "]/div[@class="pure-u-1"]//p[@class="box-data pure-u-7-24"]/small/text()' # noqa E501: line too long
        xpath_pool_name: str = '//section[@class=" pure-u-1  pure-u-md-3-4 "]/div[@class="pure-u-1"]//p[@class="titulo pure-u-17-24"]/text()' # noqa E501: line too long

        list_next_release = self.cls_html_handler.lxml_xpath(
            html_content=html_root,
            str_xpath=xpath_next_release,
        )
        list_pool_reference_period = self.cls_html_handler.lxml_xpath(
            html_content=html_root,
            str_xpath=xpath_pool_reference_period,
        )
        list_pool_name = self.cls_html_handler.lxml_xpath(
            html_content=html_root,
            str_xpath=xpath_pool_name,
        )

        df_ = pd.DataFrame({
            "DT_NEXT_RELEASE": list_next_release,
            "REFERENCED_PERIOD": [x.replace("Referencia: ", "")\
                                  .replace("Referência: ", "").strip() 
                      for x in list_pool_reference_period],
            "POOL_NAME": list_pool_name,
        })
        return df_