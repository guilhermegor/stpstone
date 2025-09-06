"""Bylaws of investment funds from brazilian SEC (CVM).

The CVM (Comissão Valores Mobiliários) is the Brazilian Securities Exchange Commission, 
which is responsible for regulating the securities market in Brazil.

The CVM has a website where you can find information about investment funds, such as their name, 
employer identification number (EIN/CNPJ), and other relevant details.
"""

from io import BytesIO
from logging import Logger
from time import sleep
from typing import Optional, Union

import backoff
import pandas as pd
from playwright.sync_api import Page as PlaywrightPage
import requests
from requests import Response, Session
from selenium.webdriver.remote.webdriver import WebDriver as SeleniumWebDriver

from stpstone._config.global_slots import YAML_INVESTMENT_FUNDS_BYLAWS
from stpstone.ingestion.abc.ingestion_abc import (
    ABCIngestionOperations,
    ContentParser,
    CoreIngestion,
)
from stpstone.utils.calendars.calendar_abc import DatesCurrent
from stpstone.utils.loggs.create_logs import CreateLog
from stpstone.utils.parsers.folders import DirFilesManagement


class InvestmentFunds(ABCIngestionOperations):
    """Class to fetch and process investment fund bylaws from brazilian SEC (CVM)."""
    
    def __init__(
        self, 
        list_apps: list[str], 
        int_pages_join: Optional[int] = 3,  
        logger: Optional[Logger] = None,
        cls_db: Optional[Session] = None,
    ) -> None:
        """Initialize the InvestmentFunds class.
        
        Parameters
        ----------
        list_apps : list[str]
            The list of apps.
        int_pages_join : Optional[int], optional
            The number of pages to join, by default 3.
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
        
        self.list_apps = list_apps
        self.int_pages_join = int_pages_join
        self.logger = logger
        self.cls_db = cls_db
        self.cls_dir_files_management = DirFilesManagement()
        self.cls_dates_current = DatesCurrent()
        self.cls_create_log = CreateLog()

    @backoff.on_exception(
        backoff.expo, 
        requests.exceptions.HTTPError, 
        max_time=60
    )
    def get_response(
        self, 
        timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (12.0, 21.0), 
        bool_verify: bool = True
    ) -> list[requests.Response]:
        """Return a list of response objects.

        Parameters
        ----------
        timeout : Optional[Union[int, float, tuple[float, float], tuple[int, int]]], optional
            The timeout, by default (12.0, 21.0)
        bool_verify : bool, optional
            Whether to verify the SSL certificate, by default True
        
        Returns
        -------
        list[requests.Response]
            A list of response objects.
        """
        fstr_url = r"https://web.cvm.gov.br/app/fundosweb/fundos/regulamento/obter/por/arquivo/{}"
        list_resp_req = list()
        for app in self.list_apps:
            self.cls_create_log.log_message(
                logger=self.logger,
                message=f"Requesting: {fstr_url.format(app)}",
                log_level="info"
            )
            resp_req = requests.get(fstr_url.format(app), timeout=timeout, verify=bool_verify)
            resp_req.raise_for_status()
            list_resp_req.append(resp_req)
            sleep(10)
        return list_resp_req
    
    def parse_raw_file(
        self, 
        resp_req: Union[Response, PlaywrightPage, SeleniumWebDriver]
    ) -> BytesIO:
        """Parse the raw file content.
        
        Parameters
        ----------
        resp_req : Union[Response, PlaywrightPage, SeleniumWebDriver]
            The response object.
        
        Returns
        -------
        BytesIO
            The parsed content.
        """
        return BytesIO(resp_req.content)
    
    def transform_data(self, list_resp_req: list[requests.Response]) -> pd.DataFrame:
        """Transform a list of response objects into a DataFrame.
        
        Parameters
        ----------
        list_resp_req : list[requests.Response]
            The response object.
        
        Returns
        -------
        pd.DataFrame
            The transformed DataFrame.
        """
        list_ser = list()

        for resp_req in list_resp_req:
            bytes_file = self.parse_raw_file(resp_req)
            df_ = self.pdf_docx_regex(
                bytes_file=bytes_file, 
                str_file_extension=self.cls_dir_files_management.get_last_file_extension(
                    file_path=resp_req.url
                ), 
                int_pages_join=self.int_pages_join, 
                dict_regex_patterns=YAML_INVESTMENT_FUNDS_BYLAWS["regex_patterns"]
            )
            df_["URL"] = resp_req.url
            list_ser.extend(df_.to_dict(orient="records"))

        return pd.DataFrame(list_ser)
    
    def run(
        self,
        timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (12.0, 21.0),
        bool_verify: bool = True,
        bool_insert_or_ignore: bool = False, 
        str_table_name: str = "br_cvm_investment_funds_bylaws",
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
            The name of the table, by default "br_cvm_investment_funds_bylaws"

        Returns
        -------
        Optional[pd.DataFrame]
            The transformed DataFrame.
        """
        list_resp_req = self.get_response(timeout=timeout, bool_verify=bool_verify)
        df_ = self.transform_data(list_resp_req)
        df_ = self.standardize_dataframe(
            df_=df_, 
            date_ref=self.cls_dates_current.curr_date(),
            dict_dtypes={
                "EVENT": str, 
                "MATCH_PATTERN": str, 
                "PATTERN_REGEX": str, 
                "URL": str
            }
        )
        if self.cls_db:
            self.insert_table_db(
                cls_db=self.cls_db, 
                str_table_name=str_table_name, 
                bool_insert_or_ignore=bool_insert_or_ignore,
                df_=df_
            )
        else:
            return df_