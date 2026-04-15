"""Implementation of BCB Banks Codes Compensation ingestion."""

from datetime import date
from io import BytesIO
from logging import Logger
from typing import Optional, Union

import backoff
import pandas as pd
import pdfplumber
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


class BCBBanksCodesCompensation(ABCIngestionOperations):
    """BCB Banks Codes Compensation ingestion class."""
    
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
        self.cls_handling_dicts = HandlingDicts()
        self.date_ref = date_ref or \
            self.cls_dates_br.add_working_days(self.cls_dates_current.curr_date(), -1)
        self.url = "https://www.bcb.gov.br/Fis/CODCOMPE/Tabela.pdf"
    
    def run(
        self,
        timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (12.0, 21.0),
        bool_verify: bool = True,
        bool_insert_or_ignore: bool = False, 
        str_table_name: str = "BR_BCB_BANKS_CODES_COMPENSATION"
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
            The name of the table, by default "BR_BCB_BANKS_CODES_COMPENSATION"

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
            date_ref=self.date_ref,
            dict_dtypes={
                "COD_COMPENSACAO": str,
                "CNPJ": str, 
                "NOME_INSTITUICAO": str, 
                "SEGMENTO": str
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
        """Return a response object with the PDF content.

        Parameters
        ----------
        timeout : Optional[Union[int, float, tuple[float, float], tuple[int, int]]], optional
            The timeout, by default (12.0, 21.0)
        bool_verify : bool
            Verify the SSL certificate, by default True
        
        Returns
        -------
        Union[Response, PlaywrightPage, SeleniumWebDriver]
            The response object.
        """
        resp_req = requests.get(self.url, timeout=timeout, verify=bool_verify)
        resp_req.raise_for_status()
        return resp_req
    
    def parse_raw_file(
        self, 
        resp_req: Union[Response, PlaywrightPage, SeleniumWebDriver]
    ) -> BytesIO:
        """Parse the raw PDF file content.
        
        Parameters
        ----------
        resp_req : Union[Response, PlaywrightPage, SeleniumWebDriver]
            The response object.
        
        Returns
        -------
        BytesIO
            The parsed PDF content as BytesIO.

        Raises
        ------
        AttributeError
            If the response object does not have the 'content' attribute.
        """
        if hasattr(resp_req, 'content'):
            return BytesIO(resp_req.content)
        else:
            raise AttributeError(
                f"Object of type {type(resp_req).__name__} has no attribute 'content'"
            )
    
    def pdf_doc_tables_response(self, bytes_pdf: BytesIO) -> pd.DataFrame:
        """Extract tables from PDF and convert to DataFrame.
        
        Parameters
        ----------
        bytes_pdf : BytesIO
            The PDF content as BytesIO.
        
        Returns
        -------
        pd.DataFrame
            The extracted data as DataFrame.
        """
        list_ser = list()
        with pdfplumber.open(bytes_pdf) as pdf:
            for page in pdf.pages:
                list_ = page.extract_tables()
                if list_ and len(list_[0]) > 1:
                    list_ser.extend(
                        self.cls_handling_dicts.pair_keys_with_values(
                            list_[0][0], 
                            list_[0][1:]
                        )
                    )
        return pd.DataFrame(list_ser)
    
    def transform_data(
        self, 
        file: BytesIO
    ) -> pd.DataFrame:
        """Transform PDF content into a DataFrame.
        
        Parameters
        ----------
        file : BytesIO
            The parsed PDF content.
        
        Returns
        -------
        pd.DataFrame
            The transformed DataFrame.
        """
        df_ = self.pdf_doc_tables_response(bytes_pdf=file)
        
        df_.columns = (df_.columns
                       .str.replace('\n', '', regex=False)
                       .str.replace('\r', '', regex=False)
                       .str.strip()
                       .str.upper()
                       .str.replace(' ', '_'))
        
        column_mapping = {
            'COD_COMPENSAÇÃO': 'COD_COMPENSACAO',
            'CODCOMPENSAÇÃO': 'COD_COMPENSACAO',
            'NOME_INSTITUIÇÃO': 'NOME_INSTITUICAO',
            'NOME_INSTITUICAO': 'NOME_INSTITUICAO'
        }
        df_ = df_.rename(columns=column_mapping)
        
        str_cols = df_.select_dtypes(include=['object']).columns
        for col in str_cols:
            if df_[col].dtype == 'object':
                df_[col] = df_[col].astype(str).str.strip()
                
        df_ = df_.dropna(how='all')
        
        return df_