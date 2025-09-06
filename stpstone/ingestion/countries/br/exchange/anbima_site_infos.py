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


class IngestionConcreteClass(ABCIngestionOperations):
    """Ingestion concrete class."""
    
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
        self.date_ref = date_ref or \
            self.cls_dates_br.add_working_days(self.cls_dates_current.curr_date(), -1)

    @backoff.on_exception(
        backoff.expo, 
        requests.exceptions.HTTPError, 
        max_time=60
    )
    def get_response(
        self, 
        timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (12.0, 21.0)
    ) -> Union[Response, PlaywrightPage, SeleniumWebDriver]:
        """Return a list of response objects.

        Parameters
        ----------
        timeout : Optional[Union[int, float, tuple[float, float], tuple[int, int]]], optional
            The timeout, by default (12.0, 21.0)
        
        Returns
        -------
        list[requests.Response]
            A list of response objects.
        """
        pass
    
    def transform_response(
        self, 
        resp_req: Union[Response, PlaywrightPage, SeleniumWebDriver]
    ) -> pd.DataFrame:
        """Transform a list of response objects into a DataFrame.
        
        Parameters
        ----------
        resp_req: Union[Response, PlaywrightPage, SeleniumWebDriver]
            The response object.
        
        Returns
        -------
        pd.DataFrame
            The transformed DataFrame.
        """
        file = self.get_file(resp_req=resp_req)
        return pd.read_csv(file, sep=";")
    
    def run(
        self, 
        bool_insert_or_ignore: bool = False, 
        str_table_name: str = "<COUNTRY_ORIGIN_NAME>"
    ) -> Optional[pd.DataFrame]:
        """Run the ingestion process.
        
        If the database session is provided, the data is inserted into the database.
        Otherwise, the transformed DataFrame is returned.

        Parameters
        ----------
        bool_insert_or_ignore : bool, optional
            Whether to insert or ignore the data, by default False
        str_table_name : str, optional
            The name of the table, by default "<COUNTRY_ORIGIN_NAME>"

        Returns
        -------
        Optional[pd.DataFrame]
            The transformed DataFrame.
        """
        resp_req = self.get_response()
        df_ = self.transform_response(resp_req)
        df_ = self.standardize_dataframe(
            df_=df_, 
            date_ref=self.date_ref,
            dict_dtypes={
                "COL_1": str,
                "COL_2": float, 
                "COL_3": int, 
                "COL_4": "date"
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