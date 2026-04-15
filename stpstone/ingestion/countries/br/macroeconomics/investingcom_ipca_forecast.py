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
from stpstone.utils.webdriver_tools.playwright_wd import PlaywrightScraper


class InvetingComIPCAForecast(ABCIngestionOperations):
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
        self.url = "https://sbcharts.investing.com/events_charts/eu/1165.json"
    
    def run(
        self,
        timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (12.0, 21.0),
        bool_verify: bool = True,
        bool_insert_or_ignore: bool = False, 
        str_table_name: str = "br_investingcom_ipca_forecasts"
    ) -> Optional[pd.DataFrame]:
        """Run the ingestion process.
        
        If the database session is provided, the data is inserted into the database.
        Otherwise, the transformed DataFrame is returned.

        Parameters
        ----------
        timeout : Optional[Union[int, float, tuple[float, float], tuple[int, int]]]
            The timeout, by default (12.0, 21.0)
        bool_verify : bool
            Whether to verify the SSL certificate, by default True
        bool_insert_or_ignore : bool
            Whether to insert or ignore the data, by default False
        str_table_name : str
            The name of the table, by default "br_investingcom_ipca_forecasts"

        Returns
        -------
        Optional[pd.DataFrame]
            The transformed DataFrame.
        """
        scraper = self.get_response(timeout=timeout, bool_verify=bool_verify)
        json_ = self.parse_raw_file(scraper)
        df_ = self.transform_data(json_=json_)
        df_ = self.standardize_dataframe(
            df_=df_, 
            date_ref=self.date_ref,
            dict_dtypes={
                "DATETIME": str,
                "ACTUAL_STATE": str,
                "ACTUAL": float, 
                "FORECAST": float, 
                "REVISED": float,
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
        max_time=120
    )
    def get_response(
        self, 
        timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (12.0, 21.0), 
        bool_verify: bool = True
    ) -> Union[Response, PlaywrightPage, SeleniumWebDriver, PlaywrightScraper]:
        """Return a list of response objects.

        Parameters
        ----------
        timeout : Optional[Union[int, float, tuple[float, float], tuple[int, int]]]
            The timeout, by default (12.0, 21.0)
        bool_verify : bool
            Verify the SSL certificate, by default True
        
        Returns
        -------
        Union[Response, PlaywrightPage, SeleniumWebDriver, PlaywrightScraper]
            A list of response objects.
        """
        return PlaywrightScraper(
            bool_headless=True, 
            int_default_timeout=5_000, 
            logger=self.logger
        )
    
    def parse_raw_file(
        self, 
        scraper: Union[Response, PlaywrightPage, SeleniumWebDriver, PlaywrightScraper], 
        timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = 12_000
    ) -> dict[str, list[list[Union[str, int, float]]]]:
        """Parse the raw file content.
        
        Parameters
        ----------
        scraper : Union[Response, PlaywrightPage, SeleniumWebDriver, PlaywrightScraper]
            The response object.
        timeout : Optional[Union[int, float, tuple[float, float], tuple[int, int]]]
            The timeout, by default 12_000
        
        Returns
        -------
        dict[str, list[list[Union[str, int, float]]]]
            The parsed content.
        """
        return scraper.get_json(
            url=self.url,
            timeout=timeout,
            cookies=None,
        )
    
    def transform_data(
        self, 
        json_: dict[str, list[list[Union[str, int, float]]]]
    ) -> pd.DataFrame:
        """Transform a list of response objects into a DataFrame.
        
        Parameters
        ----------
        json_ : dict[str, list[list[Union[str, int, float]]]]
            The list of dictionaries to transform.
        
        Returns
        -------
        pd.DataFrame
            The transformed DataFrame.
        """
        json_: list[dict[str, Union[str, int, float]]] = json_["attr"]
        int_covert_miliseconds_seconds: int = 1_000

        list_ser = [
            {
                "DATETIME": self.cls_dates_br.unix_timestamp_to_datetime(
                    int(int(dict_["timestamp"])) / int_covert_miliseconds_seconds,
                ), 
                "ACTUAL_STATE": str(dict_["actual_state"]), 
                "ACTUAL": float(dict_["actual"]) if dict_["actual"] is not None else None,
                "FORECAST": float(dict_["forecast"]) if dict_["forecast"] is not None else None,
                "REVISED": float(dict_["revised"]) if dict_["revised"] is not None else None,
            }
            for dict_ in json_
        ]

        return pd.DataFrame(list_ser)