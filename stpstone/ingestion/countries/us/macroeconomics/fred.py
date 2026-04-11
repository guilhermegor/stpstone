"""FRED US macroeconomic data series ingestion."""

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


class FredUSMacro(ABCIngestionOperations):
    """FRED US macroeconomic data series ingestion class."""

    _BASE_URL = "https://api.stlouisfed.org/"
    _DEFAULT_SLUGS = ["GNPCA", "GDPC1", "A191RL1Q225SBEA", "DGS10"]

    def __init__(
        self,
        api_key: str,
        date_ref: Optional[date] = None,
        date_start: Optional[date] = None,
        date_end: Optional[date] = None,
        list_slugs: Optional[list[str]] = None,
        logger: Optional[Logger] = None,
        cls_db: Optional[Session] = None,
    ) -> None:
        """Initialize the ingestion class.

        Parameters
        ----------
        api_key : str
            FRED API key for authenticating requests.
        date_ref : Optional[date], optional
            The date of reference, by default None.
        date_start : Optional[date], optional
            Start date for the realtime filter, by default 60 working days before date_ref.
        date_end : Optional[date], optional
            End date for the realtime filter, by default 1 working day before date_ref.
        list_slugs : Optional[list[str]], optional
            List of FRED series IDs to fetch, by default the standard macro set.
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

        self.api_key = api_key
        self.logger = logger
        self.cls_db = cls_db
        self.cls_dir_files_management = DirFilesManagement()
        self.cls_dates_current = DatesCurrent()
        self.cls_create_log = CreateLog()
        self.cls_dates_br = DatesBRAnbima()
        self.date_ref = date_ref or self.cls_dates_br.add_working_days(
            self.cls_dates_current.curr_date(), -1
        )
        self.date_start = date_start or self.cls_dates_br.add_working_days(self.date_ref, -60)
        self.date_end = date_end or self.cls_dates_br.add_working_days(self.date_ref, -1)
        self.list_slugs = list_slugs or self._DEFAULT_SLUGS

    def run(
        self,
        timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (12.0, 21.0),
        bool_verify: bool = False,
        bool_insert_or_ignore: bool = False,
        str_table_name: str = "us_fred",
    ) -> Optional[pd.DataFrame]:
        """Run the ingestion process.

        If the database session is provided, the data is inserted into the database.
        Otherwise, the transformed DataFrame is returned.

        Parameters
        ----------
        timeout : Optional[Union[int, float, tuple[float, float], tuple[int, int]]], optional
            The timeout, by default (12.0, 21.0).
        bool_verify : bool, optional
            Whether to verify the SSL certificate, by default False.
        bool_insert_or_ignore : bool, optional
            Whether to insert or ignore the data, by default False.
        str_table_name : str, optional
            The name of the table, by default 'us_fred'.

        Returns
        -------
        Optional[pd.DataFrame]
            The transformed DataFrame.
        """
        list_frames: list[pd.DataFrame] = []
        for slug in self.list_slugs:
            self.url = self._build_url(slug)
            resp_req = self.get_response(timeout=timeout, bool_verify=bool_verify)
            df_slug = self.transform_data(resp_req=resp_req)
            list_frames.append(df_slug)
        df_ = pd.concat(list_frames, ignore_index=True)
        df_ = self.standardize_dataframe(
            df_=df_,
            date_ref=self.date_ref,
            dict_dtypes={
                "ID": str,
                "REALTIME_START": "date",
                "REALTIME_END": "date",
                "TITLE": str,
                "OBSERVATION_START": "date",
                "OBSERVATION_END": "date",
                "FREQUENCY": str,
                "FREQUENCY_SHORT": str,
                "UNITS": str,
                "UNITS_SHORT": str,
                "SEASONAL_ADJUSTMENT": str,
                "SEASONAL_ADJUSTMENT_SHORT": str,
                "LAST_UPDATED": str,
                "POPULARITY": int,
                "NOTES": str,
            },
            cols_to_case="upper_constant",
            str_fmt_dt="YYYY-MM-DD",
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

    @backoff.on_exception(backoff.expo, requests.exceptions.HTTPError, max_time=60)
    def get_response(
        self,
        timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (12.0, 21.0),
        bool_verify: bool = False,
    ) -> Union[Response, PlaywrightPage, SeleniumWebDriver]:
        """Return a response object from the FRED API.

        Parameters
        ----------
        timeout : Optional[Union[int, float, tuple[float, float], tuple[int, int]]], optional
            The timeout, by default (12.0, 21.0).
        bool_verify : bool, optional
            Verify the SSL certificate, by default False.

        Returns
        -------
        Union[Response, PlaywrightPage, SeleniumWebDriver]
            A response object.
        """
        resp_req = requests.get(self.url, timeout=timeout, verify=bool_verify)
        resp_req.raise_for_status()
        return resp_req

    def parse_raw_file(
        self,
        resp_req: Union[Response, PlaywrightPage, SeleniumWebDriver],
    ) -> Union[Response, PlaywrightPage, SeleniumWebDriver]:
        """Pass through the response object for JSON parsing in transform_data.

        Parameters
        ----------
        resp_req : Union[Response, PlaywrightPage, SeleniumWebDriver]
            The response object.

        Returns
        -------
        Union[Response, PlaywrightPage, SeleniumWebDriver]
            The same response object, unchanged.
        """
        return resp_req

    def transform_data(
        self,
        resp_req: Union[Response, PlaywrightPage, SeleniumWebDriver],
    ) -> pd.DataFrame:
        """Transform a FRED API response into a DataFrame.

        Parameters
        ----------
        resp_req : Union[Response, PlaywrightPage, SeleniumWebDriver]
            The response object containing a JSON body with a 'seriess' key.

        Returns
        -------
        pd.DataFrame
            The transformed DataFrame of series metadata.
        """
        json_ = resp_req.json()
        return pd.DataFrame(json_["seriess"])

    def _build_url(self, slug: str) -> str:
        """Construct the FRED API URL for a given series ID.

        Parameters
        ----------
        slug : str
            FRED series ID (e.g. 'GNPCA').

        Returns
        -------
        str
            Fully-qualified API endpoint URL.
        """
        str_date_start = self.date_start.strftime("%Y-%m-%d")
        str_date_end = self.date_end.strftime("%Y-%m-%d")
        return (
            f"{self._BASE_URL}fred/series"
            f"?series_id={slug}"
            f"&file_type=json"
            f"&api_key={self.api_key}"
            f"&realtime_start={str_date_start}"
            f"&realtime_sup={str_date_end}"
        )
