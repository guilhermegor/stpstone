"""ADVFN worldwide daily OHLCV data ingestion."""

from datetime import date, datetime
from logging import Logger
import re
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
from stpstone.utils.parsers.dicts import HandlingDicts
from stpstone.utils.parsers.folders import DirFilesManagement


class ADVFNWW(ABCIngestionOperations):
    """ADVFN worldwide daily OHLCV data ingestion class."""

    _BASE_URL = "https://br.advfn.com/"
    _HEADERS = {
        "Cookie": "ADVFNUID=70ef3fe162e0f05e9d3bb02d7448c46b9994d44",
    }

    def __init__(
        self,
        date_ref: Optional[date] = None,
        date_start: Optional[date] = None,
        date_end: Optional[date] = None,
        str_market: str = "BOV",
        str_ticker: str = "PETR4",
        logger: Optional[Logger] = None,
        cls_db: Optional[Session] = None,
    ) -> None:
        """Initialize the ingestion class.

        Parameters
        ----------
        date_ref : Optional[date], optional
            The date of reference, by default None.
        date_start : Optional[date], optional
            The start date, by default 5 working days before date_ref.
        date_end : Optional[date], optional
            The end date, by default date_ref.
        str_market : str, optional
            The market code (e.g. 'BOV'), by default 'BOV'.
        str_ticker : str, optional
            The ticker symbol (e.g. 'PETR4'), by default 'PETR4'.
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
        self.date_start = date_start or \
            self.cls_dates_br.add_working_days(self.date_ref, -5)
        self.date_end = date_end or self.date_ref
        self.str_market = str_market
        self.str_ticker = str_ticker
        self.date_start_unix_ts = int(
            datetime.combine(self.date_start, datetime.min.time()).timestamp()
        )
        self.date_end_unix_ts = int(
            datetime.combine(self.date_end, datetime.min.time()).timestamp()
        )
        self.url = (
            f"{self._BASE_URL}common/api/histo/GetHistoricalData"
            f"?symbol={self.str_market}^{self.str_ticker}"
            f"&frequency=DAILY"
            f"&from={self.date_start_unix_ts}"
            f"&to={self.date_end_unix_ts}"
        )

    def run(
        self,
        timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (12.0, 21.0),
        bool_verify: bool = False,
        bool_insert_or_ignore: bool = False,
        str_table_name: str = "ww_advfn_daily_ohlcv",
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
            The name of the table, by default 'ww_advfn_daily_ohlcv'.

        Returns
        -------
        Optional[pd.DataFrame]
            The transformed DataFrame.
        """
        resp_req = self.get_response(timeout=timeout, bool_verify=bool_verify)
        df_ = self.transform_data(resp_req=resp_req)
        df_ = self.standardize_dataframe(
            df_=df_,
            date_ref=self.date_ref,
            dict_dtypes={
                "DATE": "date",
                "CLOSE_PRICE": float,
                "CHANGE": float,
                "CHANGE_PERCENTAGE": float,
                "OPEN_PRICE": float,
                "HIGH_PRICE": float,
                "LOW_PRICE": float,
                "VOLUME": float,
                "TICKER": str,
            },
            cols_from_case="pascal",
            cols_to_case="upper_constant",
            str_fmt_dt="unix_ts",
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
        """Return a response object from ADVFN.

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
        resp_req = requests.get(
            self.url,
            headers=self._HEADERS,
            timeout=timeout,
            verify=bool_verify,
        )
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
        """Transform an ADVFN JSON response into a DataFrame.

        Parameters
        ----------
        resp_req : Union[Response, PlaywrightPage, SeleniumWebDriver]
            The response object with a JSON body containing 'result.symbol' and 'result.rows'.

        Returns
        -------
        pd.DataFrame
            The transformed DataFrame with OHLCV rows and the extracted ticker column.
        """
        re_pattern = r"\^([^ ]+)"
        json_ = resp_req.json()
        re_match = re.search(re_pattern, json_["result"]["symbol"])
        ticker = re_match.group(1) if re_match is not None else json_["result"]["symbol"]
        list_ser = HandlingDicts().add_key_value_to_dicts(
            json_["result"]["rows"],
            key="ticker",
            value=ticker,
        )
        return pd.DataFrame(list_ser)
