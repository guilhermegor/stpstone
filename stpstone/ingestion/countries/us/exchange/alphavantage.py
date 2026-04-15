"""AlphaVantage US daily OHLCV ingestion."""

from datetime import date
from io import StringIO
from logging import Logger
from time import sleep
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


# AlphaVantage free tier allows 5 requests/minute; 12 s between requests respects that limit.
RATE_LIMIT_SLEEP_SECONDS: int = 12

_BASE_URL = "https://www.alphavantage.co/query"


class AlphaVantageUS(ABCIngestionOperations):
    """AlphaVantage daily OHLCV (not adjusted) ingestion for US equities."""

    def __init__(
        self,
        date_ref: Optional[date] = None,
        logger: Optional[Logger] = None,
        cls_db: Optional[Session] = None,
        token: Optional[str] = None,
        list_slugs: Optional[list[str]] = None,
    ) -> None:
        """Initialize the ingestion class.

        Parameters
        ----------
        date_ref : Optional[date]
            Reference date for the ingestion run, by default None (yesterday).
        logger : Optional[Logger]
            Logger instance, by default None.
        cls_db : Optional[Session]
            Database session for persistence, by default None.
        token : Optional[str]
            AlphaVantage API key, by default None.
        list_slugs : Optional[list[str]]
            Ticker symbols to fetch (e.g. ``["AAPL", "MSFT"]``), by default None.

        Returns
        -------
        None
        """
        super().__init__(cls_db=cls_db)
        CoreIngestion.__init__(self)
        ContentParser.__init__(self)

        self.logger = logger
        self.cls_db = cls_db
        self.token = token
        self.list_slugs = list_slugs or []
        self.cls_dir_files_management = DirFilesManagement()
        self.cls_dates_current = DatesCurrent()
        self.cls_create_log = CreateLog()
        self.cls_dates_br = DatesBRAnbima()
        self.date_ref = date_ref or self.cls_dates_br.add_working_days(
            self.cls_dates_current.curr_date(), -1
        )

    def _build_url(self, slug: str) -> str:
        """Build the request URL for a single ticker symbol.

        Parameters
        ----------
        slug : str
            Ticker symbol (e.g. ``"AAPL"``).

        Returns
        -------
        str
            Fully-qualified AlphaVantage query URL.
        """
        return (
            f"{_BASE_URL}?function=TIME_SERIES_DAILY"
            f"&symbol={slug}"
            f"&apikey={self.token}"
            f"&datatype=json"
            f"&outputsize=compact"
        )

    def run(
        self,
        bool_verify: bool = False,
        timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (12.0, 21.0),
        bool_insert_or_ignore: bool = False,
        str_table_name: str = "us_alphav_ohlcv_not_adjusted",
    ) -> Optional[pd.DataFrame]:
        """Run the ingestion pipeline over all configured ticker symbols.

        Iterates over ``list_slugs``, fetches and transforms OHLCV data for
        each symbol, concatenates all results, standardises types, and either
        persists to the database (when ``cls_db`` is set) or returns the
        combined DataFrame.

        Parameters
        ----------
        bool_verify : bool
            Whether to verify the SSL certificate, by default False.
        timeout : Optional[Union[int, float, tuple[float, float], tuple[int, int]]]
            Request timeout passed to ``requests.get``, by default (12.0, 21.0).
        bool_insert_or_ignore : bool
            Whether to use INSERT OR IGNORE semantics when persisting,
            by default False.
        str_table_name : str
            Target database table name, by default ``"us_alphav_ohlcv_not_adjusted"``.

        Returns
        -------
        Optional[pd.DataFrame]
            Combined OHLCV DataFrame when no database session is configured,
            otherwise ``None``.
        """
        if not self.list_slugs:
            return None

        list_frames: list[pd.DataFrame] = []

        for slug in self.list_slugs:
            url = self._build_url(slug)
            resp_req = self.get_response(url=url, timeout=timeout, bool_verify=bool_verify)
            sleep(RATE_LIMIT_SLEEP_SECONDS)
            df_slug = self.transform_data(resp_req=resp_req, slug=slug)
            if df_slug is not None and not df_slug.empty:
                list_frames.append(df_slug)

        if not list_frames:
            return None

        df_ = pd.concat(list_frames, ignore_index=True)
        df_ = self.standardize_dataframe(
            df_=df_,
            date_ref=self.date_ref,
            dict_dtypes={
                "DATE": "date",
                "TICKER": str,
                "OPEN": float,
                "HIGH": float,
                "LOW": float,
                "CLOSE": float,
                "VOLUME": float,
            },
            str_fmt_dt="YYYY-MM-DD",
            url=_BASE_URL,
        )

        if self.cls_db:
            self.insert_table_db(
                cls_db=self.cls_db,
                str_table_name=str_table_name,
                df_=df_,
                bool_insert_or_ignore=bool_insert_or_ignore,
            )
            return None

        return df_

    @backoff.on_exception(backoff.expo, requests.exceptions.HTTPError, max_time=60)
    def get_response(
        self,
        url: str,
        timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (12.0, 21.0),
        bool_verify: bool = False,
    ) -> Union[Response, PlaywrightPage, SeleniumWebDriver]:
        """Fetch the raw HTTP response for a single ticker symbol URL.

        Parameters
        ----------
        url : str
            Fully-qualified AlphaVantage query URL (from ``_build_url``).
        timeout : Optional[Union[int, float, tuple[float, float], tuple[int, int]]]
            Request timeout, by default (12.0, 21.0).
        bool_verify : bool
            Whether to verify the SSL certificate, by default False.

        Returns
        -------
        Union[Response, PlaywrightPage, SeleniumWebDriver]
            HTTP response object.
        """
        resp_req = requests.get(url, timeout=timeout, verify=bool_verify)
        resp_req.raise_for_status()
        return resp_req

    def parse_raw_file(
        self,
        resp_req: Union[Response, PlaywrightPage, SeleniumWebDriver],
    ) -> StringIO:
        """Parse the raw response into a StringIO stream.

        This method satisfies the ``ABCIngestion`` contract. For AlphaVantage,
        the actual data extraction is JSON-based and handled by ``transform_data``.

        Parameters
        ----------
        resp_req : Union[Response, PlaywrightPage, SeleniumWebDriver]
            HTTP response object.

        Returns
        -------
        StringIO
            Response text wrapped in a StringIO buffer.
        """
        return self.get_file(resp_req=resp_req)

    def transform_data(
        self,
        resp_req: Response,
        slug: str,
    ) -> Optional[pd.DataFrame]:
        """Parse the JSON response and build a flat OHLCV DataFrame.

        Parameters
        ----------
        resp_req : Response
            HTTP response whose JSON body contains AlphaVantage time-series data.
        slug : str
            Ticker symbol used as a fallback when the JSON ``Meta Data`` key
            is absent.

        Returns
        -------
        Optional[pd.DataFrame]
            Flat OHLCV DataFrame for the given ticker, or ``None`` when the
            expected JSON keys are missing (e.g. API rate-limit response).
        """
        try:
            json_ = resp_req.json()
        except Exception:
            self.cls_create_log.log_message(
                logger=self.logger,
                message=f"AlphaVantage: failed to decode JSON for slug '{slug}'",
                log_level="warning",
            )
            return None

        time_series = json_.get("Time Series (Daily)")
        if not time_series:
            self.cls_create_log.log_message(
                logger=self.logger,
                message=(
                    f"AlphaVantage: 'Time Series (Daily)' key missing for slug '{slug}'. "
                    f"Response keys: {list(json_.keys())}"
                ),
                log_level="warning",
            )
            return None

        meta_data = json_.get("Meta Data", {})
        ticker = meta_data.get("2. Symbol", slug)

        list_rows: list[dict] = [
            {
                "DATE": str_day,
                "TICKER": ticker,
                "OPEN": daily.get("1. open"),
                "HIGH": daily.get("2. high"),
                "LOW": daily.get("3. low"),
                "CLOSE": daily.get("4. close"),
                "VOLUME": daily.get("5. volume"),
            }
            for str_day, daily in time_series.items()
        ]

        return pd.DataFrame(list_rows)
