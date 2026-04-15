"""CoinPaprika OHLCV latest ingestion for crypto assets."""

from datetime import date
from io import StringIO
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


_BASE_URL = "https://api.coinpaprika.com/v1/"
_DEFAULT_SLUGS: list[str] = ["btc-bitcoin", "sol-solana", "etc-ethereum-classic"]


class CoinPaprika(ABCIngestionOperations):
    """CoinPaprika OHLCV latest ingestion class."""

    def __init__(
        self,
        date_ref: Optional[date] = None,
        logger: Optional[Logger] = None,
        cls_db: Optional[Session] = None,
        list_slugs: Optional[list[str]] = None,
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
        list_slugs : Optional[list[str]]
            CoinPaprika coin slugs to fetch (e.g. ``["btc-bitcoin"]``).
            Defaults to the canonical list when None.

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
        self.list_slugs = list_slugs if list_slugs is not None else list(_DEFAULT_SLUGS)
        self.url = _BASE_URL

    def run(
        self,
        timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (12.0, 12.0),
        bool_verify: bool = False,
        bool_insert_or_ignore: bool = False,
        str_table_name: str = "ww_coinpaprika_ohlcv_latest",
    ) -> Optional[pd.DataFrame]:
        """Run the ingestion process for all configured slugs.

        Iterates over ``list_slugs``, fetches and transforms OHLCV data for
        each coin, concatenates all results, standardises types, and either
        persists to the database (when ``cls_db`` is set) or returns the
        combined DataFrame.

        Parameters
        ----------
        timeout : Optional[Union[int, float, tuple[float, float], tuple[int, int]]], optional
            The timeout, by default (12.0, 12.0).
        bool_verify : bool
            Whether to verify the SSL certificate, by default False.
        bool_insert_or_ignore : bool
            Whether to insert or ignore the data, by default False.
        str_table_name : str
            The name of the table, by default 'ww_coinpaprika_ohlcv_latest'.

        Returns
        -------
        Optional[pd.DataFrame]
            The transformed DataFrame.
        """
        list_frames: list[pd.DataFrame] = []

        for slug in self.list_slugs:
            slug_url = f"{_BASE_URL}coins/{slug}/ohlcv/latest"
            resp_req = self.get_response(url=slug_url, timeout=timeout, bool_verify=bool_verify)
            file = self.parse_raw_file(resp_req)
            df_slug = self.transform_data(file=file, slug=slug)
            if df_slug is not None and not df_slug.empty:
                list_frames.append(df_slug)

        if not list_frames:
            return None

        df_ = pd.concat(list_frames, ignore_index=True)
        df_ = self.standardize_dataframe(
            df_=df_,
            date_ref=self.date_ref,
            dict_dtypes={
                "SLUG": str,
                "TIME_OPEN": str,
                "TIME_CLOSE": str,
                "OPEN": float,
                "HIGH": float,
                "LOW": float,
                "CLOSE": float,
                "VOLUME": float,
                "MARKET_CAP": float,
            },
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
            return None

        return df_

    @backoff.on_exception(backoff.expo, requests.exceptions.HTTPError, max_time=60)
    def get_response(
        self,
        url: str,
        timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (12.0, 12.0),
        bool_verify: bool = False,
    ) -> Union[Response, PlaywrightPage, SeleniumWebDriver]:
        """Fetch the raw HTTP response for a single coin slug.

        Parameters
        ----------
        url : str
            Fully-qualified CoinPaprika OHLCV endpoint URL.
        timeout : Optional[Union[int, float, tuple[float, float], tuple[int, int]]], optional
            The timeout, by default (12.0, 12.0).
        bool_verify : bool
            Verify the SSL certificate, by default False.

        Returns
        -------
        Union[Response, PlaywrightPage, SeleniumWebDriver]
            The HTTP response object.
        """
        resp_req = requests.get(url, timeout=timeout, verify=bool_verify)
        resp_req.raise_for_status()
        return resp_req

    def parse_raw_file(
        self,
        resp_req: Union[Response, PlaywrightPage, SeleniumWebDriver],
    ) -> StringIO:
        """Parse the raw response into a StringIO stream.

        Parameters
        ----------
        resp_req : Union[Response, PlaywrightPage, SeleniumWebDriver]
            The response object.

        Returns
        -------
        StringIO
            Response text wrapped in a StringIO buffer.
        """
        return self.get_file(resp_req=resp_req)

    def transform_data(
        self,
        file: StringIO,
        slug: str,
    ) -> Optional[pd.DataFrame]:
        """Parse the JSON response list and build a flat OHLCV DataFrame.

        Parameters
        ----------
        file : StringIO
            StringIO buffer wrapping the raw response text.
        slug : str
            CoinPaprika coin slug added as the SLUG column to each row.

        Returns
        -------
        Optional[pd.DataFrame]
            Flat OHLCV DataFrame for the given slug, or None when the
            JSON payload is empty or cannot be decoded.
        """
        import json

        try:
            json_ = json.loads(file.read())
        except (json.JSONDecodeError, Exception):
            self.cls_create_log.log_message(
                logger=self.logger,
                message=f"CoinPaprika: failed to decode JSON for slug '{slug}'",
                log_level="warning",
            )
            return None

        if not json_:
            self.cls_create_log.log_message(
                logger=self.logger,
                message=f"CoinPaprika: empty payload for slug '{slug}'",
                log_level="warning",
            )
            return None

        list_rows: list[dict] = [
            {
                "SLUG": slug,
                "TIME_OPEN": record.get("time_open"),
                "TIME_CLOSE": record.get("time_close"),
                "OPEN": record.get("open"),
                "HIGH": record.get("high"),
                "LOW": record.get("low"),
                "CLOSE": record.get("close"),
                "VOLUME": record.get("volume"),
                "MARKET_CAP": record.get("market_cap"),
            }
            for record in json_
        ]

        return pd.DataFrame(list_rows)
