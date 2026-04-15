"""Yahoo Finance market data ingestion via yfinance."""

from datetime import date
from io import StringIO
from logging import Logger
from typing import Optional, Union

import pandas as pd
from playwright.sync_api import Page as PlaywrightPage
from requests import Response, Session
from selenium.webdriver.remote.webdriver import WebDriver as SeleniumWebDriver
from yfinance import download

from stpstone.ingestion.abc.ingestion_abc import (
    ABCIngestionOperations,
    ContentParser,
    CoreIngestion,
)
from stpstone.utils.calendars.calendar_abc import DatesCurrent
from stpstone.utils.calendars.calendar_br import DatesBRAnbima
from stpstone.utils.loggs.create_logs import CreateLog
from stpstone.utils.parsers.folders import DirFilesManagement


class YFinanceWS(ABCIngestionOperations):
    """Yahoo Finance market data ingestion class via the yfinance library."""

    def __init__(
        self,
        list_tickers: list[str],
        date_ref: Optional[date] = None,
        date_start: Optional[date] = None,
        date_end: Optional[date] = None,
        session: Optional[Session] = None,
        logger: Optional[Logger] = None,
        cls_db: Optional[Session] = None,
    ) -> None:
        """Initialize the ingestion class.

        Parameters
        ----------
        list_tickers : list[str]
            List of ticker symbols to download.
        date_ref : Optional[date]
            The date of reference, by default None.
        date_start : Optional[date]
            Start date for the download window, by default 52 working days before date_ref.
        date_end : Optional[date]
            End date for the download window, by default 1 working day before date_ref.
        session : Optional[Session]
            Optional requests session (e.g. for proxy routing), by default None.
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

        self.list_tickers = list_tickers
        self.session = session
        self.logger = logger
        self.cls_db = cls_db
        self.cls_dir_files_management = DirFilesManagement()
        self.cls_dates_current = DatesCurrent()
        self.cls_create_log = CreateLog()
        self.cls_dates_br = DatesBRAnbima()
        self.date_ref = date_ref or \
            self.cls_dates_br.add_working_days(self.cls_dates_current.curr_date(), -1)
        self.date_start = date_start or \
            self.cls_dates_br.add_working_days(self.date_ref, -52)
        self.date_end = date_end or \
            self.cls_dates_br.add_working_days(self.date_ref, -1)
        self.url = "https://query1.finance.yahoo.com/"

    def run(
        self,
        timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (12.0, 21.0),
        bool_verify: bool = True,
        bool_insert_or_ignore: bool = False,
        str_table_name: str = "ww_yfinance_ohlcv",
    ) -> Optional[pd.DataFrame]:
        """Run the ingestion process.

        If the database session is provided, the data is inserted into the database.
        Otherwise, the transformed DataFrame is returned.

        Parameters
        ----------
        timeout : Optional[Union[int, float, tuple[float, float], tuple[int, int]]], optional
            Unused; kept for interface consistency, by default (12.0, 21.0).
        bool_verify : bool
            Unused; kept for interface consistency, by default True.
        bool_insert_or_ignore : bool
            Whether to insert or ignore the data, by default False.
        str_table_name : str
            The name of the table, by default 'ww_yfinance_ohlcv'.

        Returns
        -------
        Optional[pd.DataFrame]
            The downloaded OHLCV DataFrame.
        """
        df_ = self.get_response()
        if self.cls_db:
            self.insert_table_db(
                cls_db=self.cls_db,
                str_table_name=str_table_name,
                df_=df_,
                bool_insert_or_ignore=bool_insert_or_ignore,
            )
        else:
            return df_

    def get_response(
        self,
        timeout: Optional[Union[int, float, tuple[float, float], tuple[int, int]]] = (12.0, 21.0),
        bool_verify: bool = True,
    ) -> pd.DataFrame:
        """Download market data via yfinance and return as a DataFrame.

        Parameters
        ----------
        timeout : Optional[Union[int, float, tuple[float, float], tuple[int, int]]], optional
            Unused; kept for interface consistency, by default (12.0, 21.0).
        bool_verify : bool
            Unused; kept for interface consistency, by default True.

        Returns
        -------
        pd.DataFrame
            OHLCV DataFrame from yfinance, grouped by ticker.
        """
        return download(
            tickers=self.list_tickers,
            start=self.date_start.strftime("%Y-%m-%d"),
            end=self.date_end.strftime("%Y-%m-%d"),
            group_by="ticker",
            session=self.session,
        )

    def parse_raw_file(
        self,
        resp_req: Union[Response, PlaywrightPage, SeleniumWebDriver],
    ) -> Union[Response, PlaywrightPage, SeleniumWebDriver]:
        """Pass through the DataFrame returned by get_response.

        Parameters
        ----------
        resp_req : Union[Response, PlaywrightPage, SeleniumWebDriver]
            The DataFrame returned by get_response (duck-typed as a response).

        Returns
        -------
        Union[Response, PlaywrightPage, SeleniumWebDriver]
            The same object, unchanged.
        """
        return resp_req

    def transform_data(
        self,
        file: StringIO,
    ) -> pd.DataFrame:
        """Return the DataFrame as-is; no further transformation is required.

        Parameters
        ----------
        file : StringIO
            The DataFrame returned by parse_raw_file (duck-typed as StringIO).

        Returns
        -------
        pd.DataFrame
            The unchanged DataFrame.
        """
        return file
