"""USA holiday calendar data extraction from Nasdaq and Federal sources.

This module provides classes for fetching and processing holiday calendar data
from Nasdaq and US Federal sources using web scraping techniques.
"""

from datetime import date, timedelta
from logging import Logger
from typing import Optional

import pandas as pd
import requests
from requests.exceptions import RequestException

from stpstone.utils.cache.cache_manager import CacheManager
from stpstone.utils.calendars.calendar_abc import ABCCalendarOperations
from stpstone.utils.parsers.dicts import HandlingDicts
from stpstone.utils.parsers.html import HtmlHandler
from stpstone.utils.webdriver_tools.playwright_wd import PlaywrightScraper


class DatesUSANasdaq(ABCCalendarOperations):
    """NASDAQ holiday calendar data fetcher and processor.
    
    References
    ----------
    [1] https://nasdaqtrader.com/trader.aspx?id=Calendar
    """

    def __init__(
        self, 
        bool_persist_cache: bool = True, 
        bool_reuse_cache: bool = True,
        int_days_cache_expiration: int = 1,
        int_cache_ttl_days: int = 30,
        path_cache_dir: Optional[str] = None,
        logger: Optional[Logger] = None
    ) -> None:
        """Initialize Nasdaq calendar handler with HTML and dict utilities.
        
        Parameters
        ----------
        bool_persist_cache : bool
            If True, saves cache to disk; if False, uses in-memory cache only (default: True)
        bool_reuse_cache : bool
            If True, caches in-memory; if False, does not cache in-memory (default: True)
        int_days_cache_expiration : int
            Number of days after which the cache expires (default: 1)
        int_cache_ttl_days : int
            Number of days after which the cache is considered expired (default: 30)
        path_cache_dir : Optional[str]
            Path to the cache directory (default: None)
        logger : Optional[Logger]
            Logger object for logging (default: None)

        Returns
        -------
        None
        """
        self.cls_cache_manager = CacheManager(
            bool_persist_cache=bool_persist_cache,
            bool_reuse_cache=bool_reuse_cache,
            int_days_cache_expiration=int_days_cache_expiration,
            int_cache_ttl_days=int_cache_ttl_days,
            path_cache_dir=path_cache_dir,
            logger=logger
        )
        self.cls_html_handler = HtmlHandler()
        self.cls_dict_handler = HandlingDicts()

    def holidays(self) -> list[tuple[str, date]]:
        """Get list of NASDAQ holidays with descriptions and dates.

        Returns
        -------
        list[tuple[str, date]]
            list of tuples containing holiday description and date
        """
        df_ = self.get_holidays_raw()
        df_ = self.transform_holidays(df_)
        return [(row["DESCRIPTION"], row["DATE_WINS"]) for _, row in df_.iterrows()]

    @CacheManager.cache_df(key="usa_nasdaq_holidays")
    def get_holidays_raw(self, timeout: Optional[int] = 10) -> pd.DataFrame:
        """Fetch raw NASDAQ holiday calendar data from website.

        Parameters
        ----------
        timeout : Optional[int]
            Request timeout in seconds (default: 10)

        Returns
        -------
        pd.DataFrame
            Raw holiday data with DATE, DESCRIPTION, STATUS columns

        Raises
        ------
        RequestException
            If HTTP request fails or returns non-200 status
        ValueError
            If HTML parsing fails or data structure is invalid
        """
        url = "https://nasdaqtrader.com/trader.aspx?id=Calendar"
        try:
            resp_req = requests.get(url, timeout=timeout)
            resp_req.raise_for_status()
            root_html = self.cls_html_handler.lxml_parser(resp_req)
            list_td = [
                x.text 
                for x in self.cls_html_handler.lxml_xpath(root_html, "//table/tbody/tr/td")
            ]
            list_ser = self.cls_dict_handler.pair_headers_with_data(
                ["DATE", "DESCRIPTION", "STATUS"], list_td
            )
            return pd.DataFrame(list_ser)
        except RequestException as err:
            raise RequestException(
                f"Failed to fetch NASDAQ holidays: {str(err)}"
            ) from err
        except Exception as err:
            raise ValueError(f"Failed to parse NASDAQ holidays data: {str(err)}") from err

    def transform_holidays(self, df_: pd.DataFrame) -> pd.DataFrame:
        """Transform raw holiday data into structured format.

        Parameters
        ----------
        df_ : pd.DataFrame
            Raw holiday data from get_holidays_raw

        Returns
        -------
        pd.DataFrame
            Transformed data with DATE_WINS as date objects
        """
        self._validate_holidays_dataframe(df_)
        df_ = df_.astype({"DATE": str, "DESCRIPTION": str, "STATUS": str})
        df_["DATE_WINS"] = [self._parse_dates(x) for x in df_["DATE"].tolist()]
        return df_

    def _parse_dates(self, str_dt: str) -> date:
        """Parse date string into date object.

        Parameters
        ----------
        str_dt : str
            Date string in format "Month Day, Year"

        Returns
        -------
        date
            Parsed date object

        Raises
        ------
        ValueError
            If date string format is invalid or month is unrecognized
        """
        self._validate_date_string(str_dt)
        dict_mappings = {
            "January": 1, "February": 2, "March": 3, "April": 4, "May": 5, "June": 6,
            "July": 7, "August": 8, "September": 9, "October": 10, "November": 11,
            "December": 12
        }
        list_parts_dt = [x.replace(",", "") for x in str_dt.split(" ") if x]
        if list_parts_dt[0] not in dict_mappings:
            raise ValueError(f"Invalid date format: {str_dt}. Expected 'Month Day, Year'")
        month = dict_mappings[list_parts_dt[0]]
        day = int(list_parts_dt[1])
        year = int(list_parts_dt[2])
        return date(year, month, day)

    def _validate_holidays_dataframe(self, df_: pd.DataFrame) -> None:
        """Validate holidays DataFrame structure and content.

        Parameters
        ----------
        df_ : pd.DataFrame
            DataFrame to validate

        Raises
        ------
        ValueError
            If DataFrame is empty or missing required columns
        """
        if df_.empty:
            raise ValueError("Holidays DataFrame cannot be empty")
        required_columns = {"DATE", "DESCRIPTION", "STATUS"}
        if not required_columns.issubset(df_.columns):
            raise ValueError(f"DataFrame must contain columns: {required_columns}")

    def _validate_date_string(self, str_dt: str) -> None:
        """Validate date string format.

        Parameters
        ----------
        str_dt : str
            Date string to validate

        Raises
        ------
        ValueError
            If date string is empty, not a string, or has invalid format
        """
        if not str_dt:
            raise ValueError("Date string cannot be empty")
        if len(str_dt.split(" ")) < 3:
            raise ValueError("Date string must contain month, day, and year components")


class DatesUSAFederalHolidays(ABCCalendarOperations):
    """US Federal holiday calendar data fetcher and processor.
    
    References
    ----------
    [1] https://www.federalholidays.net/usa/federal-holidays-2025.html
    """

    def __init__(
        self, 
        int_year_start: int = (date.today() - timedelta(days=22)).year - 1, 
        int_year_end: int = (date.today() - timedelta(days=22)).year, 
        bool_persist_cache: bool = True, 
        bool_reuse_cache: bool = True,
        int_days_cache_expiration: int = 1,
        int_cache_ttl_days: int = 30,
        path_cache_dir: Optional[str] = None,
        logger: Optional[Logger] = None
    ) -> None:
        """Initialize Federal holidays handler with HTML and dict utilities.
        
        Parameters
        ----------
        int_year_start : int
            Starting year for holidays (default: (date.today() - timedelta(days=22)).year - 1)
        int_year_end : int
            Ending year for holidays (default: (date.today() - timedelta(days=22)).year)
        bool_persist_cache : bool
            If True, saves cache to disk; if False, uses in-memory cache only (default: True)
        bool_reuse_cache : bool
            If True, caches in-memory; if False, does not cache in-memory (default: True)
        int_days_cache_expiration : int
            Number of days after which the cache expires (default: 1)
        int_cache_ttl_days : int, optional
            Number of days after which the cache is considered expired (default: 30)
        path_cache_dir : Optional[str], optional
            Path to the cache directory (default: None)
        logger : Optional[Logger], optional
            Logger object for logging (default: None)

        Returns
        -------
        None
        """
        self.int_year_start = int_year_start
        self.int_year_end = int_year_end
        self.cls_cache_manager = CacheManager(
            bool_persist_cache=bool_persist_cache,
            bool_reuse_cache=bool_reuse_cache,
            int_days_cache_expiration=int_days_cache_expiration,
            int_cache_ttl_days=int_cache_ttl_days,
            path_cache_dir=path_cache_dir,
            logger=logger
        )
        self.cls_html_handler = HtmlHandler()
        self.cls_dict_handler = HandlingDicts()

    def holidays(self) -> list[tuple[str, date]]:
        """Get list of US Federal holidays with names and dates.

        Returns
        -------
        list[tuple[str, date]]
            list of tuples containing holiday name and date
        """
        df_ = self.get_holidays_years()
        df_ = self.transform_holidays(df_)
        return [(row["NAME"], row["DATE_WINS"]) for _, row in df_.iterrows()]

    @CacheManager.cache_df(key="usa_federal_holidays")
    def get_holidays_years(self) -> pd.DataFrame:
        """Fetch Federal holidays for multiple years.

        Parameters
        ----------
        int_year_start : int
            Starting year for holiday data (default: previous year)
        int_year_end : int
            Ending year for holiday data (default: current year)

        Returns
        -------
        pd.DataFrame
            Combined holiday data for all specified years
        """
        self._validate_year_range(self.int_year_start, self.int_year_end)
        list_ser = []
        for int_year in range(self.int_year_start, self.int_year_end + 1):
            list_ser.extend(self.get_holidays_raw(int_year).to_dict(orient="records"))
        return pd.DataFrame(list_ser)

    def get_holidays_raw(self, int_year: int, timeout: Optional[int] = 5000) -> pd.DataFrame:
        """Fetch raw Federal holiday data for specific year.

        Parameters
        ----------
        int_year : int
            Year to fetch holidays for
        timeout : Optional[int]
            Playwright timeout in milliseconds (default: 5000)

        Returns
        -------
        pd.DataFrame
            Raw holiday data with DATE, WEEKDAY, NAME, YEAR columns

        Raises
        ------
        RuntimeError
            If Playwright navigation or data extraction fails
        """
        self._validate_year(int_year)
        url = f"https://www.federalholidays.net/usa/federal-holidays-{int_year}.html"
        scraper = PlaywrightScraper(bool_headless=True, int_default_timeout=timeout)
        try:
            with scraper.launch():
                if not scraper.navigate(url):
                    raise RuntimeError(f"Failed to navigate to URL: {url}")
                list_td = [
                    x.replace(r"\n", "").strip() 
                    for x in scraper.get_list_data("//table/tbody/tr/td", selector_type="xpath")
                ]
            list_ser = self.cls_dict_handler.pair_headers_with_data(
                ["DATE", "WEEKDAY", "NAME"], list_td
            )
            df_ = pd.DataFrame(list_ser)
            df_["YEAR"] = int_year
            return df_
        except Exception as err:
            raise RuntimeError(
                f"Failed to fetch Federal holidays for {int_year}: {str(err)}") from err

    def transform_holidays(self, df_: pd.DataFrame) -> pd.DataFrame:
        """Transform raw Federal holiday data into structured format.

        Parameters
        ----------
        df_ : pd.DataFrame
            Raw holiday data from get_holidays_raw

        Returns
        -------
        pd.DataFrame
            Transformed data with DATE_WINS as date objects
        """
        self._validate_federal_holidays_dataframe(df_)
        df_ = df_.astype({"DATE": str, "WEEKDAY": str, "NAME": str, "YEAR": int})
        df_["DATE_WINS"] = [
            self._parse_dates(str_dt=row["DATE"], int_year=row["YEAR"]) 
            for _, row in df_.iterrows()
        ]
        return df_

    def _parse_dates(self, str_dt: str, int_year: int) -> date:
        """Parse date string into date object using provided year.

        Parameters
        ----------
        str_dt : str
            Date string in format "Month Day"
        int_year : int
            Year to use for date construction

        Returns
        -------
        date
            Parsed date object

        Raises
        ------
        ValueError
            If date string format is invalid or month is unrecognized
        """
        self._validate_date_string(str_dt)
        dict_mappings = {
            "January": 1, "February": 2, "March": 3, "April": 4, "May": 5, "June": 6,
            "July": 7, "August": 8, "September": 9, "October": 10, "November": 11,
            "December": 12
        }
        list_parts_dt = [x.replace(",", "") for x in str_dt.split(" ") if x]
        if list_parts_dt[0] in dict_mappings:
            month = dict_mappings[list_parts_dt[0]]
            day = int(list_parts_dt[1])
        else:
            try:
                day = int(list_parts_dt[0])
                month = dict_mappings[list_parts_dt[1]]
            except (IndexError, KeyError, ValueError) as err:
                raise ValueError(f"Invalid date format: {str_dt}. Expected 'Month Day' "
                                 f"or 'Day Month'. Error: {err}") from err
        
        return date(int_year, month, day)

    def _validate_year_range(self, int_year_start: int, int_year_end: int) -> None:
        """Validate year range parameters.

        Parameters
        ----------
        int_year_start : int
            Starting year
        int_year_end : int
            Ending year

        Raises
        ------
        ValueError
            If years are not positive integers or range is invalid
        """
        self._validate_year(int_year_start)
        self._validate_year(int_year_end)
        if int_year_start > int_year_end:
            raise ValueError("Start year must be less than or equal to end year")

    def _validate_year(self, int_year: int) -> None:
        """Validate year parameter.

        Parameters
        ----------
        int_year : int
            Year to validate

        Raises
        ------
        ValueError
            If year is not a positive integer
        """
        if int_year <= 0:
            raise ValueError("Year must be a positive integer")

    def _validate_federal_holidays_dataframe(self, df_: pd.DataFrame) -> None:
        """Validate Federal holidays DataFrame structure and content.

        Parameters
        ----------
        df_ : pd.DataFrame
            DataFrame to validate

        Raises
        ------
        ValueError
            If DataFrame is empty or missing required columns
        """
        if df_.empty:
            raise ValueError("Federal holidays DataFrame cannot be empty")
        required_columns = {"DATE", "WEEKDAY", "NAME", "YEAR"}
        if not required_columns.issubset(df_.columns):
            raise ValueError(f"DataFrame must contain columns: {required_columns}")

    def _validate_date_string(self, str_dt: str) -> None:
        """Validate date string format.

        Parameters
        ----------
        str_dt : str
            Date string to validate

        Raises
        ------
        ValueError
            If date string is empty, not a string, or has invalid format
        """
        if not str_dt:
            raise ValueError("Date string cannot be empty")
        if len(str_dt.split(" ")) < 2:
            raise ValueError("Date string must contain month and day components")