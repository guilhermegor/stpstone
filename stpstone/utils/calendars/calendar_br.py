"""Brazilian holiday calendar implementations for ANBIMA and FEBRABAN.

This module provides classes for fetching and processing Brazilian holiday data
from ANBIMA and FEBRABAN sources using requests and pandas for data handling.
"""

from datetime import date, timedelta
from io import BytesIO
from logging import Logger
from typing import Optional, Union

import pandas as pd
import requests

from stpstone.transformations.validation.metaclass_type_checker import type_checker
from stpstone.utils.cache.cache_manager import CacheManager
from stpstone.utils.calendars.calendar_abc import ABCCalendarOperations, TypeDatetimeDate
from stpstone.utils.parsers.str import StrHandler


class DatesBRAnbima(ABCCalendarOperations):
    """ANBIMA Brazilian holiday calendar implementation.

    This class fetches and processes holiday data from ANBIMA's Excel format,
    providing standardized holiday information for financial operations.

    References
    ----------
    .. [1] https://www.anbima.com.br/feriados/arqs/feriados_nacionais.xls
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
        """Initialize the DatesBRAnbima class.
        
        Parameters
        ----------
        bool_persist_cache : bool, optional
            If True, saves cache to disk; if False, uses in-memory cache only (default: True)
        bool_reuse_cache : bool, optional
            If True, caches in-memory; if False, does not cache in-memory (default: True)
        int_days_cache_expiration : int, optional
            Number of days after which the cache expires (default: 1)
        int_cache_ttl_days : int, optional
            Number of days after which the cache is considered expired (default: 30)
        path_cache_dir : Optional[str], optional
            Path to the cache directory (default: None)
        logger : Optional[Logger], optional
            The logger to use (default: None)
        
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
        self.cls_str_handler = StrHandler()

    def holidays(self) -> list[tuple[str, date]]:
        """Get list of Brazilian holidays from ANBIMA.

        Returns
        -------
        list[tuple[str, date]]
            List of holiday tuples containing (name, date)
        """
        df_ = self.get_holidays_raw_cached()
        df_ = self.transform_holidays(df_)
        return [(row["NAME"], row["DATE"]) for _, row in df_.iterrows()]

    @CacheManager.cache_df(key="br_anbima_holidays_raw")
    def get_holidays_raw_cached(
        self, 
        timeout: Union[int, float, tuple[float, float], tuple[int, int]] = (12.0, 21.0)
    ) -> pd.DataFrame:
        """Fetch raw holiday data from ANBIMA Excel file and cache it.

        Parameters
        ----------
        timeout : Union[int, float, tuple[float, float], tuple[int, int]], optional
            Timeout for HTTP request, by default (12.0, 21.0)

        Returns
        -------
        pd.DataFrame
            Raw holiday data with DATE, WEEKDAY, NAME columns
        """
        return self.get_holidays_raw(timeout=timeout)
    
    def get_holidays_raw(
        self, 
        timeout: Union[int, float, tuple[float, float], tuple[int, int]] = (12.0, 21.0)
    ) -> pd.DataFrame:
        """Fetch raw holiday data from ANBIMA Excel file.

        Parameters
        ----------
        timeout : Union[int, float, tuple[float, float], tuple[int, int]], optional
            Timeout for HTTP request, by default (12.0, 21.0)

        Returns
        -------
        pd.DataFrame
            Raw holiday data with DATE, WEEKDAY, NAME columns
        """
        url = "https://www.anbima.com.br/feriados/arqs/feriados_nacionais.xls"

        dict_headers = {
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7", # noqa E501: line too long
            "accept-language": "en-US,en;q=0.9,pt;q=0.8,es;q=0.7",
            "user-agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36", # noqa E501: line too long
        }

        resp_req = requests.get(url, headers=dict_headers, timeout=timeout)
        resp_req.raise_for_status()
        self._validate_response_content(resp_req.content)

        return pd.read_excel(
            BytesIO(resp_req.content),
            header=None,
            names=["DATE", "WEEKDAY", "NAME"],
            skiprows=1,
        )

    def transform_holidays(self, df_: pd.DataFrame) -> pd.DataFrame:
        """Transform raw holiday data into standardized format.

        Parameters
        ----------
        df_ : pd.DataFrame
            Raw holiday data from ANBIMA

        Returns
        -------
        pd.DataFrame
            Standardized holiday data with proper types
        """
        self._validate_dataframe(df_, "df_holidays_raw")

        df_ = df_.astype({
            "DATE": str,
            "WEEKDAY": str,
            "NAME": str
        })
        df_ = self._remove_footer(df_)
        df_["DATE"] = [self.timestamp_to_date(d, substr_timestamp=" ") 
                       for d in df_["DATE"].tolist()]
        df_["NAME"] = [
            self.cls_str_handler.remove_diacritics(self.cls_str_handler.latin_characters(x)) 
            for x in df_["NAME"].tolist()
        ]

        self._validate_dataframe(df_, "df_holidays_standardized")
        return df_

    def _remove_footer(self, df_: pd.DataFrame) -> pd.DataFrame:
        """Remove footer content from ANBIMA DataFrame.

        Parameters
        ----------
        df_ : pd.DataFrame
            Raw holiday DataFrame

        Returns
        -------
        pd.DataFrame
            DataFrame with footer rows removed
        """
        self._validate_dataframe(df_, "df_to_remove_footer")
        footer_index = None

        for idx, row in df_.iterrows():
            if any("fonte: anbima" in str(cell).lower() for cell in row if pd.notna(cell)):
                footer_index = idx
                break

        if footer_index is not None:
            df_ = df_.iloc[:footer_index]

        self._validate_dataframe(df_, "df_removed_footer")
        return df_

    def _validate_dataframe(self, df_: pd.DataFrame, name: str) -> None:
        """Validate DataFrame structure and content.

        Parameters
        ----------
        df_ : pd.DataFrame
            DataFrame to validate
        name : str
            Variable name for error messages

        Raises
        ------
        ValueError
            If DataFrame is empty, None, or has invalid structure
        """
        if df_ is None:
            raise ValueError(f"{name} cannot be None")
        if not isinstance(df_, pd.DataFrame):
            raise ValueError(f"{name} must be a pandas DataFrame")
        if df_.empty:
            raise ValueError(f"{name} cannot be empty")

    def _validate_response_content(self, content: bytes) -> None:
        """Validate HTTP response content.

        Parameters
        ----------
        content : bytes
            Response content to validate

        Raises
        ------
        ValueError
            If content is empty or None
        """
        if content is None:
            raise ValueError("Response content cannot be None")
        if len(content) == 0:
            raise ValueError("Response content cannot be empty")


class DatesBRFebraban(ABCCalendarOperations):
    """FEBRABAN Brazilian holiday calendar implementation.

    This class fetches and processes holiday data from FEBRABAN's JSON API,
    providing standardized holiday information for financial operations.

    References
    ----------
    .. [1] https://feriadosbancarios.febraban.org.br/
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
        """Initialize the DatesBRFebraban class.
        
        Parameters
        ----------
        int_year_start : int, optional
            Starting year for holidays (default: (date.today() - timedelta(days=22)).year - 1)
        int_year_end : int, optional
            Ending year for holidays (default: (date.today() - timedelta(days=22)).year)
        bool_persist_cache : bool, optional
            If True, saves cache to disk; if False, uses in-memory cache only (default: True)
        bool_reuse_cache : bool, optional
            If True, caches in-memory; if False, does not cache in-memory (default: True)
        int_days_cache_expiration : int, optional
            Number of days after which the cache expires (default: 1)
        int_cache_ttl_days : int, optional
            Number of days after which the cache is considered expired (default: 30)
        path_cache_dir : Optional[str], optional
            Path to the cache directory (default: None)
        logger : Optional[Logger], optional
            The logger to use (default: None)
        
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
        self.cls_str_handler = StrHandler()

    def holidays(self) -> list[tuple[str, date]]:
        """Get list of Brazilian holidays from FEBRABAN.

        Returns
        -------
        list[tuple[str, date]]
            List of holiday tuples containing (name, date)
        """
        df_ = self.get_holidays_years()
        df_ = self.transform_holidays(df_)
        return [(row["NOME_FERIADO"], row["DIA_MES_ANO"]) for _, row in df_.iterrows()]

    @CacheManager.cache_df(key="br_febraban_holidays_raw")
    def get_holidays_years(self) -> pd.DataFrame:
        """Fetch holiday data for multiple years from FEBRABAN.

        Parameters
        ----------
        int_year_start : int, optional
            Starting year, by default (date.today() - timedelta(days=22)).year - 1
        int_year_end : int, optional
            Ending year, by default (date.today() - timedelta(days=22)).year

        Returns
        -------
        pd.DataFrame
            Combined holiday data for multiple years
        """
        self._validate_year_range(self.int_year_start, self.int_year_end)
        list_holidays = []
        for int_year in range(self.int_year_start, self.int_year_end + 1):
            raw_data = self.get_holidays_raw(int_year)
            df_ = pd.DataFrame(raw_data) if isinstance(raw_data, list) else raw_data
            df_["ANO"] = int_year
            list_holidays.extend(df_.to_dict(orient="records"))
        return pd.DataFrame(list_holidays)

    def get_holidays_raw(
        self, 
        int_year: int, 
        timeout: Union[int, float, tuple[float, float], tuple[int, int]] = (12.0, 21.0)
    ) -> pd.DataFrame:
        """Fetch raw holiday data from FEBRABAN API for specific year.

        Parameters
        ----------
        int_year : int
            Year to fetch holidays for
        timeout : Union[int, float, tuple[float, float], tuple[int, int]], optional
            Timeout for HTTP request, by default (12.0, 21.0)

        Returns
        -------
        pd.DataFrame
            Raw holiday data as a DataFrame
        """
        self._validate_year(int_year)
        url = f"https://feriadosbancarios.febraban.org.br/Home/ObterFeriadosFederais?ano={int_year}"

        headers = {
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "Accept-Language": "en-US,en;q=0.9,pt;q=0.8,es;q=0.7",
            "Connection": "keep-alive",
            "Referer": "https://feriadosbancarios.febraban.org.br/",
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-origin",
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36", # noqa E501: line too long
            "X-Requested-With": "XMLHttpRequest",
            "sec-ch-ua": '"Not)A;Brand";v="8", "Chromium";v="138", "Google Chrome";v="138"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"Linux"',
        }

        cookies = {
            "cookiesession1": "678A3E1BC76EE4FED06EE2AB5ECEBCA3",
            "ai_user": "mhow/6j5BLAqjymITLwhjp|2025-08-26T19:55:49.930Z",
            "_ga": "GA1.1.1869622352.1756238150",
            "ai_session": "bZbh0ZzASkZdB8vurjJhQt|1756238150270|1756238150270",
            "_ga_KJWKM4PZXY": "GS2.1.s1756238150$o1$g0$t1756238155$j55$l0$h0",
        }

        resp_req = requests.get(url, headers=headers, cookies=cookies, timeout=timeout)
        resp_req.raise_for_status()
        json_response = resp_req.json()
        self._validate_json_response(json_response, int_year)
        return pd.DataFrame(json_response)

    def transform_holidays(self, df_: pd.DataFrame) -> pd.DataFrame:
        """Transform raw holiday data into standardized format.

        Parameters
        ----------
        df_ : pd.DataFrame
            Raw holiday data from FEBRABAN

        Returns
        -------
        pd.DataFrame
            Standardized holiday data with proper types
        """
        self._validate_dataframe(df_, "df_holidays_raw")

        df_ = df_.astype({
            "diaMes": str,
            "diaSemana": str,
            "nomeFeriado": str, 
            "ANO": int
        })
        df_["diaMesAno"] = [
            self._parse_brazillian_date(row["diaMes"], int_year=row["ANO"]) 
            for _, row in df_.iterrows()
        ]
        df_.columns = [
            self.cls_str_handler.convert_case(x, "camel", "upper_constant") 
            for x in df_.columns
        ]
        df_["NOME_FERIADO"] = [
            self.cls_str_handler.remove_diacritics(self.cls_str_handler.latin_characters(x)) 
               for x in df_["NOME_FERIADO"].tolist()
        ]
        
        self._validate_dataframe(df_, "df_holidays_standardized")
        return df_

    def _parse_brazillian_date(self, date_str: str, int_year: int) -> date:
        """Parse Brazilian date string into date object.

        Parameters
        ----------
        date_str : str
            Brazilian date string (e.g., "1 de janeiro")
        int_year : int
            Year to use for date construction

        Returns
        -------
        date
            Parsed date object

        Raises
        ------
        ValueError
            If date format is invalid or parsing fails
        """
        self._validate_date_string(date_str)
        self._validate_year(int_year)
        dict_month_map = {
            "janeiro": 1,
            "fevereiro": 2,
            "marco": 3,
            "abril": 4,
            "maio": 5,
            "junho": 6,
            "julho": 7,
            "agosto": 8,
            "setembro": 9,
            "outubro": 10,
            "novembro": 11,
            "dezembro": 12,
        }

        try:
            list_parts_date = [part for part in date_str.split(" ") if part]
            day = int(list_parts_date[0])
            month_name = list_parts_date[2].lower()
            month_name = self.cls_str_handler.remove_diacritics(
                self.cls_str_handler.latin_characters(month_name)
            )
            month = dict_month_map[month_name]
            return date(year=int_year, month=month, day=day)
        except (ValueError, KeyError, IndexError) as err:
            raise ValueError(f"Invalid date format: {date_str}") from err

    def _validate_dataframe(self, df_: pd.DataFrame, name: str) -> None:
        """Validate DataFrame structure and content.

        Parameters
        ----------
        df_ : pd.DataFrame
            DataFrame to validate
        name : str
            Variable name for error messages

        Raises
        ------
        ValueError
            If DataFrame is empty, None, or has invalid structure
        """
        if df_ is None:
            raise ValueError(f"{name} cannot be None")
        if not isinstance(df_, pd.DataFrame):
            raise ValueError(f"{name} must be a pandas DataFrame")
        if df_.empty:
            raise ValueError(f"{name} cannot be empty")

    def _validate_year(self, year: int) -> None:
        """Validate year value.

        Parameters
        ----------
        year : int
            Year to validate

        Raises
        ------
        ValueError
            If year is not positive or reasonable
        """
        if not isinstance(year, int):
            raise ValueError("Year must be an integer")
        if year < 1900 or year > 2100:
            raise ValueError(f"Year must be between 1900 and 2100, got {year}")

    def _validate_year_range(self, start_year: int, end_year: int) -> None:
        """Validate year range.

        Parameters
        ----------
        start_year : int
            Start year of range
        end_year : int
            End year of range

        Raises
        ------
        ValueError
            If range is invalid or years are unreasonable
        """
        self._validate_year(start_year)
        self._validate_year(end_year)
        if start_year > end_year:
            raise ValueError(f"Start year {start_year} cannot be after end year {end_year}")

    def _validate_date_string(self, date_str: str) -> None:
        """Validate Brazilian date string format.

        Parameters
        ----------
        date_str : str
            Date string to validate

        Raises
        ------
        ValueError
            If string is empty, None, or doesn't match expected format
        """
        if not date_str:
            raise ValueError("Date string cannot be empty")
        if not isinstance(date_str, str):
            raise ValueError("Date string must be a string")
        if " de " not in date_str:
            raise ValueError(f"Date string must contain ' de ' separator: {date_str}")

    def _validate_json_response(self, json_response: list[dict], year: int) -> None:
        """Validate FEBRABAN JSON response structure.

        Parameters
        ----------
        json_response : list[dict]
            JSON response to validate
        year : int
            Year used for the request

        Raises
        ------
        ValueError
            If response is empty, None, or has invalid structure
        """
        if json_response is None:
            raise ValueError(f"JSON response for year {year} cannot be None")
        if not isinstance(json_response, list):
            raise ValueError(f"JSON response for year {year} must be a list")
        if not json_response:
            raise ValueError(f"JSON response for year {year} cannot be empty")


class DatesBRB3(ABCCalendarOperations):

    def __init__(
        self,  
        bool_add_christmas_eve: bool = False,
        bool_persist_cache: bool = True, 
        bool_reuse_cache: bool = True,
        int_days_cache_expiration: int = 1,
        int_cache_ttl_days: int = 30,
        path_cache_dir: Optional[str] = None,
        logger: Optional[Logger] = None,
    ) -> None:
        """Initialize the DatesBRB3 class.
        
        Parameters
        ----------
        bool_add_christmas_eve : bool, optional
            If True, adds Christmas Eve to the list of holidays (default: False)
        bool_persist_cache : bool, optional
            If True, saves cache to disk; if False, uses in-memory cache only (default: True)
        bool_reuse_cache : bool, optional
            If True, caches in-memory; if False, does not cache in-memory (default: True)
        int_days_cache_expiration : int, optional
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
        self.bool_add_christmas_eve = bool_add_christmas_eve
        self.cls_cache_manager = CacheManager(
            bool_persist_cache=bool_persist_cache,
            bool_reuse_cache=bool_reuse_cache,
            int_days_cache_expiration=int_days_cache_expiration,
            int_cache_ttl_days=int_cache_ttl_days,
            path_cache_dir=path_cache_dir,
            logger=logger
        )
        self.cls_dates_br_anbima = DatesBRAnbima(
            bool_persist_cache=bool_persist_cache,
            bool_reuse_cache=bool_reuse_cache,
            int_days_cache_expiration=int_days_cache_expiration,
            int_cache_ttl_days=int_cache_ttl_days,
            path_cache_dir=path_cache_dir,
            logger=logger
        )
        self.cls_str_handler = StrHandler()

    def holidays(self) -> list[tuple[str, date]]:
        """Return a list of tuples containing holiday names and dates.
        
        Returns
        -------
        list[tuple[str, date]]
            List of tuples containing holiday names and dates
        """
        df_ = self.get_holidays_transformed()
        return [(row["NAME"], row["DATE"]) for _, row in df_.iterrows()]
    
    @CacheManager.cache_df(key="br_b3_holidays_raw")
    def get_holidays_transformed(
        self,
        timeout: Union[int, float, tuple[float, float], tuple[int, int]] = (12.0, 21.0)
    ) -> pd.DataFrame:
        """Fetch raw holiday data from ANBIMA Excel file, transform it and add B3 holidays.
        
        Parameters
        ----------
        timeout : Union[int, float, tuple[float, float], tuple[int, int]], optional
            Timeout for HTTP request, by default (12.0, 21.0)
        
        Returns
        -------
        pd.DataFrame
            DataFrame containing raw holiday data
        """
        df_ = self.get_anbima_holidays(timeout=timeout)
        return self.add_holidays_b3(df_)

    def get_anbima_holidays(
        self, 
        timeout: Union[int, float, tuple[float, float], tuple[int, int]] = (12.0, 21.0)
    ) -> pd.DataFrame:
        """Fetch raw holiday data from ANBIMA Excel file and transform it.
        
        Parameters
        ----------
        timeout : Union[int, float, tuple[float, float], tuple[int, int]], optional
            Timeout for HTTP request, by default (12.0, 21.0)
        
        Returns
        -------
        pd.DataFrame
            DataFrame containing raw holiday data
        """
        df_ = self.cls_dates_br_anbima.get_holidays_raw(timeout=timeout)
        return self.cls_dates_br_anbima.transform_holidays(df_)
    
    def add_holidays_b3(self, df_holidays_anbima: pd.DataFrame) -> pd.DataFrame:
        """Add additional holidays to the DataFrame.
        
        Parameters
        ----------
        df_holidays_anbima : pd.DataFrame
            DataFrame containing raw holiday data
        
        Returns
        -------
        pd.DataFrame
            DataFrame containing raw holiday data with additional holidays
        """
        df_holidays_to_add = pd.DataFrame(self.holidays_to_add(df_holidays_anbima), 
                                          columns=["NAME", "DATE"])
        df_holidays_to_add["WEEKDAY"] = [
            self.weekday_name(date_, bool_abbreviation=False, str_timezone="America/Sao_Paulo")
            for date_ in df_holidays_to_add["DATE"].tolist()
        ]
        df_holidays_to_add = df_holidays_to_add[["DATE", "WEEKDAY", "NAME"]]
        df_ = pd.concat([df_holidays_anbima, df_holidays_to_add], ignore_index=True)\
            .reset_index(drop=True)
        return df_.sort_values(by=["DATE"])
    
    def holidays_to_add(self, df_: pd.DataFrame) -> list[tuple[str, date]]:
        """Add additional holidays to the DataFrame.
        
        Parameters
        ----------
        df_ : pd.DataFrame
            DataFrame containing raw holiday data
        
        Returns
        -------
        list[tuple[str, date]]
            List of tuples containing holiday names and dates
        """
        list_: list[tuple[str, date]] = []
        set_anbima_holidays = {row["DATE"] for _, row in df_.iterrows()}
        
        @type_checker
        def temp_is_working_day(date_current: date) -> bool:
            """Temporary is working day function to avoid recursion.
            
            Parameters
            ----------
            date_current : date
                The date to check.
            
            Returns
            -------
            bool
                True if the date is a working day, False otherwise.
            """
            return not self.is_weekend(date_current) and date_current not in set_anbima_holidays
        
        @type_checker
        def temp_add_working_days(date_: date, int_days_to_add: int) -> date:
            """Temporary add working days function to avoid recursion.
            
            Parameters
            ----------
            date_ : date
                The date to start from.
            int_days_to_add : int
                The number of working days to add.
            
            Returns
            -------
            date
                The resulting date after adding the specified number of working days.
            """
            date_current = self.date_only(date_)
            int_days_left = abs(int_days_to_add)
            int_step = 1 if int_days_to_add >= 0 else -1
            while int_days_left > 0:
                date_current += timedelta(days=int_step)
                if temp_is_working_day(date_current):
                    int_days_left -= 1
            return date_current
        
        set_years = {self.year_number(date_) for date_ in df_["DATE"].tolist()}
        for int_year in set_years:
            last_working_day = temp_add_working_days(date(int_year + 1, 1, 1), -1)
            list_.append(("Último Dia Útil do Ano", last_working_day))
            if self.bool_add_christmas_eve:
                list_.append(("Véspera de Natal", self.get_christmas_eve(int_year)))
        return list_
    
    def get_christmas_eve(self, int_year: int) -> date:
        """Get Christmas Eve for a given year.
        
        Parameters
        ----------
        int_year : int
            Year for which to retrieve Christmas Eve.
        
        Returns
        -------
        date
            Christmas Eve for the given year.
        """
        return date(int_year, 12, 24)