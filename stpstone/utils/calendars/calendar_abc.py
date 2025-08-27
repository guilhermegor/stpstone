"""Abstract base class for calendar operations.

This module defines an abstract base class (ABC) for calendar operations,
providing a common interface for fetching and validating holidays.
"""

from abc import ABC, abstractmethod
from datetime import date, datetime, time, timedelta, timezone
import locale
import platform
from typing import Literal, Optional, TypeVar, Union
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import businesstimedelta
from dateutil.relativedelta import relativedelta
import pandas as pd

from stpstone.transformations.validation.metaclass_type_checker import ABCTypeCheckerMeta
from stpstone.utils.cache.cache_persistent import PersistentCacheDecorator
from stpstone.utils.cache.cache_reset import auto_cache_reset_methods


TypeDateFormatInput = TypeVar(
    "TypeDateFormatInput", 
    bound=Literal["DD/MM/YYYY", "YYYY-MM-DD", "YYMMDD", "DDMMYY", "DDMMYYYY", "YYYYMMDD", 
            "MM-DD-YYYY", "DD/MM/YY", "DD.MM.YY"]
)

TypeDatetimeDate = TypeVar("TypeDatetimeDate", bound=Union[datetime, date])


class ABCCalendarCore(ABC, metaclass=ABCTypeCheckerMeta):
    """Abstract base class for calendar operations."""

    def __init__(
        self, 
        bool_persist_cache: bool = True
    ) -> None:
        """Initialize the ABCCalendarCore class.
        
        Parameters
        ----------
        bool_persist_cache : bool, optional
            If True, saves cache to disk; if False, uses in-memory cache only (default: True)
        
        Returns
        -------
        None
        """
        self.bool_persist_cache = bool_persist_cache

    @abstractmethod
    def get_holidays_raw(
        self, 
        timeout: Union[int, float, tuple[float, float], tuple[int, int]] = (12.0, 21.0)
    ) -> pd.DataFrame:
        """Return a DataFrame containing raw holiday data.
        
        Returns
        -------
        pd.DataFrame
            DataFrame containing raw holiday data
        """
        pass

    @abstractmethod
    def holidays(self) -> list[tuple[str, date]]:
        """Return a list of tuples containing holiday names and dates.
        
        Returns
        -------
        list[tuple[str, date]]
            List of tuples containing holiday names and dates
        """
        pass

    @property
    def _holidays(self) -> set[date]:
        """Return a set of holiday dates.
        
        Returns
        -------
        set[date]
            Set of holiday dates
        """
        if not hasattr(self, "_holidays_cache"):
            self._holidays_cache = {tup_holiday[1] for tup_holiday in self.holidays()}
        return self._holidays_cache
    
    def clear_holidays_cache(self) -> None:
        """Clear the cached holidays to reflect updates.
        
        Returns
        -------
        None
        """
        if hasattr(self, "_holidays_cache"):
            del self._holidays_cache
        # __call__ implementation to persist cache, which will update the cache cleared
        cache_decorator = PersistentCacheDecorator(
            path_cache=f".stpstone_cache/{self.__class__.__name__.lower()}_holidays_cache.pkl",
            cache_key="holidays", 
            bool_persist_cache=self.bool_persist_cache
        )
        cache_decorator.clear_cache()

    def date_only(
        self, 
        date_: TypeDatetimeDate
    ) -> date:
        """Return the date component of a datetime or date object.
        
        Parameters
        ----------
        date_ : TypeDatetimeDate
            A datetime or date object.
        
        Returns
        -------
        date
            The date component of the input object.

        Raises
        ------
        TypeError
            If the input object is not of type datetime or date
        """
        if not isinstance(date_, (datetime, date)):
            raise TypeError(f"date_ must be of type datetime or date, got {type(date_).__name__}")
        return date_ if isinstance(date_, date) and not isinstance(date_, datetime) \
            else date_.date()
    
    def is_weekend(
        self, 
        date_: TypeDatetimeDate
    ) -> bool:
        """Return True if the given date is a weekend day, False otherwise.
        
        Parameters
        ----------
        date_ : TypeDatetimeDate
            A datetime or date object.
        
        Returns
        -------
        bool
            True if the given date is a weekend day, False otherwise.
        """
        date_ = self.date_only(date_)
        return date_.weekday() >= 5

    def is_working_day(
        self, 
        date_: TypeDatetimeDate
    ) -> bool:
        """Return True if the given date is a working day, False otherwise.
        
        Parameters
        ----------
        date_ : TypeDatetimeDate
            A datetime or date object.
        
        Returns
        -------
        bool
            True if the given date is a working day, False otherwise.
        """
        date_ = self.date_only(date_)
        return not self.is_weekend(date_) and date_ not in self._holidays
    
    def is_holiday(
        self, 
        date_: TypeDatetimeDate
    ) -> bool:
        """Return True if the given date is a holiday, False otherwise.
        
        Parameters
        ----------
        date_ : TypeDatetimeDate
            A datetime or date object.
        
        Returns
        -------
        bool
            True if the given date is a holiday, False otherwise.
        """
        date_ = self.date_only(date_)
        return date_ in self._holidays

    def holidays_in_year(
        self, 
        int_year: int
    ) -> list[int]:
        """Return a list of holiday days in the given year.
        
        Parameters
        ----------
        int_year : int
            The year for which to retrieve holiday days.
        
        Returns
        -------
        list[int]
            A list of holiday days in the given year.
        """
        return [
            int(date_.strftime("%d")) 
            for date_ in self._holidays 
            if int(date_.strftime("%Y")) == int_year
        ]

class ABCDateManipulation(ABCCalendarCore):
    """Abstract class for date manipulation operations."""

    @abstractmethod
    def get_holidays_raw(self) -> pd.DataFrame:
        """Return a DataFrame containing raw holiday data.
        
        Returns
        -------
        pd.DataFrame
            DataFrame containing raw holiday data
        """
        pass

    @abstractmethod
    def holidays(self) -> list[tuple[str, date]]:
        """Holidays abstract method implementation.
        
        Returns
        -------
        list[tuple[str, date]]
            List of tuples containing holiday names and dates
        """
        pass

    def add_working_days(
        self, 
        date_: TypeDatetimeDate, 
        int_days_to_add: int
    ) -> date:
        """Add the specified number of working days to the given date.
        
        Parameters
        ----------
        date_ : TypeDatetimeDate
            A datetime or date object.
        int_days_to_add : int
            The number of working days to add.
        
        Returns
        -------
        date
            The resulting date after adding the specified number of working days.
        """
        date_ = self.date_only(date_)
        if int_days_to_add == 0:
            return date_
        int_step = 0
        int_target_days = abs(int_days_to_add)
        int_direction = 1 if int_days_to_add > 0 else -1
        date_current = date_

        while int_step < int_target_days:
            date_current += timedelta(days=int_direction)
            if self.is_working_day(date_current):
                int_step += 1
        return date_current

    def add_calendar_days(
        self, 
        date_: TypeDatetimeDate, 
        int_days_to_add: int
    ) -> date:
        """Add the specified number of calendar days to the given date.
        
        Parameters
        ----------
        date_ : TypeDatetimeDate
            A datetime or date object.
        int_days_to_add : int
            The number of calendar days to add.
        
        Returns
        -------
        date
            The resulting date after adding the specified number of calendar days.
        """
        date_ = self.date_only(date_)
        return date_ + timedelta(days=int_days_to_add)

    def add_months(
        self, 
        date_: datetime, 
        int_months_to_add: int
    ) -> datetime:
        """Add the specified number of months to the given date.
        
        Parameters
        ----------
        date_ : datetime
            A datetime object.
        int_months_to_add : int
            The number of months to add.
        
        Returns
        -------
        datetime
            The resulting datetime after adding the specified number of months.
        """
        return date_ + relativedelta(months=int_months_to_add)

    def build_date(
        self, 
        year: int, 
        month: int, 
        day: int
    ) -> date:
        """Build a date object from the given year, month, and day.
        
        Parameters
        ----------
        year : int
            The year component of the date.
        month : int
            The month component of the date.
        day : int
            The day component of the date.
        
        Returns
        -------
        date
            The built date object.
        """
        return date(year=year, month=month, day=day)

    def build_datetime(
        self, 
        year: int, 
        month: int, 
        day: int, 
        hour: int, 
        minute: int, 
        second: int, 
        str_timezone: Optional[str] = "UTC"
    ) -> datetime:
        """Build a datetime object from the given year, month, day, hour, minute, and second.
        
        Parameters
        ----------
        year : int
            The year component of the datetime.
        month : int
            The month component of the datetime.
        day : int
            The day component of the datetime.
        hour : int
            The hour component of the datetime.
        minute : int
            The minute component of the datetime.
        second : int
            The second component of the datetime.
        str_timezone : Optional[str], optional
            The timezone component of the datetime, by default "UTC".
        
        Returns
        -------
        datetime
            The built datetime object.

        Raises
        ------
        ZoneInfoNotFoundError
            If the timezone is empty or None
        ValueError
            If the date components are invalid
        """
        if str_timezone == "" or str_timezone is None:
            raise ZoneInfoNotFoundError("Timezone cannot be empty or None")
        try:
            return datetime(
                year=year, 
                month=month, 
                day=day, 
                hour=hour, 
                minute=minute, 
                second=second, 
                tzinfo=ZoneInfo(str_timezone)
            )
        except ValueError as err:
            raise ValueError(f"Invalid date components: {err}") from err

    def nearest_working_day(
        self, 
        date_: TypeDatetimeDate, 
        bool_next: bool = True
    ) -> date:
        """Find the nearest working day to the given date.
        
        Parameters
        ----------
        date_ : TypeDatetimeDate
            A datetime or date object.
        bool_next : bool, optional
            If True, returns the nearest working day after the given date; if False, 
            returns the nearest working day before the given date, by default True
        
        Returns
        -------
        date
            The nearest working day to the given date.
        """
        date_ = self.date_only(date_)
        date_ref = self.add_working_days(self.add_working_days(date_, -1), 1)
        if bool_next:
            return date_ref
        else:
            return self.add_working_days(date_ref, -1) if date_ref > date_ else date_ref

    def str_date_to_date(
        self, 
        str_date: str, 
        format_input: TypeDateFormatInput = "DD/MM/YYYY"
    ) -> date:
        """Convert a string representation of a date to a date object.
        
        Parameters
        ----------
        str_date : str
            The string representation of the date.
        format_input : TypeDateFormatInput, optional
            The format of the input date string, by default "DD/MM/YYYY"
        
        Returns
        -------
        date
            The date object corresponding to the input string.

        Raises
        ------
        ValueError
            If the input date string is not in the specified format.
        """
        format_map = {
            "DD/MM/YYYY": "%d/%m/%Y",
            "YYYY-MM-DD": "%Y-%m-%d",
            "YYMMDD": "%y%m%d",
            "DDMMYY": "%d%m%y",
            "DDMMYYYY": "%d%m%Y",
            "YYYYMMDD": "%Y%m%d",
            "MM-DD-YYYY": "%m-%d-%Y",
            "DD/MM/YY": "%d/%m/%y",
            "DD.MM.YY": "%d.%m.%y"
        }
        
        if format_input not in format_map or format_input is None:
            raise ValueError(f"Not a valid date format: {format_input}")
        
        try:
            dt = datetime.strptime(str_date, format_map[format_input])
            return dt.date()
        except ValueError as err:
            raise ValueError(
                f"Invalid date string '{str_date}' for format {format_input}: {err}") from err

    def timestamp_to_date(
        self,
        timestamp_: str,
        substr_timestamp: str = "T",
    ) -> date:
        """Convert a string representation of a timestamp to a date object.
        
        Parameters
        ----------
        timestamp_ : str
            The string representation of the timestamp.
        substr_timestamp : str, optional
            The substring to split the timestamp on, by default "T"
        
        Returns
        -------
        date
            The date object corresponding to the input timestamp string.
        """
        return self.str_date_to_date(timestamp_.split(substr_timestamp)[0], "YYYY-MM-DD")

    def timestamp_to_datetime(
        self,
        timestamp_: str,
        substr_timestamp: str = "T",
    ) -> datetime:
        """Convert a string representation of a timestamp to a datetime object.
        
        Parameters
        ----------
        timestamp_ : str
            The string representation of the timestamp.
        substr_timestamp : str, optional
            The substring to split the timestamp on, by default "T"
        
        Returns
        -------
        datetime
            The datetime object corresponding to the input timestamp string.

        Raises
        ------
        ValueError
            If the input timestamp string is not in the expected format.
        """
        try:
            return datetime.fromisoformat(timestamp_.replace("Z", "+00:00"))
        except ValueError:
            try:
                return datetime.strptime(timestamp_, f"%Y-%m-%d{substr_timestamp}%H:%M:%S")
            except ValueError as err:
                raise ValueError(
                    f"Failed to parse timestamp '{timestamp_}' in format "
                    f"'YYYY-MM-DD{substr_timestamp}HH:MM:SS' or ISO 8601: {str(err)}"
                ) from err
    
    def to_integer(
        self, 
        date_: TypeDatetimeDate
    ) -> int:
        """Convert a date object to an integer.
        
        Parameters
        ----------
        date_ : TypeDatetimeDate
            A datetime or date object.
        
        Returns
        -------
        int
            The integer representation of the date.
        """
        return 10000 * date_.year + 100 * date_.month + date_.day

    def excel_float_to_date(
        self, 
        int_excel_date: int
    ) -> date:
        """Convert an Excel float to a date object.
        
        Parameters
        ----------
        int_excel_date : int
            The Excel float to convert.
        
        Returns
        -------
        date
            The date object corresponding to the Excel float.

        Raises
        ------
        ValueError
            If int_excel_date is None or negative
        """
        if int_excel_date is None:
            raise ValueError("int_excel_date cannot be None")
        if int_excel_date < 0:
            raise ValueError("int_excel_date cannot be negative")
        return (datetime(1899, 12, 30) + timedelta(days=int_excel_date)).date()


class ABCTimezoneAware(ABCDateManipulation):
    """Abstract class for date manipulation with timezone support."""

    @abstractmethod
    def get_holidays_raw(self) -> pd.DataFrame:
        """Return a DataFrame containing raw holiday data.
        
        Returns
        -------
        pd.DataFrame
            DataFrame containing raw holiday data
        """
        pass

    @abstractmethod
    def holidays(self) -> list[tuple[str, date]]:
        """Holidays abstract method implementation.
        
        Returns
        -------
        list[tuple[str, date]]
            List of tuples containing holiday names and dates
        """
        pass

    def str_date_to_datetime(
        self, 
        str_date: str, 
        format_input: TypeDateFormatInput = "DD/MM/YYYY",
        str_timezone: Optional[str] = "UTC",
    ) -> datetime:
        """Convert a string representation of a date to a datetime object.
        
        Parameters
        ----------
        str_date : str
            The string representation of the date.
        format_input : TypeDateFormatInput, optional
            The format of the input date string, by default "DD/MM/YYYY"
        str_timezone : Optional[str], optional
            The timezone to use for the resulting datetime object, by default "UTC"
        
        Returns
        -------
        datetime
            The datetime object corresponding to the input string.
        """
        date_obj = self.str_date_to_date(str_date, format_input)
        return datetime.combine(date_obj, time(0, 0), tzinfo=ZoneInfo(str_timezone))

    def change_timezone(
        self,
        date_: TypeDatetimeDate,
        target_tz: str = "UTC",
        source_tz: Optional[str] = None
    ) -> datetime:
        """Change the timezone of a datetime or date object.
        
        Parameters
        ----------
        date_ : TypeDatetimeDate
            The datetime or date object to change the timezone of.
        target_tz : str, optional
            The target timezone, by default "UTC"
        source_tz : Optional[str], optional
            The source timezone, by default None
        
        Returns
        -------
        datetime
            The datetime object with the changed timezone.

        Raises
        ------
        ValueError
            If date_ is a date object and source_tz is None
        """
        if isinstance(date_, date) and not isinstance(date_, datetime):
            date_ = self.date_to_datetime(date_, str_timezone=target_tz)
        elif date_.tzinfo is None:
            if source_tz is None:
                raise ValueError("Cannot change timezone of naive datetime without source_tz")
            date_ = date_.replace(tzinfo=ZoneInfo(source_tz))
        return date_.astimezone(ZoneInfo(target_tz))

    def date_to_datetime(
        self, 
        date_: date,
        str_timezone: Optional[str] = "UTC"
    ) -> datetime:
        """Convert a date object to a datetime object.
        
        Parameters
        ----------
        date_ : date
            The date object to convert.
        str_timezone : Optional[str], optional
            The timezone to use, by default "UTC"
        
        Returns
        -------
        datetime
            The datetime object corresponding to the input date.
        """
        return datetime.combine(date_, time(0, 0), tzinfo=ZoneInfo(str_timezone))

    def to_unix_timestamp(
        self, 
        date_: Union[TypeDatetimeDate, time], 
        str_timezone: Optional[str] = "UTC"
    ) -> int:
        """Convert date to unix timestamp (seconds since January 1, 1970, 00:00:00 UTC).

        Parameters
        ----------
        date_ : Union[TypeDatetimeDate, time]
            Date to convert.
        str_timezone : Optional[str]
            Timezone to use, defaults to UTC.

        Returns
        -------
        int
            Unix timestamp.
        """
        if isinstance(date_, time):
            today = date.today()
            date_ = datetime.combine(today, date_)
            date_ = date_.replace(tzinfo=ZoneInfo(str_timezone))
        elif isinstance(date_, date) and not isinstance(date_, datetime):
            date_ = self.date_to_datetime(date_, str_timezone=str_timezone)
        if date_.tzinfo is None:
            date_ = date_.replace(tzinfo=ZoneInfo(str_timezone))
        return int(date_.timestamp())

    def unix_timestamp_to_datetime(
        self, 
        unix_timestamp: Union[float, int], 
        str_timezone: Optional[str] = "UTC"
    ) -> datetime:
        """Convert unix timestamp to datetime object.
        
        Parameters
        ----------
        unix_timestamp : Union[float, int]
            The unix timestamp to convert.
        str_timezone : Optional[str], optional
            The timezone to use, by default "UTC"
        
        Returns
        -------
        datetime
            The datetime object corresponding to the input unix timestamp.
        """
        return datetime.fromtimestamp(unix_timestamp, tz=ZoneInfo(str_timezone))

    def unix_timestamp_to_date(
        self, 
        unix_timestamp: Union[float, int], 
        str_timezone: Optional[str] = "UTC"
    ) -> date:
        """Convert unix timestamp to date object.
        
        Parameters
        ----------
        unix_timestamp : Union[float, int]
            The unix timestamp to convert.
        str_timezone : Optional[str], optional
            The timezone to use, by default "UTC"
        
        Returns
        -------
        date
            The date object corresponding to the input unix timestamp.
        """
        return self.unix_timestamp_to_datetime(unix_timestamp, str_timezone=str_timezone).date()

    def iso_to_unix_timestamp(
        self, 
        iso_timestamp: str,
        str_timezone: Optional[str] = "UTC"
    ) -> int:
        """Convert ISO timestamp to unix timestamp (seconds since January 1, 1970, 00:00:00 UTC).
        
        Parameters
        ----------
        iso_timestamp : str
            The ISO timestamp to convert.
        str_timezone : Optional[str], optional
            The timezone to use, by default "UTC"
        
        Returns
        -------
        int
            The unix timestamp corresponding to the input ISO timestamp.
        """
        date_ = datetime.fromisoformat(iso_timestamp)
        date_ = date_.astimezone(tz=ZoneInfo(str_timezone))
        return date_.timestamp()
    
    def excel_float_to_datetime(
        self, 
        float_date: float, 
        str_timezone: Optional[str] = "UTC"
    ) -> datetime:
        """Convert Excel float date to datetime object.
        
        Parameters
        ----------
        float_date : float
            The Excel float date to convert.
        str_timezone : Optional[str], optional
            The timezone to use, by default "UTC"
        
        Returns
        -------
        datetime
            The datetime object corresponding to the input Excel float date.
        """
        date_ref = datetime(1899, 12, 30, tzinfo=ZoneInfo(str_timezone))
        int_days = int(float_date)
        int_seconds_day = 86400

        float_fractional_days = float_date - int_days
        int_days += 1 if int_days > 60 else 0
        date_ref = date_ref + timedelta(days=int_days)
        int_seconds = int(float_fractional_days * int_seconds_day)
        date_ref = date_ref + timedelta(seconds=int_seconds)

        return date_ref


class ABCRangeDatesDelta(ABCDateManipulation):
    """Abstract class for range dates and delta operations."""

    @abstractmethod
    def get_holidays_raw(self) -> pd.DataFrame:
        """Return a DataFrame containing raw holiday data.
        
        Returns
        -------
        pd.DataFrame
            DataFrame containing raw holiday data
        """
        pass

    @abstractmethod
    def holidays(self) -> list[tuple[str, date]]:
        """Holidays abstract method implementation.
        
        Returns
        -------
        list[tuple[str, date]]
            List of tuples containing holiday names and dates
        """
        pass

    def working_days_range(
        self, 
        date_start: TypeDatetimeDate, 
        date_end: TypeDatetimeDate
    ) -> set[date]:
        """Return a set of working days between two dates.
        
        Parameters
        ----------
        date_start : TypeDatetimeDate
            Start date.
        date_end : TypeDatetimeDate
            End date.
        
        Returns
        -------
        set[date]
            Set of working days between two dates.

        Raises
        ------
        ValueError
            If date_end is less than date_start
        """
        date_start = self.date_only(date_start)
        date_end = self.date_only(date_end)
        if date_end < date_start:
            raise ValueError("date_end must be greater than date_start")
        
        return {
            date_ for date_ in \
                (date_start + timedelta(days=i) for i in range((date_end - date_start).days + 1))
            if self.is_working_day(date_)
        }

    def calendar_days_range(
        self, 
        date_start: TypeDatetimeDate, 
        date_end: TypeDatetimeDate
    ) -> set[date]:
        """Return a set of calendar days between two dates.
        
        Parameters
        ----------
        date_start : TypeDatetimeDate
            Start date.
        date_end : TypeDatetimeDate
            End date.
        
        Returns
        -------
        set[date]
            Set of calendar days between two dates.

        Raises
        ------
        ValueError
            If date_end is less than date_start
        """
        date_start = self.date_only(date_start)
        date_end = self.date_only(date_end)
        if date_end < date_start:
            raise ValueError("date_end must be greater than date_start")
        
        return {date_start + timedelta(days=i) for i in range((date_end - date_start).days + 1)}

    def years_between_dates(
        self, 
        date_start: TypeDatetimeDate, 
        date_end: TypeDatetimeDate
    ) -> set[int]:
        """Return a set of years between two dates.
        
        Parameters
        ----------
        date_start : TypeDatetimeDate
            Start date.
        date_end : TypeDatetimeDate
            End date.
        
        Returns
        -------
        set[int]
            Set of years between two dates.

        Raises
        ------
        ValueError
            If date_end is less than date_start
        """
        date_start = self.date_only(date_start)
        date_end = self.date_only(date_end)
        if date_end < date_start:
            raise ValueError("date_end must be greater than date_start")
        
        list_ = self.calendar_days_range(date_start, date_end)
        return set(int(date_.strftime("%Y")) for date_ in list_)
    
    def delta_working_days(
        self, 
        date_start: TypeDatetimeDate, 
        date_end: TypeDatetimeDate
    ) -> int:
        """Return the number of working days between two dates.
        
        Parameters
        ----------
        date_start : TypeDatetimeDate
            Start date.
        date_end : TypeDatetimeDate
            End date.
        
        Returns
        -------
        int
            Number of working days between two dates.

        Raises
        ------
        ValueError
            If date_end is less than date_start
        """
        date_start = self.date_only(date_start)
        date_end = self.date_only(date_end)
        if date_end < date_start:
            raise ValueError("date_end must be greater than date_start")
        
        return len(self.working_days_range(date_start, date_end)) - 1 \
            if self.is_working_day(date_start) \
            else len(self.working_days_range(date_start, date_end))
    
    def delta_calendar_days(
        self, 
        date_start: TypeDatetimeDate, 
        date_end: TypeDatetimeDate
    ) -> int:
        """Return the number of calendar days between two dates.
        
        Parameters
        ----------
        date_start : TypeDatetimeDate
            Start date.
        date_end : TypeDatetimeDate
            End date.
        
        Returns
        -------
        int
            Number of calendar days between two dates.

        Raises
        ------
        ValueError
            If date_end is less than date_start
        """
        date_start = self.date_only(date_start)
        date_end = self.date_only(date_end)
        if date_end < date_start:
            raise ValueError("date_end must be greater than date_start")
        
        return (date_end - date_start).days
    
    def get_start_end_day_month(
        self, 
        date_: TypeDatetimeDate, 
        bool_working_days: bool = False
    ) -> tuple[date, date]:
        """Return the start and end date of the month of the given date.
        
        Parameters
        ----------
        date_ : TypeDatetimeDate
            Date.
        bool_working_days : bool, optional
            If True, the start and end date will be the nearest working day.
            Default is False.
        
        Returns
        -------
        tuple[date, date]
            Start and end date of the month of the given date.
        """
        date_ = self.date_only(date_)

        date_start = date(date_.year, date_.month, 1)
        date_start = self.nearest_working_day(date_start, bool_next=True) \
            if bool_working_days else date_start
        
        date_end = date(date_.year, date_.month + 1, 1) if date_.month < 12 \
            else date(date_.year + 1, 1, 1)
        date_end = self.nearest_working_day(date_end, bool_next=False) \
            if bool_working_days else date_end
        
        return date_start, date_end
    
    def get_dates_weekday_month(
        self,
        year: int,
        month: int,
        weekday: int,
    ) -> list[date]:
        """Return a list of dates for the given year, month, and weekday.
        
        Parameters
        ----------
        year : int
            Year.
        month : int
            Month.
        weekday : int
            Weekday.
        
        Returns
        -------
        list[date]
            List of dates for the given year, month, and weekday.

        Raises
        ------
        ValueError
            If month is not between 1 and 12
            If weekday is not between 0 (Monday) and 6 (Sunday)
        """
        if not 1 <= month <= 12:
            raise ValueError("Month must be between 1 and 12")
        if not 0 <= weekday <= 6:
            raise ValueError("Weekday must be between 0 (Monday) and 6 (Sunday)")
        
        list_ = []
        date_start = date(year, month, 1)
        date_end = date(year, month + 1, 1) if month < 12 else date(year + 1, 1, 1)
        date_ = date_start

        while date_.weekday() != weekday:
            date_ += timedelta(days=1)
        
        while date_ <= date_end:
            list_.append(date_)
            date_ += timedelta(days=7)

        return list_

    def get_nth_weekday_month(
        self,
        year: int,
        month: int,
        weekday: int,
        n: int, 
        bool_working_days: bool = True, 
        bool_next_working_day: bool = True
    ) -> date:
        """Return the nth weekday of the month.
        
        Parameters
        ----------
        year : int
            Year.
        month : int
            Month.
        weekday : int
            Weekday.
        n : int
            Nth.
        bool_working_days : bool, optional
            If True, the date will be the nearest working day.
            Default is True.
        bool_next_working_day : bool, optional
            If True, the date will be the next working day.
            Default is True.
        
        Returns
        -------
        date
            Nth weekday of the month.

        Raises
        ------
        ValueError
            If month is not between 1 and 12
            If weekday is not between 0 (Monday) and 6 (Sunday)
            If n is 0
            If n is greater than the number of weekdays in the month
        """
        if not 1 <= month <= 12:
            raise ValueError("Month must be between 1 and 12")
        if not 0 <= weekday <= 6:
            raise ValueError("Weekday must be between 0 (Monday) and 6 (Sunday)")
        if n == 0:
            raise ValueError("n must be non-zero")

        date_ref = self.get_dates_weekday_month(year, month, weekday)[n - 1]
        int_len_dates_weekday_month = len(self.get_dates_weekday_month(year, month, weekday))
        if n > int_len_dates_weekday_month:
            raise ValueError(f"n must be less than or equal to {int_len_dates_weekday_month}")

        return self.nearest_working_day(date_ref, bool_next=bool_next_working_day) \
            if bool_working_days else date_ref

    def last_working_day_years(
        self, 
        list_years: list[int]
    ) -> list[date]:
        """Return a list of last working days in the given years.
        
        Parameters
        ----------
        list_years : list[int]
            List of years.
        
        Returns
        -------
        list[date]
            List of last working days in the given years.
        """
        return [self.add_working_days(date(y + 1, 1, 1), -1) for y in list_years]

    def delta_working_hours(
        self,
        timestamp_start: str,
        timestamp_end: str,
        int_hour_start_office: int = 8,
        int_minute_start_office: int = 0,
        int_hour_end_office: int = 18,
        int_minute_end_office: int = 0,
        int_hour_start_lunch: int = 12,
        int_minute_start_lunch: int = 0,
        int_hour_end_lunch: int = 13,
        int_minute_end_lunch: int = 0,
        list_working_days_range: Optional[list[int]] = None,
        substr_timestamp: str = "T",
    ) -> int:
        """Return the number of working hours between two timestamps.
        
        Parameters
        ----------
        timestamp_start : str
            Start timestamp.
        timestamp_end : str
            End timestamp.
        int_hour_start_office : int, optional
            Start hour of office.
            Default is 8.
        int_minute_start_office : int, optional
            Start minute of office.
            Default is 0.
        int_hour_end_office : int, optional
            End hour of office.
            Default is 18.
        int_minute_end_office : int, optional
            End minute of office.
            Default is 0.
        int_hour_start_lunch : int, optional
            Start hour of lunch.
            Default is 12.
        int_minute_start_lunch : int, optional
            Start minute of lunch.
            Default is 0.
        int_hour_end_lunch : int, optional
            End hour of lunch.
            Default is 13.
        int_minute_end_lunch : int, optional
            End minute of lunch.
            Default is 0.
        list_working_days_range : Optional[list[int]], optional
            List of working days.
            Default is [0, 1, 2, 3, 4] (monday to friday).
        substr_timestamp : str, optional
            Substring to remove from the timestamp.
            Default is "T".
        
        Returns
        -------
        int
            Number of working hours between two timestamps.
        """
        timestamp_start = self.timestamp_to_datetime(timestamp_start, substr_timestamp)
        timestamp_end = self.timestamp_to_datetime(timestamp_end, substr_timestamp)
        
        if list_working_days_range is None:
            list_working_days_range = [0, 1, 2, 3, 4]  # monday to friday
        
        # get holidays for the relevant years
        holidays_dict = {}
        for year in range(timestamp_start.year, timestamp_end.year + 1):
            for holiday_name, holiday_date in self.holidays():
                if holiday_date.year == year:
                    holidays_dict[holiday_date] = holiday_name
        
        # create business time rules
        workday = businesstimedelta.WorkDayRule(
            start_time=time(int_hour_start_office, int_minute_start_office),
            end_time=time(int_hour_end_office, int_minute_end_office),
            working_days=list_working_days_range,
        )
        
        lunchbreak = businesstimedelta.LunchTimeRule(
            start_time=time(int_hour_start_lunch, int_minute_start_lunch),
            end_time=time(int_hour_end_lunch, int_minute_end_lunch),
            working_days=list_working_days_range,
        )
        
        rules = [workday, lunchbreak]
        
        if holidays_dict:
            holidays_rule = businesstimedelta.HolidayRule(holidays_dict)
            rules.append(holidays_rule)
        
        businesshrs = businesstimedelta.Rules(rules)
        delta = businesshrs.difference(timestamp_start, timestamp_end)
        
        # convert timedelta to hours
        return int(delta.hours)


class ABCCurrentDate(ABCCalendarCore):
    """Abstract class for getting current date and time."""

    @abstractmethod
    def get_holidays_raw(self) -> pd.DataFrame:
        """Return a DataFrame containing raw holiday data.
        
        Returns
        -------
        pd.DataFrame
            DataFrame containing raw holiday data
        """
        pass

    @abstractmethod
    def holidays(self) -> list[tuple[str, date]]:
        """Holidays abstract method implementation.
        
        Returns
        -------
        list[tuple[str, date]]
            List of tuples containing holiday names and dates
        """
        pass

    def curr_date(self) -> date:
        """Return the current date.
        
        Returns
        -------
        date
            Current date.
        """
        return date.today()

    def curr_datetime(
        self, 
        str_timezone: Optional[str] = "UTC"
    ) -> datetime:
        """Return the current datetime.
        
        Parameters
        ----------
        str_timezone : Optional[str], optional
            The timezone to use, by default "UTC"
        
        Returns
        -------
        datetime
            Current datetime.
        """
        return datetime.now(tz=ZoneInfo(str_timezone))

    def curr_time(
        self, 
        str_timezone: Optional[str] = "UTC"
    ) -> time:
        """Return the current time.
        
        Parameters
        ----------
        str_timezone : Optional[str], optional
            The timezone to use, by default "UTC"
        
        Returns
        -------
        time
            Current time.
        """
        return self.curr_datetime(str_timezone=str_timezone).time()

    def current_timestamp_string(
        self, 
        format_output: str = "%Y%m%d_%H%M%S", 
        str_timezone: Optional[str] = "UTC"
    ) -> str:
        """Return the current timestamp as a string.
        
        Parameters
        ----------
        format_output : str, optional
            The format to use, by default "%Y%m%d_%H%M%S"
        str_timezone : Optional[str], optional
            The timezone to use, by default "UTC"
        
        Returns
        -------
        str
            Current timestamp as a string.
        """
        return self.curr_datetime(str_timezone=str_timezone).strftime(format_output)
    

class ABCDateFormatter(ABCCalendarCore):
    """Abstract class for date formatting."""

    @abstractmethod
    def get_holidays_raw(self) -> pd.DataFrame:
        """Return a DataFrame containing raw holiday data.
        
        Returns
        -------
        pd.DataFrame
            DataFrame containing raw holiday data
        """
        pass

    @abstractmethod
    def holidays(self) -> list[tuple[str, date]]:
        """Holidays abstract method implementation.
        
        Returns
        -------
        list[tuple[str, date]]
            List of tuples containing holiday names and dates
        """
        pass

    def get_platform_locale(
        self,
        str_locale: Optional[str] = None,
        str_timezone: Optional[str] = None
    ) -> str:
        """Return the platform locale.
        
        Parameters
        ----------
        str_locale : Optional[str], optional
            The locale to use, by default None
        str_timezone : Optional[str], optional
            The timezone to use, by default None
        
        Returns
        -------
        str
            The platform locale.

        Raises
        ------
        ValueError
            If the locale is not found
        """
        dict_tz_to_locale_map = {
            "America/Sao_Paulo": "pt-BR",
            "America/New_York": "en-US",
            "America/Chicago": "en-US",
            "America/Los_Angeles": "en-US",
            "Europe/Madrid": "es-ES",
            "Europe/Paris": "fr-FR",
            "Europe/London": "en-GB",
            "Asia/Tokyo": "ja-JP",
            "Asia/Shanghai": "zh-CN",
            "Asia/Seoul": "ko-KR",
            "UTC": "en-GB"
        }
        if str_locale is None and str_timezone is not None \
            and str_timezone in dict_tz_to_locale_map:
            str_locale = dict_tz_to_locale_map[str_timezone]
        if str_locale is None:
            str_locale = "en-GB"
        base_locale = str_locale.replace("_", "-").replace(".UTF-8", "")
        normalized_locale = base_locale if platform.system() == "Windows" \
            else f"{base_locale.replace('-', '_')}.UTF-8"
        try:
            locale.setlocale(locale.LC_TIME, normalized_locale)
            return normalized_locale
        except locale.Error:
            normalized_locale = "en_GB.UTF-8" if platform.system() != "Windows" else "en-GB"
            try:
                locale.setlocale(locale.LC_TIME, normalized_locale)
                return normalized_locale
            except locale.Error as err:
                raise ValueError(f"Invalid or unsupported locale: {str_locale}. "
                                 f"Error: {err}") from err

    def year_number(
        self, 
        date_: TypeDatetimeDate
    ) -> int:
        """Return the year number.
        
        Parameters
        ----------
        date_ : TypeDatetimeDate
            The date to get the year number from.
        
        Returns
        -------
        int
            The year number.
        """
        date_ = self.date_only(date_)
        return int(date_.strftime("%Y"))

    def month_number(
        self, date_: TypeDatetimeDate, 
        bool_month_mm: bool = False
    ) -> Union[int, str]:
        """Return the month number.
        
        Parameters
        ----------
        date_ : TypeDatetimeDate
            The date to get the month number from.
        bool_month_mm : bool, optional
            Whether to return the month number or the month name, by default False
        
        Returns
        -------
        Union[int, str]
            The month number or the month name.
        """
        date_ = self.date_only(date_)
        if not bool_month_mm:
            return int(date_.strftime("%m"))
        else:
            return date_.strftime("%m")

    def week_number(
        self, 
        date_: TypeDatetimeDate
    ) -> str:
        """Return the week number.
        
        Parameters
        ----------
        date_ : TypeDatetimeDate
            The date to get the week number from.
        
        Returns
        -------
        str
            The week number.
        """
        date_ = self.date_only(date_)
        return date_.strftime("%w")

    def day_number(
        self, 
        date_: TypeDatetimeDate
    ) -> int:
        """Return the day number.
        
        Parameters
        ----------
        date_ : TypeDatetimeDate
            The date to get the day number from.
        
        Returns
        -------
        int
            The day number.
        """
        date_ = self.date_only(date_)
        return int(date_.strftime("%d"))

    def month_name(
        self,
        date_: TypeDatetimeDate,
        bool_abbreviation: bool = False,
        str_timezone: Optional[str] = "UTC",
    ) -> str:
        """Return the month name.
        
        Parameters
        ----------
        date_ : TypeDatetimeDate
            The date to get the month name from.
        bool_abbreviation : bool, optional
            Whether to return the month name or the month abbreviation, by default False
        str_timezone : Optional[str], optional
            The timezone to use, by default "UTC"
        
        Returns
        -------
        str
            The month name or the month abbreviation.
        """
        date_ = self.date_only(date_)
        str_locale = self.get_platform_locale(str_timezone=str_timezone)
        locale.setlocale(locale.LC_TIME, str_locale)
        return date_.strftime("%b" if bool_abbreviation else "%B")            

    def week_name(
        self, 
        date_: TypeDatetimeDate, 
        bool_abbreviation: bool = False, 
        str_timezone: Optional[str] = "UTC"
    ) -> str:
        """Return the week name.
        
        Parameters
        ----------
        date_ : TypeDatetimeDate
            The date to get the week name from.
        bool_abbreviation : bool, optional
            Whether to return the week name or the week abbreviation, by default False
        str_timezone : Optional[str], optional
            The timezone to use, by default "UTC"
        
        Returns
        -------
        str
            The week name or the week abbreviation.
        """
        date_ = self.date_only(date_)
        str_locale = self.get_platform_locale(str_timezone=str_timezone)
        locale.setlocale(locale.LC_TIME, str_locale)
        return date_.strftime("%a" if bool_abbreviation else "%A")

    def utc_log_ts(self) -> datetime:
        """Return the current UTC datetime.
        
        Returns
        -------
        datetime
            The current UTC datetime.
        """
        return datetime.now(timezone.utc)
    

@auto_cache_reset_methods([
    ("holidays", ["clear_holidays_cache"])
])
class ABCCalendarOperations(
    ABCTimezoneAware, 
    ABCRangeDatesDelta, 
    ABCCurrentDate, 
    ABCDateFormatter
):
    """Abstract class for calendar operations."""

    @abstractmethod
    def get_holidays_raw(self) -> pd.DataFrame:
        """Return a DataFrame containing raw holiday data.
        
        Returns
        -------
        pd.DataFrame
            DataFrame containing raw holiday data
        """
        pass

    @abstractmethod
    def holidays(self) -> list[tuple[str, date]]:
        """Holidays abstract method implementation.
        
        Returns
        -------
        list[tuple[str, date]]
            List of tuples containing holiday names and dates
        """
        pass