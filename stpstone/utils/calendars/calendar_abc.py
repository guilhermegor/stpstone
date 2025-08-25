"""Abstract base class for calendar operations.

This module defines an abstract base class (ABC) for calendar operations,
providing a common interface for fetching and validating holidays.
"""

from abc import ABC, abstractmethod
from datetime import date, datetime, time, timedelta, timezone
import locale
import platform
from typing import Literal, Optional, TypeVar, Union
from zoneinfo import ZoneInfo

import businesstimedelta
from dateutil.relativedelta import relativedelta

from stpstone.transformations.validation.metaclass_type_checker import ABCTypeCheckerMeta
from stpstone.utils.cache.cache_persistent import PersistentCacheDecorator
from stpstone.utils.cache.cache_reset import auto_cache_reset_methods


TypeDateFormatInput = TypeVar(
    "TypeDateFormatInput", 
    Literal["DD/MM/YYYY", "YYYY-MM-DD", "YYMMDD", "DDMMYY", "DDMMYYYY", "YYYYMMDD", 
            "MM-DD-YYYY", "DD/MM/YY", "DD.MM.YY"]
)

TypeDatetimeDate = TypeVar("TypeDatetimeDate", Union[datetime, date])


class ABCCalendarCore(ABC, metaclass=ABCTypeCheckerMeta):

    def __init__(
        self, 
        bool_persist_cache: bool = True
    ) -> None:
        self.bool_persist_cache = bool_persist_cache

    @abstractmethod
    def holidays(self) -> list[tuple[str, date]]:
        pass

    @property
    def _holidays(self) -> set[date]:
        if not hasattr(self, "_holidays_cache"):
            self._holidays_cache = {tup_holiday[1] for tup_holiday in self.holidays()}
        return self._holidays_cache
    
    def clear_holidays_cache(self) -> None:
        """Clear the cached holidays to reflect updates."""
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
        return date_ if isinstance(date_, date) and not isinstance(date_, datetime) \
            else date_.date()
    
    def is_weekend(
        self, 
        date_: TypeDatetimeDate
    ) -> bool:
        date_ = self.date_only(date_)
        return date_.weekday() >= 5

    def is_working_day(
        self, 
        date_: TypeDatetimeDate
    ) -> bool:
        date_ = self.date_only(date_)
        return not self.is_weekend(date_) and date_ not in self._holidays
    
    def is_holiday(
        self, 
        date_: TypeDatetimeDate
    ) -> bool:
        date_ = self.date_only(date_)
        return date_ in self._holidays

    def holidays_in_year(
        self, 
        int_year: int
    ) -> list[int]:
        return [
            int(date_.strftime("%d")) 
            for date_ in self._holidays 
            if int(date_.strftime("%Y")) == int_year
        ]

class ABCDateManipulation(ABCCalendarCore):

    @abstractmethod
    def holidays(self) -> list[tuple[str, date]]:
        pass

    def add_working_days(
        self, 
        date_: TypeDatetimeDate, 
        int_days_to_add: int
    ) -> date:
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
        date_ = self.date_only(date_)
        return date_ + timedelta(days=int_days_to_add)

    def add_months(
        self, 
        date_: datetime, 
        int_months_to_add: int
    ) -> datetime:
        return date_ + relativedelta(months=int_months_to_add)

    def build_date(
        self, 
        year: int, 
        month: int, 
        day: int
    ) -> date:
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
        return datetime(
            year=year, 
            month=month, 
            day=day, 
            hour=hour, 
            minute=minute, 
            second=second, 
            tzinfo=ZoneInfo(str_timezone)
        )

    def nearest_working_day(
        self, 
        date_: TypeDatetimeDate, 
        bool_next: bool = True
    ) -> date:
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
        except ValueError as e:
            raise ValueError(f"Invalid date string '{str_date}' for format {format_input}: {e}")

    def timestamp_to_date(
        self,
        timestamp_: str,
        substr_timestamp: str = "T",
    ) -> date:
        return self.str_date_to_date(timestamp_.split(substr_timestamp)[0], "YYYY-MM-DD")

    def timestamp_to_datetime(
        self,
        timestamp_: str,
        substr_timestamp: str = "T",
    ) -> datetime:
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
        return 10000 * date_.year + 100 * date_.month + date_.day

    def excel_float_to_date(
        self, 
        int_excel_date: int
    ) -> date:
        return (datetime(1899, 12, 30) + timedelta(days=int_excel_date)).date()


class ABCTimezoneAware(ABCDateManipulation):

    @abstractmethod
    def holidays(self) -> list[tuple[str, date]]:
        pass

    def str_date_to_datetime(
        self, 
        str_date: str, 
        format_input: TypeDateFormatInput = "DD/MM/YYYY",
        str_timezone: Optional[str] = "UTC",
    ) -> datetime:
        date_obj = self.str_date_to_date(str_date, format_input)
        return datetime.combine(date_obj, time(0, 0), tzinfo=ZoneInfo(str_timezone))

    def change_timezone(
        self,
        date_: TypeDatetimeDate,
        target_tz: str = "UTC",
        source_tz: Optional[str] = None
    ) -> datetime:
        if isinstance(date_, date) and not isinstance(date_, datetime):
            date_ = self.date_to_datetime(date_, ZoneInfo(target_tz))
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
        return datetime.combine(date_, time(0, 0), tzinfo=ZoneInfo(str_timezone))

    def to_unix_timestamp(
        self, 
        date_: Union[date, datetime, time], 
        str_timezone: Optional[str] = "UTC"
    ) -> int:
        """Convert date to unix timestamp (seconds since January 1, 1970, 00:00:00 UTC).

        Parameters
        ----------
        date_ : date
            Date to convert.
        str_timezone: Optional[str]
            Timezone to use, defaults to UTC.

        Returns
        -------
        int
            Unix timestamp.
        """
        if isinstance(date_, time):
            date_ = datetime.combine(date.today(), date_)
        elif isinstance(date_, date) and not isinstance(date_, datetime):
            date_ = self.date_to_datetime(date_, str_timezone=str_timezone)
        if date_.tzinfo is None:
            date_ = date_.astimezone(tz=ZoneInfo(str_timezone))
        return int(date_.timestamp())

    def unix_timestamp_to_datetime(
        self, 
        unix_timestamp: Union[float, int], 
        str_timezone: Optional[str] = "UTC"
    ) -> datetime:
        return datetime.fromtimestamp(unix_timestamp, tz=ZoneInfo(str_timezone))

    def unix_timestamp_to_date(
        self, 
        unix_timestamp: Union[float, int], 
        str_timezone: Optional[str] = "UTC"
    ) -> date:
        return self.unix_timestamp_to_datetime(unix_timestamp, str_timezone=str_timezone).date()

    def iso_to_unix_timestamp(
        self, 
        iso_timestamp: str,
        str_timezone: Optional[str] = "UTC"
    ) -> int:
        date_ = datetime.fromisoformat(iso_timestamp)
        date_ = date_.astimezone(tz=ZoneInfo(str_timezone))
        return date_.timestamp()
    
    def excel_float_to_datetime(
        self, 
        float_date: float, 
        str_timezone: Optional[str] = "UTC"
    ) -> datetime:
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

    @abstractmethod
    def holidays(self) -> list[tuple[str, date]]:
        pass

    def working_days_range(
        self, 
        date_start: TypeDatetimeDate, 
        date_end: TypeDatetimeDate
    ) -> set[date]:
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
        return [self.add_working_days(date(y + 1, 1, 1), -1) for y in list_years]

    def delta_working_hours(
        self,
        timestamp_start: str,
        timestamp_end: str,
        int_hour_start_office: int = 8,
        int_hour_end_office: int = 18,
        int_hour_start_lunch: int = 12,
        int_hour_end_lunch: int = 13,
        list_working_days_range: Optional[list[int]] = None,
        substr_timestamp: str = "T",
    ) -> int:
        timestamp_start = self.timestamp_to_datetime(timestamp_start, substr_timestamp)
        timestamp_end = self.timestamp_to_datetime(timestamp_end, substr_timestamp)
        if list_working_days_range is None:
            list_working_days_range = [0, 1, 2, 3, 4]
        holidays = {tup[1]: tup[0] for y in range(timestamp_start.year, timestamp_end.year + 1)
                    for tup in self.holidays(y)}
        workday = businesstimedelta.WorkDayRule(
            start_time=time(int_hour_start_office),
            end_time=time(int_hour_end_office),
            working_days=list_working_days_range,
        )
        lunchbreak = businesstimedelta.LunchTimeRule(
            start_time=time(int_hour_start_lunch),
            end_time=time(int_hour_end_lunch),
            working_days=list_working_days_range,
        )
        holidays_rule = businesstimedelta.HolidayRule(holidays)
        businesshrs = businesstimedelta.Rules([workday, lunchbreak, holidays_rule])
        return int(businesshrs.difference(timestamp_start, timestamp_end).seconds / 3600)


class ABCCurrentDate(ABCCalendarCore):

    @abstractmethod
    def holidays(self) -> list[tuple[str, date]]:
        pass

    def curr_date(self) -> date:
        return date.today()

    def curr_datetime(
        self, 
        str_timezone: Optional[str] = "UTC"
    ) -> datetime:
        return datetime.now(tz=ZoneInfo(str_timezone))

    def curr_time(
        self, 
        str_timezone: Optional[str] = "UTC"
    ) -> time:
        return self.curr_datetime(str_timezone=str_timezone).time()

    def current_timestamp_string(
        self, 
        format_output: str = "%Y%m%d_%H%M%S", 
        str_timezone: Optional[str] = "UTC"
    ) -> str:
        return self.curr_datetime(str_timezone=str_timezone).strftime(format_output)
    

class ABCDateFormatter(ABCCalendarCore):

    @abstractmethod
    def holidays(self) -> list[tuple[str, date]]:
        pass

    def get_platform_locale(
        self,
        str_locale: Optional[str] = None,
        str_timezone: Optional[str] = None
    ) -> str:
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
            except locale.Error:
                raise ValueError(f"Invalid or unsupported locale: {str_locale}")

    def year_number(
        self, 
        date_: TypeDatetimeDate
    ) -> int:
        return int(date_.strftime("%Y"))

    def month_number(
        self, date_: TypeDatetimeDate, 
        bool_month_mm: bool = False
    ) -> Union[int, str]:
        if not bool_month_mm:
            return int(date_.strftime("%m"))
        else:
            return date_.strftime("%m")

    def week_number(
        self, 
        date_: TypeDatetimeDate
    ) -> str:
        date_ = self.date_only(date_)
        return date_.strftime("%w")

    def day_number(
        self, 
        date_: TypeDatetimeDate
    ) -> int:
        date_ = self.date_only(date_)
        return int(date_.strftime("%d"))

    def month_name(
        self,
        date_: TypeDatetimeDate,
        bool_abbreviation: bool = False,
        str_timezone: Optional[str] = "UTC",
    ) -> str:
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
        date_ = self.date_only(date_)
        str_locale = self.get_platform_locale(str_timezone=str_timezone)
        locale.setlocale(locale.LC_TIME, str_locale)
        return date_.strftime("%a" if bool_abbreviation else "%A")

    def utc_log_ts(self) -> datetime:
        return datetime.now(timezone.utc)
    

@auto_cache_reset_methods([
    "holidays", ["clear_holidays_cache"]
])
class ABCCalendarOperations(
    ABCTimezoneAware, 
    ABCRangeDatesDelta, 
    ABCCurrentDate, 
    ABCDateFormatter
):
    
    @abstractmethod
    def holidays(self) -> list[tuple[str, date]]:
        pass