"""Module containing the DatesBR class for Brazilian date operations.

This module provides comprehensive functionality for working with dates in the Brazilian context,
including handling business days, holidays, and various date conversions and calculations.
"""

from datetime import date, datetime, time, timedelta, timezone
import locale
import platform
from typing import Optional, Union
from zoneinfo import ZoneInfo

import businesstimedelta
from dateutil.relativedelta import relativedelta
from more_itertools import unique_everseen

from stpstone.transformations.validation.metaclass_type_checker import TypeChecker
from stpstone.utils.cals.br_bzdays import BrazilBankCalendar
from stpstone.utils.parsers.str import StrHandler


class DatesBR(BrazilBankCalendar, metaclass=TypeChecker):
    """A class for handling Brazilian dates with business day and holiday calculations.

    This class extends BrazilBankCalendar to provide additional functionality for working
    with dates in the Brazilian context, including business day calculations, holiday
    handling, and various date conversion utilities.
    """

    def build_date(self, year: int, month: int, day: int) -> date:
        """Create a date object from year, month, and day components.

        Parameters
        ----------
        year : int
            The year component
        month : int
            The month component (1-12)
        day : int
            The day component (1-31)

        Returns
        -------
        date
            The constructed date object
        """
        return date(year=year, month=month, day=day)

    def build_datetime(
        self, year: int, month: int, day: int, hour: int, minute: int, second: int
    ) -> datetime:
        """Create a datetime object from components.

        Parameters
        ----------
        year : int
            The year component
        month : int
            The month component (1-12)
        day : int
            The day component (1-31)
        hour : int
            The hour component (0-23)
        minute : int
            The minute component (0-59)
        second : int
            The second component (0-59)

        Returns
        -------
        datetime
            The constructed datetime object
        """
        return datetime(
            year=year, month=month, day=day, hour=hour, minute=minute, second=second
        )

    def date_to_datetime(self, date: date, bl_timestamp: bool = False) -> Union[int, datetime]:
        """Convert a date object to datetime with optional timestamp conversion.

        Parameters
        ----------
        date : date
            The date to convert
        bl_crop_time : bool, optional
            Whether to crop time to midnight, by default True
        bl_timestamp : bool, optional
            Whether to convert to timestamp, by default True

        Returns
        -------
        datetime
            The resulting datetime object
        """
        datetime_ = datetime.combine(date, datetime.min.time())
        if bl_timestamp:
            return int(datetime_.timestamp())
        return datetime_

    def to_integer(self, dt_time: datetime) -> int:
        """Convert a datetime to integer in YYYYMMDD format.

        Parameters
        ----------
        dt_time : datetime
            The datetime to convert

        Returns
        -------
        int
            Integer representation in YYYYMMDD format
        """
        return 10000 * dt_time.year + 100 * dt_time.month + dt_time.day

    def excel_float_to_date(self, int_excel_date: int) -> date:
        """Convert Excel int date to Python date object.

        Parameters
        ----------
        int_excel_date : int
            Excel date value as int

        Returns
        -------
        date
            Converted date object
        """
        return (datetime(1899, 12, 30) + timedelta(days=int_excel_date)).date()

    def excel_float_to_datetime(self, int_excel_date: int) -> datetime:
        """Convert Excel int date to Python datetime object.

        Parameters
        ----------
        int_excel_date : int
            Excel date value as int

        Returns
        -------
        datetime
            Converted datetime object
        """
        return datetime.fromordinal(
            datetime(1900, 1, 1).toordinal() + int_excel_date - 2
        )

    def check_is_date(self, dt_: datetime) -> bool:
        """Check if an object is a date instance.

        Parameters
        ----------
        dt_ : datetime
            The object to check

        Returns
        -------
        bool
            True if the object is a date instance, False otherwise
        """
        return isinstance(dt_, date)

    def str_date_to_datetime(
        self, date_str: str, format_input: str = "DD/MM/YYYY"
    ) -> datetime:
        """Convert string date to datetime object based on specified format.

        Parameters
        ----------
        date_str : str
            Date string to convert
        format_input : str, optional
            Format of the input string, by default "DD/MM/YYYY"
            Valid formats: 'DD/MM/YYYY', 'YYYY-MM-DD', 'YYMMDD', 'DDMMYY',
            'DDMMYYYY', 'YYYYMMDD', 'MM-DD-YYYY', 'DD/MM/YY', 'DD.MM.YY'

        Returns
        -------
        datetime
            Converted datetime object

        Raises
        ------
        Exception
            If the format is not recognized
        """
        if format_input == "DD/MM/YYYY":
            return date(int(date_str[-4:]), int(date_str[3:5]), int(date_str[0:2]))
        elif format_input == "DDMMYY":
            return date(
                int("20" + date_str[-2:]), int(date_str[2:4]), int(date_str[0:2])
            )
        elif format_input == "DDMMYYYY":
            return date(int(date_str[-4:]), int(date_str[2:4]), int(date_str[0:2]))
        elif format_input == "YYYYMMDD":
            return date(int(date_str[:4]), int(date_str[4:6]), int(date_str[6:]))
        elif format_input == "YYYY-MM-DD":
            return date(int(date_str[0:4]), int(date_str[5:7]), int(date_str[-2:]))
        elif format_input == "MM-DD-YYYY":
            return date(int(date_str[-4:]), int(date_str[0:2]), int(date_str[3:5]))
        elif format_input == "YYMMDD":
            return date(
                int("20" + date_str[0:2]), int(date_str[2:4]), int(date_str[-2:])
            )
        elif format_input == "DD/MM/YY" or format_input == "DD.MM.YY":
            return date(
                int("20" + date_str[-2:]), int(date_str[3:5]), int(date_str[0:2])
            )
        else:
            raise Exception(f"Not a valid date format {format_input}")

    def list_wds(self, dt_start: datetime, dt_end: datetime) -> list[datetime]:
        """Generate list of working days between two dates.

        Parameters
        ----------
        dt_start : datetime
            Start date
        dt_end : datetime
            End date

        Returns
        -------
        list[datetime]
            list of working days as datetime objects
        """
        list_ = []
        dt_curr = dt_start
        while dt_curr <= dt_end:
            working_day = super().find_following_working_day(day=dt_curr)
            list_.append(working_day)
            dt_curr += timedelta(days=1)
        return list(unique_everseen(list_))

    def list_cds(self, dt_start: datetime, dt_end: datetime) -> list[datetime]:
        """Generate list of calendar days between two dates.

        Parameters
        ----------
        dt_start : datetime
            Start date
        dt_end : datetime
            End date

        Returns
        -------
        list[datetime]
            list of calendar days as datetime objects
        """
        list_ = list()
        for x in range(int((dt_end - dt_start).days)):
            list_.append(dt_start + timedelta(days=x))
        return list(unique_everseen(list_))

    def list_years(self, dt_start: datetime, dt_end: datetime) -> list[int]:
        """Generate list of years between two dates.

        Parameters
        ----------
        dt_start : datetime
            Start date
        dt_end : datetime
            End date

        Returns
        -------
        list[int]
            list of years
        """
        list_years = list()
        for x in range(int((dt_end - dt_start).days)):
            list_years.append((dt_start + timedelta(days=x)).year)
        return list(unique_everseen(list_years))

    def curr_date(self) -> date:
        """Get current date.

        Returns
        -------
        date
            Current date
        """
        return date.today()

    def curr_time(self) -> datetime:
        """Get current time.

        Returns
        -------
        datetime
            Current time
        """
        return datetime.now().time()

    def curr_date_time(
        self, bl_timestamp: bool = False, bl_crop_time: bool = False
    ) -> Union[int, datetime]:
        """Get current datetime with optional timestamp conversion.

        Parameters
        ----------
        bl_timestamp : bool, optional
            Whether to return as timestamp, by default False
        bl_crop_time : bool, optional
            Whether to crop time, by default False

        Returns
        -------
        Union[int, datetime]
            Current datetime or timestamp
        """
        datetime_ = datetime.now().timestamp_dt() if bl_timestamp else datetime.now()
        if bl_crop_time:
            return int(datetime_)
        else:
            return datetime_

    def testing_dates(self, dt_start: datetime, dt_end: datetime) -> bool:
        """Test if end date is greater than start date.

        Parameters
        ----------
        dt_start : datetime
            Start date
        dt_end : datetime
            End date

        Returns
        -------
        bool
            True if end date is greater than or equal to start date, False otherwise
        """
        return int((dt_end - dt_start).days) >= 0

    def year_number(self, dt_: Union[date, datetime]) -> int:
        """Extract year from date/datetime as integer.

        Parameters
        ----------
        dt_ : Union[date, datetime]
            Date/datetime object

        Returns
        -------
        int
            Year component
        """
        return int(dt_.strftime("%Y"))

    def day_number(self, dt_: Union[date, datetime]) -> int:
        """Extract day from date/datetime as integer.

        Parameters
        ----------
        dt_ : Union[date, datetime]
            Date/datetime object

        Returns
        -------
        int
            Day component
        """
        return int(dt_.strftime("%d"))

    def month_name(
        self,
        dt_: Union[date, datetime],
        bl_abbrv: bool = False,
        local_zone: Optional[str] = None,
    ) -> str:
        """Get month name in local language.

        Parameters
        ----------
        dt_ : Union[date, datetime]
            Date/datetime object
        bl_abbrv : bool, optional
            Whether to return abbreviated name, by default False
        local_zone : str, optional
            Locale setting, by default "pt-BR"

        Returns
        -------
        str
            Month name in specified language
        """
        if local_zone is None:
            local_zone = "pt-BR" if platform.system() == "Windows" else "pt_BR.UTF-8"
        try:
            locale.setlocale(locale.LC_TIME, local_zone)
        except locale.Error:
            locale.setlocale(locale.LC_TIME, "")
        if bl_abbrv:
            return dt_.strftime("%b")
        else:
            return dt_.strftime("%B")

    def dates_inf_sup_month(self, dt_: datetime, last_month_year: int = 12) -> tuple[date, date]:
        """Get first and last working days of a month.

        Parameters
        ----------
        dt_ : datetime
            Date in the month of interest
        last_month_year : int, optional
            Last month of the year, by default 12

        Returns
        -------
        tuple[date, date]
            tuple of (first working day, last working day) of the month
        """
        year = self.year_number(dt_)
        month = self.month_number(dt_)
        day = 1
        dt_start = self.find_working_day(self.build_date(year, month, day))
        if month < last_month_year:
            dt_end = self.sub_working_days(self.build_date(year, month + 1, day), 1)
        else:
            dt_end = self.sub_working_days(self.build_date(year + 1, 1, day), 1)
        # returning dates
        return dt_start, dt_end

    def month_number(self, dt_: datetime, bl_month_mm: bool = False) -> Union[int, str]:
        """Extract month from datetime as integer or string.

        Parameters
        ----------
        dt_ : datetime
            Datetime object
        bl_month_mm : bool, optional
            Whether to return as two-digit string, by default False

        Returns
        -------
        Union[int, str]
            Month component as integer or string
        """
        if not bl_month_mm:
            return int(dt_.strftime("%m"))
        else:
            return dt_.strftime("%m")

    def week_name(
        self, dt_: datetime, bl_abbrv: bool = False, local_zone: Optional[str] = None
    ) -> str:
        """Get weekday name in local language.

        Parameters
        ----------
        dt_ : datetime
            Datetime object
        bl_abbrv : bool, optional
            Whether to return abbreviated name, by default False
        local_zone : str, optional
            Locale setting, by default "pt-BR"

        Returns
        -------
        str
            Weekday name in specified language
        """
        if local_zone is None:
            local_zone = "pt-BR" if platform.system() == "Windows" else "pt_BR.UTF-8"
        try:
            locale.setlocale(locale.LC_TIME, local_zone)
        except locale.Error:
            locale.setlocale(locale.LC_TIME, "")
        if bl_abbrv:
            return dt_.strftime("%a")
        else:
            return dt_.strftime("%A")

    def week_number(self, dt_: datetime) -> str:
        """Get weekday number (0-6, where 0 is Sunday).

        Parameters
        ----------
        dt_ : datetime
            Datetime object

        Returns
        -------
        str
            Weekday number as string
        """
        return dt_.strftime("%w")

    def find_working_day(self, dt_: datetime) -> datetime:
        """Find the next working day from a given date.

        Parameters
        ----------
        dt_ : datetime
            Starting date

        Returns
        -------
        datetime
            Next working day
        """
        return self.add_working_days(self.sub_working_days(dt_, 1), 1)

    def nth_weekday_month(
        self,
        dt_start: datetime,
        dt_end: datetime,
        int_weekday: int,
        nth_rpt: int,
        int_days_week: int = 7,
    ) -> list[datetime]:
        """Get nth weekday of month between two dates.

        Parameters
        ----------
        dt_start : datetime
            Start date
        dt_end : datetime
            End date
        int_weekday : int
            Weekday number (0-6, where 0 is Sunday)
        nth_rpt : int
            Which occurrence to find (1st, 2nd, etc.)
        int_days_week : int, optional
            Days in a week, by default 7

        Returns
        -------
        list[datetime]
            list of matching dates
        """
        list_ = self.list_wds(dt_start, dt_end)
        return [
            self.add_working_days(self.sub_working_days(d, 1), 1)
            for d in list_
            if int(self.week_number(d)) == int_weekday
        ]

    def delta_calendar_days(self, dt_start: datetime, dt_end: datetime) -> int:
        """Calculate difference in calendar days between two dates.

        Parameters
        ----------
        dt_start : datetime
            Start date
        dt_end : datetime
            End date

        Returns
        -------
        int
            Number of calendar days between dates
        """
        return (dt_end - dt_start).days

    def add_months(self, dt_: datetime, int_months: int) -> datetime:
        """Add months to a date.

        Parameters
        ----------
        dt_ : datetime
            Starting date
        int_months : int
            Number of months to add

        Returns
        -------
        datetime
            Resulting date
        """
        return dt_ + relativedelta(months=int_months)

    def add_calendar_days(self, original_date: datetime, days_to_add: int) -> datetime:
        """Add calendar days to a date.

        Parameters
        ----------
        original_date : datetime
            Starting date
        days_to_add : int
            Number of days to add

        Returns
        -------
        datetime
            Resulting date
        """
        return original_date + timedelta(days=days_to_add)

    def delta_working_hours(
        self,
        timestamp_inf: str,
        timestamp_sup: str,
        int_hour_start_office: int = 8,
        int_hour_sup_office: int = 18,
        int_hour_start_lunch: int = 0,
        int_hour_sup_lunch: int = 0,
        list_wds: Optional[list[int]] = None,
    ) -> int:
        """Calculate working hours between two timestamps.

        Parameters
        ----------
        timestamp_inf : str
            Start timestamp
        timestamp_sup : str
            End timestamp
        int_hour_start_office : int, optional
            Start of work day hour, by default 8
        int_hour_sup_office : int, optional
            End of work day hour, by default 18
        int_hour_start_lunch : int, optional
            Start of lunch hour, by default 0
        int_hour_sup_lunch : int, optional
            End of lunch hour, by default 0
        list_wds : list[int], optional
            list of working days (0=Sunday), by default [0,1,2,3,4]

        Returns
        -------
        int
            Number of working hours between timestamps
        """
        if list_wds is None:
            list_wds = [0, 1, 2, 3, 4]
        # timestamp_dt convertation to datetime
        y_inf, mt_inf, d_inf = (
            int(timestamp_inf.split(" ")[0].split("-")[0]),
            int(timestamp_inf.split(" ")[0].split("-")[1]),
            int(timestamp_inf.split(" ")[0].split("-")[2]),
        )
        h_inf, m_inf, s_inf = (
            int(timestamp_inf.split(" ")[1].split(":")[0]),
            int(timestamp_inf.split(" ")[1].split(":")[1]),
            int(timestamp_inf.split(" ")[1].split(":")[2]),
        )
        y_sup, mt_sup, d_sup = (
            int(timestamp_sup.split(" ")[0].split("-")[0]),
            int(timestamp_sup.split(" ")[0].split("-")[1]),
            int(timestamp_sup.split(" ")[0].split("-")[2]),
        )
        h_sup, m_sup, s_sup = (
            int(timestamp_sup.split(" ")[1].split(":")[0]),
            int(timestamp_sup.split(" ")[1].split(":")[1]),
            int(timestamp_sup.split(" ")[1].split(":")[2]),
        )
        timestamp_inf = datetime(y_inf, mt_inf, d_inf, h_inf, m_inf, s_inf)
        timestamp_sup = datetime(y_sup, mt_sup, d_sup, h_sup, m_sup, s_sup)
        # dict of holidays
        dict_holidays_raw = dict()
        for y in range(timestamp_inf.year, timestamp_sup.year + 1):
            dict_holidays_raw[y] = self.holidays(y)
        dict_holidays_trt = dict()
        for _, v in dict_holidays_raw.items():
            for t in v:
                dict_holidays_trt[t[0]] = t[1]
        # office hours for working days
        workday = businesstimedelta.WorkDayRule(
            start_time=time(int_hour_start_office),
            end_time=time(int_hour_sup_office),
            working_days=list_wds,
        )
        lunchbreak = businesstimedelta.LunchTimeRule(
            start_time=time(int_hour_start_lunch),
            end_time=time(int_hour_sup_lunch),
            working_days=list_wds,
        )
        holidays = businesstimedelta.HolidayRule(dict_holidays_trt)
        businesshrs = businesstimedelta.Rules([workday, lunchbreak, holidays])
        # output
        return businesshrs.difference(timestamp_inf, timestamp_sup).timedelta

    def last_wd_years(self, list_years: list[int]) -> list[datetime]:
        """Get last working days of given years.

        Parameters
        ----------
        list_years : list[int]
            list of years

        Returns
        -------
        list[datetime]
            list of last working days for each year
        """
        return [self.sub_working_days(datetime(y + 1, 1, 1), 1) for y in list_years]

    def add_holidays_not_considered_anbima(
        self,
        dt_start: datetime,
        dt_end: datetime,
        list_last_week_year_day: list[datetime],
        local_zone: Optional[str] = None,
        list_holidays_not_considered: Optional[list[str]] = None,
        list_dates_not_considered: Optional[list[str]] = None,
        list_non_bzdays_week: Optional[list[str]] = None,
    ) -> int:
        """Add holidays not considered by ANBIMA.

        Parameters
        ----------
        dt_start : datetime
            Start date
        dt_end : datetime
            End date
        list_last_week_year_day : list[datetime]
            list of last days of years
        local_zone : str, optional
            Locale setting, by default "pt-BR"
        list_holidays_not_considered : list[str], optional
            Holidays not considered, by default ["25/01"]
        list_dates_not_considered : list[str], optional
            Dates not considered, by default ["05/03/2025", "18/02/2026"]
        list_non_bzdays_week : list[str], optional
            Non-business days of week, by default ["sábado", "domingo"]

        Returns
        -------
        int
            Count of additional holidays not considered by ANBIMA
        """
        if list_holidays_not_considered is None:
            list_holidays_not_considered = ["25/01"]
        if list_dates_not_considered is None:
            list_dates_not_considered = ["05/03/2025", "18/02/2026"]
        if list_non_bzdays_week is None:
            list_non_bzdays_week = ["sábado", "domingo"]
        if local_zone is None:
            local_zone = "pt-BR" if platform.system() == "Windows" else "pt_BR.UTF-8"
        try:
            locale.setlocale(locale.LC_TIME, local_zone)
        except locale.Error:
            locale.setlocale(locale.LC_TIME, "")
        return len(
            [
                d
                for d in self.list_calendar_days(dt_start, dt_end)
                if (
                    d.strftime("%d/%m") in list_holidays_not_considered
                    and self.week_name(d) not in list_non_bzdays_week
                    or d in list_last_week_year_day
                    or d.strftime("%d/%m/%Y") in list_dates_not_considered
                )
            ]
        )

    def unix_timestamp_to_datetime(
        self, unix_timestamp: Union[float, int], str_tz: str = "UTC"
    ) -> datetime:
        """Convert Unix timestamp to datetime with timezone.

        Parameters
        ----------
        unix_timestamp : Union[float, int]
            Unix timestamp
        str_tz : str, optional
            Timezone string, by default "UTC"

        Returns
        -------
        datetime
            Datetime object with timezone
        """
        tz_obj = ZoneInfo("UTC") if str_tz == "UTC" else ZoneInfo(str_tz)
        return datetime.fromtimestamp(unix_timestamp, tz=tz_obj)

    def unix_timestamp_to_date(
        self, unix_timestamp: Union[float, int], str_tz: str = "UTC"
    ) -> datetime:
        """Convert Unix timestamp to date with timezone.

        Parameters
        ----------
        unix_timestamp : Union[float, int]
            Unix timestamp
        str_tz : str, optional
            Timezone string, by default "UTC"

        Returns
        -------
        datetime
            Date object with timezone
        """
        tz_obj = ZoneInfo("UTC") if str_tz == "UTC" else ZoneInfo(str_tz)
        return datetime.fromtimestamp(unix_timestamp, tz=tz_obj).date()

    def iso_to_unix_timestamp(self, iso_timestamp: str) -> int:
        """Convert ISO timestamp to Unix timestamp.

        Parameters
        ----------
        iso_timestamp : str
            ISO format timestamp

        Returns
        -------
        int
            Unix timestamp
        """
        dt_ = datetime.fromisoformat(iso_timestamp)
        dt_utc = dt_.astimezone(timezone.utc)
        return dt_utc.timestamp()

    def datetime_to_unix_timestamp(
        self, dt_: Union[date, datetime, time]
    ) -> int:
        """Convert datetime/date/time to Unix timestamp.

        Parameters
        ----------
        dt_ : Union[date, datetime, time]
            Date/datetime/time object to convert

        Returns
        -------
        int
            Unix timestamp (seconds since epoch)
        """
        # If input is time, combine with today's date
        if isinstance(dt_, time):
            dt_ = datetime.combine(date.today(), dt_)
        # If input is date, convert to datetime at midnight
        elif isinstance(dt_, date) and not isinstance(dt_, datetime):
            dt_ = datetime.combine(dt_, time.min)

        # If datetime is timezone-naive, assume local timezone
        if dt_.tzinfo is None:
            dt_ = dt_.astimezone()  # Convert to local timezone

        # Convert to UTC and return timestamp
        return int(dt_.astimezone(timezone.utc).timestamp())

    def timestamp_to_date(
        self,
        timestamp: Union[str, int],
        substring_datetime: Optional[str] = "T",
        format_output: str = "YYYY-MM-DD",
    ) -> datetime:
        """Convert timestamp to date.

        Parameters
        ----------
        timestamp : Union[str, int]
            Timestamp to convert
        substring_datetime : Optional[str], optional
            Substring separator, by default "T"
        format_output : str, optional
            Output format, by default "YYYY-MM-DD"

        Returns
        -------
        datetime
            Converted date object
        """
        if substring_datetime is None:
            return datetime.fromtimestamp(int(timestamp) / 1000, tz=timezone.utc)
        return self.str_date_to_datetime(
            StrHandler().get_string_until_substr(str(timestamp), substring_datetime),
            format_output,
        )

    def timestamp_to_datetime(
        self,
        timestamp_dt: Union[int, float],
        tz: Optional[Union[str, timezone]] = None
    ) -> datetime:
        """Convert Unix timestamp to datetime with optional timezone conversion.

        Parameters
        ----------
        timestamp_dt : Union[int, float]
            Unix timestamp (seconds since epoch)
        tz : Optional[Union[str, timezone]]
            Timezone to convert to. Can be:
            - None: returns timezone-naive datetime in local time
            - 'UTC': returns timezone-aware datetime in UTC
            - timezone object or string (e.g. 'America/Sao_Paulo')

        Returns
        -------
        datetime
            Converted datetime object
        """
        utc_dt = datetime.fromtimestamp(timestamp_dt, tz=timezone.utc)

        if tz is None:
            return utc_dt.astimezone().replace(tzinfo=None)
        elif tz == 'UTC':
            return utc_dt
        else:
            if isinstance(tz, str):
                tz = ZoneInfo(tz)
            return utc_dt.astimezone(tz)

    def current_timestamp_string(self, format_output: str = "%Y%m%d_%H%M%S") -> str:
        """Get current timestamp as formatted string.

        Parameters
        ----------
        format_output : str, optional
            Format string, by default "%Y%m%d_%H%M%S"

        Returns
        -------
        str
            Formatted current timestamp
        """
        return self.curr_date_time().strftime(format_output)

    def utc_log_ts(self) -> datetime:
        """Get current UTC timestamp.

        Returns
        -------
        datetime
            Current UTC timestamp
        """
        return datetime.now(timezone.utc)

    def utc_from_dt(self, dt_: datetime) -> datetime:
        """Convert datetime to UTC.

        Parameters
        ----------
        dt_ : datetime
            Datetime to convert

        Returns
        -------
        datetime
            UTC datetime
        """
        dt_ = datetime.combine(dt_, datetime.min.time())
        return dt_.replace(tzinfo=ZoneInfo("UTC"))

    def month_year_string(
        self,
        dt_: str,
    ) -> str:
        """Convert month/year string between formats.

        Parameters
        ----------
        dt_ : str
            Input date string
        format_input : str, optional
            Input format, by default "%b/%Y"
        format_output : str, optional
            Output format, by default "%Y-%m"
        bl_dtbr : bool, optional
            Whether to use Brazilian month abbreviations, by default True

        Returns
        -------
        str
            Formatted date string
        """
        month_mapping = {
            "JAN": "01",
            "FEB": "02",
            "MAR": "03",
            "APR": "04",
            "MAY": "05",
            "JUN": "06",
            "JUL": "07",
            "AUG": "08",
            "SEP": "09",
            "OCT": "10",
            "NOV": "11",
            "DEC": "12",
        }
        month_abbr, year = dt_.split("/")
        month = month_mapping[month_abbr.upper()]
        return f"{year}-{month}"
