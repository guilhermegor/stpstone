from datetime import date, datetime, timedelta, timezone
from datetime import time as datetime_time
import locale
import time
from typing import List, Optional, Tuple, Union
from zoneinfo import ZoneInfo

import businesstimedelta
from dateutil.relativedelta import relativedelta
from more_itertools import unique_everseen
import pandas as pd
from workalendar.core import SAT, SUN

from stpstone.transformations.validation.metaclass_type_checker import TypeChecker
from stpstone.utils.cals.br_bzdays import BrazilBankCalendar
from stpstone.utils.parsers.str import StrHandler


class DatesBR(BrazilBankCalendar, metaclass=TypeChecker):
    def build_date(self, year: int, month: int, day: int) -> date:
        return date(year=year, month=month, day=day)

    def build_datetime(
        self, year: int, month: int, day: int, hour: int, minute: int, second: int
    ) -> datetime:
        return datetime(
            year=year, month=month, day=day, hour=hour, minute=minute, second=second
        )

    def date_to_datetime(
        self, date: date, bl_crop_time: bool = True, bl_tinestamp: bool = True
    ) -> datetime:
        datetime_ = datetime.combine(date, datetime.min.time())
        if bl_tinestamp == True:
            datetime_ = datetime_.timestamp_dt()
        if bl_crop_time == True:
            return int(datetime_)
        else:
            return datetime_

    def to_integer(self, dt_time: datetime) -> int:
        return 10000 * dt_time.year + 100 * dt_time.month + dt_time.day

    def excel_float_to_date(self, float_excel_date: float) -> date:
        return datetime.fromordinal(date(1900, 1, 1).toordinal() + float_excel_date - 2)

    def excel_float_to_datetime(self, float_excel_date):
        return datetime.fromordinal(
            datetime(1900, 1, 1).toordinal() + float_excel_date - 2
        )

    def check_is_date(self, dt_: datetime) -> bool:
        return isinstance(dt_, date)

    def str_date_to_datetime(
        self, date_str: str, format_input: str = "DD/MM/YYYY"
    ) -> datetime:
        """
        String date to datetime
        Args:
            date_str (str): date in string format
            format_input (str): output format - valid formats: 'DD/MM/YYYY', 'YYYY-MM-DD',
                'YYMMDD', 'DDMMYY', 'DDMMYYYY', 'DD/MM/YY'
        Returns:
            datetime
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
        elif format_input == "DD/MM/YY":
            return date(
                int("20" + date_str[-2:]), int(date_str[3:5]), int(date_str[0:2])
            )
        elif format_input == "DD.MM.YY":
            return date(
                int("20" + date_str[-2:]), int(date_str[3:5]), int(date_str[0:2])
            )
        else:
            raise Exception(f"Not a valid date format {format_input}")

    def list_wds(
        self, dt_start: datetime, dt_end: datetime, int_wd_bef: int
    ) -> List[int]:
        """
        List of working days between two dates
        Args:
            dt_start (datetime): start date
            dt_end (datetime): end date
            int_wd_bef (int): number of working days before the start date
        Returns:
            List[int]
        """
        date_ref = self.sub_working_days(self.dt_(), int_wd_bef)
        wd_inf = self.get_working_days_delta(dt_start, date_ref)
        wd_sup = self.get_working_days_delta(dt_end, date_ref)
        return list(range(wd_sup, wd_inf + 1))

    def list_wds(self, dt_start: str, dt_end: str) -> Union[List[datetime], List[str]]:
        """
        List of working days between two dates
        Args:
            dt_start (datetime): start date
            dt_end (datetime): end date
        Returns:
            List[datetime]
        """
        list_wds = list()
        for x in range(int((dt_end - dt_start).days) + 1):
            list_wds.append(
                super().find_following_working_day(day=dt_start + timedelta(days=x))
            )
        return list(unique_everseen(list_wds))

    def list_cds(self, dt_start: datetime, dt_end: datetime) -> List[datetime]:
        """
        List of calendar days between two dates
        Args:
            dt_start (datetime): start date
            dt_end (datetime): end date
            format_data (str): format date
        Returns:
            List[datetime]
        """
        list_wds = list()
        for x in range(int((dt_end - dt_start).days)):
            list_wds.append(dt_start + timedelta(days=x))
        return list(unique_everseen(list_wds))

    def list_years(self, dt_start: datetime, dt_end: datetime) -> List[int]:
        """
        List of years between two dates
        Args:
            dt_start (datetime): start date
            dt_end (datetime): end date
        Returns:
            List[int]
        """
        list_years = list()
        for x in range(int((dt_end - dt_start).days)):
            list_years.append((dt_start + timedelta(days=x)).year)
        return list(unique_everseen(list_years))

    @property
    def curr_date(self) -> date:
        return date.today()

    @property
    def curr_time(self) -> datetime:
        return datetime.now().time()

    def curr_date_time(
        self, bl_timestamp: bool = False, bl_crop_time: bool = False
    ) -> Union[int, datetime]:
        if bl_timestamp == True:
            datetime_ = datetime.now().timestamp_dt()
        else:
            datetime_ = datetime.now()
        if bl_crop_time == True:
            return int(datetime_)
        else:
            return datetime_

    def testing_dates(self, dt_start: datetime, dt_end: datetime) -> bool:
        """
        Test if dt_end is greater than dt_start
        Args:
            dt_start (datetime): start date
            dt_end (datetime): end date
        Returns:
            Boolean
        """
        if int((dt_end - dt_start).days) >= 0:
            return True
        else:
            return False

    def year_number(self, dt_: Union[date, datetime]) -> int:
        return int(dt_.strftime("%Y"))

    def day_number(self, dt_: Union[date, datetime]) -> int:
        return int(dt_.strftime("%d"))

    def month_name(
        self,
        dt_: Union[date, datetime],
        bl_abbrv: bool = False,
        local_zone: str = "pt-BR",
    ) -> str:
        """
        Name of the month in the local language
        Args:
            dt_ (date): date
            bl_abbrv (bool): abbreviation
            local_zone (str): local zone
        Returns:
            str
        """
        locale.setlocale(locale.LC_TIME, local_zone)
        if bl_abbrv == True:
            return dt_.strftime("%b")
        else:
            return dt_.strftime("%B")

    def dates_inf_sup_month(self, dt_, last_month_year=12) -> Tuple[date, date]:
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
        if bl_month_mm == False:
            return int(dt_.strftime("%m"))
        else:
            return dt_.strftime("%m")

    def week_name(
        self, dt_: datetime, bl_abbrv: bool = False, local_zone: str = "pt-BR"
    ) -> str:
        """
        Name of the weekday in the local language
        Args:
            dt_ (date): date
            bl_abbrv (bool): abbreviation
            local_zone (str): local zone
        Returns:
            str
        """
        locale.setlocale(locale.LC_TIME, local_zone)
        if bl_abbrv == True:
            return dt_.strftime("%a")
        else:
            return dt_.strftime("%A")

    def week_number(self, dt_: datetime) -> str:
        return dt_.strftime("%w")

    def find_working_day(self, dt_: datetime) -> datetime:
        return self.add_working_days(self.sub_working_days(dt_, 1), 1)

    def nth_weekday_month(
        self,
        dt_start: datetime,
        dt_end: datetime,
        int_weekday: int,
        nth_rpt: int,
        format_output: str = "DD/MM/YYYY",
        int_days_week: int = 7,
    ) -> List[datetime]:
        """
        Get nth weekday of month
        Args:
            dt_start (datetime): start date
            dt_end (datetime): end date
            int_weekday (int): weekday number
            nth_rpt (int): nth repetition
            format_output (str): format output
            int_days_week (int): number of days in a week
        Returns:
            List[datetime]
        """
        list_wds = self.list_wds(dt_start, dt_end, format_output)
        return [
            self.add_working_days(self.sub_working_days(d, 1), 1)
            for d in list_wds
            if (
                self.week_number(d) == int_weekday
                and d.day >= (nth_rpt * int_days_week - int_days_week)
                and d.day <= (nth_rpt * int_days_week)
            )
        ]

    def delta_calendar_days(self, dt_start: datetime, dt_end: datetime) -> int:
        return (dt_end - dt_start).days

    def add_months(self, dt_, int_months) -> datetime:
        return dt_ + relativedelta(months=int_months)

    def add_calendar_days(self, original_date, days_to_add):
        return original_date + timedelta(days=days_to_add)

    def delta_working_hours(
        self,
        timestamp_inf: str,
        timestamp_sup: str,
        int_hour_start_office: int = 8,
        int_hour_sup_office: int = 18,
        int_hour_start_lunch: int = 0,
        int_hour_sup_lunch: int = 0,
        list_wds: List[int] = [0, 1, 2, 3, 4],
    ) -> int:
        """
        Calculate the number of working hours between two timestamps
        Args:
            timestamp_inf (str): start timestamp_dt
            timestamp_sup (str): end timestamp_dt
            int_hour_start_office (int): start hour office
            int_hour_sup_office (int): end hour office
            int_hour_start_lunch (int): start hour lunch
            int_hour_sup_lunch (int): end hour lunch
            list_wds (List[int]): list of working days
        Returns:
            int
        References: https://pypi.org/project/businesstimedelta/
        """
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
        for k, v in dict_holidays_raw.items():
            for t in v:
                dict_holidays_trt[t[0]] = t[1]
        # office hours for working days
        workday = businesstimedelta.WorkDayRule(
            start_time=time(int_hour_start_office),
            end_time=time(int_hour_sup_office),
            list_wds=list_wds,
        )
        lunchbreak = businesstimedelta.LunchTimeRule(
            start_time=time(int_hour_start_lunch),
            end_time=time(int_hour_sup_lunch),
            list_wds=list_wds,
        )
        holidays = businesstimedelta.HolidayRule(dict_holidays_trt)
        businesshrs = businesstimedelta.Rules([workday, lunchbreak, holidays])
        # output
        return businesshrs.difference(timestamp_inf, timestamp_sup).timedelta

    def last_wd_years(self, list_years: List[int]) -> List[datetime]:
        """
        Last days of years
        Args:
            list_years (List[int]): list of years
        Returns:
            List[datetime]
        """
        return [self.sub_working_days(datetime(y + 1, 1, 1), 1) for y in list_years]

    def add_holidays_not_considered_anbima(
        self,
        dt_start: datetime,
        dt_end: datetime,
        list_last_week_year_day,
        local_zone="pt-BR",
        list_holidays_not_considered: List[str] = ["25/01"],
        list_dates_not_considered: List[str] = ["05/03/2025", "18/02/2026"],
        list_non_bzdays_week: List[str] = ["sábado", "domingo"],
    ):
        """
        Add holidays not considered by ANBIMA
        Args:
            dt_start (datetime): start date
            dt_end (datetime): end date
            list_last_week_year_day (List[datetime]): list of last days of years
            local_zone (str): locale zone
            list_holidays_not_considered (List[str]): list of holidays not considered
            list_dates_not_considered (List[str]): list of dates not considered
            list_non_bzdays_week (List[str]): list of non business days of the week
        Returns:
            int
        """
        locale.setlocale(locale.LC_TIME, local_zone)
        return len(
            [
                d
                for d in self.list_calendar_days(dt_start, dt_end)
                if (
                    d.strftime("%d/%m") in list_holidays_not_considered
                    and not self.week_name(d) in list_non_bzdays_week
                    or d in list_last_week_year_day
                    or d.strftime("%d/%m/%Y") in list_dates_not_considered
                )
            ]
        )

    def unix_timestamp_to_datetime(
        self, unix_timestamp: Union[float, int], str_tz: str = "UTC"
    ) -> datetime:
        tz_obj = ZoneInfo("UTC") if str_tz == "UTC" else ZoneInfo(str_tz)
        return datetime.fromtimestamp(unix_timestamp, tz=tz_obj)

    def unix_timestamp_to_date(
        self, unix_timestamp: Union[float, int], str_tz: str = "UTC"
    ) -> datetime:
        tz_obj = ZoneInfo("UTC") if str_tz == "UTC" else ZoneInfo(str_tz)
        return datetime.fromtimestamp(unix_timestamp, tz=tz_obj).date()

    def iso_to_unix_timestamp(self, iso_timestamp: str) -> int:
        dt_ = datetime.fromisoformat(iso_timestamp)
        dt_utc = dt_.astimezone(timezone.utc)
        return dt_utc.timestamp()

    def datetime_to_unix_timestamp(
        self, dt_: Union[date, datetime, datetime_time]
    ) -> int:
        """
        Convert a datetime/date/time object to a Unix timestamp (seconds since epoch).

        Args:
            dt_ (Union[date, datetime, time]): Date, datetime or time object to convert

        Returns:
            int: Unix timestamp (seconds since 1970-01-01 00:00:00 UTC)
        """
        # If input is time, combine with today's date
        if isinstance(dt_, datetime_time):
            dt_ = datetime.combine(date.today(), dt_)
        # If input is date, convert to datetime at midnight
        elif isinstance(dt_, date) and not isinstance(dt_, datetime):
            dt_ = datetime.combine(dt_, datetime_time.min)

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
        if substring_datetime == None:
            return datetime.fromtimestamp(int(timestamp) / 1000, tz=timezone.utc)
        return self.str_date_to_datetime(
            StrHandler().get_string_until_substr(str(timestamp), substring_datetime),
            format_output,
        )

    def timestamp_to_datetime(
        self, timestamp_dt: datetime, bl_return_from_utc: bool = False
    ) -> Union[datetime, str]:
        if bl_return_from_utc == True:
            return pd.to_datetime(timestamp_dt, unit="s", utc=True).tz_convert(
                ZoneInfo("America/Sao_Paulo")
            )
        else:
            return pd.to_datetime(timestamp_dt, unit="s", utc=True).strftime("%Y%m%d")

    @property
    def current_timestamp_string(self, format_output: str = "%Y%m%d_%H%M%S") -> str:
        return self.curr_date_time().strftime(format_output)

    @property
    def utc_log_ts(self) -> datetime:
        return datetime.now(timezone.utc)

    def utc_from_dt(self, dt_):
        dt_ = datetime.combine(dt_, datetime.min.time())
        return dt_.replace(tzinfo=ZoneInfo("UTC"))

    def month_year_string(
        self,
        dt_: str,
        format_input: str = "%b/%Y",
        format_output: str = "%Y-%m",
        bl_dtbr: bool = True,
    ):
        if bl_dtbr == True:
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
        else:
            return datetime.strptime(dt_, format_input).strftime(format_output)
