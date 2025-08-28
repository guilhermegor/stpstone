"""Unit tests for calendar operations classes.

This module tests the functionality of calendar operation classes including:
- Date manipulation and formatting
- Timezone handling
- Working day calculations
- Date range operations
- Holiday and weekend checks
"""

from datetime import date, datetime, time, timezone
import locale
import platform
from unittest.mock import patch
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import pandas as pd
import pytest
from pytest_mock import MockerFixture

from stpstone.utils.calendars.calendar_abc import (
    CalendarCore,
    DateFormatter,
    DateManipulation,
    DatesCurrent,
    DatesRangeDelta,
    DateTimezoneAware,
)


# --------------------------
# Fixtures
# --------------------------
@pytest.fixture
def calendar_core() -> CalendarCore:
    """Fixture providing a CalendarCore instance.

    Returns
    -------
    CalendarCore
        Instance of CalendarCore
    """
    return CalendarCore()

@pytest.fixture
def date_manipulation() -> DateManipulation:
    """Fixture providing a DateManipulation instance.

    Returns
    -------
    DateManipulation
        Instance of DateManipulation
    """
    return DateManipulation()

@pytest.fixture
def date_timezone_aware() -> DateTimezoneAware:
    """Fixture providing a DateTimezoneAware instance.

    Returns
    -------
    DateTimezoneAware
        Instance of DateTimezoneAware
    """
    return DateTimezoneAware()

@pytest.fixture
def dates_range_delta() -> DatesRangeDelta:
    """Fixture providing a DatesRangeDelta instance.

    Returns
    -------
    DatesRangeDelta
        Instance of DatesRangeDelta
    """
    return DatesRangeDelta()

@pytest.fixture
def date_formatter() -> DateFormatter:
    """Fixture providing a DateFormatter instance.

    Returns
    -------
    DateFormatter
        Instance of DateFormatter
    """
    return DateFormatter()

@pytest.fixture
def sample_date() -> date:
    """Fixture providing a sample date.

    Returns
    -------
    date
        Sample date object (2023-06-15)
    """
    return date(2023, 6, 15)

@pytest.fixture
def sample_datetime() -> datetime:
    """Fixture providing a sample datetime.

    Returns
    -------
    datetime
        Sample datetime object (2023-06-15 14:30:00 UTC)
    """
    return datetime(2023, 6, 15, 14, 30, 0, tzinfo=ZoneInfo("UTC"))

@pytest.fixture
def mock_holidays(mocker: MockerFixture) -> object:
    """Mock holidays method to return a specific list of holidays.

    Parameters
    ----------
    mocker : MockerFixture
        Pytest-mock fixture for creating mocks

    Returns
    -------
    object
        Mock object for holidays method
    """
    return mocker.patch.object(
        CalendarCore,
        "holidays",
        return_value=[("New Year", date(2023, 1, 1)), ("Holiday", date(2023, 6, 16))]
    )

# --------------------------
# Tests for CalendarCore
# --------------------------
def test_get_holidays_raw_default(calendar_core: CalendarCore) -> None:
    """Test default implementation of get_holidays_raw returns empty DataFrame.

    Verifies
    --------
    - Returns a pandas DataFrame
    - DataFrame is empty
    - DataFrame has correct columns

    Parameters
    ----------
    calendar_core : CalendarCore
        Instance of CalendarCore

    Returns
    -------
    None
    """
    result = calendar_core.get_holidays_raw()
    assert isinstance(result, pd.DataFrame)
    assert result.empty
    assert list(result.columns) == ["name", "date"]

def test_get_holidays_raw_timeout_types(calendar_core: CalendarCore) -> None:
    """Test get_holidays_raw accepts various timeout types.

    Verifies
    --------
    - Accepts int timeout
    - Accepts float timeout
    - Accepts tuple of floats timeout
    - Accepts tuple of ints timeout

    Parameters
    ----------
    calendar_core : CalendarCore
        Instance of CalendarCore

    Returns
    -------
    None
    """
    timeouts = [10, 10.5, (10.0, 20.0), (10, 20)]
    for timeout in timeouts:
        result = calendar_core.get_holidays_raw(timeout=timeout)
        assert isinstance(result, pd.DataFrame)
        assert result.empty

def test_holidays_default(calendar_core: CalendarCore) -> None:
    """Test default implementation of holidays returns empty list.

    Verifies
    --------
    - Returns a list
    - List is empty
    - List contains tuples of (str, date)

    Parameters
    ----------
    calendar_core : CalendarCore
        Instance of CalendarCore

    Returns
    -------
    None
    """
    result = calendar_core.holidays()
    assert isinstance(result, list)
    assert len(result) == 0

def test_holidays_cache(calendar_core: CalendarCore, mock_holidays: object) -> None:
    """Test _holidays property caching.

    Verifies
    --------
    - Returns cached set of holiday dates
    - Calls holidays method only once
    - Returns correct set of dates

    Parameters
    ----------
    calendar_core : CalendarCore
        CalendarCore instance
    mock_holidays : object
        Mock for holidays method

    Returns
    -------
    None
    """
    result1 = calendar_core._holidays
    result2 = calendar_core._holidays
    assert result1 == {date(2023, 1, 1), date(2023, 6, 16)}
    assert result1 is result2  # Same object, cached
    mock_holidays.assert_called_once()

def test_date_only_valid_date(calendar_core: CalendarCore, sample_date: date) -> None:
    """Test date_only with valid date input.

    Verifies
    --------
    - Returns date object unchanged
    - Maintains correct date value

    Parameters
    ----------
    calendar_core : CalendarCore
        CalendarCore instance
    sample_date : date
        Sample date object

    Returns
    -------
    None
    """
    result = calendar_core.date_only(sample_date)
    assert isinstance(result, date)
    assert result == sample_date

def test_date_only_valid_datetime(
    calendar_core: CalendarCore, 
    sample_datetime: datetime
) -> None:
    """Test date_only with valid datetime input.

    Verifies
    --------
    - Returns date component of datetime
    - Returns correct date value

    Parameters
    ----------
    calendar_core : CalendarCore
        CalendarCore instance
    sample_datetime : datetime
        Sample datetime object

    Returns
    -------
    None
    """
    result = calendar_core.date_only(sample_datetime)
    assert isinstance(result, date)
    assert result == date(2023, 6, 15)

def test_date_only_invalid_type(calendar_core: CalendarCore) -> None:
    """Test date_only with invalid input type.

    Verifies
    --------
    - Raises TypeError for non-date/datetime input
    - Error message contains type information

    Parameters
    ----------
    calendar_core : CalendarCore
        CalendarCore instance

    Returns
    -------
    None
    """
    with pytest.raises(TypeError, match="date_ must be of type datetime or date"):
        calendar_core.date_only("2023-06-15")

def test_is_weekend_weekday(calendar_core: CalendarCore) -> None:
    """Test is_weekend with weekday input.

    Verifies
    --------
    - Returns False for weekday
    - Correctly identifies Monday (weekday 0)

    Parameters
    ----------
    calendar_core : CalendarCore
        CalendarCore instance

    Returns
    -------
    None
    """
    monday = date(2023, 6, 12)  # Monday
    assert not calendar_core.is_weekend(monday)

def test_is_weekend_weekend(calendar_core: CalendarCore) -> None:
    """Test is_weekend with weekend input.

    Verifies
    --------
    - Returns True for weekend day
    - Correctly identifies Saturday (weekday 5)

    Parameters
    ----------
    calendar_core : CalendarCore
        CalendarCore instance

    Returns
    -------
    None
    """
    saturday = date(2023, 6, 17)  # Saturday
    assert calendar_core.is_weekend(saturday)

def test_is_working_day_weekday(
    calendar_core: CalendarCore, 
    mock_holidays: object
) -> None:
    """Test is_working_day with weekday input.

    Verifies
    --------
    - Returns True for non-holiday weekday
    - Correctly handles weekday check

    Parameters
    ----------
    calendar_core : CalendarCore
        CalendarCore instance
    mock_holidays : object
        Mock for holidays method

    Returns
    -------
    None
    """
    weekday = date(2023, 6, 14)  # Wednesday
    assert calendar_core.is_working_day(weekday)

def test_is_working_day_holiday(
    calendar_core: CalendarCore, 
    mock_holidays: object
) -> None:
    """Test is_working_day with holiday input.

    Verifies
    --------
    - Returns False for holiday
    - Correctly checks holiday cache

    Parameters
    ----------
    calendar_core : CalendarCore
        CalendarCore instance
    mock_holidays : object
        Mock for holidays method

    Returns
    -------
    None
    """
    holiday = date(2023, 6, 16)  # Holiday from mock
    assert not calendar_core.is_working_day(holiday)

def test_is_holiday_true(
    calendar_core: CalendarCore, 
    mock_holidays: object
) -> None:
    """Test is_holiday with holiday date.

    Verifies
    --------
    - Returns True for holiday date
    - Correctly checks holiday cache

    Parameters
    ----------
    calendar_core : CalendarCore
        CalendarCore instance
    mock_holidays : object
        Mock for holidays method

    Returns
    -------
    None
    """
    holiday = date(2023, 6, 16)
    assert calendar_core.is_holiday(holiday)

def test_is_holiday_false(
    calendar_core: CalendarCore, 
    mock_holidays: object
) -> None:
    """Test is_holiday with non-holiday date.

    Verifies
    --------
    - Returns False for non-holiday date
    - Correctly checks holiday cache

    Parameters
    ----------
    calendar_core : CalendarCore
        CalendarCore instance
    mock_holidays : object
        Mock for holidays method

    Returns
    -------
    None
    """
    non_holiday = date(2023, 6, 15)
    assert not calendar_core.is_holiday(non_holiday)

def test_holidays_in_year(
    calendar_core: CalendarCore, 
    mock_holidays: object
) -> None:
    """Test holidays_in_year with specific year.

    Verifies
    --------
    - Returns correct list of holiday days
    - Only includes holidays from specified year

    Parameters
    ----------
    calendar_core : CalendarCore
        CalendarCore instance
    mock_holidays : object
        Mock for holidays method

    Returns
    -------
    None
    """
    result = calendar_core.holidays_in_year(2023)
    assert result == [1, 16]  # Day numbers from mock holidays

# --------------------------
# Tests for DateManipulation
# --------------------------
def test_add_working_days_zero(
    date_manipulation: DateManipulation, 
    sample_date: date
) -> None:
    """Test add_working_days with zero days.

    Verifies
    --------
    - Returns same date when adding zero days
    - Handles date input correctly

    Parameters
    ----------
    date_manipulation : DateManipulation
        DateManipulation instance
    sample_date : date
        Sample date object

    Returns
    -------
    None
    """
    result = date_manipulation.add_working_days(sample_date, 0)
    assert result == sample_date

def test_add_working_days_positive(
    date_manipulation: DateManipulation, 
    sample_date: date, 
    mock_holidays: object
) -> None:
    """Test add_working_days with positive days.

    Verifies
    --------
    - Correctly adds working days
    - Skips weekends and holidays
    - Returns correct date

    Parameters
    ----------
    date_manipulation : DateManipulation
        DateManipulation instance
    sample_date : date
        Sample date object
    mock_holidays : object
        Mock for holidays method

    Returns
    -------
    None
    """
    # June 15, 2023 is Thursday, adding 1 working day skips June 16 (holiday)
    # and June 17-18 (weekend), landing on June 19 (Monday)
    result = date_manipulation.add_working_days(sample_date, 1)
    assert result == date(2023, 6, 19)

def test_add_working_days_negative(
    date_manipulation: DateManipulation, 
    sample_date: date, 
    mock_holidays: object
) -> None:
    """Test add_working_days with negative days.

    Verifies
    --------
    - Correctly subtracts working days
    - Skips weekends and holidays
    - Returns correct date

    Parameters
    ----------
    date_manipulation : DateManipulation
        DateManipulation instance
    sample_date : date
        Sample date object
    mock_holidays : object
        Mock for holidays method

    Returns
    -------
    None
    """
    # June 15, 2023 is Thursday, subtracting 1 working day lands on June 14 (Wednesday)
    result = date_manipulation.add_working_days(sample_date, -1)
    assert result == date(2023, 6, 14)

def test_add_calendar_days(
    date_manipulation: DateManipulation, 
    sample_date: date
) -> None:
    """Test add_calendar_days with various inputs.

    Verifies
    --------
    - Correctly adds calendar days
    - Handles positive and negative inputs
    - Returns correct date

    Parameters
    ----------
    date_manipulation : DateManipulation
        DateManipulation instance
    sample_date : date
        Sample date object

    Returns
    -------
    None
    """
    assert date_manipulation.add_calendar_days(sample_date, 5) == date(2023, 6, 20)
    assert date_manipulation.add_calendar_days(sample_date, -5) == date(2023, 6, 10)

def test_add_months(
    date_manipulation: DateManipulation, 
    sample_datetime: datetime
) -> None:
    """Test add_months with various inputs.

    Verifies
    --------
    - Correctly adds months
    - Maintains time and timezone
    - Returns correct datetime

    Parameters
    ----------
    date_manipulation : DateManipulation
        DateManipulation instance
    sample_datetime : datetime
        Sample datetime object

    Returns
    -------
    None
    """
    result = date_manipulation.add_months(sample_datetime, 2)
    assert result == datetime(2023, 8, 15, 14, 30, 0, tzinfo=ZoneInfo("UTC"))

def test_build_date_valid(date_manipulation: DateManipulation) -> None:
    """Test build_date with valid inputs.

    Verifies
    --------
    - Creates correct date object
    - Handles valid year, month, day

    Parameters
    ----------
    date_manipulation : DateManipulation
        DateManipulation instance

    Returns
    -------
    None
    """
    result = date_manipulation.build_date(2023, 6, 15)
    assert result == date(2023, 6, 15)

def test_build_date_invalid(date_manipulation: DateManipulation) -> None:
    """Test build_date with invalid inputs.

    Verifies
    --------
    - Raises ValueError for invalid date components

    Parameters
    ----------
    date_manipulation : DateManipulation
        DateManipulation instance

    Returns
    -------
    None
    """
    with pytest.raises(ValueError):
        date_manipulation.build_date(2023, 13, 1)  # Invalid month

def test_build_datetime_valid(date_manipulation: DateManipulation) -> None:
    """Test build_datetime with valid inputs.

    Verifies
    --------
    - Creates correct datetime object
    - Sets correct timezone
    - Handles valid components

    Parameters
    ----------
    date_manipulation : DateManipulation
        DateManipulation instance

    Returns
    -------
    None
    """
    result = date_manipulation.build_datetime(2023, 6, 15, 14, 30, 0, "UTC")
    assert result == datetime(2023, 6, 15, 14, 30, 0, tzinfo=ZoneInfo("UTC"))

def test_build_datetime_invalid_timezone(date_manipulation: DateManipulation) -> None:
    """Test build_datetime with invalid timezone.

    Verifies
    --------
    - Raises ZoneInfoNotFoundError for empty or None timezone

    Parameters
    ----------
    date_manipulation : DateManipulation
        DateManipulation instance

    Returns
    -------
    None
    """
    with pytest.raises(ZoneInfoNotFoundError, match="Timezone cannot be empty or None"):
        date_manipulation.build_datetime(2023, 6, 15, 14, 30, 0, None)
    with pytest.raises(ZoneInfoNotFoundError, match="Timezone cannot be empty or None"):
        date_manipulation.build_datetime(2023, 6, 15, 14, 30, 0, "")

def test_nearest_working_day_next(
    date_manipulation: DateManipulation, 
    sample_date: date, 
    mock_holidays: object
) -> None:
    """Test nearest_working_day with next=True.

    Verifies
    --------
    - Returns next working day
    - Skips holidays and weekends

    Parameters
    ----------
    date_manipulation : DateManipulation
        DateManipulation instance
    sample_date : date
        Sample date object
    mock_holidays : object
        Mock for holidays method

    Returns
    -------
    None
    """
    # June 15, 2023 (Thursday), next working day after June 16 (holiday)
    # and June 17-18 (weekend) is June 19 (Monday)
    result = date_manipulation.nearest_working_day(sample_date, bool_next=True)
    assert result == date(2023, 6, 19)

def test_nearest_working_day_previous(
    date_manipulation: DateManipulation, 
    sample_date: date, 
    mock_holidays: object
) -> None:
    """Test nearest_working_day with next=False.

    Verifies
    --------
    - Returns previous working day
    - Skips holidays and weekends

    Parameters
    ----------
    date_manipulation : DateManipulation
        DateManipulation instance
    sample_date : date
        Sample date object
    mock_holidays : object
        Mock for holidays method

    Returns
    -------
    None
    """
    # June 15, 2023 (Thursday), previous working day is June 14 (Wednesday)
    result = date_manipulation.nearest_working_day(sample_date, bool_next=False)
    assert result == date(2023, 6, 14)

def test_str_date_to_date_valid(date_manipulation: DateManipulation) -> None:
    """Test str_date_to_date with valid inputs.

    Verifies
    --------
    - Correctly converts string to date
    - Supports various date formats

    Parameters
    ----------
    date_manipulation : DateManipulation
        DateManipulation instance

    Returns
    -------
    None
    """
    test_cases = [
        ("15/06/2023", "DD/MM/YYYY", date(2023, 6, 15)),
        ("2023-06-15", "YYYY-MM-DD", date(2023, 6, 15)),
        ("230615", "YYMMDD", date(2023, 6, 15)),
        ("150623", "DDMMYY", date(2023, 6, 15)),
        ("15062023", "DDMMYYYY", date(2023, 6, 15)),
        ("20230615", "YYYYMMDD", date(2023, 6, 15)),
        ("06-15-2023", "MM-DD-YYYY", date(2023, 6, 15)),
        ("15/06/23", "DD/MM/YY", date(2023, 6, 15)),
        ("15.06.23", "DD.MM.YY", date(2023, 6, 15)),
    ]
    for str_date, format_input, expected in test_cases:
        result = date_manipulation.str_date_to_date(str_date, format_input)
        assert result == expected

def test_str_date_to_date_invalid(date_manipulation: DateManipulation) -> None:
    """Test str_date_to_date with invalid inputs.

    Verifies
    --------
    - Raises ValueError for invalid date string
    - Raises ValueError for invalid format

    Parameters
    ----------
    date_manipulation : DateManipulation
        DateManipulation instance

    Returns
    -------
    None
    """
    with pytest.raises(ValueError, match="Not a valid date format"):
        date_manipulation.str_date_to_date("15/06/2023", "INVALID")
    with pytest.raises(ValueError, match="Invalid date string"):
        date_manipulation.str_date_to_date("2023/06/15", "DD/MM/YYYY")

def test_timestamp_to_date(date_manipulation: DateManipulation) -> None:
    """Test timestamp_to_date with valid input.

    Verifies
    --------
    - Correctly converts timestamp to date
    - Handles different timestamp separators

    Parameters
    ----------
    date_manipulation : DateManipulation
        DateManipulation instance

    Returns
    -------
    None
    """
    result = date_manipulation.timestamp_to_date("2023-06-15T14:30:00", "T")
    assert result == date(2023, 6, 15)

def test_timestamp_to_datetime_valid(date_manipulation: DateManipulation) -> None:
    """Test timestamp_to_datetime with valid inputs.

    Verifies
    --------
    - Correctly converts timestamp to datetime
    - Handles ISO format and custom format

    Parameters
    ----------
    date_manipulation : DateManipulation
        DateManipulation instance

    Returns
    -------
    None
    """
    result = date_manipulation.timestamp_to_datetime("2023-06-15T14:30:00")
    assert result == datetime(2023, 6, 15, 14, 30, 0)
    result = date_manipulation.timestamp_to_datetime("2023-06-15T14:30:00", "T")
    assert result == datetime(2023, 6, 15, 14, 30, 0)

def test_timestamp_to_datetime_invalid(date_manipulation: DateManipulation) -> None:
    """Test timestamp_to_datetime with invalid input.

    Verifies
    --------
    - Raises ValueError for invalid timestamp format

    Parameters
    ----------
    date_manipulation : DateManipulation
        DateManipulation instance

    Returns
    -------
    None
    """
    with pytest.raises(ValueError, match="Failed to parse timestamp"):
        date_manipulation.timestamp_to_datetime("invalid-timestamp")

def test_to_integer(date_manipulation: DateManipulation, sample_date: date) -> None:
    """Test to_integer with valid input.

    Verifies
    --------
    - Correctly converts date to integer format
    - Returns expected integer value

    Parameters
    ----------
    date_manipulation : DateManipulation
        DateManipulation instance
    sample_date : date
        Sample date object

    Returns
    -------
    None
    """
    result = date_manipulation.to_integer(sample_date)
    assert result == 20230615

def test_excel_float_to_date_valid(date_manipulation: DateManipulation) -> None:
    """Test excel_float_to_date with valid input.

    Verifies
    --------
    - Correctly converts Excel float to date
    - Handles valid input correctly

    Parameters
    ----------
    date_manipulation : DateManipulation
        DateManipulation instance

    Returns
    -------
    None
    """
    result = date_manipulation.excel_float_to_date(44727)
    assert result == date(2022, 6, 15)

def test_excel_float_to_date_invalid(date_manipulation: DateManipulation) -> None:
    """Test excel_float_to_date with invalid inputs.

    Verifies
    --------
    - Raises ValueError for None or negative input

    Parameters
    ----------
    date_manipulation : DateManipulation
        DateManipulation instance

    Returns
    -------
    None
    """
    with pytest.raises(ValueError, match="int_excel_date cannot be None"):
        date_manipulation.excel_float_to_date(None)
    with pytest.raises(ValueError, match="int_excel_date cannot be negative"):
        date_manipulation.excel_float_to_date(-1)

# --------------------------
# Tests for DateTimezoneAware
# --------------------------
def test_str_date_to_datetime(
    date_timezone_aware: DateTimezoneAware, 
    sample_date: date
) -> None:
    """Test str_date_to_datetime with valid inputs.

    Verifies
    --------
    - Correctly converts string to datetime with timezone
    - Sets correct timezone

    Parameters
    ----------
    date_timezone_aware : DateTimezoneAware
        DateTimezoneAware instance
    sample_date : date
        Sample date object

    Returns
    -------
    None
    """
    result = date_timezone_aware.str_date_to_datetime("15/06/2023", "DD/MM/YYYY", "UTC")
    assert result == datetime(2023, 6, 15, 0, 0, tzinfo=ZoneInfo("UTC"))

def test_change_timezone_datetime(
    date_timezone_aware: DateTimezoneAware, 
    sample_datetime: datetime
) -> None:
    """Test change_timezone with datetime input.

    Verifies
    --------
    - Correctly changes timezone
    - Maintains correct time in new timezone

    Parameters
    ----------
    date_timezone_aware : DateTimezoneAware
        DateTimezoneAware instance
    sample_datetime : datetime
        Sample datetime object

    Returns
    -------
    None
    """
    result = date_timezone_aware.change_timezone(sample_datetime, "America/New_York")
    assert result == datetime(2023, 6, 15, 10, 30, 0, tzinfo=ZoneInfo("America/New_York"))

def test_change_timezone_date(
    date_timezone_aware: DateTimezoneAware, 
    sample_date: date
) -> None:
    """Test change_timezone with date input.

    Verifies
    --------
    - Correctly converts date to datetime with target timezone
    - Sets correct timezone

    Parameters
    ----------
    date_timezone_aware : DateTimezoneAware
        DateTimezoneAware instance
    sample_date : date
        Sample date object

    Returns
    -------
    None
    """
    result = date_timezone_aware.change_timezone(sample_date, "UTC")
    assert result == datetime(2023, 6, 15, 0, 0, tzinfo=ZoneInfo("UTC"))

def test_change_timezone_naive_datetime_no_source(
    date_timezone_aware: DateTimezoneAware
) -> None:
    """Test change_timezone with naive datetime and no source timezone.

    Verifies
    --------
    - Raises ValueError for naive datetime without source_tz

    Parameters
    ----------
    date_timezone_aware : DateTimezoneAware
        DateTimezoneAware instance

    Returns
    -------
    None
    """
    naive_dt = datetime(2023, 6, 15, 14, 30, 0)
    with pytest.raises(ValueError, match="Cannot change timezone of naive datetime"):
        date_timezone_aware.change_timezone(naive_dt, "UTC")

def test_date_to_datetime(
    date_timezone_aware: DateTimezoneAware, 
    sample_date: date
) -> None:
    """Test date_to_datetime with valid input.

    Verifies
    --------
    - Correctly converts date to datetime
    - Sets correct timezone

    Parameters
    ----------
    date_timezone_aware : DateTimezoneAware
        DateTimezoneAware instance
    sample_date : date
        Sample date object

    Returns
    -------
    None
    """
    result = date_timezone_aware.date_to_datetime(sample_date, "UTC")
    assert result == datetime(2023, 6, 15, 0, 0, tzinfo=ZoneInfo("UTC"))

def test_to_unix_timestamp_datetime(
    date_timezone_aware: DateTimezoneAware, 
    sample_datetime: datetime
) -> None:
    """Test to_unix_timestamp with datetime input.

    Verifies
    --------
    - Correctly converts datetime to Unix timestamp
    - Handles timezone correctly

    Parameters
    ----------
    date_timezone_aware : DateTimezoneAware
        DateTimezoneAware instance
    sample_datetime : datetime
        Sample datetime object

    Returns
    -------
    None
    """
    result = date_timezone_aware.to_unix_timestamp(sample_datetime, "UTC")
    assert result == 1686841800

def test_to_unix_timestamp_time(
    date_timezone_aware: DateTimezoneAware, 
    mocker: MockerFixture
) -> None:
    """Test to_unix_timestamp with time input.

    Verifies
    --------
    - Correctly converts time to Unix timestamp using current date
    - Handles timezone correctly

    Parameters
    ----------
    date_timezone_aware : DateTimezoneAware
        DateTimezoneAware instance
    mocker : MockerFixture
        Pytest-mock fixture for mocking date.today()

    Returns
    -------
    None
    """
    mocker.patch("datetime.date.today", return_value=date(2023, 6, 15))
    t = time(14, 30, 0)
    result = date_timezone_aware.to_unix_timestamp(t, "UTC")
    assert result == 1686841800

def test_unix_timestamp_to_datetime(
    date_timezone_aware: DateTimezoneAware
) -> None:
    """Test unix_timestamp_to_datetime with valid input.

    Verifies
    --------
    - Correctly converts Unix timestamp to datetime
    - Sets correct timezone

    Parameters
    ----------
    date_timezone_aware : DateTimezoneAware
        DateTimezoneAware instance

    Returns
    -------
    None
    """
    result = date_timezone_aware.unix_timestamp_to_datetime(1686841800, "UTC")
    assert result == datetime(2023, 6, 15, 14, 30, 0, tzinfo=ZoneInfo("UTC"))

def test_unix_timestamp_to_date(
    date_timezone_aware: DateTimezoneAware
) -> None:
    """Test unix_timestamp_to_date with valid input.

    Verifies
    --------
    - Correctly converts Unix timestamp to date
    - Handles timezone correctly

    Parameters
    ----------
    date_timezone_aware : DateTimezoneAware
        DateTimezoneAware instance

    Returns
    -------
    None
    """
    result = date_timezone_aware.unix_timestamp_to_date(1686841800, "UTC")
    assert result == date(2023, 6, 15)

def test_iso_to_unix_timestamp(
    date_timezone_aware: DateTimezoneAware
) -> None:
    """Test iso_to_unix_timestamp with valid input.

    Verifies
    --------
    - Correctly converts ISO timestamp to Unix timestamp
    - Handles timezone correctly

    Parameters
    ----------
    date_timezone_aware : DateTimezoneAware
        DateTimezoneAware instance

    Returns
    -------
    None
    """
    result = date_timezone_aware.iso_to_unix_timestamp("2023-06-15T14:30:00Z", "UTC")
    assert result == 1686841800

def test_excel_float_to_datetime(
    date_timezone_aware: DateTimezoneAware
) -> None:
    """Test excel_float_to_datetime with valid input.

    Verifies
    --------
    - Correctly converts Excel float to datetime
    - Handles timezone and fractional days

    Parameters
    ----------
    date_timezone_aware : DateTimezoneAware
        DateTimezoneAware instance

    Returns
    -------
    None
    """
    result = date_timezone_aware.excel_float_to_datetime(44727.5, "UTC")
    assert result == datetime(2022, 6, 15, 12, 0, 0, tzinfo=ZoneInfo("UTC"))

# --------------------------
# Tests for DatesRangeDelta
# --------------------------
def test_working_days_range_valid(
    dates_range_delta: DatesRangeDelta, 
    mock_holidays: object
) -> None:
    """Test working_days_range with valid input range.

    Verifies
    --------
    - Returns correct set of working days
    - Excludes weekends and holidays

    Parameters
    ----------
    dates_range_delta : DatesRangeDelta
        DatesRangeDelta instance
    mock_holidays : object
        Mock for holidays method

    Returns
    -------
    None
    """
    start = date(2023, 6, 12)  # Monday
    end = date(2023, 6, 19)    # Monday
    result = dates_range_delta.working_days_range(start, end)
    expected = {date(2023, 6, 12), date(2023, 6, 13), date(2023, 6, 14), date(2023, 6, 15),
                date(2023, 6, 19)}
    assert result == expected

def test_working_days_range_invalid(
    dates_range_delta: DatesRangeDelta
) -> None:
    """Test working_days_range with invalid date range.

    Verifies
    --------
    - Raises ValueError when end date is before start date

    Parameters
    ----------
    dates_range_delta : DatesRangeDelta
        DatesRangeDelta instance

    Returns
    -------
    None
    """
    with pytest.raises(ValueError, match="date_end must be greater than date_start"):
        dates_range_delta.working_days_range(date(2023, 6, 15), date(2023, 6, 14))

def test_calendar_days_range(
    dates_range_delta: DatesRangeDelta
) -> None:
    """Test calendar_days_range with valid input range.

    Verifies
    --------
    - Returns complete set of calendar days
    - Includes all days in range

    Parameters
    ----------
    dates_range_delta : DatesRangeDelta
        DatesRangeDelta instance

    Returns
    -------
    None
    """
    start = date(2023, 6, 12)
    end = date(2023, 6, 14)
    result = dates_range_delta.calendar_days_range(start, end)
    expected = {date(2023, 6, 12), date(2023, 6, 13), date(2023, 6, 14)}
    assert result == expected

def test_years_between_dates(
    dates_range_delta: DatesRangeDelta
) -> None:
    """Test years_between_dates with valid input range.

    Verifies
    --------
    - Returns correct set of years
    - Handles multi-year range

    Parameters
    ----------
    dates_range_delta : DatesRangeDelta
        DatesRangeDelta instance

    Returns
    -------
    None
    """
    start = date(2022, 6, 15)
    end = date(2023, 6, 15)
    result = dates_range_delta.years_between_dates(start, end)
    assert result == {2022, 2023}

def test_delta_working_days(
    dates_range_delta: DatesRangeDelta, 
    mock_holidays: object
) -> None:
    """Test delta_working_days with valid input range.

    Verifies
    --------
    - Correctly counts working days
    - Excludes weekends and holidays

    Parameters
    ----------
    dates_range_delta : DatesRangeDelta
        DatesRangeDelta instance
    mock_holidays : object
        Mock for holidays method

    Returns
    -------
    None
    """
    start = date(2023, 6, 12)  # Monday
    end = date(2023, 6, 19)    # Monday
    result = dates_range_delta.delta_working_days(start, end)
    assert result == 4  # Excludes June 16 (holiday), June 17-18 (weekend)

def test_delta_calendar_days(
    dates_range_delta: DatesRangeDelta
) -> None:
    """Test delta_calendar_days with valid input range.

    Verifies
    --------
    - Correctly counts calendar days
    - Includes all days in range

    Parameters
    ----------
    dates_range_delta : DatesRangeDelta
        DatesRangeDelta instance

    Returns
    -------
    None
    """
    start = date(2023, 6, 12)
    end = date(2023, 6, 15)
    result = dates_range_delta.delta_calendar_days(start, end)
    assert result == 3

def test_get_start_end_day_month(
    dates_range_delta: DatesRangeDelta, 
    mock_holidays: object
) -> None:
    """Test get_start_end_day_month with various inputs.

    Verifies
    --------
    - Returns correct start and end dates
    - Handles working day option
    - Handles year-end edge case

    Parameters
    ----------
    dates_range_delta : DatesRangeDelta
        DatesRangeDelta instance
    mock_holidays : object
        Mock for holidays method

    Returns
    -------
    None
    """
    date_ = date(2023, 6, 15)
    result = dates_range_delta.get_start_end_day_month(date_, bool_working_days=False)
    assert result == (date(2023, 6, 1), date(2023, 7, 1))

    result = dates_range_delta.get_start_end_day_month(date_, bool_working_days=True)
    assert result == (date(2023, 6, 1), date(2023, 6, 30))  # June 30 is last working day

    # Test December edge case
    date_ = date(2023, 12, 15)
    result = dates_range_delta.get_start_end_day_month(date_, bool_working_days=False)
    assert result == (date(2023, 12, 1), date(2024, 1, 1))

def test_get_dates_weekday_month(
    dates_range_delta: DatesRangeDelta
) -> None:
    """Test get_dates_weekday_month with valid inputs.

    Verifies
    --------
    - Returns correct list of dates for given weekday
    - Handles month boundaries

    Parameters
    ----------
    dates_range_delta : DatesRangeDelta
        DatesRangeDelta instance

    Returns
    -------
    None
    """
    result = dates_range_delta.get_dates_weekday_month(2023, 6, 2)  # Wednesdays
    expected = [date(2023, 6, 7), date(2023, 6, 14), date(2023, 6, 21), date(2023, 6, 28)]
    assert result == expected

def test_get_dates_weekday_month_invalid(
    dates_range_delta: DatesRangeDelta
) -> None:
    """Test get_dates_weekday_month with invalid inputs.

    Verifies
    --------
    - Raises ValueError for invalid month or weekday

    Parameters
    ----------
    dates_range_delta : DatesRangeDelta
        DatesRangeDelta instance

    Returns
    -------
    None
    """
    with pytest.raises(ValueError, match="Month must be between 1 and 12"):
        dates_range_delta.get_dates_weekday_month(2023, 13, 2)
    with pytest.raises(ValueError, match="Weekday must be between 0 and 6"):
        dates_range_delta.get_dates_weekday_month(2023, 6, 7)

def test_get_nth_weekday_month(
    dates_range_delta: DatesRangeDelta, 
    mock_holidays: object
) -> None:
    """Test get_nth_weekday_month with valid inputs.

    Verifies
    --------
    - Returns correct nth weekday
    - Handles working day option

    Parameters
    ----------
    dates_range_delta : DatesRangeDelta
        DatesRangeDelta instance
    mock_holidays : object
        Mock for holidays method

    Returns
    -------
    None
    """
    result = dates_range_delta.get_nth_weekday_month(2023, 6, 2, 2, bool_working_days=False)
    assert result == date(2023, 6, 14)  # Second Wednesday

    result = dates_range_delta.get_nth_weekday_month(2023, 6, 2, 2, bool_working_days=True)
    assert result == date(2023, 6, 14)  # Second Wednesday is a working day

def test_get_nth_weekday_month_invalid(
    dates_range_delta: DatesRangeDelta
) -> None:
    """Test get_nth_weekday_month with invalid inputs.

    Verifies
    --------
    - Raises ValueError for invalid n, month, or weekday

    Parameters
    ----------
    dates_range_delta : DatesRangeDelta
        DatesRangeDelta instance

    Returns
    -------
    None
    """
    with pytest.raises(ValueError, match="n must be non-zero"):
        dates_range_delta.get_nth_weekday_month(2023, 6, 2, 0)
    with pytest.raises(ValueError, match="n must be less than or equal to"):
        dates_range_delta.get_nth_weekday_month(2023, 6, 2, 6)

def test_last_working_day_years(
    dates_range_delta: DatesRangeDelta, 
    mock_holidays: object
) -> None:
    """Test last_working_day_years with valid inputs.

    Verifies
    --------
    - Returns correct last working days for given years
    - Handles holidays correctly

    Parameters
    ----------
    dates_range_delta : DatesRangeDelta
        DatesRangeDelta instance
    mock_holidays : object
        Mock for holidays method

    Returns
    -------
    None
    """
    result = dates_range_delta.last_working_day_years([2022, 2023])
    assert result == [date(2022, 12, 30), date(2023, 12, 29)]  # Last working days

def test_delta_working_hours(
    dates_range_delta: DatesRangeDelta, 
    mocker: MockerFixture
) -> None:
    """Test delta_working_hours with valid inputs.

    Verifies
    --------
    - Correctly calculates working hours
    - Handles office hours and lunch breaks
    - Excludes holidays and weekends

    Parameters
    ----------
    dates_range_delta : DatesRangeDelta
        DatesRangeDelta instance
    mocker : MockerFixture
        Pytest-mock fixture for mocking holidays

    Returns
    -------
    None
    """
    mocker.patch.object(
        DatesRangeDelta,
        "holidays",
        return_value=[("Holiday", date(2023, 6, 14))]
    )
    start = "2023-06-13T09:00:00"
    end = "2023-06-15T17:00:00"
    result = dates_range_delta.delta_working_hours(start, end)
    assert result == 17  # June 13: 9 hours (9-18), June 15: 8 hours (9-17)

# --------------------------
# Tests for DatesCurrent
# --------------------------
def test_curr_date(dates_current: DatesCurrent, mocker: MockerFixture) -> None:
    """Test curr_date returns current date.

    Verifies
    --------
    - Returns correct date
    - Returns date object

    Parameters
    ----------
    dates_current : DatesCurrent
        DatesCurrent instance
    mocker : MockerFixture
        Pytest-mock fixture for mocking date.today()

    Returns
    -------
    None
    """
    mocker.patch("datetime.date.today", return_value=date(2023, 6, 15))
    result = dates_current.curr_date()
    assert result == date(2023, 6, 15)

def test_curr_datetime(dates_current: DatesCurrent, mocker: MockerFixture) -> None:
    """Test curr_datetime returns current datetime.

    Verifies
    --------
    - Returns datetime with correct timezone
    - Returns datetime object

    Parameters
    ----------
    dates_current : DatesCurrent
        DatesCurrent instance
    mocker : MockerFixture
        Pytest-mock fixture for mocking分辨

    Returns
    -------
    None
    """
    mocker.patch("datetime.datetime.now", 
                 return_value=datetime(2023, 6, 15, 14, 30, 0, tzinfo=ZoneInfo("UTC")))
    result = dates_current.curr_datetime("UTC")
    assert result == datetime(2023, 6, 15, 14, 30, 0, tzinfo=ZoneInfo("UTC"))

def test_curr_time(dates_current: DatesCurrent, mocker: MockerFixture) -> None:
    """Test curr_time returns current time.

    Verifies
    --------
    - Returns correct time
    - Returns time object

    Parameters
    ----------
    dates_current : DatesCurrent
        DatesCurrent instance
    mocker : MockerFixture
        Pytest-mock fixture for mocking datetime.now()

    Returns
    -------
    None
    """
    mocker.patch("datetime.datetime.now", 
                 return_value=datetime(2023, 6, 15, 14, 30, 0, tzinfo=ZoneInfo("UTC")))
    result = dates_current.curr_time("UTC")
    assert result == time(14, 30, 0)

def test_current_timestamp_string(
    dates_current: DatesCurrent, 
    mocker: MockerFixture
) -> None:
    """Test current_timestamp_string returns formatted timestamp.

    Verifies
    --------
    - Returns correct string format
    - Handles custom format and timezone

    Parameters
    ----------
    dates_current : DatesCurrent
        DatesCurrent instance
    mocker : MockerFixture
        Pytest-mock fixture for mocking datetime.now()

    Returns
    -------
    None
    """
    mocker.patch("datetime.datetime.now", 
                 return_value=datetime(2023, 6, 15, 14, 30, 0, tzinfo=ZoneInfo("UTC")))
    result = dates_current.current_timestamp_string("%Y%m%d_%H%M%S", "UTC")
    assert result == "20230615_143000"

# --------------------------
# Tests for DateFormatter
# --------------------------
def test_get_platform_locale(date_formatter: DateFormatter) -> None:
    """Test get_platform_locale with various inputs.

    Verifies
    --------
    - Returns correct locale
    - Handles timezone to locale mapping
    - Falls back to default locale

    Parameters
    ----------
    date_formatter : DateFormatter
        DateFormatter instance

    Returns
    -------
    None
    """
    result = date_formatter.get_platform_locale(str_timezone="America/Sao_Paulo")
    assert result == "pt-BR" if platform.system() == "Windows" else "pt_BR.UTF-8"

    result = date_formatter.get_platform_locale(str_locale="en-US")
    assert result == "en-US" if platform.system() == "Windows" else "en_US.UTF-8"

def test_get_platform_locale_invalid(date_formatter: DateFormatter) -> None:
    """Test get_platform_locale with invalid locale.

    Verifies
    --------
    - Falls back to default locale on invalid input
    - Handles locale errors gracefully

    Parameters
    ----------
    date_formatter : DateFormatter
        DateFormatter instance

    Returns
    -------
    None
    """
    with patch("locale.setlocale", side_effect=locale.Error):
        result = date_formatter.get_platform_locale(str_locale="invalid")
        expected = "en-GB" if platform.system() == "Windows" else "en_GB.UTF-8"
        assert result == expected

def test_year_number(date_formatter: DateFormatter, sample_date: date) -> None:
    """Test year_number with valid input.

    Verifies
    --------
    - Returns correct year number
    - Handles date input correctly

    Parameters
    ----------
    date_formatter : DateFormatter
        DateFormatter instance
    sample_date : date
        Sample date object

    Returns
    -------
    None
    """
    result = date_formatter.year_number(sample_date)
    assert result == 2023

def test_month_str(date_formatter: DateFormatter, sample_date: date) -> None:
    """Test month_str with valid input.

    Verifies
    --------
    - Returns correct month name
    - Handles date input correctly

    Parameters
    ----------
    date_formatter : DateFormatter
        DateFormatter instance
    sample_date : date
        Sample date object

    Returns
    -------
    None
    """
    result = date_formatter.month_str(sample_date)
    assert result == "June"

def test_month_number(
    date_formatter: DateFormatter, 
    sample_date: date
) -> None:
    """Test month_number with various inputs.

    Verifies
    --------
    - Returns correct month number or string
    - Handles bool_month_mm parameter

    Parameters
    ----------
    date_formatter : DateFormatter
        DateFormatter instance
    sample_date : date
        Sample date object

    Returns
    -------
    None
    """
    result = date_formatter.month_number(sample_date, bool_month_mm=False)
    assert result == 6
    result = date_formatter.month_number(sample_date, bool_month_mm=True)
    assert result == "06"

def test_week_number(date_formatter: DateFormatter, sample_date: date) -> None:
    """Test week_number with valid input.

    Verifies
    --------
    - Returns correct week number
    - Handles date input correctly

    Parameters
    ----------
    date_formatter : DateFormatter
        DateFormatter instance
    sample_date : date
        Sample date object

    Returns
    -------
    None
    """
    result = date_formatter.week_number(sample_date)
    assert result == "4"  # June 15, 2023 is Thursday

def test_day_number(date_formatter: DateFormatter, sample_date: date) -> None:
    """Test day_number with valid input.

    Verifies
    --------
    - Returns correct day number
    - Handles date input correctly

    Parameters
    ----------
    date_formatter : DateFormatter
        DateFormatter instance
    sample_date : date
        Sample date object

    Returns
    -------
    None
    """
    result = date_formatter.day_number(sample_date)
    assert result == 15

def test_month_name(date_formatter: DateFormatter, sample_date: date) -> None:
    """Test month_name with various inputs.

    Verifies
    --------
    - Returns correct month name or abbreviation
    - Handles timezone and abbreviation parameters

    Parameters
    ----------
    date_formatter : DateFormatter
        DateFormatter instance
    sample_date : date
        Sample date object

    Returns
    -------
    None
    """
    with patch("locale.setlocale"):
        result = date_formatter.month_name(sample_date, bool_abbreviation=False, 
                                           str_timezone="UTC")
        assert result == "June"
        result = date_formatter.month_name(sample_date, bool_abbreviation=True, 
                                           str_timezone="UTC")
        assert result == "Jun"

def test_week_name(date_formatter: DateFormatter, sample_date: date) -> None:
    """Test week_name with various inputs.

    Verifies
    --------
    - Returns correct week name or abbreviation
    - Handles timezone and abbreviation parameters

    Parameters
    ----------
    date_formatter : DateFormatter
        DateFormatter instance
    sample_date : date
        Sample date object

    Returns
    -------
    None
    """
    with patch("locale.setlocale"):
        result = date_formatter.week_name(sample_date, bool_abbreviation=False, 
                                          str_timezone="UTC")
        assert result == "Thursday"
        result = date_formatter.week_name(sample_date, bool_abbreviation=True, 
                                          str_timezone="UTC")
        assert result == "Thu"

def test_utc_log_ts(date_formatter: DateFormatter, mocker: MockerFixture) -> None:
    """Test utc_log_ts returns UTC datetime.

    Verifies
    --------
    - Returns correct UTC datetime
    - Handles timezone correctly

    Parameters
    ----------
    date_formatter : DateFormatter
        DateFormatter instance
    mocker : MockerFixture
        Pytest-mock fixture for mocking datetime.now()

    Returns
    -------
    None
    """
    mocker.patch("datetime.datetime.now", return_value=datetime(2023, 6, 15, 14, 30, 0, 
                                                                tzinfo=timezone.utc))
    result = date_formatter.utc_log_ts()
    assert result == datetime(2023, 6, 15, 14, 30, 0, tzinfo=timezone.utc)

# --------------------------
# Edge Cases and Error Conditions
# --------------------------
@pytest.mark.parametrize("invalid_date", [None, "", 123, []])
def test_date_only_invalid_types(
    calendar_core: CalendarCore, 
    invalid_date: object
) -> None:
    """Test date_only with various invalid types.

    Verifies
    --------
    - Raises TypeError for non-date/datetime inputs
    - Includes correct error message

    Parameters
    ----------
    calendar_core : CalendarCore
        CalendarCore instance
    invalid_date : object
        Invalid input type

    Returns
    -------
    None
    """
    with pytest.raises(TypeError, match="date_ must be of type datetime or date"):
        calendar_core.date_only(invalid_date)

@pytest.mark.parametrize("invalid_timestamp", ["invalid", "2023-13-15T14:30:00", ""])
def test_timestamp_to_datetime_invalid_format(
    date_manipulation: DateManipulation, 
    invalid_timestamp: str
) -> None:
    """Test timestamp_to_datetime with invalid timestamp formats.

    Verifies
    --------
    - Raises ValueError for malformed timestamps
    - Includes appropriate error message

    Parameters
    ----------
    date_manipulation : DateManipulation
        DateManipulation instance
    invalid_timestamp : str
        Invalid timestamp string

    Returns
    -------
    None
    """
    with pytest.raises(ValueError, match="Failed to parse timestamp"):
        date_manipulation.timestamp_to_datetime(invalid_timestamp)

# --------------------------
# Reload Logic
# --------------------------
def test_module_reload() -> None:
    """Test module reload behavior.

    Verifies
    --------
    - Module can be reloaded without errors
    - Classes maintain their functionality post-reload

    Returns
    -------
    None
    """
    import importlib
    import sys
    importlib.reload(sys.modules["stpstone.analytics.calendars.calendar_operations"])
    calendar = DateFormatter()
    assert isinstance(calendar.curr_date(), date)

# --------------------------
# Coverage for All Branches
# --------------------------
def test_add_working_days_all_branches(
    date_manipulation: DateManipulation, 
    mock_holidays: object
) -> None:
    """Test all branches of add_working_days.

    Verifies
    --------
    - Zero days case
    - Positive days case
    - Negative days case
    - Weekend and holiday skipping

    Parameters
    ----------
    date_manipulation : DateManipulation
        DateManipulation instance
    mock_holidays : object
        Mock for holidays method

    Returns
    -------
    None
    """
    date_ = date(2023, 6, 15)
    assert date_manipulation.add_working_days(date_, 0) == date_
    assert date_manipulation.add_working_days(date_, 1) == date(2023, 6, 19)
    assert date_manipulation.add_working_days(date_, -1) == date(2023, 6, 14)

def test_get_platform_locale_all_branches(
    date_formatter: DateFormatter, 
    mocker: MockerFixture
) -> None:
    """Test all branches of get_platform_locale.

    Verifies
    --------
    - Timezone mapping
    - Explicit locale
    - Fallback to default locale
    - Windows vs non-Windows handling

    Parameters
    ----------
    date_formatter : DateFormatter
        DateFormatter instance
    mocker : MockerFixture
        Pytest-mock fixture for mocking platform.system()

    Returns
    -------
    None
    """
    mocker.patch("platform.system", return_value="Windows")
    assert date_formatter.get_platform_locale(str_timezone="America/Sao_Paulo") == "pt-BR"
    mocker.patch("platform.system", return_value="Linux")
    assert date_formatter.get_platform_locale(str_timezone="America/Sao_Paulo") == "pt_BR.UTF-8"
    assert date_formatter.get_platform_locale(str_locale="en-US") == "en-US" \
        if platform.system() == "Windows" else "en_US.UTF-8"
    with patch("locale.setlocale", side_effect=locale.Error):
        assert date_formatter.get_platform_locale() == "en-GB" \
            if platform.system() == "Windows" else "en_GB.UTF-8"