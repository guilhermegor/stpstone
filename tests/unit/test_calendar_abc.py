"""Unit tests for ABCCalendarOperations and its parent classes.

Tests the calendar operations functionality with various input scenarios including:
- Initialization with valid and invalid inputs
- Date and time manipulation methods
- Holiday and working day calculations
- Timezone handling
- Edge cases and error conditions
"""

from datetime import date, datetime, time, timezone
import locale
import platform
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from freezegun import freeze_time
import pytest
from pytest_mock import MockerFixture

from stpstone.utils.cache.cache_persistent import PersistentCacheDecorator
from stpstone.utils.calendars.calendar_abc import (
    ABCCalendarCore,
    ABCCalendarOperations,
    TypeDateFormatInput,
)


# --------------------------
# Mock Classes and Fixtures
# --------------------------
class MockCalendar(ABCCalendarOperations):
    """Mock implementation of ABCCalendarOperations for testing."""

    def holidays(self) -> list[tuple[str, date]]:
        """Return a list of mock holidays.
        
        Returns
        -------
        list[tuple[str, date]]
            List of tuples containing holiday names and dates
        """
        return [
            ("New Year's Day", date(2025, 1, 1)),
            ("Christmas", date(2025, 12, 25))
        ]

@pytest.fixture
def mock_calendar() -> MockCalendar:
    """Fixture providing a MockCalendar instance.
    
    Returns
    -------
    MockCalendar
        Mock calendar instance
    """
    return MockCalendar(bool_persist_cache=False)

@pytest.fixture
def sample_date() -> date:
    """Fixture providing a sample date.
    
    Returns
    -------
    date
        Sample date
    """
    return date(2025, 6, 16)  # Changed to Monday to align with working day tests

@pytest.fixture
def sample_datetime() -> datetime:
    """Fixture providing a sample datetime.
    
    Returns
    -------
    datetime
        Sample datetime
    """
    return datetime(2025, 6, 16, 12, 30, tzinfo=ZoneInfo("UTC"))

@pytest.fixture
def mock_persistent_cache(mocker: MockerFixture) -> object:
    """Mock PersistentCacheDecorator for cache operations.
    
    Parameters
    ----------
    mocker : MockerFixture
        Pytest-mock fixture for creating mocks

    Returns
    -------
    object
        Mocked PersistentCacheDecorator
    """
    return mocker.patch.object(PersistentCacheDecorator, "clear_cache")

# --------------------------
# Tests for ABCCalendarCore
# --------------------------
def test_init_valid(mock_calendar: MockCalendar) -> None:
    """Test initialization with valid inputs.
    
    Parameters
    ----------
    mock_calendar : MockCalendar
        Mock calendar instance
    
    Returns
    -------
    None
    """
    assert mock_calendar.bool_persist_cache is False
    assert isinstance(mock_calendar, ABCCalendarCore)

def test_holidays_property(mock_calendar: MockCalendar) -> None:
    """Test _holidays property returns correct set of dates.
    
    Parameters
    ----------
    mock_calendar : MockCalendar
        Mock calendar instance
    
    Returns
    -------
    None
    """
    expected = {date(2025, 1, 1), date(2025, 12, 25)}
    assert mock_calendar._holidays == expected

def test_clear_holidays_cache(mock_calendar: MockCalendar, mock_persistent_cache: object) -> None:
    """Test clear_holidays_cache removes cache and calls PersistentCacheDecorator.
    
    Parameters
    ----------
    mock_calendar : MockCalendar
        Mock calendar instance
    mock_persistent_cache : object
        Mocked PersistentCacheDecorator
    
    Returns
    -------
    None
    """
    mock_calendar._holidays # noqa B018: useless expression - populate cache
    mock_calendar.clear_holidays_cache()
    assert not hasattr(mock_calendar, "_holidays_cache")
    mock_persistent_cache.assert_called_once()

def test_date_only_date_input(mock_calendar: MockCalendar, sample_date: date) -> None:
    """Test date_only with date input.
    
    Parameters
    ----------
    mock_calendar : MockCalendar
        Mock calendar instance
    sample_date : date
        Sample date
    
    Returns
    -------
    None
    """
    result = mock_calendar.date_only(sample_date)
    assert result == sample_date
    assert isinstance(result, date)
    assert not isinstance(result, datetime)

def test_date_only_datetime_input(
    mock_calendar: MockCalendar, 
    sample_datetime: datetime, 
    sample_date: date
) -> None:
    """Test date_only with datetime input.
    
    Parameters
    ----------
    mock_calendar : MockCalendar
        Mock calendar instance
    sample_datetime : datetime
        Sample datetime
    sample_date : date
        Sample date
    
    Returns
    -------
    None
    """
    result = mock_calendar.date_only(sample_datetime)
    assert result == sample_date
    assert isinstance(result, date)
    assert not isinstance(result, datetime)

@pytest.mark.parametrize("invalid_input", [None, "2025-06-16", 123, []])
def test_date_only_invalid_input(
    mock_calendar: MockCalendar, 
    invalid_input: Any # noqa ANN401: typing.Any is not allowed
) -> None:
    """Test date_only with invalid inputs raises TypeError.
    
    Parameters
    ----------
    mock_calendar : MockCalendar
        Mock calendar instance
    invalid_input : Any
        Invalid input
    
    Returns
    -------
    None
    """
    with pytest.raises(TypeError, match="must be of type"):
        mock_calendar.date_only(invalid_input)

def test_is_weekend_weekday(mock_calendar: MockCalendar, sample_date: date) -> None:
    """Test is_weekend with a weekday (Monday, June 16, 2025).
    
    Parameters
    ----------
    mock_calendar : MockCalendar
        Mock calendar instance
    sample_date : date
        Sample date
    
    Returns
    -------
    None
    """
    assert not mock_calendar.is_weekend(sample_date)  # Monday is not a weekend

def test_is_weekend_weekend(mock_calendar: MockCalendar) -> None:
    """Test is_weekend with a weekend day.
    
    Parameters
    ----------
    mock_calendar : MockCalendar
        Mock calendar instance
    
    Returns
    -------
    None
    """
    weekend_date = date(2025, 6, 14)  # Saturday
    assert mock_calendar.is_weekend(weekend_date)

def test_is_working_day_non_holiday_weekday(
    mock_calendar: MockCalendar, 
    sample_date: date
) -> None:
    """Test is_working_day with a non-holiday weekday.
    
    Parameters
    ----------
    mock_calendar : MockCalendar
        Mock calendar instance
    sample_date : date
        Sample date
    
    Returns
    -------
    None
    """
    assert mock_calendar.is_working_day(sample_date)  # Monday is a working day

def test_is_working_day_holiday(mock_calendar: MockCalendar) -> None:
    """Test is_working_day with a holiday.
    
    Parameters
    ----------
    mock_calendar : MockCalendar
        Mock calendar instance
    
    Returns
    -------
    None
    """
    holiday_date = date(2025, 1, 1)
    assert not mock_calendar.is_working_day(holiday_date)

def test_is_working_day_weekend(mock_calendar: MockCalendar) -> None:
    """Test is_working_day with a weekend day.
    
    Parameters
    ----------
    mock_calendar : MockCalendar
        Mock calendar instance
    
    Returns
    -------
    None
    """
    weekend_date = date(2025, 6, 14)  # Saturday
    assert not mock_calendar.is_working_day(weekend_date)

def test_is_holiday_holiday(mock_calendar: MockCalendar) -> None:
    """Test is_holiday with a holiday date.
    
    Parameters
    ----------
    mock_calendar : MockCalendar
        Mock calendar instance
    
    Returns
    -------
    None
    """
    holiday_date = date(2025, 1, 1)
    assert mock_calendar.is_holiday(holiday_date)

def test_is_holiday_non_holiday(mock_calendar: MockCalendar, sample_date: date) -> None:
    """Test is_holiday with a non-holiday date.
    
    Parameters
    ----------
    mock_calendar : MockCalendar
        Mock calendar instance
    sample_date : date
        Sample date
    
    Returns
    -------
    None
    """
    assert not mock_calendar.is_holiday(sample_date)

def test_holidays_in_year(mock_calendar: MockCalendar) -> None:
    """Test holidays_in_year returns correct days for a given year.
    
    Parameters
    ----------
    mock_calendar : MockCalendar
        Mock calendar instance
    
    Returns
    -------
    None
    """
    result = mock_calendar.holidays_in_year(2025)
    assert sorted(result) == [1, 25]  # Fix: Use sorted to ignore order

def test_holidays_in_year_no_holidays(mock_calendar: MockCalendar) -> None:
    """Test holidays_in_year with a year having no holidays.
    
    Parameters
    ----------
    mock_calendar : MockCalendar
        Mock calendar instance
    
    Returns
    -------
    None
    """
    result = mock_calendar.holidays_in_year(2024)
    assert result == []

# --------------------------
# Tests for ABCDateManipulation
# --------------------------
def test_add_working_days_zero(mock_calendar: MockCalendar, sample_date: date) -> None:
    """Test add_working_days with zero days.
    
    Parameters
    ----------
    mock_calendar : MockCalendar
        Mock calendar instance
    sample_date : date
        Sample date
    
    Returns
    -------
    None
    """
    result = mock_calendar.add_working_days(sample_date, 0)
    assert result == sample_date

def test_add_working_days_positive(mock_calendar: MockCalendar, sample_date: date) -> None:
    """Test add_working_days with positive days.
    
    Parameters
    ----------
    mock_calendar : MockCalendar
        Mock calendar instance
    sample_date : date
        Sample date
    
    Returns
    -------
    None
    """
    result = mock_calendar.add_working_days(sample_date, 2)
    expected = date(2025, 6, 18)  # Skips weekend
    assert result == expected

def test_add_working_days_negative(mock_calendar: MockCalendar, sample_date: date) -> None:
    """Test add_working_days with negative days.
    
    Parameters
    ----------
    mock_calendar : MockCalendar
        Mock calendar instance
    sample_date : date
        Sample date
    
    Returns
    -------
    None
    """
    result = mock_calendar.add_working_days(sample_date, -2)
    expected = date(2025, 6, 12)  # Skips weekend
    assert result == expected

def test_add_calendar_days(mock_calendar: MockCalendar, sample_date: date) -> None:
    """Test add_calendar_days with positive and negative days.
    
    Parameters
    ----------
    mock_calendar : MockCalendar
        Mock calendar instance
    sample_date : date
        Sample date
    
    Returns
    -------
    None
    """
    assert mock_calendar.add_calendar_days(sample_date, 5) == date(2025, 6, 21)
    assert mock_calendar.add_calendar_days(sample_date, -5) == date(2025, 6, 11)

@pytest.mark.parametrize("invalid_input", [None, "2025-06-16", 123, []])
def test_add_calendar_days_invalid_input(
    mock_calendar: MockCalendar, 
    invalid_input: Any # noqa ANN401: typing.Any is not allowed
) -> None:
    """Test add_calendar_days with invalid date input.
    
    Parameters
    ----------
    mock_calendar : MockCalendar
        Mock calendar instance
    invalid_input : Any
        Invalid input
    
    Returns
    -------
    None
    """
    with pytest.raises(TypeError, match="must be of type"):
        mock_calendar.add_calendar_days(invalid_input, 5)

def test_add_months(mock_calendar: MockCalendar, sample_datetime: datetime) -> None:
    """Test add_months with positive and negative months.
    
    Parameters
    ----------
    mock_calendar : MockCalendar
        Mock calendar instance
    sample_datetime : datetime
        Sample datetime
    
    Returns
    -------
    None
    """
    result = mock_calendar.add_months(sample_datetime, 2)
    assert result == datetime(2025, 8, 16, 12, 30, tzinfo=ZoneInfo("UTC"))
    result = mock_calendar.add_months(sample_datetime, -2)
    assert result == datetime(2025, 4, 16, 12, 30, tzinfo=ZoneInfo("UTC"))

@pytest.mark.parametrize("invalid_input", [None, "2025-06-16", 123, [], date(2025, 6, 16)])
def test_add_months_invalid_input(
    mock_calendar: MockCalendar, 
    invalid_input: Any # noqa ANN401: typing.Any is not allowed
) -> None:
    """Test add_months with invalid datetime input.
    
    Parameters
    ----------
    mock_calendar : MockCalendar
        Mock calendar instance
    invalid_input : Any
        Invalid input
    
    Returns
    -------
    None
    """
    with pytest.raises(TypeError, match="must be of type"):
        mock_calendar.add_months(invalid_input, 2)

def test_build_date(mock_calendar: MockCalendar) -> None:
    """Test build_date with valid inputs.
    
    Parameters
    ----------
    mock_calendar : MockCalendar
        Mock calendar instance
    
    Returns
    -------
    None
    """
    result = mock_calendar.build_date(2025, 6, 16)
    assert result == date(2025, 6, 16)

@pytest.mark.parametrize("year,month,day", [
    (0, 6, 16),  # Invalid year
    (2025, 0, 16),  # Invalid month
    (2025, 6, 0),  # Invalid day
])
def test_build_date_invalid(mock_calendar: MockCalendar, year: int, month: int, day: int) -> None:
    """Test build_date with invalid inputs.
    
    Parameters
    ----------
    mock_calendar : MockCalendar
        Mock calendar instance
    year : int
        Invalid year
    month : int
        Invalid month
    day : int
        Invalid day
    
    Returns
    -------
    None
    """
    with pytest.raises(ValueError):
        mock_calendar.build_date(year, month, day)

def test_build_datetime(mock_calendar: MockCalendar) -> None:
    """Test build_datetime with valid inputs.
    
    Parameters
    ----------
    mock_calendar : MockCalendar
        Mock calendar instance
    
    Returns
    -------
    None
    """
    result = mock_calendar.build_datetime(2025, 6, 16, 12, 30, 0, "UTC")
    assert result == datetime(2025, 6, 16, 12, 30, tzinfo=ZoneInfo("UTC"))

@pytest.mark.parametrize("invalid_input", ["Invalid/TZ", ""])
def test_build_datetime_invalid_tz(mock_calendar: MockCalendar, invalid_input: str) -> None:
    """Test build_datetime with invalid timezone.
    
    Parameters
    ----------
    mock_calendar : MockCalendar
        Mock calendar instance
    invalid_input : str
        Invalid timezone
    
    Returns
    -------
    None
    """
    with pytest.raises(ZoneInfoNotFoundError):
        mock_calendar.build_datetime(2025, 6, 16, 12, 30, 0, invalid_input)

def test_nearest_working_day_next(mock_calendar: MockCalendar, sample_date: date) -> None:
    """Test nearest_working_day with bool_next=True.
    
    Parameters
    ----------
    mock_calendar : MockCalendar
        Mock calendar instance
    sample_date : date
        Sample date
    
    Returns
    -------
    None
    """
    holiday_date = date(2025, 1, 1)  # Holiday (Wednesday)
    result = mock_calendar.nearest_working_day(holiday_date, bool_next=True)
    assert result == date(2025, 1, 2)  # Next working day

def test_nearest_working_day_previous(mock_calendar: MockCalendar, sample_date: date) -> None:
    """Test nearest_working_day with bool_next=False.
    
    Parameters
    ----------
    mock_calendar : MockCalendar
        Mock calendar instance
    sample_date : date
        Sample date
    
    Returns
    -------
    None
    """
    holiday_date = date(2025, 1, 1)  # Holiday (Wednesday)
    result = mock_calendar.nearest_working_day(holiday_date, bool_next=False)
    assert result == date(2024, 12, 31)  # Previous working day

@pytest.mark.parametrize("str_date,format_input,expected", [
    ("16/06/2025", "DD/MM/YYYY", date(2025, 6, 16)),
    ("2025-06-16", "YYYY-MM-DD", date(2025, 6, 16)),
    ("250616", "YYMMDD", date(2025, 6, 16)),
])
def test_str_date_to_date(
    mock_calendar: MockCalendar, 
    str_date: str, 
    format_input: TypeDateFormatInput, 
    expected: date
) -> None:
    """Test str_date_to_date with valid inputs.
    
    Parameters
    ----------
    mock_calendar : MockCalendar
        Mock calendar instance
    str_date : str
        The string representation of the date
    format_input : TypeDateFormatInput
        The format of the input date string
    expected : date
        The expected date object
    
    Returns
    -------
    None
    """
    result = mock_calendar.str_date_to_date(str_date, format_input)
    assert result == expected

@pytest.mark.parametrize("str_date,format_input", [
    ("invalid", "DD/MM/YYYY"),
    ("16/06/2025", "INVALID"),
    ("", "DD/MM/YYYY"),
])
def test_str_date_to_date_invalid(
    mock_calendar: MockCalendar, 
    str_date: str, 
    format_input: str
) -> None:
    """Test str_date_to_date with invalid inputs.
    
    Parameters
    ----------
    mock_calendar : MockCalendar
        Mock calendar instance
    str_date : str
        The string representation of the date
    format_input : str
        The format of the input date string
    
    Returns
    -------
    None
    """
    with pytest.raises(ValueError):
        mock_calendar.str_date_to_date(str_date, format_input)

def test_timestamp_to_date(mock_calendar: MockCalendar) -> None:
    """Test timestamp_to_date with valid input.
    
    Parameters
    ----------
    mock_calendar : MockCalendar
        Mock calendar instance
    
    Returns
    -------
    None
    """
    result = mock_calendar.timestamp_to_date("2025-06-16T12:30:00")
    assert result == date(2025, 6, 16)

def test_timestamp_to_date_invalid(mock_calendar: MockCalendar) -> None:
    """Test timestamp_to_date with invalid input.
    
    Parameters
    ----------
    mock_calendar : MockCalendar
        Mock calendar instance
    
    Returns
    -------
    None
    """
    with pytest.raises(ValueError):
        mock_calendar.timestamp_to_date("invalid")

def test_timestamp_to_datetime(mock_calendar: MockCalendar) -> None:
    """Test timestamp_to_datetime with valid inputs.
    
    Parameters
    ----------
    mock_calendar : MockCalendar
        Mock calendar instance
    
    Returns
    -------
    None
    """
    result = mock_calendar.timestamp_to_datetime("2025-06-16T12:30:00")
    assert result == datetime(2025, 6, 16, 12, 30)

def test_timestamp_to_datetime_iso(mock_calendar: MockCalendar) -> None:
    """Test timestamp_to_datetime with ISO format.
    
    Parameters
    ----------
    mock_calendar : MockCalendar
        Mock calendar instance
    
    Returns
    -------
    None
    """
    result = mock_calendar.timestamp_to_datetime("2025-06-16T12:30:00Z")
    assert result == datetime(2025, 6, 16, 12, 30, tzinfo=timezone.utc)

def test_timestamp_to_datetime_invalid(mock_calendar: MockCalendar) -> None:
    """Test timestamp_to_datetime with invalid input.
    
    Parameters
    ----------
    mock_calendar : MockCalendar
        Mock calendar instance
    
    Returns
    -------
    None
    """
    with pytest.raises(ValueError):
        mock_calendar.timestamp_to_datetime("invalid")

def test_to_integer(mock_calendar: MockCalendar, sample_date: date) -> None:
    """Test to_integer converts date to correct integer format.
    
    Parameters
    ----------
    mock_calendar : MockCalendar
        Mock calendar instance
    sample_date : date
        Sample date
    
    Returns
    -------
    None
    """
    result = mock_calendar.to_integer(sample_date)
    assert result == 20250616

def test_excel_float_to_date(mock_calendar: MockCalendar) -> None:
    """Test excel_float_to_date with valid input.
    
    Parameters
    ----------
    mock_calendar : MockCalendar
        Mock calendar instance
    
    Returns
    -------
    None
    """
    result = mock_calendar.excel_float_to_date(44728)
    assert result == date(2022, 6, 16)

@pytest.mark.parametrize("invalid_input", [None, -1])
def test_excel_float_to_date_invalid(
    mock_calendar: MockCalendar, 
    invalid_input: Any # noqa ANN401: typing.Any is not allowed
) -> None:
    """Test excel_float_to_date with invalid inputs.
    
    Parameters
    ----------
    mock_calendar : MockCalendar
        Mock calendar instance
    invalid_input : Any
        Invalid input
    
    Returns
    -------
    None
    """
    if invalid_input is None:
        with pytest.raises(TypeError, match="int_excel_date must be of type int"):
            mock_calendar.excel_float_to_date(invalid_input)
    else:
        with pytest.raises(ValueError, match="int_excel_date cannot be negative"):
            mock_calendar.excel_float_to_date(invalid_input)

# --------------------------
# Tests for ABCTimezoneAware
# --------------------------
def test_str_date_to_datetime(mock_calendar: MockCalendar) -> None:
    """Test str_date_to_datetime with valid inputs.
    
    Parameters
    ----------
    mock_calendar : MockCalendar
        Mock calendar instance
    
    Returns
    -------
    None
    """
    result = mock_calendar.str_date_to_datetime("16/06/2025", "DD/MM/YYYY", "UTC")
    assert result == datetime(2025, 6, 16, 0, 0, tzinfo=ZoneInfo("UTC"))

def test_change_timezone_datetime(mock_calendar: MockCalendar, sample_datetime: datetime) -> None:
    """Test change_timezone with datetime input.
    
    Parameters
    ----------
    mock_calendar : MockCalendar
        Mock calendar instance
    sample_datetime : datetime
        Sample datetime
    
    Returns
    -------
    None
    """
    result = mock_calendar.change_timezone(sample_datetime, "America/New_York")
    assert result.tzinfo == ZoneInfo("America/New_York")
    assert result == datetime(2025, 6, 16, 8, 30, tzinfo=ZoneInfo("America/New_York"))

def test_change_timezone_date(mock_calendar: MockCalendar, sample_date: date) -> None:
    """Test change_timezone with date input.
    
    Parameters
    ----------
    mock_calendar : MockCalendar
        Mock calendar instance
    sample_date : date
        Sample date
    
    Returns
    -------
    None
    """
    result = mock_calendar.change_timezone(sample_date, target_tz="UTC")
    assert result == datetime(2025, 6, 16, 0, 0, tzinfo=ZoneInfo("UTC"))

def test_change_timezone_naive_no_source(mock_calendar: MockCalendar) -> None:
    """Test change_timezone with naive datetime and no source_tz.
    
    Parameters
    ----------
    mock_calendar : MockCalendar
        Mock calendar instance
    
    Returns
    -------
    None
    """
    naive_dt = datetime(2025, 6, 16, 12, 30)
    with pytest.raises(ValueError, match="Cannot change timezone of naive datetime"):
        mock_calendar.change_timezone(naive_dt, "UTC")

def test_date_to_datetime(mock_calendar: MockCalendar, sample_date: date) -> None:
    """Test date_to_datetime conversion.
    
    Parameters
    ----------
    mock_calendar : MockCalendar
        Mock calendar instance
    sample_date : date
        Sample date
    
    Returns
    -------
    None
    """
    result = mock_calendar.date_to_datetime(sample_date, "UTC")
    assert result == datetime(2025, 6, 16, 0, 0, tzinfo=ZoneInfo("UTC"))

def test_to_unix_timestamp_datetime(
    mock_calendar: MockCalendar, 
    sample_datetime: datetime
) -> None:
    """Test to_unix_timestamp with datetime input.
    
    Parameters
    ----------
    mock_calendar : MockCalendar
        Mock calendar instance
    sample_datetime : datetime
        Sample datetime
    
    Returns
    -------
    None
    """
    result = mock_calendar.to_unix_timestamp(sample_datetime)
    expected = int(sample_datetime.timestamp())
    assert result == expected

def test_to_unix_timestamp_date(mock_calendar: MockCalendar, sample_date: date) -> None:
    """Test to_unix_timestamp with date input.
    
    Parameters
    ----------
    mock_calendar : MockCalendar
        Mock calendar instance
    sample_date : date
        Sample date
    
    Returns
    -------
    None
    """
    result = mock_calendar.to_unix_timestamp(sample_date)
    expected = int(datetime(2025, 6, 16, 0, 0, tzinfo=ZoneInfo("UTC")).timestamp())
    assert result == expected

def test_to_unix_timestamp_time(mock_calendar: MockCalendar, mocker: MockerFixture) -> None:
    """Test to_unix_timestamp with time input.
    
    Parameters
    ----------
    mock_calendar : MockCalendar
        Mock calendar instance
    mocker : MockerFixture
        Mocker fixture
    
    Returns
    -------
    None
    """
    t = time(12, 30)
    with freeze_time("2025-06-16"):
        result = mock_calendar.to_unix_timestamp(t)
        expected = int(datetime(2025, 6, 16, 12, 30, tzinfo=ZoneInfo("UTC")).timestamp())
        assert result == expected

def test_unix_timestamp_to_datetime(mock_calendar: MockCalendar) -> None:
    """Test unix_timestamp_to_datetime with valid input.
    
    Parameters
    ----------
    mock_calendar : MockCalendar
        Mock calendar instance
    
    Returns
    -------
    None
    """
    ts = int(datetime(2025, 6, 16, 12, 30, tzinfo=ZoneInfo("UTC")).timestamp())
    result = mock_calendar.unix_timestamp_to_datetime(ts)
    assert result == datetime(2025, 6, 16, 12, 30, tzinfo=ZoneInfo("UTC"))

def test_unix_timestamp_to_date(mock_calendar: MockCalendar) -> None:
    """Test unix_timestamp_to_date with valid input.
    
    Parameters
    ----------
    mock_calendar : MockCalendar
        Mock calendar instance
    
    Returns
    -------
    None
    """
    ts = 1750050600
    result = mock_calendar.unix_timestamp_to_date(ts)
    assert result == date(2025, 6, 16)

def test_iso_to_unix_timestamp(mock_calendar: MockCalendar) -> None:
    """Test iso_to_unix_timestamp with valid input.
    
    Parameters
    ----------
    mock_calendar : MockCalendar
        Mock calendar instance
    
    Returns
    -------
    None
    """
    result = mock_calendar.iso_to_unix_timestamp("2025-06-16T12:30:00+00:00", str_timezone="UTC")
    expected = int(datetime(2025, 6, 16, 12, 30, tzinfo=timezone.utc).timestamp())
    assert result == expected

def test_excel_float_to_datetime(mock_calendar: MockCalendar) -> None:
    """Test excel_float_to_datetime with valid input.
    
    Parameters
    ----------
    mock_calendar : MockCalendar
        Mock calendar instance
    
    Returns
    -------
    None
    """
    result = mock_calendar.excel_float_to_datetime(44728.5)
    assert result == datetime(2022, 6, 17, 12, 0, tzinfo=ZoneInfo("UTC"))

# --------------------------
# Tests for ABCRangeDatesDelta
# --------------------------
def test_working_days_range(mock_calendar: MockCalendar, sample_date: date) -> None:
    """Test working_days_range with valid inputs.
    
    Parameters
    ----------
    mock_calendar : MockCalendar
        Mock calendar instance
    sample_date : date
        Sample date
    
    Returns
    -------
    None
    """
    end_date = date(2025, 6, 20)
    result = mock_calendar.working_days_range(sample_date, end_date)
    expected = {date(2025, 6, 16), date(2025, 6, 17), date(2025, 6, 18), 
                date(2025, 6, 19), date(2025, 6, 20)}
    assert result == expected

def test_working_days_range_invalid(mock_calendar: MockCalendar, sample_date: date) -> None:
    """Test working_days_range with end date before start date.
    
    Parameters
    ----------
    mock_calendar : MockCalendar
        Mock calendar instance
    sample_date : date
        Sample date
    
    Returns
    -------
    None
    """
    with pytest.raises(ValueError, match="date_end must be greater than date_start"):
        mock_calendar.working_days_range(sample_date, date(2025, 6, 10))

def test_calendar_days_range(mock_calendar: MockCalendar, sample_date: date) -> None:
    """Test calendar_days_range with valid inputs.
    
    Parameters
    ----------
    mock_calendar : MockCalendar
        Mock calendar instance
    sample_date : date
        Sample date
    
    Returns
    -------
    None
    """
    end_date = date(2025, 6, 18)
    result = mock_calendar.calendar_days_range(sample_date, end_date)
    expected = {date(2025, 6, 16), date(2025, 6, 17), date(2025, 6, 18)}
    assert result == expected

def test_years_between_dates(mock_calendar: MockCalendar) -> None:
    """Test years_between_dates with valid inputs.
    
    Parameters
    ----------
    mock_calendar : MockCalendar
        Mock calendar instance
    
    Returns
    -------
    None
    """
    start_date = date(2024, 12, 31)
    end_date = date(2025, 1, 2)
    result = mock_calendar.years_between_dates(start_date, end_date)
    assert result == {2024, 2025}

def test_delta_working_days(mock_calendar: MockCalendar, sample_date: date) -> None:
    """Test delta_working_days with valid inputs.
    
    Parameters
    ----------
    mock_calendar : MockCalendar
        Mock calendar instance
    sample_date : date
        Sample date
    
    Returns
    -------
    None
    """
    end_date = date(2025, 6, 20)
    result = mock_calendar.delta_working_days(sample_date, end_date)
    assert result == 4

def test_delta_calendar_days(mock_calendar: MockCalendar, sample_date: date) -> None:
    """Test delta_calendar_days with valid inputs.
    
    Parameters
    ----------
    mock_calendar : MockCalendar
        Mock calendar instance
    sample_date : date
        Sample date
    
    Returns
    -------
    None
    """
    end_date = date(2025, 6, 20)
    result = mock_calendar.delta_calendar_days(sample_date, end_date)
    assert result == 4

def test_get_start_end_day_month(mock_calendar: MockCalendar, sample_date: date) -> None:
    """Test get_start_end_day_month with valid inputs.
    
    Parameters
    ----------
    mock_calendar : MockCalendar
        Mock calendar instance
    sample_date : date
        Sample date
    
    Returns
    -------
    None
    """
    result = mock_calendar.get_start_end_day_month(sample_date)
    assert result == (date(2025, 6, 1), date(2025, 7, 1))

def test_get_start_end_day_month_working_days(
    mock_calendar: MockCalendar, 
    sample_date: date
) -> None:
    """Test get_start_end_day_month with working days.
    
    Parameters
    ----------
    mock_calendar : MockCalendar
        Mock calendar instance
    sample_date : date
        Sample date
    
    Returns
    -------
    None
    """
    result = mock_calendar.get_start_end_day_month(sample_date, bool_working_days=True)
    assert result == (date(2025, 6, 2), date(2025, 7, 1))

def test_get_dates_weekday_month(mock_calendar: MockCalendar) -> None:
    """Test get_dates_weekday_month with valid inputs.
    
    Parameters
    ----------
    mock_calendar : MockCalendar
        Mock calendar instance
    
    Returns
    -------
    None
    """
    result = mock_calendar.get_dates_weekday_month(2025, 6, 0)
    expected = [date(2025, 6, 2), date(2025, 6, 9), date(2025, 6, 16), date(2025, 6, 23), 
                date(2025, 6, 30)]
    assert result == expected

@pytest.mark.parametrize("month,weekday", [(0, 0), (13, 0), (1, -1), (1, 7)])
def test_get_dates_weekday_month_invalid(
    mock_calendar: MockCalendar, 
    month: int, 
    weekday: int
) -> None:
    """Test get_dates_weekday_month with invalid inputs.
    
    Parameters
    ----------
    mock_calendar : MockCalendar
        Mock calendar instance
    month : int
        Month
    weekday : int
        Weekday
    
    Returns
    -------
    None
    """
    with pytest.raises(ValueError):
        mock_calendar.get_dates_weekday_month(2025, month, weekday)

def test_get_nth_weekday_month(mock_calendar: MockCalendar) -> None:
    """Test get_nth_weekday_month with valid inputs.
    
    Parameters
    ----------
    mock_calendar : MockCalendar
        Mock calendar instance
    
    Returns
    -------
    None
    """
    result = mock_calendar.get_nth_weekday_month(2025, 6, 0, 2)
    assert result == date(2025, 6, 9)

@pytest.mark.parametrize("month,weekday,n", [(0, 0, 1), (1, -1, 1), (1, 0, 0)])
def test_get_nth_weekday_month_invalid(
    mock_calendar: MockCalendar, 
    month: int, 
    weekday: int, 
    n: int
) -> None:
    """Test get_nth_weekday_month with invalid inputs.
    
    Parameters
    ----------
    mock_calendar : MockCalendar
        Mock calendar instance
    month : int
        Month
    weekday : int
        Weekday
    n : int
        Nth
    
    Returns
    -------
    None
    """
    with pytest.raises(ValueError):
        mock_calendar.get_nth_weekday_month(2025, month, weekday, n)

def test_get_nth_weekday_month_out_of_range(mock_calendar: MockCalendar) -> None:
    """Test get_nth_weekday_month with n out of range.
    
    Parameters
    ----------
    mock_calendar : MockCalendar
        Mock calendar instance
    
    Returns
    -------
    None
    """
    with pytest.raises(IndexError):
        mock_calendar.get_nth_weekday_month(2025, 1, 0, 6)

def test_last_working_day_years(mock_calendar: MockCalendar) -> None:
    """Test last_working_day_years with valid inputs.
    
    Parameters
    ----------
    mock_calendar : MockCalendar
        Mock calendar instance
    
    Returns
    -------
    None
    """
    result = mock_calendar.last_working_day_years([2024, 2025])
    assert result == [date(2024, 12, 31), date(2025, 12, 31)]

def test_delta_working_hours(mock_calendar: MockCalendar, mocker: MockerFixture) -> None:
    """Test delta_working_hours with valid inputs.
    
    Parameters
    ----------
    mock_calendar : MockCalendar
        Mock calendar instance
    mocker : MockerFixture
        Pytest mocker fixture

    Returns
    -------
    None
    """
    mocker.patch.object(mock_calendar, "holidays", return_value=[])
    result = mock_calendar.delta_working_hours(
        "2025-06-16T08:00:00",
        "2025-06-16T17:00:00",
        int_hour_start_office=8,
        int_minute_start_office=0,
        int_hour_end_office=17,
        int_minute_end_office=30,
        int_hour_start_lunch=12,
        int_minute_start_lunch=30,
        int_hour_end_lunch=14,
        int_minute_end_lunch=30,
        list_working_days_range=[0, 1, 2, 3, 4],
    )
    assert result == 7

# --------------------------
# Tests for ABCCurrentDate
# --------------------------
def test_curr_date(mock_calendar: MockCalendar, mocker: MockerFixture) -> None:
    """Test curr_date returns today's date.
    
    Parameters
    ----------
    mock_calendar : MockCalendar
        Mock calendar instance
    mocker : MockerFixture
        Pytest mocker fixture

    Returns
    -------
    None
    """
    with freeze_time("2025-06-16"):
        assert mock_calendar.curr_date() == date(2025, 6, 16)
    
def test_curr_datetime(mock_calendar: MockCalendar, mocker: MockerFixture) -> None:
    """Test curr_datetime returns current datetime with correct timezone.
    
    Parameters
    ----------
    mock_calendar : MockCalendar
        Mock calendar instance
    mocker : MockerFixture
        Pytest mocker fixture

    Returns
    -------
    None
    """
    with freeze_time("2025-06-16 12:30:00+00:00"):
        result = mock_calendar.curr_datetime("UTC")
        assert result == datetime(2025, 6, 16, 12, 30, tzinfo=ZoneInfo("UTC"))

def test_curr_time(mock_calendar: MockCalendar, mocker: MockerFixture) -> None:
    """Test curr_time returns current time with correct timezone.
    
    Parameters
    ----------
    mock_calendar : MockCalendar
        Mock calendar instance
    mocker : MockerFixture
        Pytest mocker fixture

    Returns
    -------
    None
    """
    mocker.patch.object(
        mock_calendar, 
        "curr_datetime", 
        return_value=datetime(2025, 6, 16, 12, 30, tzinfo=ZoneInfo("UTC"))
    )
    result = mock_calendar.curr_time("UTC")
    assert result == time(12, 30)

def test_current_timestamp_string(mock_calendar: MockCalendar, mocker: MockerFixture) -> None:
    """Test current_timestamp_string returns formatted string.
    
    Parameters
    ----------
    mock_calendar : MockCalendar
        Mock calendar instance
    mocker : MockerFixture
        Pytest mocker fixture

    Returns
    -------
    None
    """
    mocker.patch.object(
        mock_calendar, 
        "curr_datetime", 
        return_value=datetime(2025, 6, 16, 12, 30, tzinfo=ZoneInfo("UTC"))
    )
    result = mock_calendar.current_timestamp_string()
    assert result == "20250616_123000"

# --------------------------
# Tests for ABCDateFormatter
# --------------------------
def test_get_platform_locale(mock_calendar: MockCalendar, mocker: MockerFixture) -> None:
    """Test get_platform_locale returns correct locale.
    
    Parameters
    ----------
    mock_calendar : MockCalendar
        Mock calendar instance
    mocker : MockerFixture
        Pytest mocker fixture

    Returns
    -------
    None
    """
    mocker.patch("locale.setlocale")
    result = mock_calendar.get_platform_locale(str_timezone="America/New_York")
    assert result == "en-US" if platform.system() == "Windows" else "en_US.UTF-8"

def test_get_platform_locale_fallback(mock_calendar: MockCalendar, mocker: MockerFixture) -> None:
    """Test get_platform_locale falls back to default locale.
    
    Parameters
    ----------
    mock_calendar : MockCalendar
        Mock calendar instance
    mocker : MockerFixture
        Pytest mocker fixture

    Returns
    -------
    None
    """
    mocker.patch("locale.setlocale", side_effect=[locale.Error, None])
    result = mock_calendar.get_platform_locale()
    assert result == "en-GB" if platform.system() == "Windows" else "en_GB.UTF-8"

def test_year_number(mock_calendar: MockCalendar, sample_date: date) -> None:
    """Test year_number returns correct year.
    
    Parameters
    ----------
    mock_calendar : MockCalendar
        Mock calendar instance
    sample_date : date
        Sample date

    Returns
    -------
    None
    """
    result = mock_calendar.year_number(sample_date)
    assert result == 2025

def test_month_number(mock_calendar: MockCalendar, sample_date: date) -> None:
    """Test month_number returns correct month.
    
    Parameters
    ----------
    mock_calendar : MockCalendar
        Mock calendar instance
    sample_date : date
        Sample date

    Returns
    -------
    None
    """
    result = mock_calendar.month_number(sample_date)
    assert result == 6
    result = mock_calendar.month_number(sample_date, bool_month_mm=True)
    assert result == "06"

def test_week_number(mock_calendar: MockCalendar, sample_date: date) -> None:
    """Test week_number returns correct week number.
    
    Parameters
    ----------
    mock_calendar : MockCalendar
        Mock calendar instance
    sample_date : date
        Sample date

    Returns
    -------
    None
    """
    result = mock_calendar.week_number(sample_date)
    assert result == "1"  # Monday

def test_day_number(mock_calendar: MockCalendar, sample_date: date) -> None:
    """Test day_number returns correct day.
    
    Parameters
    ----------
    mock_calendar : MockCalendar
        Mock calendar instance
    sample_date : date
        Sample date

    Returns
    -------
    None
    """
    result = mock_calendar.day_number(sample_date)
    assert result == 16

def test_month_name(
    mock_calendar: MockCalendar, 
    sample_date: date, 
    mocker: MockerFixture
) -> None:
    """Test month_name returns correct month name.
    
    Parameters
    ----------
    mock_calendar : MockCalendar
        Mock calendar instance
    sample_date : date
        Sample date
    mocker : MockerFixture
        Pytest mocker fixture

    Returns
    -------
    None
    """
    mocker.patch("locale.setlocale")
    result = mock_calendar.month_name(sample_date, str_timezone="UTC")
    assert result == "June"
    result = mock_calendar.month_name(sample_date, bool_abbreviation=True, str_timezone="UTC")
    assert result == "Jun"

def test_week_name(mock_calendar: MockCalendar, sample_date: date, mocker: MockerFixture) -> None:
    """Test week_name returns correct weekday name.
    
    Parameters
    ----------
    mock_calendar : MockCalendar
        Mock calendar instance
    sample_date : date
        Sample date
    mocker : MockerFixture
        Pytest mocker fixture

    Returns
    -------
    None
    """
    mocker.patch("locale.setlocale")
    result = mock_calendar.week_name(sample_date, str_timezone="UTC")
    assert result == "Monday"
    result = mock_calendar.week_name(sample_date, bool_abbreviation=True, str_timezone="UTC")
    assert result == "Mon"

def test_utc_log_ts(mock_calendar: MockCalendar, mocker: MockerFixture) -> None:
    """Test utc_log_ts returns current UTC datetime.
    
    Parameters
    ----------
    mock_calendar : MockCalendar
        Mock calendar instance
    mocker : MockerFixture
        Pytest mocker fixture

    Returns
    -------
    None
    """
    with freeze_time("2025-06-16 12:30:00+00:00"):
        result = mock_calendar.utc_log_ts()
        assert result == datetime(2025, 6, 16, 12, 30, tzinfo=timezone.utc)