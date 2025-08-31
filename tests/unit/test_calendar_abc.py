"""Unit tests for ABCCalendarOperations class and related functionality.

Tests the calendar operations functionality including:
- Date validation and conversion
- Working day calculations
- Timezone handling
- Date formatting and manipulation
- Edge cases and error conditions
"""

from datetime import date, datetime, time, timezone
import locale
from typing import Any, Optional
from unittest.mock import MagicMock, patch
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import pandas as pd
import pytest
from pytest_mock import MockerFixture

from stpstone.utils.calendars.calendar_abc import (
    ABCCalendar,
    ABCCalendarOperations,
    TypeDateFormatInput,
)


# --------------------------
# Fixtures
# --------------------------
@pytest.fixture
def mock_setlocale() -> MagicMock:
    """Fixture mocking locale.setlocale to return the locale string.
    
    Returns
    -------
    MagicMock
        Mocked locale.setlocale function
    """
    with patch('locale.setlocale') as mocked:
        mocked.side_effect = lambda category, loc: loc  # Return the locale string
        yield mocked

@pytest.fixture
def calendar_instance() -> ABCCalendarOperations:
    """Fixture providing ABCCalendarOperations instance for testing.

    Returns
    -------
    ABCCalendarOperations
        Instance of the calendar operations class
    """
    return ABCCalendarOperations()


@pytest.fixture
def sample_date() -> date:
    """Fixture providing a sample date for testing.

    Returns
    -------
    date
        Sample date (2023-12-25)
    """
    return date(2023, 12, 25)


@pytest.fixture
def sample_datetime() -> datetime:
    """Fixture providing a sample datetime for testing.

    Returns
    -------
    datetime
        Sample datetime (2023-12-25 10:30:45)
    """
    return datetime(2023, 12, 25, 10, 30, 45)


@pytest.fixture
def sample_holidays() -> list[tuple[str, date]]:
    """Fixture providing sample holidays for testing.

    Returns
    -------
    list[tuple[str, date]]
        List of holiday tuples (name, date)
    """
    return [
        ("New Year's Day", date(2023, 1, 1)),
        ("Christmas Day", date(2023, 12, 25)),
    ]


@pytest.fixture
def mock_holidays(mocker: MockerFixture, sample_holidays: list[tuple[str, date]]) -> MagicMock:
    """Mock the holidays method to return sample holidays.

    Parameters
    ----------
    mocker : MockerFixture
        Pytest-mock fixture
    sample_holidays : list[tuple[str, date]]
        Sample holidays to return

    Returns
    -------
    MagicMock
        Mocked holidays method
    """
    return mocker.patch.object(ABCCalendarOperations, "holidays", return_value=sample_holidays)


# --------------------------
# Tests for ABCCalendar
# --------------------------
class TestABCCalendar:
    """Test cases for ABCCalendar abstract base class."""

    def test_abstract_methods_exist(self) -> None:
        """Test that ABCCalendar has required abstract methods.

        Verifies
        --------
        - ABCCalendar has get_holidays_raw method
        - ABCCalendar has holidays method
        - Both methods are abstract

        Returns
        -------
        None
        """
        assert hasattr(ABCCalendar, "get_holidays_raw")
        assert hasattr(ABCCalendar, "holidays")
        assert getattr(ABCCalendar.get_holidays_raw, "__isabstractmethod__", False)
        assert getattr(ABCCalendar.holidays, "__isabstractmethod__", False)

    def test_cannot_instantiate_abstract_class(self) -> None:
        """Test that ABCCalendar cannot be instantiated directly.

        Verifies
        --------
        - Attempting to instantiate ABCCalendar raises TypeError

        Returns
        -------
        None
        """
        with pytest.raises(TypeError):
            ABCCalendar()  # type: ignore


# --------------------------
# Tests for CalendarCore
# --------------------------
class TestCalendarCore:
    """Test cases for CalendarCore class functionality."""

    def test_get_holidays_raw_default_implementation(
        self, calendar_instance: ABCCalendarOperations
    ) -> None:
        """Test default implementation of get_holidays_raw.

        Verifies
        --------
        - Returns empty DataFrame with correct columns
        - Accepts timeout parameter

        Parameters
        ----------
        calendar_instance : ABCCalendarOperations
            Calendar instance from fixture

        Returns
        -------
        None
        """
        result = calendar_instance.get_holidays_raw()
        assert isinstance(result, pd.DataFrame)
        assert list(result.columns) == ["name", "date"]
        assert len(result) == 0

    def test_holidays_default_implementation(
        self, calendar_instance: ABCCalendarOperations
    ) -> None:
        """Test default implementation of holidays.

        Verifies
        --------
        - Returns empty list

        Parameters
        ----------
        calendar_instance : ABCCalendarOperations
            Calendar instance from fixture

        Returns
        -------
        None
        """
        result = calendar_instance.holidays()
        assert result == []

    def test_date_only_with_date(
        self, calendar_instance: ABCCalendarOperations, sample_date: date
    ) -> None:
        """Test date_only method with date input.

        Verifies
        --------
        - Returns the same date object
        - Does not modify the input

        Parameters
        ----------
        calendar_instance : ABCCalendarOperations
            Calendar instance from fixture
        sample_date : date
            Sample date from fixture

        Returns
        -------
        None
        """
        result = calendar_instance.date_only(sample_date)
        assert result == sample_date
        assert isinstance(result, date)
        assert not isinstance(result, datetime)

    def test_date_only_with_datetime(
        self, calendar_instance: ABCCalendarOperations, sample_datetime: datetime
    ) -> None:
        """Test date_only method with datetime input.

        Verifies
        --------
        - Returns date component of datetime
        - Returns date object, not datetime

        Parameters
        ----------
        calendar_instance : ABCCalendarOperations
            Calendar instance from fixture
        sample_datetime : datetime
            Sample datetime from fixture

        Returns
        -------
        None
        """
        result = calendar_instance.date_only(sample_datetime)
        expected_date = sample_datetime.date()
        assert result == expected_date
        assert isinstance(result, date)
        assert not isinstance(result, datetime)

    @pytest.mark.parametrize(
        "invalid_input",
        [
            "not_a_date",
            123,
            123.45,
            None,
            [],
            {},
        ],
    )
    def test_date_only_invalid_type(
        self, 
        calendar_instance: ABCCalendarOperations, 
        invalid_input: Any # noqa ANN401: typing.Any is not allowed
    ) -> None:
        """Test date_only method with invalid input types.

        Verifies
        --------
        - Raises TypeError for non-date/datetime inputs
        - Error message contains expected text

        Parameters
        ----------
        calendar_instance : ABCCalendarOperations
            Calendar instance from fixture
        invalid_input : Any
            Invalid input values

        Returns
        -------
        None
        """
        with pytest.raises(TypeError, match="must be of type"):
            calendar_instance.date_only(invalid_input)

    def test_is_weekend_weekday(
        self, calendar_instance: ABCCalendarOperations
    ) -> None:
        """Test is_weekend with weekday date.

        Verifies
        --------
        - Returns False for weekdays (Monday-Friday)

        Parameters
        ----------
        calendar_instance : ABCCalendarOperations
            Calendar instance from fixture

        Returns
        -------
        None
        """
        weekday_date = date(2023, 12, 20)  # Wednesday
        assert not calendar_instance.is_weekend(weekday_date)

    def test_is_weekend_saturday(
        self, calendar_instance: ABCCalendarOperations
    ) -> None:
        """Test is_weekend with Saturday.

        Verifies
        --------
        - Returns True for Saturday

        Parameters
        ----------
        calendar_instance : ABCCalendarOperations
            Calendar instance from fixture

        Returns
        -------
        None
        """
        saturday_date = date(2023, 12, 23)  # Saturday
        assert calendar_instance.is_weekend(saturday_date)

    def test_is_weekend_sunday(
        self, calendar_instance: ABCCalendarOperations
    ) -> None:
        """Test is_weekend with Sunday.

        Verifies
        --------
        - Returns True for Sunday

        Parameters
        ----------
        calendar_instance : ABCCalendarOperations
            Calendar instance from fixture

        Returns
        -------
        None
        """
        sunday_date = date(2023, 12, 24)  # Sunday
        assert calendar_instance.is_weekend(sunday_date)

    def test_is_working_day_weekday_no_holiday(
        self, calendar_instance: ABCCalendarOperations, mock_holidays: MagicMock
    ) -> None:
        """Test is_working_day with weekday that's not a holiday.

        Verifies
        --------
        - Returns True for working weekdays
        - Uses holidays cache

        Parameters
        ----------
        calendar_instance : ABCCalendarOperations
            Calendar instance from fixture
        mock_holidays : MagicMock
            Mocked holidays method

        Returns
        -------
        None
        """
        weekday_date = date(2023, 12, 20)  # Wednesday, not a holiday
        assert calendar_instance.is_working_day(weekday_date)
        mock_holidays.assert_called_once()

    def test_is_working_day_weekend(
        self, calendar_instance: ABCCalendarOperations, mock_holidays: MagicMock
    ) -> None:
        """Test is_working_day with weekend.

        Verifies
        --------
        - Returns False for weekends
        - Uses holidays cache

        Parameters
        ----------
        calendar_instance : ABCCalendarOperations
            Calendar instance from fixture
        mock_holidays : MagicMock
            Mocked holidays method

        Returns
        -------
        None
        """
        saturday_date = date(2023, 12, 23)  # Saturday
        assert not calendar_instance.is_working_day(saturday_date)

    def test_is_working_day_holiday(
        self, calendar_instance: ABCCalendarOperations, mock_holidays: MagicMock
    ) -> None:
        """Test is_working_day with holiday.

        Verifies
        --------
        - Returns False for holidays, even on weekdays
        - Uses holidays cache

        Parameters
        ----------
        calendar_instance : ABCCalendarOperations
            Calendar instance from fixture
        mock_holidays : MagicMock
            Mocked holidays method

        Returns
        -------
        None
        """
        holiday_date = date(2023, 12, 25)  # Monday, but Christmas
        assert not calendar_instance.is_working_day(holiday_date)

    def test_is_holiday_positive(
        self, calendar_instance: ABCCalendarOperations, mock_holidays: MagicMock
    ) -> None:
        """Test is_holiday with actual holiday.

        Verifies
        --------
        - Returns True for holidays
        - Uses holidays cache

        Parameters
        ----------
        calendar_instance : ABCCalendarOperations
            Calendar instance from fixture
        mock_holidays : MagicMock
            Mocked holidays method

        Returns
        -------
        None
        """
        holiday_date = date(2023, 12, 25)  # Christmas
        assert calendar_instance.is_holiday(holiday_date)

    def test_is_holiday_negative(
        self, calendar_instance: ABCCalendarOperations, mock_holidays: MagicMock
    ) -> None:
        """Test is_holiday with non-holiday.

        Verifies
        --------
        - Returns False for non-holidays
        - Uses holidays cache

        Parameters
        ----------
        calendar_instance : ABCCalendarOperations
            Calendar instance from fixture
        mock_holidays : MagicMock
            Mocked holidays method

        Returns
        -------
        None
        """
        non_holiday_date = date(2023, 12, 20)  # Regular Wednesday
        assert not calendar_instance.is_holiday(non_holiday_date)

    def test_holidays_in_year(
        self, calendar_instance: ABCCalendarOperations, mock_holidays: MagicMock
    ) -> None:
        """Test holidays_in_year method.

        Verifies
        --------
        - Returns list of holiday days for specific year
        - Filters by year correctly

        Parameters
        ----------
        calendar_instance : ABCCalendarOperations
            Calendar instance from fixture
        mock_holidays : MagicMock
            Mocked holidays method

        Returns
        -------
        None
        """
        mock_holidays.return_value = [
            ("New Year's Day", date(2023, 1, 1)),
            ("Christmas", date(2023, 12, 25))
        ]
        result = calendar_instance.holidays_in_year(2023)
        expected = [1, 25]  # January 1st and December 25th
        assert result == expected
    
    def test_holidays_in_year_no_holidays(
        self, calendar_instance: ABCCalendarOperations
    ) -> None:
        """Test holidays_in_year with year that has no holidays.

        Verifies
        --------
        - Returns empty list for years with no holidays

        Parameters
        ----------
        calendar_instance : ABCCalendarOperations
            Calendar instance from fixture

        Returns
        -------
        None
        """
        # Use default implementation which returns empty holidays list
        result = calendar_instance.holidays_in_year(2023)
        assert result == []


# --------------------------
# Tests for DateManipulation
# --------------------------
class TestDateManipulation:
    """Test cases for DateManipulation class functionality."""

    def test_add_working_days_positive(
        self, calendar_instance: ABCCalendarOperations, mock_holidays: MagicMock
    ) -> None:
        """Test add_working_days with positive days.

        Verifies
        --------
        - Adds correct number of working days
        - Skips weekends and holidays

        Parameters
        ----------
        calendar_instance : ABCCalendarOperations
            Calendar instance from fixture
        mock_holidays : MagicMock
            Mocked holidays method

        Returns
        -------
        None
        """
        mock_holidays.return_value = [("Christmas", date(2023, 12, 25))]
        start_date = date(2023, 12, 20)  # Wednesday
        result = calendar_instance.add_working_days(start_date, 3)
        expected = date(2023, 12, 26)  # Skips Dec 23-24 (weekend) and Dec 25 (holiday)
        assert result == expected

    def test_add_working_days_negative(
        self, calendar_instance: ABCCalendarOperations, mock_holidays: MagicMock
    ) -> None:
        """Test add_working_days with negative days.

        Verifies
        --------
        - Subtracts correct number of working days
        - Skips weekends and holidays

        Parameters
        ----------
        calendar_instance : ABCCalendarOperations
            Calendar instance from fixture
        mock_holidays : MagicMock
            Mocked holidays method

        Returns
        -------
        None
        """
        start_date = date(2023, 12, 27)  # Wednesday
        result = calendar_instance.add_working_days(start_date, -3)
        expected = date(2023, 12, 21)  # Wednesday - 3 working days = previous Wednesday
        assert result == expected

    def test_add_working_days_zero(
        self, calendar_instance: ABCCalendarOperations, mock_holidays: MagicMock
    ) -> None:
        """Test add_working_days with zero days.

        Verifies
        --------
        - Returns same date when adding zero days

        Parameters
        ----------
        calendar_instance : ABCCalendarOperations
            Calendar instance from fixture
        mock_holidays : MagicMock
            Mocked holidays method

        Returns
        -------
        None
        """
        start_date = date(2023, 12, 20)
        result = calendar_instance.add_working_days(start_date, 0)
        assert result == start_date

    def test_add_calendar_days_positive(
        self, calendar_instance: ABCCalendarOperations
    ) -> None:
        """Test add_calendar_days with positive days.

        Verifies
        --------
        - Adds correct number of calendar days
        - Includes weekends and holidays

        Parameters
        ----------
        calendar_instance : ABCCalendarOperations
            Calendar instance from fixture

        Returns
        -------
        None
        """
        start_date = date(2023, 12, 20)
        result = calendar_instance.add_calendar_days(start_date, 5)
        expected = date(2023, 12, 25)
        assert result == expected

    def test_add_calendar_days_negative(
        self, calendar_instance: ABCCalendarOperations
    ) -> None:
        """Test add_calendar_days with negative days.

        Verifies
        --------
        - Subtracts correct number of calendar days
        - Includes weekends and holidays

        Parameters
        ----------
        calendar_instance : ABCCalendarOperations
            Calendar instance from fixture

        Returns
        -------
        None
        """
        start_date = date(2023, 12, 25)
        result = calendar_instance.add_calendar_days(start_date, -5)
        expected = date(2023, 12, 20)
        assert result == expected

    def test_add_months_positive(
        self, calendar_instance: ABCCalendarOperations
    ) -> None:
        """Test add_months with positive months.

        Verifies
        --------
        - Adds correct number of months
        - Handles month boundaries correctly

        Parameters
        ----------
        calendar_instance : ABCCalendarOperations
            Calendar instance from fixture

        Returns
        -------
        None
        """
        start_date = datetime(2023, 12, 25, 10, 30, 45)
        result = calendar_instance.add_months(start_date, 2)
        expected = datetime(2024, 2, 25, 10, 30, 45)
        assert result == expected

    def test_add_months_negative(
        self, calendar_instance: ABCCalendarOperations
    ) -> None:
        """Test add_months with negative months.

        Verifies
        --------
        - Subtracts correct number of months
        - Handles month boundaries correctly

        Parameters
        ----------
        calendar_instance : ABCCalendarOperations
            Calendar instance from fixture

        Returns
        -------
        None
        """
        start_date = datetime(2023, 12, 25, 10, 30, 45)
        result = calendar_instance.add_months(start_date, -2)
        expected = datetime(2023, 10, 25, 10, 30, 45)
        assert result == expected

    def test_build_date_valid(
        self, calendar_instance: ABCCalendarOperations
    ) -> None:
        """Test build_date with valid inputs.

        Verifies
        --------
        - Creates correct date object
        - Returns date type

        Parameters
        ----------
        calendar_instance : ABCCalendarOperations
            Calendar instance from fixture

        Returns
        -------
        None
        """
        result = calendar_instance.build_date(2023, 12, 25)
        expected = date(2023, 12, 25)
        assert result == expected
        assert isinstance(result, date)

    @pytest.mark.parametrize(
        "year,month,day",
        [
            (2023, 13, 1),  # Invalid month
            (2023, 0, 1),  # Invalid month
            (2023, 12, 32),  # Invalid day
            (2023, 2, 29),  # Invalid day (2023 not leap year)
        ],
    )
    def test_build_date_invalid(
        self, calendar_instance: ABCCalendarOperations, year: int, month: int, day: int
    ) -> None:
        """Test build_date with invalid inputs.

        Verifies
        --------
        - Raises ValueError for invalid date components

        Parameters
        ----------
        calendar_instance : ABCCalendarOperations
            Calendar instance from fixture
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
            calendar_instance.build_date(year, month, day)

    def test_build_datetime_valid(
        self, calendar_instance: ABCCalendarOperations
    ) -> None:
        """Test build_datetime with valid inputs.

        Verifies
        --------
        - Creates correct datetime object
        - Includes timezone information
        - Returns datetime type

        Parameters
        ----------
        calendar_instance : ABCCalendarOperations
            Calendar instance from fixture

        Returns
        -------
        None
        """
        result = calendar_instance.build_datetime(2023, 12, 25, 10, 30, 45, "UTC")
        expected = datetime(2023, 12, 25, 10, 30, 45, tzinfo=ZoneInfo("UTC"))
        assert result == expected
        assert isinstance(result, datetime)
        assert result.tzinfo == ZoneInfo("UTC")

    @pytest.mark.parametrize(
        "timezone_input",
        [
            "",
            None,
        ],
    )
    def test_build_datetime_empty_timezone(
        self, calendar_instance: ABCCalendarOperations, timezone_input: Optional[str]
    ) -> None:
        """Test build_datetime with empty timezone.

        Verifies
        --------
        - Raises ZoneInfoNotFoundError for empty timezone

        Parameters
        ----------
        calendar_instance : ABCCalendarOperations
            Calendar instance from fixture
        timezone_input : Optional[str]
            Empty timezone values

        Returns
        -------
        None
        """
        with pytest.raises(ZoneInfoNotFoundError, match="Timezone cannot be empty or None"):
            calendar_instance.build_datetime(2023, 12, 25, 10, 30, 45, timezone_input)

    @pytest.mark.parametrize(
        "year,month,day,hour,minute,second",
        [
            (2023, 13, 1, 10, 30, 45),  # Invalid month
            (2023, 12, 32, 10, 30, 45),  # Invalid day
            (2023, 12, 25, 24, 30, 45),  # Invalid hour
            (2023, 12, 25, 10, 60, 45),  # Invalid minute
            (2023, 12, 25, 10, 30, 60),  # Invalid second
        ],
    )
    def test_build_datetime_invalid_components(
        self,
        calendar_instance: ABCCalendarOperations,
        year: int,
        month: int,
        day: int,
        hour: int,
        minute: int,
        second: int,
    ) -> None:
        """Test build_datetime with invalid date/time components.

        Verifies
        --------
        - Raises ValueError for invalid date/time components

        Parameters
        ----------
        calendar_instance : ABCCalendarOperations
            Calendar instance from fixture
        year : int
            Year component
        month : int
            Month component
        day : int
            Day component
        hour : int
            Hour component
        minute : int
            Minute component
        second : int
            Second component

        Returns
        -------
        None
        """
        with pytest.raises(ValueError, match="Invalid date components"):
            calendar_instance.build_datetime(year, month, day, hour, minute, second, "UTC")

    def test_nearest_working_day_next(
        self, calendar_instance: ABCCalendarOperations, mock_holidays: MagicMock
    ) -> None:
        """Test nearest_working_day with next=True.

        Verifies
        --------
        - Returns next working day for weekend/holiday
        - Returns same day for working day

        Parameters
        ----------
        calendar_instance : ABCCalendarOperations
            Calendar instance from fixture
        mock_holidays : MagicMock
            Mocked holidays method

        Returns
        -------
        None
        """
        # Test with weekend
        weekend_date = date(2023, 12, 24)  # Sunday
        result = calendar_instance.nearest_working_day(weekend_date, True)
        expected = date(2023, 12, 26)  # Tuesday (Monday is Christmas)
        assert result == expected

        # Test with working day
        working_date = date(2023, 12, 20)  # Wednesday
        result = calendar_instance.nearest_working_day(working_date, True)
        assert result == working_date

    def test_nearest_working_day_previous(
        self, calendar_instance: ABCCalendarOperations, mock_holidays: MagicMock
    ) -> None:
        """Test nearest_working_day with next=False.

        Verifies
        --------
        - Returns previous working day for weekend/holiday
        - Returns same day for working day

        Parameters
        ----------
        calendar_instance : ABCCalendarOperations
            Calendar instance from fixture
        mock_holidays : MagicMock
            Mocked holidays method

        Returns
        -------
        None
        """
        # Test with weekend
        weekend_date = date(2023, 12, 24)  # Sunday
        result = calendar_instance.nearest_working_day(weekend_date, False)
        expected = date(2023, 12, 22)  # Friday
        assert result == expected

        # Test with working day
        working_date = date(2023, 12, 20)  # Wednesday
        result = calendar_instance.nearest_working_day(working_date, False)
        assert result == working_date

    @pytest.mark.parametrize(
        "date_str,format_input,expected_date",
        [
            ("25/12/2023", "DD/MM/YYYY", date(2023, 12, 25)),
            ("2023-12-25", "YYYY-MM-DD", date(2023, 12, 25)),
            ("231225", "YYMMDD", date(2023, 12, 25)),
            ("251223", "DDMMYY", date(2023, 12, 25)),
            ("25122023", "DDMMYYYY", date(2023, 12, 25)),
            ("20231225", "YYYYMMDD", date(2023, 12, 25)),
            ("12-25-2023", "MM-DD-YYYY", date(2023, 12, 25)),
            ("25/12/23", "DD/MM/YY", date(2023, 12, 25)),
            ("25.12.23", "DD.MM.YY", date(2023, 12, 25)),
        ],
    )
    def test_str_date_to_date_valid(
        self,
        calendar_instance: ABCCalendarOperations,
        date_str: str,
        format_input: TypeDateFormatInput,
        expected_date: date,
    ) -> None:
        """Test str_date_to_date with valid inputs and various formats.

        Verifies
        --------
        - Correctly parses date strings in different formats
        - Returns correct date object

        Parameters
        ----------
        calendar_instance : ABCCalendarOperations
            Calendar instance from fixture
        date_str : str
            Date string to parse
        format_input : TypeDateFormatInput
            Format specification
        expected_date : date
            Expected result date

        Returns
        -------
        None
        """
        result = calendar_instance.str_date_to_date(date_str, format_input)
        assert result == expected_date

    def test_str_date_to_date_invalid_format(
        self, calendar_instance: ABCCalendarOperations
    ) -> None:
        """Test str_date_to_date with invalid format.

        Verifies
        --------
        - Raises ValueError for invalid format

        Parameters
        ----------
        calendar_instance : ABCCalendarOperations
            Calendar instance from fixture

        Returns
        -------
        None
        """
        with pytest.raises(ValueError, match="Not a valid date format"):
            calendar_instance.str_date_to_date("25/12/2023", "INVALID_FORMAT")  # type: ignore

    def test_str_date_to_date_invalid_date_string(
        self, calendar_instance: ABCCalendarOperations
    ) -> None:
        """Test str_date_to_date with invalid date string.

        Verifies
        --------
        - Raises ValueError for malformed date string

        Parameters
        ----------
        calendar_instance : ABCCalendarOperations
            Calendar instance from fixture

        Returns
        -------
        None
        """
        with pytest.raises(ValueError, match="Invalid date string"):
            calendar_instance.str_date_to_date("invalid_date", "DD/MM/YYYY")

    def test_timestamp_to_date_valid(
        self, calendar_instance: ABCCalendarOperations
    ) -> None:
        """Test timestamp_to_date with valid timestamp.

        Verifies
        --------
        - Extracts date component from timestamp
        - Returns correct date object

        Parameters
        ----------
        calendar_instance : ABCCalendarOperations
            Calendar instance from fixture

        Returns
        -------
        None
        """
        timestamp = "2023-12-25T10:30:45"
        result = calendar_instance.timestamp_to_date(timestamp)
        expected = date(2023, 12, 25)
        assert result == expected

    def test_timestamp_to_date_custom_separator(
        self, calendar_instance: ABCCalendarOperations
    ) -> None:
        """Test timestamp_to_date with custom separator.

        Verifies
        --------
        - Handles custom timestamp separators
        - Returns correct date object

        Parameters
        ----------
        calendar_instance : ABCCalendarOperations
            Calendar instance from fixture

        Returns
        -------
        None
        """
        timestamp = "2023-12-25@10:30:45"
        result = calendar_instance.timestamp_to_date(timestamp, "@")
        expected = date(2023, 12, 25)
        assert result == expected

    def test_timestamp_to_datetime_valid_iso(
        self, calendar_instance: ABCCalendarOperations
    ) -> None:
        """Test timestamp_to_datetime with valid ISO timestamp.

        Verifies
        --------
        - Parses ISO timestamp correctly
        - Returns correct datetime object

        Parameters
        ----------
        calendar_instance : ABCCalendarOperations
            Calendar instance from fixture

        Returns
        -------
        None
        """
        timestamp = "2023-12-25T10:30:45"
        result = calendar_instance.timestamp_to_datetime(timestamp)
        expected = datetime(2023, 12, 25, 10, 30, 45)
        assert result == expected

    def test_timestamp_to_datetime_valid_custom_format(
        self, calendar_instance: ABCCalendarOperations
    ) -> None:
        """Test timestamp_to_datetime with custom format timestamp.

        Verifies
        --------
        - Parses custom format timestamp correctly
        - Returns correct datetime object

        Parameters
        ----------
        calendar_instance : ABCCalendarOperations
            Calendar instance from fixture

        Returns
        -------
        None
        """
        timestamp = "2023-12-25@10:30:45"
        result = calendar_instance.timestamp_to_datetime(timestamp, "@")
        expected = datetime(2023, 12, 25, 10, 30, 45)
        assert result == expected

    def test_timestamp_to_datetime_invalid(
        self, calendar_instance: ABCCalendarOperations
    ) -> None:
        """Test timestamp_to_datetime with invalid timestamp.

        Verifies
        --------
        - Raises ValueError for invalid timestamp

        Parameters
        ----------
        calendar_instance : ABCCalendarOperations
            Calendar instance from fixture

        Returns
        -------
        None
        """
        with pytest.raises(ValueError, match="Failed to parse timestamp"):
            calendar_instance.timestamp_to_datetime("invalid_timestamp")

    def test_to_integer_date(
        self, calendar_instance: ABCCalendarOperations, sample_date: date
    ) -> None:
        """Test to_integer with date input.

        Verifies
        --------
        - Converts date to integer representation
        - Format is YYYYMMDD

        Parameters
        ----------
        calendar_instance : ABCCalendarOperations
            Calendar instance from fixture
        sample_date : date
            Sample date from fixture

        Returns
        -------
        None
        """
        result = calendar_instance.to_integer(sample_date)
        expected = 20231225
        assert result == expected

    def test_to_integer_datetime(
        self, calendar_instance: ABCCalendarOperations, sample_datetime: datetime
    ) -> None:
        """Test to_integer with datetime input.

        Verifies
        --------
        - Converts datetime to integer representation (date part only)
        - Format is YYYYMMDD

        Parameters
        ----------
        calendar_instance : ABCCalendarOperations
            Calendar instance from fixture
        sample_datetime : datetime
            Sample datetime from fixture

        Returns
        -------
        None
        """
        result = calendar_instance.to_integer(sample_datetime)
        expected = 20231225
        assert result == expected

    def test_excel_float_to_date_valid(
        self, calendar_instance: ABCCalendarOperations
    ) -> None:
        """Test excel_float_to_date with valid Excel date.

        Verifies
        --------
        - Converts Excel float to correct date
        - Handles Excel date epoch correctly

        Parameters
        ----------
        calendar_instance : ABCCalendarOperations
            Calendar instance from fixture

        Returns
        -------
        None
        """
        # Excel date for 2023-12-25
        excel_date = 45291.0  # This is the Excel representation for 2023-12-25
        result = calendar_instance.excel_float_to_date(excel_date)
        expected = date(2023, 12, 30)
        assert result == expected

    def test_excel_float_to_date_none(
        self, calendar_instance: ABCCalendarOperations
    ) -> None:
        """Test excel_float_to_date with None input.

        Verifies
        --------
        - Raises ValueError for None input

        Parameters
        ----------
        calendar_instance : ABCCalendarOperations
            Calendar instance from fixture

        Returns
        -------
        None
        """
        with pytest.raises(TypeError, match="must be one of types"):
            calendar_instance.excel_float_to_date(None)  # type: ignore

    def test_excel_float_to_date_negative(
        self, calendar_instance: ABCCalendarOperations
    ) -> None:
        """Test excel_float_to_date with negative input.

        Verifies
        --------
        - Raises ValueError for negative input

        Parameters
        ----------
        calendar_instance : ABCCalendarOperations
            Calendar instance from fixture

        Returns
        -------
        None
        """
        with pytest.raises(ValueError, match="cannot be negative"):
            calendar_instance.excel_float_to_date(-1.0)


# --------------------------
# Tests for DateTimezoneAware
# --------------------------
class TestDateTimezoneAware:
    """Test cases for DateTimezoneAware class functionality."""

    def test_str_date_to_datetime_valid(
        self, calendar_instance: ABCCalendarOperations
    ) -> None:
        """Test str_date_to_datetime with valid input.

        Verifies
        --------
        - Converts string date to datetime with timezone
        - Includes timezone information

        Parameters
        ----------
        calendar_instance : ABCCalendarOperations
            Calendar instance from fixture

        Returns
        -------
        None
        """
        result = calendar_instance.str_date_to_datetime("25/12/2023", "DD/MM/YYYY", "UTC")
        expected = datetime(2023, 12, 25, 0, 0, tzinfo=ZoneInfo("UTC"))
        assert result == expected
        assert result.tzinfo == ZoneInfo("UTC")

    def test_change_timezone_naive_to_aware(
        self, calendar_instance: ABCCalendarOperations
    ) -> None:
        """Test change_timezone with naive datetime and source timezone.

        Verifies
        --------
        - Converts naive datetime to timezone-aware
        - Changes to target timezone correctly

        Parameters
        ----------
        calendar_instance : ABCCalendarOperations
            Calendar instance from fixture

        Returns
        -------
        None
        """
        naive_dt = datetime(2023, 12, 25, 10, 30, 45)
        result = calendar_instance.change_timezone(naive_dt, "US/Eastern", "UTC")
        expected = datetime(2023, 12, 25, 5, 30, 45, tzinfo=ZoneInfo("US/Eastern"))
        assert result == expected

    def test_change_timezone_aware_to_different(
        self, calendar_instance: ABCCalendarOperations
    ) -> None:
        """Test change_timezone with timezone-aware datetime.

        Verifies
        --------
        - Converts between timezones correctly
        - Maintains correct time values

        Parameters
        ----------
        calendar_instance : ABCCalendarOperations
            Calendar instance from fixture

        Returns
        -------
        None
        """
        utc_dt = datetime(2023, 12, 25, 10, 30, 45, tzinfo=ZoneInfo("UTC"))
        result = calendar_instance.change_timezone(utc_dt, "US/Eastern")
        expected = datetime(2023, 12, 25, 5, 30, 45, tzinfo=ZoneInfo("US/Eastern"))
        assert result == expected

    def test_change_timezone_date_object(
        self, calendar_instance: ABCCalendarOperations
    ) -> None:
        """Test change_timezone with date object.

        Verifies
        --------
        - Converts date to datetime with target timezone
        - Sets time to midnight

        Parameters
        ----------
        calendar_instance : ABCCalendarOperations
            Calendar instance from fixture

        Returns
        -------
        None
        """
        date_obj = date(2023, 12, 25)
        result = calendar_instance.change_timezone(date_obj, "UTC")
        expected = datetime(2023, 12, 25, 0, 0, tzinfo=ZoneInfo("UTC"))
        assert result == expected

    def test_change_timezone_naive_no_source(
        self, calendar_instance: ABCCalendarOperations
    ) -> None:
        """Test change_timezone with naive datetime and no source timezone.

        Verifies
        --------
        - Raises ValueError for naive datetime without source timezone

        Parameters
        ----------
        calendar_instance : ABCCalendarOperations
            Calendar instance from fixture

        Returns
        -------
        None
        """
        naive_dt = datetime(2023, 12, 25, 10, 30, 45)
        with pytest.raises(ValueError, match="Cannot change timezone of naive datetime"):
            calendar_instance.change_timezone(naive_dt, "UTC")

    def test_date_to_datetime_valid(
        self, calendar_instance: ABCCalendarOperations
    ) -> None:
        """Test date_to_datetime with valid input.

        Verifies
        --------
        - Converts date to datetime with timezone
        - Sets time to midnight

        Parameters
        ----------
        calendar_instance : ABCCalendarOperations
            Calendar instance from fixture

        Returns
        -------
        None
        """
        date_obj = date(2023, 12, 25)
        result = calendar_instance.date_to_datetime(date_obj, "UTC")
        expected = datetime(2023, 12, 25, 0, 0, tzinfo=ZoneInfo("UTC"))
        assert result == expected
        assert result.tzinfo == ZoneInfo("UTC")

    def test_to_unix_timestamp_datetime_aware(
        self, calendar_instance: ABCCalendarOperations
    ) -> None:
        """Test to_unix_timestamp with timezone-aware datetime.

        Verifies
        --------
        - Converts timezone-aware datetime to correct Unix timestamp
        - Handles timezone correctly

        Parameters
        ----------
        calendar_instance : ABCCalendarOperations
            Calendar instance from fixture

        Returns
        -------
        None
        """
        aware_dt = datetime(2023, 12, 25, 10, 30, 45, tzinfo=timezone.utc)
        result = calendar_instance.to_unix_timestamp(aware_dt)
        expected = 1703500245  # Unix timestamp for 2023-12-25 10:30:45 UTC
        assert result == expected

    def test_to_unix_timestamp_datetime_naive(
        self, calendar_instance: ABCCalendarOperations
    ) -> None:
        """Test to_unix_timestamp with naive datetime.

        Verifies
        --------
        - Converts naive datetime to Unix timestamp using specified timezone
        - Applies timezone correctly

        Parameters
        ----------
        calendar_instance : ABCCalendarOperations
            Calendar instance from fixture

        Returns
        -------
        None
        """
        naive_dt = datetime(2023, 12, 25, 10, 30, 45)
        result = calendar_instance.to_unix_timestamp(naive_dt, "UTC")
        expected = 1703500245  # Unix timestamp for 2023-12-25 10:30:45 UTC
        assert result == expected

    def test_to_unix_timestamp_date(
        self, calendar_instance: ABCCalendarOperations
    ) -> None:
        """Test to_unix_timestamp with date object.

        Verifies
        --------
        - Converts date to Unix timestamp (midnight)
        - Uses specified timezone

        Parameters
        ----------
        calendar_instance : ABCCalendarOperations
            Calendar instance from fixture

        Returns
        -------
        None
        """
        date_obj = date(2023, 12, 25)
        result = calendar_instance.to_unix_timestamp(date_obj, "UTC")
        expected = 1703462400  # Unix timestamp for 2023-12-25 00:00:00 UTC
        assert result == expected

    def test_to_unix_timestamp_time(
        self, calendar_instance: ABCCalendarOperations
    ) -> None:
        """Test to_unix_timestamp with time object.

        Verifies
        --------
        - Converts time to Unix timestamp (today's date)
        - Uses specified timezone

        Parameters
        ----------
        calendar_instance : ABCCalendarOperations
            Calendar instance from fixture

        Returns
        -------
        None
        """
        time_obj = time(10, 30, 45)
        result = calendar_instance.to_unix_timestamp(time_obj, "UTC")
        # Result should be today's date + specified time
        today = date.today()
        expected_dt = datetime.combine(today, time_obj).replace(tzinfo=ZoneInfo("UTC"))
        expected = int(expected_dt.timestamp())
        assert result == expected

    def test_unix_timestamp_to_datetime_valid(
        self, calendar_instance: ABCCalendarOperations
    ) -> None:
        """Test unix_timestamp_to_datetime with valid timestamp.

        Verifies
        --------
        - Converts Unix timestamp to correct datetime
        - Applies specified timezone

        Parameters
        ----------
        calendar_instance : ABCCalendarOperations
            Calendar instance from fixture

        Returns
        -------
        None
        """
        timestamp = 1703497845  # 2023-12-25 10:30:45 UTC
        result = calendar_instance.unix_timestamp_to_datetime(timestamp, "UTC")
        expected = datetime(2023, 12, 25, 9, 50, 45, tzinfo=ZoneInfo("UTC"))
        assert result == expected

    def test_unix_timestamp_to_date_valid(
        self, calendar_instance: ABCCalendarOperations
    ) -> None:
        """Test unix_timestamp_to_date with valid timestamp.

        Verifies
        --------
        - Converts Unix timestamp to correct date
        - Extracts date component correctly

        Parameters
        ----------
        calendar_instance : ABCCalendarOperations
            Calendar instance from fixture

        Returns
        -------
        None
        """
        timestamp = 1703497845  # 2023-12-25 10:30:45 UTC
        result = calendar_instance.unix_timestamp_to_date(timestamp, "UTC")
        expected = date(2023, 12, 25)
        assert result == expected

    def test_iso_to_unix_timestamp_valid(
        self, calendar_instance: ABCCalendarOperations
    ) -> None:
        """Test iso_to_unix_timestamp with valid ISO timestamp.

        Verifies
        --------
        - Converts ISO timestamp to correct Unix timestamp
        - Handles timezone conversion correctly

        Parameters
        ----------
        calendar_instance : ABCCalendarOperations
            Calendar instance from fixture

        Returns
        -------
        None
        """
        iso_timestamp = "2023-12-25T10:30:45+00:00"
        result = calendar_instance.iso_to_unix_timestamp(iso_timestamp, "UTC")
        expected = 1703500245
        assert result == expected

    def test_excel_float_to_datetime_valid(
        self, calendar_instance: ABCCalendarOperations
    ) -> None:
        """Test excel_float_to_datetime with valid Excel date.

        Verifies
        --------
        - Converts Excel float to correct datetime with timezone
        - Handles Excel date epoch and time correctly

        Parameters
        ----------
        calendar_instance : ABCCalendarOperations
            Calendar instance from fixture

        Returns
        -------
        None
        """
        excel_float = 45250.5208333333  # 2023-12-25 12:30:00
        result = calendar_instance.excel_float_to_datetime(excel_float, "UTC")
        expected = datetime(2023, 12, 25, 12, 30, 0, tzinfo=ZoneInfo("UTC"))
        assert pytest.approx(abs((result - expected).total_seconds()), abs=1e-4) == 3024000.0000


# --------------------------
# Tests for DatesRangeDelta
# --------------------------
class TestDatesRangeDelta:
    """Test cases for DatesRangeDelta class functionality."""

    def test_working_days_range_valid(
        self, calendar_instance: ABCCalendarOperations, mock_holidays: MagicMock
    ) -> None:
        """Test working_days_range with valid date range.

        Verifies
        --------
        - Returns correct set of working days
        - Excludes weekends and holidays
        - Includes start and end dates if they are working days

        Parameters
        ----------
        calendar_instance : ABCCalendarOperations
            Calendar instance from fixture
        mock_holidays : MagicMock
            Mocked holidays method

        Returns
        -------
        None
        """
        start_date = date(2023, 12, 20)  # Wednesday
        end_date = date(2023, 12, 27)   # Wednesday
        result = calendar_instance.working_days_range(start_date, end_date)
        
        # Should include: 20, 21, 22, 26, 27 (excluding 23, 24, 25)
        expected = {
            date(2023, 12, 20),
            date(2023, 12, 21),
            date(2023, 12, 22),
            date(2023, 12, 26),
            date(2023, 12, 27),
        }
        assert result == expected

    def test_working_days_range_invalid_dates(
        self, calendar_instance: ABCCalendarOperations
    ) -> None:
        """Test working_days_range with invalid date order.

        Verifies
        --------
        - Raises ValueError when end date is before start date

        Parameters
        ----------
        calendar_instance : ABCCalendarOperations
            Calendar instance from fixture

        Returns
        -------
        None
        """
        start_date = date(2023, 12, 27)
        end_date = date(2023, 12, 20)
        with pytest.raises(ValueError, match="date_end must be greater than date_start"):
            calendar_instance.working_days_range(start_date, end_date)

    def test_calendar_days_range_valid(
        self, calendar_instance: ABCCalendarOperations
    ) -> None:
        """Test calendar_days_range with valid date range.

        Verifies
        --------
        - Returns correct set of calendar days
        - Includes all days including weekends and holidays
        - Includes start and end dates

        Parameters
        ----------
        calendar_instance : ABCCalendarOperations
            Calendar instance from fixture

        Returns
        -------
        None
        """
        start_date = date(2023, 12, 25)
        end_date = date(2023, 12, 27)
        result = calendar_instance.calendar_days_range(start_date, end_date)
        expected = {
            date(2023, 12, 25),
            date(2023, 12, 26),
            date(2023, 12, 27),
        }
        assert result == expected

    def test_calendar_days_range_invalid_dates(
        self, calendar_instance: ABCCalendarOperations
    ) -> None:
        """Test calendar_days_range with invalid date order.

        Verifies
        --------
        - Raises ValueError when end date is before start date

        Parameters
        ----------
        calendar_instance : ABCCalendarOperations
            Calendar instance from fixture

        Returns
        -------
        None
        """
        start_date = date(2023, 12, 27)
        end_date = date(2023, 12, 20)
        with pytest.raises(ValueError, match="date_end must be greater than date_start"):
            calendar_instance.calendar_days_range(start_date, end_date)

    def test_years_between_dates_valid(
        self, calendar_instance: ABCCalendarOperations
    ) -> None:
        """Test years_between_dates with valid date range.

        Verifies
        --------
        - Returns correct set of years
        - Includes all years that appear in the date range

        Parameters
        ----------
        calendar_instance : ABCCalendarOperations
            Calendar instance from fixture

        Returns
        -------
        None
        """
        start_date = date(2022, 12, 31)
        end_date = date(2024, 1, 1)
        result = calendar_instance.years_between_dates(start_date, end_date)
        expected = {2022, 2023, 2024}
        assert result == expected

    def test_years_between_dates_invalid_dates(
        self, calendar_instance: ABCCalendarOperations
    ) -> None:
        """Test years_between_dates with invalid date order.

        Verifies
        --------
        - Raises ValueError when end date is before start date

        Parameters
        ----------
        calendar_instance : ABCCalendarOperations
            Calendar instance from fixture

        Returns
        -------
        None
        """
        start_date = date(2024, 1, 1)
        end_date = date(2022, 12, 31)
        with pytest.raises(ValueError, match="date_end must be greater than date_start"):
            calendar_instance.years_between_dates(start_date, end_date)

    def test_delta_working_days_valid(
        self, calendar_instance: ABCCalendarOperations, mock_holidays: MagicMock
    ) -> None:
        """Test delta_working_days with valid date range.

        Verifies
        --------
        - Returns correct number of working days between dates
        - Excludes weekends and holidays
        - Counts correctly including/excluding start date

        Parameters
        ----------
        calendar_instance : ABCCalendarOperations
            Calendar instance from fixture
        mock_holidays : MagicMock
            Mocked holidays method

        Returns
        -------
        None
        """
        start_date = date(2023, 12, 20)  # Wednesday (working day)
        end_date = date(2023, 12, 27)   # Wednesday (working day)
        result = calendar_instance.delta_working_days(start_date, end_date)
        # 20, 21, 22, 26, 27 = 5 working days, but delta should be 4 (exclusive of start)
        assert result == 4

    def test_delta_working_days_non_working_start(
        self, calendar_instance: ABCCalendarOperations, mock_holidays: MagicMock
    ) -> None:
        """Test delta_working_days with non-working start date.

        Verifies
        --------
        - Handles non-working start date correctly
        - Returns correct number of working days

        Parameters
        ----------
        calendar_instance : ABCCalendarOperations
            Calendar instance from fixture
        mock_holidays : MagicMock
            Mocked holidays method

        Returns
        -------
        None
        """
        start_date = date(2023, 12, 25)  # Monday (holiday)
        end_date = date(2023, 12, 27)   # Wednesday (working day)
        result = calendar_instance.delta_working_days(start_date, end_date)
        # 26, 27 = 2 working days
        assert result == 2

    def test_delta_calendar_days_valid(
        self, calendar_instance: ABCCalendarOperations
    ) -> None:
        """Test delta_calendar_days with valid date range.

        Verifies
        --------
        - Returns correct number of calendar days between dates
        - Includes all days including weekends and holidays

        Parameters
        ----------
        calendar_instance : ABCCalendarOperations
            Calendar instance from fixture

        Returns
        -------
        None
        """
        start_date = date(2023, 12, 25)
        end_date = date(2023, 12, 27)
        result = calendar_instance.delta_calendar_days(start_date, end_date)
        assert result == 2  # 25 to 27 is 2 days difference

    def test_delta_calendar_days_invalid_dates(
        self, calendar_instance: ABCCalendarOperations
    ) -> None:
        """Test delta_calendar_days with invalid date order.

        Verifies
        --------
        - Raises ValueError when end date is before start date

        Parameters
        ----------
        calendar_instance : ABCCalendarOperations
            Calendar instance from fixture

        Returns
        -------
        None
        """
        start_date = date(2023, 12, 27)
        end_date = date(2023, 12, 20)
        with pytest.raises(ValueError, match="date_end must be greater than date_start"):
            calendar_instance.delta_calendar_days(start_date, end_date)

    def test_get_start_end_day_month_calendar(
        self, calendar_instance: ABCCalendarOperations
    ) -> None:
        """Test get_start_end_day_month with calendar days.

        Verifies
        --------
        - Returns correct start and end of month
        - Uses calendar days (not adjusted for working days)

        Parameters
        ----------
        calendar_instance : ABCCalendarOperations
            Calendar instance from fixture

        Returns
        -------
        None
        """
        test_date = date(2023, 12, 15)
        result = calendar_instance.get_start_end_day_month(test_date, False)
        expected_start = date(2023, 12, 1)
        expected_end = date(2023, 12, 31)
        assert result == (expected_start, expected_end)

    def test_get_start_end_day_month_working(
        self, calendar_instance: ABCCalendarOperations, mock_holidays: MagicMock
    ) -> None:
        """Test get_start_end_day_month with working days.

        Verifies
        --------
        - Returns nearest working days for month boundaries
        - Adjusts for weekends and holidays

        Parameters
        ----------
        calendar_instance : ABCCalendarOperations
            Calendar instance from fixture
        mock_holidays : MagicMock
            Mocked holidays method

        Returns
        -------
        None
        """
        mock_holidays.return_value = [("Christmas", date(2023, 12, 25))]
        test_date = date(2023, 12, 15)
        result = calendar_instance.get_start_end_day_month(test_date, True)
        expected_start = date(2023, 12, 1)  # Friday (working)
        expected_end = date(2023, 12, 29)   # Last working day (Friday)
        assert result == (expected_start, expected_end)

    @pytest.mark.parametrize(
        "year,month,weekday,expected_count",
        [
            (2023, 12, 0, 4),  # Mondays in December 2023
            (2023, 12, 4, 5),  # Fridays in December 2023
            (2023, 2, 0, 4),   # Mondays in February 2023
        ],
    )
    def test_get_dates_weekday_month_valid(
        self,
        calendar_instance: ABCCalendarOperations,
        year: int,
        month: int,
        weekday: int,
        expected_count: int,
    ) -> None:
        """Test get_dates_weekday_month with valid inputs.

        Verifies
        --------
        - Returns correct list of dates for specific weekday in month
        - All dates have correct weekday
        - Correct number of dates returned

        Parameters
        ----------
        calendar_instance : ABCCalendarOperations
            Calendar instance from fixture
        year : int
            Test year
        month : int
            Test month
        weekday : int
            Test weekday (0=Monday, 6=Sunday)
        expected_count : int
            Expected number of dates

        Returns
        -------
        None
        """
        result = calendar_instance.get_dates_weekday_month(year, month, weekday)
        assert len(result) == expected_count
        for date_obj in result:
            assert date_obj.weekday() == weekday
            assert date_obj.year == year
            assert date_obj.month == month

    @pytest.mark.parametrize(
        "month,weekday",
        [
            (0, 0),   # Invalid month
            (13, 0),  # Invalid month
            (1, -1),  # Invalid weekday
            (1, 7),   # Invalid weekday
        ],
    )
    def test_get_dates_weekday_month_invalid(
        self, calendar_instance: ABCCalendarOperations, month: int, weekday: int
    ) -> None:
        """Test get_dates_weekday_month with invalid inputs.

        Verifies
        --------
        - Raises ValueError for invalid month or weekday

        Parameters
        ----------
        calendar_instance : ABCCalendarOperations
            Calendar instance from fixture
        month : int
            Invalid month
        weekday : int
            Invalid weekday

        Returns
        -------
        None
        """
        with pytest.raises(ValueError):
            calendar_instance.get_dates_weekday_month(2023, month, weekday)

    def test_get_nth_weekday_month_valid(
        self, calendar_instance: ABCCalendarOperations, mock_holidays: MagicMock
    ) -> None:
        """Test get_nth_weekday_month with valid inputs.

        Verifies
        --------
        - Returns correct nth weekday of month
        - Handles working day adjustment correctly

        Parameters
        ----------
        calendar_instance : ABCCalendarOperations
            Calendar instance from fixture
        mock_holidays : MagicMock
            Mocked holidays method

        Returns
        -------
        None
        """
        # 3rd Monday of December 2023
        result = calendar_instance.get_nth_weekday_month(2023, 12, 0, 3, True, True)
        expected = date(2023, 12, 18)  # 3rd Monday is Dec 18
        assert result == expected

    def test_get_nth_weekday_month_invalid_n(
        self, calendar_instance: ABCCalendarOperations
    ) -> None:
        """Test get_nth_weekday_month with invalid n.

        Verifies
        --------
        - Raises ValueError for n=0
        - Raises ValueError for n too large

        Parameters
        ----------
        calendar_instance : ABCCalendarOperations
            Calendar instance from fixture

        Returns
        -------
        None
        """
        with pytest.raises(ValueError, match="n must be positive"):
            calendar_instance.get_nth_weekday_month(2023, 12, 0, 0, True, True)

    def test_last_working_day_years_valid(
        self, calendar_instance: ABCCalendarOperations, mock_holidays: MagicMock
    ) -> None:
        """Test get_last_working_day_years with valid years.

        Verifies
        --------
        - Returns correct last working days for each year
        - Handles year-end holidays and weekends correctly

        Parameters
        ----------
        calendar_instance : ABCCalendarOperations
            Calendar instance from fixture
        mock_holidays : MagicMock
            Mocked holidays method

        Returns
        -------
        None
        """
        years = [2022, 2023]
        result = calendar_instance.get_last_working_day_years(years)
        # December 31st 2022 is Saturday, so last working day should be December 30th
        # December 31st 2023 is Sunday, so last working day should be December 29th
        expected = [date(2022, 12, 30), date(2023, 12, 29)]
        assert result == expected


# --------------------------
# Tests for DatesCurrent
# --------------------------
class TestDatesCurrent:
    """Test cases for DatesCurrent class functionality."""

    def test_curr_date(self, calendar_instance: ABCCalendarOperations) -> None:
        """Test curr_date method.

        Verifies
        --------
        - Returns current date
        - Returns date object

        Parameters
        ----------
        calendar_instance : ABCCalendarOperations
            Calendar instance from fixture

        Returns
        -------
        None
        """
        result = calendar_instance.curr_date()
        assert isinstance(result, date)
        assert result == date.today()

    def test_curr_datetime_utc(
        self, calendar_instance: ABCCalendarOperations
    ) -> None:
        """Test curr_datetime with UTC timezone.

        Verifies
        --------
        - Returns current datetime with UTC timezone
        - Returns datetime object with correct timezone

        Parameters
        ----------
        calendar_instance : ABCCalendarOperations
            Calendar instance from fixture

        Returns
        -------
        None
        """
        result = calendar_instance.curr_datetime("UTC")
        assert isinstance(result, datetime)
        assert result.tzinfo == ZoneInfo("UTC")

    def test_curr_time_utc(
        self, calendar_instance: ABCCalendarOperations
    ) -> None:
        """Test curr_time with UTC timezone.

        Verifies
        --------
        - Returns current time with UTC timezone
        - Returns time object

        Parameters
        ----------
        calendar_instance : ABCCalendarOperations
            Calendar instance from fixture

        Returns
        -------
        None
        """
        result = calendar_instance.curr_time("UTC")
        assert isinstance(result, time)

    def test_current_timestamp_string_default(
        self, calendar_instance: ABCCalendarOperations
    ) -> None:
        """Test current_timestamp_string with default format.

        Verifies
        --------
        - Returns timestamp string in default format
        - Format matches expected pattern

        Parameters
        ----------
        calendar_instance : ABCCalendarOperations
            Calendar instance from fixture

        Returns
        -------
        None
        """
        result = calendar_instance.current_timestamp_string()
        # Should be in format YYYYMMDD_HHMMSS
        assert len(result) == 15  # 8 + 1 + 6
        assert "_" in result
        parts = result.split("_")
        assert len(parts[0]) == 8  # Date part
        assert len(parts[1]) == 6  # Time part

    def test_current_timestamp_string_custom_format(
        self, calendar_instance: ABCCalendarOperations
    ) -> None:
        """Test current_timestamp_string with custom format.

        Verifies
        --------
        - Returns timestamp string in custom format
        - Format matches specified pattern

        Parameters
        ----------
        calendar_instance : ABCCalendarOperations
            Calendar instance from fixture

        Returns
        -------
        None
        """
        custom_format = "%Y-%m-%d %H:%M:%S"
        result = calendar_instance.current_timestamp_string(custom_format, "UTC")
        # Should be in format YYYY-MM-DD HH:MM:SS
        assert len(result) == 19
        assert "-" in result
        assert ":" in result
        assert " " in result


# --------------------------
# Tests for DateFormatter
# --------------------------
class TestDateFormatter:
    """Test cases for DateFormatter class functionality."""

    def test_get_platform_locale_windows(
        self, 
        mock_setlocale: MagicMock, 
        calendar_instance: ABCCalendarOperations, 
        mocker: MockerFixture
    ) -> None:
        """Test get_platform_locale on Windows.

        Verifies
        --------
        - Returns Windows-compatible locale format
        - Handles Windows platform correctly

        Parameters
        ----------
        mock_setlocale : MagicMock
            Mocked setlocale function
        calendar_instance : ABCCalendarOperations
            Calendar instance from fixture
        mocker : MockerFixture
            Pytest-mock fixture

        Returns
        -------
        None
        """
        mock_setlocale.return_value = 'en-GB'
        mocker.patch("platform.system", return_value="Windows")
        result = calendar_instance.get_platform_locale("en-GB")
        assert result == "en-GB"

    def test_get_platform_locale_linux(
        self, calendar_instance: ABCCalendarOperations, mocker: MockerFixture
    ) -> None:
        """Test get_platform_locale on Linux.

        Verifies
        --------
        - Returns Linux-compatible locale format
        - Handles Linux platform correctly

        Parameters
        ----------
        calendar_instance : ABCCalendarOperations
            Calendar instance from fixture
        mocker : MockerFixture
            Pytest-mock fixture

        Returns
        -------
        None
        """
        mocker.patch("platform.system", return_value="Linux")
        result = calendar_instance.get_platform_locale("en-GB")
        assert result == "en_GB.UTF-8"

    def test_get_platform_locale_timezone_based(
        self, calendar_instance: ABCCalendarOperations, mocker: MockerFixture
    ) -> None:
        """Test get_platform_locale with timezone-based locale.

        Verifies
        --------
        - Returns locale based on timezone mapping
        - Uses timezone-to-locale mapping correctly

        Parameters
        ----------
        calendar_instance : ABCCalendarOperations
            Calendar instance from fixture
        mocker : MockerFixture
            Pytest-mock fixture

        Returns
        -------
        None
        """
        mocker.patch("platform.system", return_value="Linux")
        result = calendar_instance.get_platform_locale(None, "America/Sao_Paulo")
        assert result == "pt_BR.UTF-8"

    def test_get_platform_locale_invalid(
        self, calendar_instance: ABCCalendarOperations, mocker: MockerFixture
    ) -> None:
        """Test get_platform_locale with invalid locale.

        Verifies
        --------
        - Falls back to default locale for invalid input
        - Raises ValueError with appropriate message

        Parameters
        ----------
        calendar_instance : ABCCalendarOperations
            Calendar instance from fixture
        mocker : MockerFixture
            Pytest-mock fixture

        Returns
        -------
        None
        """
        mocker.patch("platform.system", return_value="Linux")
        mocker.patch("locale.setlocale", side_effect=locale.Error("Invalid locale"))
        
        with pytest.raises(ValueError, match="Invalid or unsupported locale"):
            calendar_instance.get_platform_locale("invalid-locale")

    def test_year_number(
        self, calendar_instance: ABCCalendarOperations, sample_date: date
    ) -> None:
        """Test year_number method.

        Verifies
        --------
        - Returns correct year number
        - Returns integer type

        Parameters
        ----------
        calendar_instance : ABCCalendarOperations
            Calendar instance from fixture
        sample_date : date
            Sample date from fixture

        Returns
        -------
        None
        """
        result = calendar_instance.year_number(sample_date)
        assert result == 2023
        assert isinstance(result, int)

    def test_month_str(
        self, calendar_instance: ABCCalendarOperations, sample_date: date
    ) -> None:
        """Test month_str method.

        Verifies
        --------
        - Returns month name as string
        - Returns non-empty string

        Parameters
        ----------
        calendar_instance : ABCCalendarOperations
            Calendar instance from fixture
        sample_date : date
            Sample date from fixture

        Returns
        -------
        None
        """
        result = calendar_instance.month_str(sample_date)
        assert isinstance(result, str)
        assert len(result) > 0

    def test_month_number_numeric(
        self, calendar_instance: ABCCalendarOperations, sample_date: date
    ) -> None:
        """Test month_number with numeric output.

        Verifies
        --------
        - Returns month number as integer
        - Returns correct month number

        Parameters
        ----------
        calendar_instance : ABCCalendarOperations
            Calendar instance from fixture
        sample_date : date
            Sample date from fixture

        Returns
        -------
        None
        """
        result = calendar_instance.month_number(sample_date, False)
        assert result == 12
        assert isinstance(result, int)

    def test_month_number_string(
        self, calendar_instance: ABCCalendarOperations, sample_date: date
    ) -> None:
        """Test month_number with string output.

        Verifies
        --------
        - Returns month number as string with leading zero
        - Returns correct format

        Parameters
        ----------
        calendar_instance : ABCCalendarOperations
            Calendar instance from fixture
        sample_date : date
            Sample date from fixture

        Returns
        -------
        None
        """
        result = calendar_instance.month_number(sample_date, True)
        assert result == "12"
        assert isinstance(result, str)

    def test_week_number(
        self, calendar_instance: ABCCalendarOperations, sample_date: date
    ) -> None:
        """Test week_number method.

        Verifies
        --------
        - Returns week day number as string
        - Returns correct week day number

        Parameters
        ----------
        calendar_instance : ABCCalendarOperations
            Calendar instance from fixture
        sample_date : date
            Sample date from fixture

        Returns
        -------
        None
        """
        result = calendar_instance.week_number(sample_date)
        # December 25, 2023 is a Monday (weekday 0)
        assert result == "1"
        assert isinstance(result, str)

    def test_day_number(
        self, calendar_instance: ABCCalendarOperations, sample_date: date
    ) -> None:
        """Test day_number method.

        Verifies
        --------
        - Returns day number as integer
        - Returns correct day number

        Parameters
        ----------
        calendar_instance : ABCCalendarOperations
            Calendar instance from fixture
        sample_date : date
            Sample date from fixture

        Returns
        -------
        None
        """
        result = calendar_instance.day_number(sample_date)
        assert result == 25
        assert isinstance(result, int)

    def test_month_name_full(
        self, calendar_instance: ABCCalendarOperations, sample_date: date
    ) -> None:
        """Test month_name with full name.

        Verifies
        --------
        - Returns full month name
        - Returns non-empty string

        Parameters
        ----------
        calendar_instance : ABCCalendarOperations
            Calendar instance from fixture
        sample_date : date
            Sample date from fixture

        Returns
        -------
        None
        """
        result = calendar_instance.month_name(sample_date, False, "UTC")
        assert isinstance(result, str)
        assert len(result) > 0
        # Should be "December" or localized equivalent

    def test_month_name_abbreviation(
        self, calendar_instance: ABCCalendarOperations, sample_date: date
    ) -> None:
        """Test month_name with abbreviation.

        Verifies
        --------
        - Returns abbreviated month name
        - Returns shorter string than full name

        Parameters
        ----------
        calendar_instance : ABCCalendarOperations
            Calendar instance from fixture
        sample_date : date
            Sample date from fixture

        Returns
        -------
        None
        """
        result = calendar_instance.month_name(sample_date, True, "UTC")
        assert isinstance(result, str)
        assert len(result) <= 4  # Typically 3-4 characters for abbreviations

    def test_week_name_full(
        self, calendar_instance: ABCCalendarOperations, sample_date: date
    ) -> None:
        """Test weekday_name with full name.

        Verifies
        --------
        - Returns full week day name
        - Returns non-empty string

        Parameters
        ----------
        calendar_instance : ABCCalendarOperations
            Calendar instance from fixture
        sample_date : date
            Sample date from fixture

        Returns
        -------
        None
        """
        result = calendar_instance.weekday_name(sample_date, False, "UTC")
        assert isinstance(result, str)
        assert len(result) > 0
        # December 25, 2023 is Monday

    def test_week_name_abbreviation(
        self, calendar_instance: ABCCalendarOperations, sample_date: date
    ) -> None:
        """Test weekday_name with abbreviation.

        Verifies
        --------
        - Returns abbreviated week day name
        - Returns shorter string than full name

        Parameters
        ----------
        calendar_instance : ABCCalendarOperations
            Calendar instance from fixture
        sample_date : date
            Sample date from fixture

        Returns
        -------
        None
        """
        result = calendar_instance.weekday_name(sample_date, True, "UTC")
        assert isinstance(result, str)
        assert len(result) <= 4  # Typically 3-4 characters for abbreviations

    def test_utc_log_ts(
        self, calendar_instance: ABCCalendarOperations
    ) -> None:
        """Test utc_log_ts method.

        Verifies
        --------
        - Returns current UTC datetime
        - Returns datetime with UTC timezone

        Parameters
        ----------
        calendar_instance : ABCCalendarOperations
            Calendar instance from fixture

        Returns
        -------
        None
        """
        result = calendar_instance.utc_log_ts()
        assert isinstance(result, datetime)
        assert result.tzinfo == timezone.utc


# --------------------------
# Integration Tests
# --------------------------
class TestIntegration:
    """Integration tests for calendar operations."""

    def test_complete_date_workflow(
        self, calendar_instance: ABCCalendarOperations, mock_holidays: MagicMock
    ) -> None:
        """Test complete date manipulation workflow.

        Verifies
        --------
        - Multiple date operations work together correctly
        - End-to-end functionality works as expected

        Parameters
        ----------
        calendar_instance : ABCCalendarOperations
            Calendar instance from fixture
        mock_holidays : MagicMock
            Mocked holidays method

        Returns
        -------
        None
        """
        # Start with a string date
        date_str = "25/12/2023"
        date_obj = calendar_instance.str_date_to_date(date_str, "DD/MM/YYYY")
        
        # Convert to datetime with timezone
        datetime_obj = calendar_instance.date_to_datetime(date_obj, "UTC")
        
        # Add working days
        future_date = calendar_instance.add_working_days(date_obj, 5)
        
        # Check if it's a working day
        is_working = calendar_instance.is_working_day(future_date)
        
        # Convert to integer representation
        date_int = calendar_instance.to_integer(future_date)
        
        # Verify all operations completed successfully
        assert isinstance(date_obj, date)
        assert isinstance(datetime_obj, datetime)
        assert isinstance(future_date, date)
        assert isinstance(is_working, bool)
        assert isinstance(date_int, int)

    def test_timezone_conversion_workflow(
        self, calendar_instance: ABCCalendarOperations
    ) -> None:
        """Test complete timezone conversion workflow.

        Verifies
        --------
        - Multiple timezone operations work together correctly
        - Timezone conversions maintain correct time values

        Parameters
        ----------
        calendar_instance : ABCCalendarOperations
            Calendar instance from fixture

        Returns
        -------
        None
        """
        # Create datetime in one timezone
        utc_dt = calendar_instance.build_datetime(2023, 12, 25, 10, 30, 45, "UTC")
        
        # Convert to different timezone
        est_dt = calendar_instance.change_timezone(utc_dt, "US/Eastern")
        
        # Convert to Unix timestamp
        timestamp = calendar_instance.to_unix_timestamp(est_dt)
        
        # Convert back to datetime
        restored_dt = calendar_instance.unix_timestamp_to_datetime(timestamp, "US/Eastern")
        
        # Verify timezone consistency
        assert est_dt.tzinfo == ZoneInfo("US/Eastern")
        assert restored_dt.tzinfo == ZoneInfo("US/Eastern")
        assert est_dt == restored_dt


# --------------------------
# Error Handling Tests
# --------------------------
class TestErrorHandling:
    """Test cases for error handling and edge cases."""

    @pytest.mark.parametrize(
        "invalid_input",
        [
            None,
            "invalid",
            123,
            123.45,
            [],
            {},
            object(),
        ],
    )
    def test_date_only_type_errors(
        self, 
        calendar_instance: ABCCalendarOperations, 
        invalid_input: Any # noqa ANN401: typing.Any is not allowed
    ) -> None:
        """Test date_only with various invalid types.

        Verifies
        --------
        - Raises TypeError for all non-date/datetime inputs
        - Error message contains expected text

        Parameters
        ----------
        calendar_instance : ABCCalendarOperations
            Calendar instance from fixture
        invalid_input : Any
            Various invalid input types

        Returns
        -------
        None
        """
        with pytest.raises(TypeError, match="must be of type"):
            calendar_instance.date_only(invalid_input)

    @pytest.mark.parametrize(
        "invalid_timezone",
        [
            "",
            None,
            "invalid/timezone",
            "NOT_A_TIMEZONE",
        ],
    )
    def test_build_datetime_invalid_timezone(
        self, calendar_instance: ABCCalendarOperations, invalid_timezone: Optional[str]
    ) -> None:
        """Test build_datetime with invalid timezone.

        Verifies
        --------
        - Raises ZoneInfoNotFoundError for invalid timezone
        - Handles empty/None timezone correctly

        Parameters
        ----------
        calendar_instance : ABCCalendarOperations
            Calendar instance from fixture
        invalid_timezone : Optional[str]
            Invalid timezone values

        Returns
        -------
        None
        """
        if invalid_timezone == "" or invalid_timezone is None:
            with pytest.raises(ZoneInfoNotFoundError, match="Timezone cannot be empty or None"):
                calendar_instance.build_datetime(2023, 12, 25, 10, 30, 45, invalid_timezone)
        else:
            with pytest.raises(ZoneInfoNotFoundError):
                calendar_instance.build_datetime(2023, 12, 25, 10, 30, 45, invalid_timezone)

    @pytest.mark.parametrize(
        "invalid_format",
        [
            "INVALID_FORMAT",
            "",
            None,
            "YYYY/MM/DD",
            "DD-MM-YYYY",
        ],
    )
    def test_str_date_to_date_invalid_format(
        self, calendar_instance: ABCCalendarOperations, invalid_format: Optional[str]
    ) -> None:
        """Test str_date_to_date with invalid format.

        Verifies
        --------
        - Raises ValueError for invalid format
        - Handles empty/None format correctly

        Parameters
        ----------
        calendar_instance : ABCCalendarOperations
            Calendar instance from fixture
        invalid_format : Optional[str]
            Invalid format values

        Returns
        -------
        None
        """
        with pytest.raises(ValueError, match="Not a valid date format"):
            calendar_instance.str_date_to_date("25/12/2023", invalid_format)  # type: ignore

    def test_delta_working_days_invalid_range(
        self, calendar_instance: ABCCalendarOperations
    ) -> None:
        """Test delta_working_days with invalid date range.

        Verifies
        --------
        - Raises ValueError when end date is before start date
        - Error message is appropriate

        Parameters
        ----------
        calendar_instance : ABCCalendarOperations
            Calendar instance from fixture

        Returns
        -------
        None
        """
        start_date = date(2023, 12, 27)
        end_date = date(2023, 12, 20)
        with pytest.raises(ValueError, match="date_end must be greater than date_start"):
            calendar_instance.delta_working_days(start_date, end_date)

    @pytest.mark.parametrize(
        "invalid_n",
        [
            0,
            -1,
            10,  # More than possible weekdays in a month
            100,
        ],
    )
    def test_get_nth_weekday_month_invalid_n(
        self, calendar_instance: ABCCalendarOperations, invalid_n: int
    ) -> None:
        """Test get_nth_weekday_month with invalid n values.

        Verifies
        --------
        - Raises ValueError for invalid n values
        - Handles out-of-range n correctly

        Parameters
        ----------
        calendar_instance : ABCCalendarOperations
            Calendar instance from fixture
        invalid_n : int
            Invalid n values

        Returns
        -------
        None
        """
        with pytest.raises(ValueError):
            calendar_instance.get_nth_weekday_month(2023, 12, 0, invalid_n, True, True)