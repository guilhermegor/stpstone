"""Unit tests for USA holiday calendar data extraction.

Tests the functionality of Nasdaq and Federal holiday calendar classes including:
- Data fetching from external sources
- Date parsing and transformation
- Input validation and error handling
- Edge cases and type validation
"""

from datetime import date
from unittest.mock import MagicMock, Mock, patch

import pandas as pd
import pytest
import requests

from stpstone.utils.calendars.calendar_usa import (
    DatesUSAFederalHolidays,
    DatesUSANasdaq,
)
from stpstone.utils.parsers.dicts import HandlingDicts
from stpstone.utils.parsers.html import HtmlHandler


# --------------------------
# Fixtures
# --------------------------
@pytest.fixture
def nasdaq_calendar() -> DatesUSANasdaq:
    """Fixture providing DatesUSANasdaq instance.

    Returns
    -------
    DatesUSANasdaq
        Instance of Nasdaq calendar handler
    """
    return DatesUSANasdaq()


@pytest.fixture
def federal_calendar() -> DatesUSAFederalHolidays:
    """Fixture providing DatesUSAFederalHolidays instance.

    Returns
    -------
    DatesUSAFederalHolidays
        Instance of Federal calendar handler
    """
    return DatesUSAFederalHolidays()


@pytest.fixture
def sample_nasdaq_raw_data() -> list[dict[str, str]]:
    """Fixture providing sample raw Nasdaq holiday data.

    Returns
    -------
    list[dict[str, str]]
        list of dictionaries with DATE, DESCRIPTION, STATUS
    """
    return [
        {"DATE": "January 1, 2023", "DESCRIPTION": "New Year's Day", "STATUS": "Open"},
        {"DATE": "December 25, 2023", "DESCRIPTION": "Christmas Day", "STATUS": "Closed"},
    ]


@pytest.fixture
def sample_federal_raw_data() -> list[dict[str, str]]:
    """Fixture providing sample raw Federal holiday data.

    Returns
    -------
    list[dict[str, str]]
        list of dictionaries with DATE, WEEKDAY, NAME, YEAR
    """
    return [
        {"DATE": "January 1", "WEEKDAY": "Sunday", "NAME": "New Year's Day", "YEAR": 2023},
        {"DATE": "December 25", "WEEKDAY": "Monday", "NAME": "Christmas Day", "YEAR": 2023},
    ]


@pytest.fixture
def mock_html_handler() -> Mock:
    """Fixture providing mocked HtmlHandler.

    Returns
    -------
    Mock
        Mocked HtmlHandler instance
    """
    mock_handler = Mock()
    mock_handler.lxml_parser.return_value = Mock()
    mock_handler.lxml_xpath.return_value = [Mock(text="January 1, 2023"), Mock(text="New Year")]
    return mock_handler


@pytest.fixture
def mock_dict_handler() -> Mock:
    """Fixture providing mocked HandlingDicts.

    Returns
    -------
    Mock
        Mocked HandlingDicts instance
    """
    mock_handler = Mock()
    mock_handler.pair_headers_with_data.return_value = [
        {"DATE": "January 1, 2023", "DESCRIPTION": "New Year", "STATUS": "Open"}
    ]
    return mock_handler


@pytest.fixture
def mock_playwright_scraper() -> Mock:
    """Fixture providing mocked PlaywrightScraper.

    Returns
    -------
    Mock
        Mocked PlaywrightScraper instance
    """
    mock_scraper = Mock()
    mock_scraper.navigate.return_value = True
    mock_scraper.get_list_data.return_value = ["January 1", "Sunday", "New Year's Day"]
    
    # Configure context manager support
    mock_context = MagicMock()  # Use MagicMock instead of Mock
    mock_context.__enter__.return_value = mock_scraper
    mock_context.__exit__.return_value = None
    mock_scraper.launch.return_value = mock_context
    
    return mock_scraper


# --------------------------
# Tests for DatesUSANasdaq
# --------------------------
class TestDatesUSANasdaq:
    """Test cases for DatesUSANasdaq class."""

    def test_init(self, nasdaq_calendar: DatesUSANasdaq) -> None:
        """Test initialization with proper attributes.

        Verifies
        --------
        - Instance is created successfully
        - Required handler objects are initialized

        Parameters
        ----------
        nasdaq_calendar : DatesUSANasdaq
            Nasdaq calendar fixture

        Returns
        -------
        None
        """
        assert hasattr(nasdaq_calendar, "cls_html_handler")
        assert hasattr(nasdaq_calendar, "cls_dict_handler")
        assert isinstance(nasdaq_calendar.cls_html_handler, HtmlHandler)
        assert isinstance(nasdaq_calendar.cls_dict_handler, HandlingDicts)

    @patch("stpstone.utils.calendars.calendar_usa.requests.get")
    def test_get_holidays_raw_success(
        self,
        mock_get: Mock,
        nasdaq_calendar: DatesUSANasdaq,
        mock_html_handler: Mock,
        mock_dict_handler: Mock,
    ) -> None:
        """Test successful raw holiday data fetching.

        Verifies
        --------
        - HTTP request is made with correct parameters
        - HTML parsing and data extraction work correctly
        - Returns DataFrame with expected structure

        Parameters
        ----------
        mock_get : Mock
            Mocked requests.get function
        nasdaq_calendar : DatesUSANasdaq
            Nasdaq calendar fixture
        mock_html_handler : Mock
            Mocked HTML handler
        mock_dict_handler : Mock
            Mocked dict handler

        Returns
        -------
        None
        """
        mock_response = Mock()
        mock_response.raise_for_status.return_value = None
        mock_response.content = b"<html>content</html>"
        mock_get.return_value = mock_response

        nasdaq_calendar.cls_html_handler = mock_html_handler
        nasdaq_calendar.cls_dict_handler = mock_dict_handler

        result = nasdaq_calendar.get_holidays_raw(timeout=15)

        mock_get.assert_called_once_with(
            "https://nasdaqtrader.com/trader.aspx?id=Calendar", timeout=15
        )
        assert isinstance(result, pd.DataFrame)
        assert "DATE" in result.columns
        assert "DESCRIPTION" in result.columns
        assert "STATUS" in result.columns

    @patch("stpstone.utils.calendars.calendar_usa.requests.get")
    def test_get_holidays_raw_http_error(
        self, mock_get: Mock, nasdaq_calendar: DatesUSANasdaq
    ) -> None:
        """Test HTTP error handling in raw data fetching.

        Verifies
        --------
        - HTTP errors are properly caught and re-raised
        - Exception message contains meaningful information

        Parameters
        ----------
        mock_get : Mock
            Mocked requests.get function
        nasdaq_calendar : DatesUSANasdaq
            Nasdaq calendar fixture

        Returns
        -------
        None
        """
        mock_get.side_effect = requests.exceptions.RequestException("Connection failed")

        with pytest.raises(requests.exceptions.RequestException) as excinfo:
            nasdaq_calendar.get_holidays_raw()
        assert "Failed to fetch NASDAQ holidays" in str(excinfo.value)

    def test_transform_holidays_success(
        self, nasdaq_calendar: DatesUSANasdaq, sample_nasdaq_raw_data: list[dict[str, str]]
    ) -> None:
        """Test successful holiday data transformation.

        Verifies
        --------
        - DataFrame is properly transformed
        - DATE_WINS column contains date objects
        - Data types are correctly converted

        Parameters
        ----------
        nasdaq_calendar : DatesUSANasdaq
            Nasdaq calendar fixture
        sample_nasdaq_raw_data : list[dict[str, str]]
            Sample raw data fixture

        Returns
        -------
        None
        """
        df_input = pd.DataFrame(sample_nasdaq_raw_data)
        result = nasdaq_calendar.transform_holidays(df_input)

        assert "DATE_WINS" in result.columns
        assert all(isinstance(d, date) for d in result["DATE_WINS"])
        assert result["DATE"].dtype == "object"
        assert result["DESCRIPTION"].dtype == "object"
        assert result["STATUS"].dtype == "object"

    def test_transform_holidays_empty_dataframe(self, nasdaq_calendar: DatesUSANasdaq) -> None:
        """Test transformation with empty DataFrame.

        Verifies
        --------
        - Empty DataFrame raises ValueError
        - Error message is appropriate

        Parameters
        ----------
        nasdaq_calendar : DatesUSANasdaq
            Nasdaq calendar fixture

        Returns
        -------
        None
        """
        empty_df = pd.DataFrame()

        with pytest.raises(ValueError, match="Holidays DataFrame cannot be empty"):
            nasdaq_calendar.transform_holidays(empty_df)

    def test_transform_holidays_missing_columns(self, nasdaq_calendar: DatesUSANasdaq) -> None:
        """Test transformation with missing required columns.

        Verifies
        --------
        - Missing columns raise ValueError
        - Error message lists required columns

        Parameters
        ----------
        nasdaq_calendar : DatesUSANasdaq
            Nasdaq calendar fixture

        Returns
        -------
        None
        """
        incomplete_df = pd.DataFrame({"DATE": ["Jan 1"], "DESCRIPTION": ["Test"]})

        with pytest.raises(ValueError, match="DataFrame must contain columns"):
            nasdaq_calendar.transform_holidays(incomplete_df)

    def test_parse_dates_valid(self, nasdaq_calendar: DatesUSANasdaq) -> None:
        """Test valid date string parsing.

        Verifies
        --------
        - Valid date strings are parsed correctly
        - Returns proper date objects

        Parameters
        ----------
        nasdaq_calendar : DatesUSANasdaq
            Nasdaq calendar fixture

        Returns
        -------
        None
        """
        test_cases = [
            ("January 1, 2023", date(2023, 1, 1)),
            ("December 25, 2023", date(2023, 12, 25)),
            ("February 28, 2024", date(2024, 2, 28)),
        ]

        for date_str, expected in test_cases:
            result = nasdaq_calendar._parse_dates(date_str)
            assert result == expected

    @pytest.mark.parametrize(
        "invalid_date",
        [
            "",
            "InvalidDate",
            "January",
            "January 2023",
            "January 1",
            "1 January 2023",
        ],
    )
    def test_parse_dates_invalid(
        self, 
        nasdaq_calendar: DatesUSANasdaq, 
        invalid_date: str
    ) -> None:
        """Test invalid date string parsing.

        Verifies
        --------
        - Invalid date strings raise ValueError

        Parameters
        ----------
        nasdaq_calendar : DatesUSANasdaq
            Nasdaq calendar fixture
        invalid_date : str
            Invalid date string to test

        Returns
        -------
        None
        """
        with pytest.raises(ValueError):
            nasdaq_calendar._parse_dates(invalid_date)

    def test_holidays_method(self, nasdaq_calendar: DatesUSANasdaq) -> None:
        """Test main holidays method integration.

        Verifies
        --------
        - Returns list of tuples with string and date
        - list is not empty

        Parameters
        ----------
        nasdaq_calendar : DatesUSANasdaq
            Nasdaq calendar fixture

        Returns
        -------
        None
        """
        with patch.object(nasdaq_calendar, "get_holidays_raw") as mock_raw, patch.object(
            nasdaq_calendar, "transform_holidays"
        ) as mock_transform:
            mock_df = pd.DataFrame(
                {
                    "DESCRIPTION": ["Test Holiday"],
                    "DATE_WINS": [date(2023, 1, 1)],
                }
            )
            mock_transform.return_value = mock_df
            mock_raw.return_value = pd.DataFrame()

            result = nasdaq_calendar.holidays()

            assert isinstance(result, list)
            assert len(result) == 1
            assert isinstance(result[0], tuple)
            assert isinstance(result[0][0], str)
            assert isinstance(result[0][1], date)

    def test_validate_date_string_valid(self, nasdaq_calendar: DatesUSANasdaq) -> None:
        """Test valid date string validation.

        Verifies
        --------
        - Valid date strings pass validation
        - No exceptions are raised

        Parameters
        ----------
        nasdaq_calendar : DatesUSANasdaq
            Nasdaq calendar fixture

        Returns
        -------
        None
        """
        valid_dates = ["January 1, 2023", "December 25, 2024", "February 28, 2025"]

        for date_str in valid_dates:
            try:
                nasdaq_calendar._validate_date_string(date_str)
            except ValueError:
                pytest.fail(f"Valid date string {date_str} failed validation")

    @pytest.mark.parametrize(
        "invalid_date,expected_match",
        [
            ("", "Date string cannot be empty"),
            ("January", "must contain month, day, and year components"),
            ("2023", "must contain month, day, and year components"),
        ],
    )
    def test_validate_date_string_invalid(
        self, nasdaq_calendar: DatesUSANasdaq, invalid_date: str, expected_match: str
    ) -> None:
        """Test invalid date string validation.

        Verifies
        --------
        - Invalid date strings raise ValueError
        - Error messages contain expected text

        Parameters
        ----------
        nasdaq_calendar : DatesUSANasdaq
            Nasdaq calendar fixture
        invalid_date : str
            Invalid date string to test
        expected_match : str
            Expected error message pattern

        Returns
        -------
        None
        """
        with pytest.raises(ValueError, match=expected_match):
            nasdaq_calendar._validate_date_string(invalid_date)


# --------------------------
# Tests for DatesUSAFederalHolidays
# --------------------------
class TestDatesUSAFederalHolidays:
    """Test cases for DatesUSAFederalHolidays class."""

    def test_init(self, federal_calendar: DatesUSAFederalHolidays) -> None:
        """Test initialization with proper attributes.

        Verifies
        --------
        - Instance is created successfully
        - Required handler objects are initialized

        Parameters
        ----------
        federal_calendar : DatesUSAFederalHolidays
            Federal calendar fixture

        Returns
        -------
        None
        """
        assert hasattr(federal_calendar, "cls_html_handler")
        assert hasattr(federal_calendar, "cls_dict_handler")
        assert isinstance(federal_calendar.cls_html_handler, HtmlHandler)
        assert isinstance(federal_calendar.cls_dict_handler, HandlingDicts)

    def test_get_holidays_years_valid_range(
        self, 
        federal_calendar: DatesUSAFederalHolidays
    ) -> None:
        """Test valid year range handling.

        Verifies
        --------
        - Valid year ranges are accepted
        - Returns DataFrame with expected structure

        Parameters
        ----------
        federal_calendar : DatesUSAFederalHolidays
            Federal calendar fixture

        Returns
        -------
        None
        """
        with patch.object(federal_calendar, "get_holidays_raw") as mock_raw:
            mock_raw.return_value = pd.DataFrame(
                {
                    "DATE": ["January 1"], 
                    "WEEKDAY": ["Sunday"], 
                    "NAME": ["New Year"], 
                    "YEAR": [2023]
                }
            )

            result = federal_calendar.get_holidays_years(2023, 2024)

            assert isinstance(result, pd.DataFrame)
            assert mock_raw.call_count == 2

    def test_get_holidays_years_invalid_range(
        self, 
        federal_calendar: DatesUSAFederalHolidays
    ) -> None:
        """Test invalid year range validation.

        Verifies
        --------
        - Invalid year ranges raise ValueError
        - Error message is appropriate

        Parameters
        ----------
        federal_calendar : DatesUSAFederalHolidays
            Federal calendar fixture

        Returns
        -------
        None
        """
        with pytest.raises(ValueError, match="Start year must be less than or equal to end year"):
            federal_calendar.get_holidays_years(2024, 2023)

    @pytest.mark.parametrize("invalid_year", [0, -1, -2023])
    def test_validate_year_negative(
        self, 
        federal_calendar: DatesUSAFederalHolidays, 
        invalid_year: int
    ) -> None:
        """Test negative year validation.

        Verifies
        --------
        - Negative years raise ValueError
        - Error message is appropriate

        Parameters
        ----------
        federal_calendar : DatesUSAFederalHolidays
            Federal calendar fixture
        invalid_year : int
            Invalid year to test

        Returns
        -------
        None
        """
        with pytest.raises(ValueError, match="Year must be a positive integer"):
            federal_calendar._validate_year(invalid_year)

    def test_get_holidays_raw_success(
        self,
        federal_calendar: DatesUSAFederalHolidays,
        mock_playwright_scraper: Mock,
        mock_dict_handler: Mock,
    ) -> None:
        """Test successful raw Federal holiday data fetching.

        Verifies
        --------
        - Playwright navigation and data extraction work
        - Returns DataFrame with expected structure
        - Year column is properly added

        Parameters
        ----------
        federal_calendar : DatesUSAFederalHolidays
            Federal calendar fixture
        mock_playwright_scraper : Mock
            Mocked Playwright scraper
        mock_dict_handler : Mock
            Mocked dict handler

        Returns
        -------
        None
        """
        federal_calendar.cls_dict_handler = mock_dict_handler
        mock_dict_handler.pair_headers_with_data.return_value = [
            {"DATE": "January 1", "WEEKDAY": "Sunday", "NAME": "New Year's Day"}
        ]

        with patch(
            "stpstone.utils.calendars.calendar_usa.PlaywrightScraper",
            return_value=mock_playwright_scraper,
        ):
            result = federal_calendar.get_holidays_raw(2023, timeout=6000)

            assert isinstance(result, pd.DataFrame)
            assert "YEAR" in result.columns
            assert result["YEAR"].iloc[0] == 2023
            mock_playwright_scraper.navigate.assert_called_once()
            mock_playwright_scraper.get_list_data.assert_called_once()

    def test_get_holidays_raw_navigation_failure(
        self, federal_calendar: DatesUSAFederalHolidays, mock_playwright_scraper: Mock
    ) -> None:
        """Test navigation failure handling.

        Verifies
        --------
        - Navigation failures raise RuntimeError
        - Error message contains URL information

        Parameters
        ----------
        federal_calendar : DatesUSAFederalHolidays
            Federal calendar fixture
        mock_playwright_scraper : Mock
            Mocked Playwright scraper

        Returns
        -------
        None
        """
        mock_playwright_scraper.navigate.return_value = False
        mock_playwright_scraper.launch.return_value.__enter__.return_value = \
            mock_playwright_scraper
        mock_playwright_scraper.launch.return_value.__exit__.return_value = None

        with patch(
            "stpstone.utils.calendars.calendar_usa.PlaywrightScraper",
            return_value=mock_playwright_scraper,
        ), pytest.raises(RuntimeError, match="Failed to navigate to URL"):
            federal_calendar.get_holidays_raw(2023)

    def test_transform_holidays_success(
        self, 
        federal_calendar: DatesUSAFederalHolidays, 
        sample_federal_raw_data: list[dict[str, str]]
    ) -> None:
        """Test successful Federal holiday data transformation.

        Verifies
        --------
        - DataFrame is properly transformed
        - DATE_WINS column contains date objects
        - Data types are correctly converted

        Parameters
        ----------
        federal_calendar : DatesUSAFederalHolidays
            Federal calendar fixture
        sample_federal_raw_data : list[dict[str, str]]
            Sample raw data fixture

        Returns
        -------
        None
        """
        df_input = pd.DataFrame(sample_federal_raw_data)
        result = federal_calendar.transform_holidays(df_input)

        assert "DATE_WINS" in result.columns
        assert all(isinstance(d, date) for d in result["DATE_WINS"])
        assert result["DATE"].dtype == "object"
        assert result["WEEKDAY"].dtype == "object"
        assert result["NAME"].dtype == "object"
        assert result["YEAR"].dtype == "int64"

    def test_transform_holidays_empty_dataframe(
        self, 
        federal_calendar: DatesUSAFederalHolidays
    ) -> None:
        """Test transformation with empty DataFrame.

        Verifies
        --------
        - Empty DataFrame raises ValueError
        - Error message is appropriate

        Parameters
        ----------
        federal_calendar : DatesUSAFederalHolidays
            Federal calendar fixture

        Returns
        -------
        None
        """
        empty_df = pd.DataFrame()

        with pytest.raises(ValueError, match="Federal holidays DataFrame cannot be empty"):
            federal_calendar.transform_holidays(empty_df)

    def test_parse_dates_with_year_valid(self, federal_calendar: DatesUSAFederalHolidays) -> None:
        """Test valid date string parsing with year parameter.

        Verifies
        --------
        - Valid date strings with year are parsed correctly
        - Returns proper date objects

        Parameters
        ----------
        federal_calendar : DatesUSAFederalHolidays
            Federal calendar fixture

        Returns
        -------
        None
        """
        test_cases = [
            (("January 1", 2023), date(2023, 1, 1)),
            (("December 25", 2024), date(2024, 12, 25)),
            (("February 28", 2025), date(2025, 2, 28)),
        ]

        for (date_str, year), expected in test_cases:
            result = federal_calendar._parse_dates(date_str, year)
            assert result == expected

    @pytest.mark.parametrize(
    "invalid_date",
        [
            "",
            "InvalidDate",
            "January",  # Missing day
            "1",  # Missing month
            "January 1st",  # Invalid day format
            "32 January",  # Invalid day number
        ],
    )
    def test_parse_dates_with_year_invalid(
        self, federal_calendar: DatesUSAFederalHolidays, invalid_date: str
    ) -> None:
        """Test invalid date string parsing with year parameter.

        Verifies
        --------
        - Invalid date strings raise ValueError
        - Various invalid formats are rejected

        Parameters
        ----------
        federal_calendar : DatesUSAFederalHolidays
            Federal calendar fixture
        invalid_date : str
            Invalid date string to test

        Returns
        -------
        None
        """
        with pytest.raises(ValueError):
            federal_calendar._parse_dates(invalid_date, 2023)

    def test_holidays_method(self, federal_calendar: DatesUSAFederalHolidays) -> None:
        """Test main holidays method integration.

        Verifies
        --------
        - Returns list of tuples with string and date
        - list is not empty

        Parameters
        ----------
        federal_calendar : DatesUSAFederalHolidays
            Federal calendar fixture

        Returns
        -------
        None
        """
        with patch.object(federal_calendar, "get_holidays_years") as mock_years, patch.object(
            federal_calendar, "transform_holidays"
        ) as mock_transform:
            mock_df = pd.DataFrame(
                {
                    "NAME": ["Test Holiday"],
                    "DATE_WINS": [date(2023, 1, 1)],
                }
            )
            mock_transform.return_value = mock_df
            mock_years.return_value = pd.DataFrame()

            result = federal_calendar.holidays()

            assert isinstance(result, list)
            assert len(result) == 1
            assert isinstance(result[0], tuple)
            assert isinstance(result[0][0], str)
            assert isinstance(result[0][1], date)


# --------------------------
# Edge Case Tests
# --------------------------
class TestEdgeCases:
    """Test edge cases for both calendar classes."""

    @pytest.mark.parametrize(
        "year_start,year_end,expected_calls",
        [
            (2023, 2023, 1),
            (2023, 2024, 2),
            (2023, 2025, 3),
        ],
    )
    def test_multiple_year_processing(
        self,
        federal_calendar: DatesUSAFederalHolidays,
        year_start: int,
        year_end: int,
        expected_calls: int,
    ) -> None:
        """Test processing of multiple year ranges.

        Verifies
        --------
        - Correct number of years are processed
        - get_holidays_raw is called appropriate times

        Parameters
        ----------
        federal_calendar : DatesUSAFederalHolidays
            Federal calendar fixture
        year_start : int
            Starting year for test
        year_end : int
            Ending year for test
        expected_calls : int
            Expected number of method calls

        Returns
        -------
        None
        """
        with patch.object(federal_calendar, "get_holidays_raw") as mock_raw:
            mock_raw.return_value = pd.DataFrame(
                {
                    "DATE": ["January 1"], 
                    "WEEKDAY": ["Sunday"], 
                    "NAME": ["New Year"], 
                    "YEAR": [2023]
                }
            )

            federal_calendar.get_holidays_years(year_start, year_end)
            assert mock_raw.call_count == expected_calls

    def test_current_year_defaults(self, federal_calendar: DatesUSAFederalHolidays) -> None:
        """Test default year range calculation.

        Verifies
        --------
        - Default years are calculated correctly
        - Uses current date as reference

        Parameters
        ----------
        federal_calendar : DatesUSAFederalHolidays
            Federal calendar fixture

        Returns
        -------
        None
        """
        current_year = date.today().year
        expected_start = current_year - 1
        expected_end = current_year

        with patch.object(federal_calendar, "get_holidays_raw") as mock_raw:
            mock_raw.return_value = pd.DataFrame()

            federal_calendar.get_holidays_years()
            
            calls = [call.args[0] for call in mock_raw.call_args_list]
            assert expected_start in calls
            assert expected_end in calls

    @pytest.mark.parametrize(
        "month_str,expected_month",
        [
            ("January", 1),
            ("February", 2),
            ("March", 3),
            ("April", 4),
            ("May", 5),
            ("June", 6),
            ("July", 7),
            ("August", 8),
            ("September", 9),
            ("October", 10),
            ("November", 11),
            ("December", 12),
        ],
    )
    def test_all_month_mappings(
        self, nasdaq_calendar: DatesUSANasdaq, month_str: str, expected_month: int
    ) -> None:
        """Test all month name to number mappings.

        Verifies
        --------
        - All month names are mapped correctly
        - Returns proper month numbers

        Parameters
        ----------
        nasdaq_calendar : DatesUSANasdaq
            Nasdaq calendar fixture
        month_str : str
            Month name to test
        expected_month : int
            Expected month number

        Returns
        -------
        None
        """
        date_str = f"{month_str} 15, 2023"
        result = nasdaq_calendar._parse_dates(date_str)
        assert result.month == expected_month


# --------------------------
# Error Handling Tests
# --------------------------
class TestErrorHandling:
    """Test error handling scenarios."""

    def test_nasdaq_network_timeout(self, nasdaq_calendar: DatesUSANasdaq) -> None:
        """Test network timeout handling for Nasdaq.

        Verifies
        --------
        - Timeout errors are properly handled
        - Appropriate exception is raised

        Parameters
        ----------
        nasdaq_calendar : DatesUSANasdaq
            Nasdaq calendar fixture

        Returns
        -------
        None
        """
        with patch("stpstone.utils.calendars.calendar_usa.requests.get") as mock_get:
            mock_get.side_effect = requests.exceptions.Timeout("Request timed out")

            with pytest.raises(requests.exceptions.RequestException, match="Failed to fetch"):
                nasdaq_calendar.get_holidays_raw()

    def test_federal_scraper_exception(self, federal_calendar: DatesUSAFederalHolidays) -> None:
        """Test Playwright exception handling.

        Verifies
        --------
        - Playwright exceptions are properly wrapped
        - RuntimeError is raised with meaningful message

        Parameters
        ----------
        federal_calendar : DatesUSAFederalHolidays
            Federal calendar fixture

        Returns
        -------
        None
        """
        with patch("stpstone.utils.calendars.calendar_usa.PlaywrightScraper") as mock_scraper:
            mock_instance = mock_scraper.return_value
            mock_instance.__enter__.return_value = mock_instance
            mock_instance.navigate.side_effect = Exception("Playwright error")

            with pytest.raises(RuntimeError, match="Failed to fetch Federal holidays"):
                federal_calendar.get_holidays_raw(2023)

    def test_dataframe_validation_missing_columns_federal(
        self, federal_calendar: DatesUSAFederalHolidays
    ) -> None:
        """Test Federal DataFrame validation with missing columns.

        Verifies
        --------
        - Missing required columns raise ValueError
        - Error message lists all required columns

        Parameters
        ----------
        federal_calendar : DatesUSAFederalHolidays
            Federal calendar fixture

        Returns
        -------
        None
        """
        incomplete_df = pd.DataFrame({"DATE": ["Jan 1"], "NAME": ["Test"]})

        with pytest.raises(ValueError, match="DataFrame must contain columns"):
            federal_calendar._validate_federal_holidays_dataframe(incomplete_df)

    def test_year_validation_zero(self, federal_calendar: DatesUSAFederalHolidays) -> None:
        """Test year validation with zero.

        Verifies
        --------
        - Zero year raises ValueError
        - Error message is appropriate

        Parameters
        ----------
        federal_calendar : DatesUSAFederalHolidays
            Federal calendar fixture

        Returns
        -------
        None
        """
        with pytest.raises(ValueError, match="Year must be a positive integer"):
            federal_calendar._validate_year(0)