"""Unit tests for USA holiday calendar data extraction.

Tests the functionality of Nasdaq and Federal holiday calendar data fetchers,
covering initialization, data fetching, transformation, and validation.
"""

from datetime import date
from logging import Logger
from unittest.mock import Mock

import pandas as pd
import pytest
from pytest_mock import MockerFixture
from requests.exceptions import RequestException

from stpstone.utils.calendars.calendar_usa import DatesUSAFederalHolidays, DatesUSANasdaq


# --------------------------
# Fixtures
# --------------------------
@pytest.fixture
def nasdaq_instance() -> DatesUSANasdaq:
    """Fixture providing a DatesUSANasdaq instance.

    Returns
    -------
    DatesUSANasdaq
        Instance initialized with default parameters
    """
    return DatesUSANasdaq(bool_reuse_cache=False)


@pytest.fixture
def federal_instance() -> DatesUSAFederalHolidays:
    """Fixture providing a DatesUSAFederalHolidays instance.

    Returns
    -------
    DatesUSAFederalHolidays
        Instance initialized with default parameters
    """
    return DatesUSAFederalHolidays(bool_reuse_cache=False)


@pytest.fixture
def sample_nasdaq_df() -> pd.DataFrame:
    """Fixture providing sample Nasdaq holiday DataFrame.

    Returns
    -------
    pd.DataFrame
        DataFrame with sample Nasdaq holiday data including DATE_WINS
    """
    return pd.DataFrame({
        "DATE": ["January 1, 2025", "July 4, 2025"],
        "DESCRIPTION": ["New Year's Day", "Independence Day"],
        "STATUS": ["Closed", "Closed"],
        "DATE_WINS": [date(2025, 1, 1), date(2025, 7, 4)]
    })


@pytest.fixture
def sample_federal_df() -> pd.DataFrame:
    """Fixture providing sample Federal holiday DataFrame.

    Returns
    -------
    pd.DataFrame
        DataFrame with sample Federal holiday data including DATE_WINS
    """
    return pd.DataFrame({
        "DATE": ["January 1", "July 4"],
        "WEEKDAY": ["Wednesday", "Friday"],
        "NAME": ["New Year's Day", "Independence Day"],
        "YEAR": [2025, 2025],
        "DATE_WINS": [date(2025, 1, 1), date(2025, 7, 4)]
    })


# --------------------------
# Tests for DatesUSANasdaq
# --------------------------
def test_nasdaq_init_valid() -> None:
    """Test initialization of DatesUSANasdaq with valid parameters.

    Verifies
    --------
    - Instance is created with valid default parameters
    - Cache manager and handlers are properly initialized

    Returns
    -------
    None
    """
    instance = DatesUSANasdaq(
        bool_persist_cache=False,
        bool_reuse_cache=False,
        int_days_cache_expiration=2,
        int_cache_ttl_days=60,
        path_cache_dir="/tmp/cache", # noqa S108: probable insecure usage of temporary file or directory
        logger=Logger("test")
    )
    assert isinstance(instance.cls_cache_manager, object)
    assert isinstance(instance.cls_html_handler, object)
    assert isinstance(instance.cls_dict_handler, object)


def test_nasdaq_holidays(mocker: MockerFixture, sample_nasdaq_df: pd.DataFrame) -> None:
    """Test holidays method of DatesUSANasdaq.

    Verifies
    --------
    - Returns correct list of tuples with holiday description and date
    - Correctly calls get_holidays_raw and transform_holidays

    Parameters
    ----------
    mocker : MockerFixture
        Pytest-mock fixture for mocking dependencies
    sample_nasdaq_df : pd.DataFrame
        Sample Nasdaq holiday data

    Returns
    -------
    None
    """
    mocker.patch.object(DatesUSANasdaq, "get_holidays_raw", return_value=sample_nasdaq_df)
    mocker.patch.object(DatesUSANasdaq, "transform_holidays", return_value=sample_nasdaq_df)
    instance = DatesUSANasdaq(bool_reuse_cache=False)
    result = instance.holidays()
    assert isinstance(result, list)
    assert len(result) == 2
    assert result[0] == ("New Year's Day", date(2025, 1, 1))
    assert result[1] == ("Independence Day", date(2025, 7, 4))


def test_nasdaq_get_holidays_raw_success(mocker: MockerFixture) -> None:
    """Test successful fetching of raw Nasdaq holiday data.

    Verifies
    --------
    - Correctly fetches and parses HTML data
    - Returns valid DataFrame with expected columns

    Parameters
    ----------
    mocker : MockerFixture
        Pytest-mock fixture for mocking dependencies

    Returns
    -------
    None
    """
    mock_response = Mock()
    mock_response.text = "<table><tbody><tr><td>January 1, 2025</td><td>New Year's Day"\
        + "</td><td>Closed</td></tr></tbody></table>"
    mocker.patch("requests.get", return_value=mock_response)
    mocker.patch(
        "stpstone.utils.parsers.html.HtmlHandler.lxml_parser", 
        return_value=mock_response.text
    )
    mocker.patch("stpstone.utils.parsers.html.HtmlHandler.lxml_xpath", return_value=[
        Mock(text="January 1, 2025"), Mock(text="New Year's Day"), Mock(text="Closed")
    ])
    mocker.patch("stpstone.utils.cache.cache_manager.CacheManager.cache_df", lambda x: x)
    instance = DatesUSANasdaq(bool_reuse_cache=False)
    result = instance.get_holidays_raw(timeout=5)
    assert isinstance(result, pd.DataFrame)
    assert set(result.columns) == {"DATE", "DESCRIPTION", "STATUS"}
    assert len(result) == 1


def test_nasdaq_get_holidays_raw_request_error(mocker: MockerFixture) -> None:
    """Test handling of RequestException in get_holidays_raw.

    Verifies
    --------
    - Raises RequestException with appropriate message

    Parameters
    ----------
    mocker : MockerFixture
        Pytest-mock fixture for mocking dependencies

    Returns
    -------
    None
    """
    mocker.patch("requests.get", side_effect=RequestException("Network error"))
    mocker.patch("stpstone.utils.cache.cache_manager.CacheManager.cache_df", lambda x: x)
    instance = DatesUSANasdaq(bool_reuse_cache=False)
    with pytest.raises(RequestException, match="Failed to fetch NASDAQ holidays"):
        instance.get_holidays_raw()


def test_nasdaq_transform_holidays_valid(sample_nasdaq_df: pd.DataFrame) -> None:
    """Test transformation of Nasdaq holiday data.

    Verifies
    --------
    - Correctly transforms DataFrame with date parsing
    - Adds DATE_WINS column with date objects

    Parameters
    ----------
    sample_nasdaq_df : pd.DataFrame
        Sample Nasdaq holiday data

    Returns
    -------
    None
    """
    instance = DatesUSANasdaq()
    result = instance.transform_holidays(sample_nasdaq_df)
    assert "DATE_WINS" in result.columns
    assert isinstance(result["DATE_WINS"].iloc[0], date)
    assert result["DATE"].dtype == "object"
    assert result["DESCRIPTION"].dtype == "object"
    assert result["STATUS"].dtype == "object"


def test_nasdaq_validate_holidays_dataframe_empty() -> None:
    """Test validation of empty Nasdaq holidays DataFrame.

    Verifies
    --------
    - Raises ValueError for empty DataFrame

    Returns
    -------
    None
    """
    instance = DatesUSANasdaq()
    with pytest.raises(ValueError, match="Holidays DataFrame cannot be empty"):
        instance._validate_holidays_dataframe(pd.DataFrame())


def test_nasdaq_validate_holidays_dataframe_missing_columns() -> None:
    """Test validation of Nasdaq holidays DataFrame with missing columns.

    Verifies
    --------
    - Raises ValueError for missing required columns

    Returns
    -------
    None
    """
    instance = DatesUSANasdaq()
    df_ = pd.DataFrame({"DATE": ["January 1, 2025"], "DESCRIPTION": ["New Year's Day"]})
    with pytest.raises(ValueError, match="DataFrame must contain columns"):
        instance._validate_holidays_dataframe(df_)


def test_nasdaq_parse_dates_valid() -> None:
    """Test parsing of valid Nasdaq date string.

    Verifies
    --------
    - Correctly parses date string into date object

    Returns
    -------
    None
    """
    instance = DatesUSANasdaq()
    result = instance._parse_dates("January 1, 2025")
    assert result == date(2025, 1, 1)


def test_nasdaq_parse_dates_invalid() -> None:
    """Test parsing of invalid Nasdaq date string.

    Verifies
    --------
    - Raises ValueError for invalid date format

    Returns
    -------
    None
    """
    instance = DatesUSANasdaq()
    with pytest.raises(ValueError, match="Date string must contain month, day, and year"):
        instance._parse_dates("January 1")


def test_nasdaq_validate_date_string_empty() -> None:
    """Test validation of empty Nasdaq date string.

    Verifies
    --------
    - Raises ValueError for empty date string

    Returns
    -------
    None
    """
    instance = DatesUSANasdaq()
    with pytest.raises(ValueError, match="Date string cannot be empty"):
        instance._validate_date_string("")


def test_nasdaq_validate_date_string_invalid_format() -> None:
    """Test validation of Nasdaq date string with invalid format.

    Verifies
    --------
    - Raises ValueError for invalid format

    Returns
    -------
    None
    """
    instance = DatesUSANasdaq()
    with pytest.raises(ValueError, match="Date string must contain month, day, and year"):
        instance._validate_date_string("January 1")


# --------------------------
# Tests for DatesUSAFederalHolidays
# --------------------------
def test_federal_init_valid() -> None:
    """Test initialization of DatesUSAFederalHolidays with valid parameters.

    Verifies
    --------
    - Instance is created with valid parameters
    - Year range and cache manager are properly initialized

    Returns
    -------
    None
    """
    instance = DatesUSAFederalHolidays(
        int_year_start=2024,
        int_year_end=2025,
        bool_persist_cache=False,
        bool_reuse_cache=False,
        int_days_cache_expiration=2,
        int_cache_ttl_days=60,
        path_cache_dir="/tmp/cache", # noqa S108: probable insecure usage of temporary file or directory
        logger=Logger("test")
    )
    assert instance.int_year_start == 2024
    assert instance.int_year_end == 2025
    assert isinstance(instance.cls_cache_manager, object)


def test_federal_holidays(mocker: MockerFixture, sample_federal_df: pd.DataFrame) -> None:
    """Test holidays method of DatesUSAFederalHolidays.

    Verifies
    --------
    - Returns correct list of tuples with holiday name and date
    - Correctly calls get_holidays_years and transform_holidays

    Parameters
    ----------
    mocker : MockerFixture
        Pytest-mock fixture for mocking dependencies
    sample_federal_df : pd.DataFrame
        Sample Federal holiday data

    Returns
    -------
    None
    """
    mocker.patch.object(
        DatesUSAFederalHolidays, "get_holidays_years", return_value=sample_federal_df)
    mocker.patch.object(
        DatesUSAFederalHolidays, "transform_holidays", return_value=sample_federal_df)
    instance = DatesUSAFederalHolidays(bool_reuse_cache=False)
    result = instance.holidays()
    assert isinstance(result, list)
    assert len(result) == 2
    assert result[0] == ("New Year's Day", date(2025, 1, 1))
    assert result[1] == ("Independence Day", date(2025, 7, 4))


def test_federal_get_holidays_years_valid(
    mocker: MockerFixture, 
    sample_federal_df: pd.DataFrame
) -> None:
    """Test fetching Federal holidays for multiple years.

    Verifies
    --------
    - Correctly aggregates holiday data for year range
    - Returns valid DataFrame

    Parameters
    ----------
    mocker : MockerFixture
        Pytest-mock fixture for mocking dependencies
    sample_federal_df : pd.DataFrame
        Sample Federal holiday data

    Returns
    -------
    None
    """
    mocker.patch.object(
        DatesUSAFederalHolidays, "get_holidays_raw", return_value=sample_federal_df)
    mocker.patch("stpstone.utils.cache.cache_manager.CacheManager.cache_df", lambda x: x)
    instance = DatesUSAFederalHolidays(
        int_year_start=2024, int_year_end=2025, bool_reuse_cache=False)
    result = instance.get_holidays_years()
    assert isinstance(result, pd.DataFrame)
    assert len(result) == 4  # 2 holidays per year for 2 years
    assert set(result.columns) == {"DATE", "WEEKDAY", "NAME", "YEAR", "DATE_WINS"}


def test_federal_get_holidays_raw_success(mocker: MockerFixture) -> None:
    """Test successful fetching of raw Federal holiday data.

    Verifies
    --------
    - Correctly fetches and parses data using Playwright
    - Returns valid DataFrame with expected columns

    Parameters
    ----------
    mocker : MockerFixture
        Pytest-mock fixture for mocking dependencies

    Returns
    -------
    None
    """
    mocker.patch("stpstone.utils.webdriver_tools.playwright_wd.PlaywrightScraper.navigate", 
                 return_value=True)
    mocker.patch("stpstone.utils.webdriver_tools.playwright_wd.PlaywrightScraper.get_list_data", 
                 return_value=["January 1", "Wednesday", "New Year's Day"])
    mocker.patch("stpstone.utils.cache.cache_manager.CacheManager.cache_df", lambda x: x)
    instance = DatesUSAFederalHolidays(bool_reuse_cache=False)
    result = instance.get_holidays_raw(2025)
    assert isinstance(result, pd.DataFrame)
    assert set(result.columns) == {"DATE", "WEEKDAY", "NAME", "YEAR"}
    assert len(result) == 1


def test_federal_get_holidays_raw_navigation_failure(mocker: MockerFixture) -> None:
    """Test handling of navigation failure in get_holidays_raw.

    Verifies
    --------
    - Raises RuntimeError when navigation fails

    Parameters
    ----------
    mocker : MockerFixture
        Pytest-mock fixture for mocking dependencies

    Returns
    -------
    None
    """
    mocker.patch(
        "stpstone.utils.webdriver_tools.playwright_wd.PlaywrightScraper.navigate", 
        return_value=False
    )
    mocker.patch("stpstone.utils.cache.cache_manager.CacheManager.cache_df", lambda x: x)
    instance = DatesUSAFederalHolidays(bool_reuse_cache=False)
    with pytest.raises(RuntimeError, match="Failed to navigate to URL"):
        instance.get_holidays_raw(2025)


def test_federal_transform_holidays_valid(sample_federal_df: pd.DataFrame) -> None:
    """Test transformation of Federal holiday data.

    Verifies
    --------
    - Correctly transforms DataFrame with date parsing
    - Adds DATE_WINS column with date objects

    Parameters
    ----------
    sample_federal_df : pd.DataFrame
        Sample Federal holiday data

    Returns
    -------
    None
    """
    instance = DatesUSAFederalHolidays()
    result = instance.transform_holidays(sample_federal_df)
    assert "DATE_WINS" in result.columns
    assert isinstance(result["DATE_WINS"].iloc[0], date)
    assert result["DATE"].dtype == "object"
    assert result["WEEKDAY"].dtype == "object"
    assert result["NAME"].dtype == "object"
    assert result["YEAR"].dtype == "int64"


def test_federal_parse_dates_valid() -> None:
    """Test parsing of valid Federal date string.

    Verifies
    --------
    - Correctly parses date string into date object

    Returns
    -------
    None
    """
    instance = DatesUSAFederalHolidays()
    result = instance._parse_dates("January 1", 2025)
    assert result == date(2025, 1, 1)


def test_federal_parse_dates_invalid() -> None:
    """Test parsing of invalid Federal date string.

    Verifies
    --------
    - Raises ValueError for invalid date format

    Returns
    -------
    None
    """
    instance = DatesUSAFederalHolidays()
    with pytest.raises(ValueError, match="Date string must contain month and day"):
        instance._parse_dates("January", 2025)


def test_federal_validate_year_range_invalid() -> None:
    """Test validation of invalid Federal year range.

    Verifies
    --------
    - Raises ValueError when start year is greater than end year

    Returns
    -------
    None
    """
    instance = DatesUSAFederalHolidays()
    with pytest.raises(ValueError, match="Start year must be less than or equal to end year"):
        instance._validate_year_range(2025, 2024)


def test_federal_validate_year_negative() -> None:
    """Test validation of negative Federal year.

    Verifies
    --------
    - Raises ValueError for non-positive year

    Returns
    -------
    None
    """
    instance = DatesUSAFederalHolidays()
    with pytest.raises(ValueError, match="Year must be a positive integer"):
        instance._validate_year(-1)


def test_federal_validate_federal_holidays_dataframe_empty() -> None:
    """Test validation of empty Federal holidays DataFrame.

    Verifies
    --------
    - Raises ValueError for empty DataFrame

    Returns
    -------
    None
    """
    instance = DatesUSAFederalHolidays()
    with pytest.raises(ValueError, match="Federal holidays DataFrame cannot be empty"):
        instance._validate_federal_holidays_dataframe(pd.DataFrame())


def test_federal_validate_federal_holidays_dataframe_missing_columns() -> None:
    """Test validation of Federal holidays DataFrame with missing columns.

    Verifies
    --------
    - Raises ValueError for missing required columns

    Returns
    -------
    None
    """
    instance = DatesUSAFederalHolidays()
    df_ = pd.DataFrame({"DATE": ["January 1"], "NAME": ["New Year's Day"]})
    with pytest.raises(ValueError, match="DataFrame must contain columns"):
        instance._validate_federal_holidays_dataframe(df_)


def test_federal_validate_date_string_empty() -> None:
    """Test validation of empty Federal date string.

    Verifies
    --------
    - Raises ValueError for empty date string

    Returns
    -------
    None
    """
    instance = DatesUSAFederalHolidays()
    with pytest.raises(ValueError, match="Date string cannot be empty"):
        instance._validate_date_string("")


def test_federal_validate_date_string_invalid_format() -> None:
    """Test validation of Federal date string with invalid format.

    Verifies
    --------
    - Raises ValueError for invalid format

    Returns
    -------
    None
    """
    instance = DatesUSAFederalHolidays()
    with pytest.raises(ValueError, match="Date string must contain month and day"):
        instance._validate_date_string("January")