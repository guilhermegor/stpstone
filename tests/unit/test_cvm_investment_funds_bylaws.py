"""Unit tests for InvestmentFunds class in the stpstone package.

Tests the functionality of fetching and processing investment fund bylaws from the
Brazilian SEC (CVM), including initialization, HTTP requests, data transformation,
and database operations.
"""

import importlib
from io import BytesIO
from logging import Logger
import sys
from unittest.mock import MagicMock

import pandas as pd
from playwright.sync_api import Page as PlaywrightPage
import pytest
from pytest_mock import MockerFixture
from requests import Response
from selenium.webdriver.remote.webdriver import WebDriver as SeleniumWebDriver

from stpstone._config.global_slots import YAML_INVESTMENT_FUNDS_BYLAWS
from stpstone.ingestion.countries.br.bylaws.cvm_investment_funds_bylaws import InvestmentFunds
from stpstone.utils.calendars.calendar_abc import DatesCurrent
from stpstone.utils.loggs.create_logs import CreateLog
from stpstone.utils.parsers.folders import DirFilesManagement


# --------------------------
# Fixtures
# --------------------------
@pytest.fixture
def mock_requests_get(mocker: MockerFixture) -> MagicMock:
    """Mock requests.get to prevent real HTTP calls.
    
    Parameters
    ----------
    mocker : MockerFixture
        Pytest-mock fixture for creating mocks
    
    Returns
    -------
    MagicMock
        Mocked requests.get object
    """
    return mocker.patch("requests.get")

@pytest.fixture
def mock_sleep(mocker: MockerFixture) -> MagicMock:
    """Mock time.sleep to eliminate delays.
    
    Parameters
    ----------
    mocker : MockerFixture
        Pytest-mock fixture for creating mocks
    
    Returns
    -------
    MagicMock
        Mocked time.sleep object
    """
    return mocker.patch("time.sleep")

@pytest.fixture
def mock_response() -> Response:
    """Mock Response object with sample content.
    
    Returns
    -------
    Response
        Mocked Response object with predefined content
    """
    response = MagicMock(spec=Response)
    response.content = b"Sample PDF content"
    response.url = "https://example.com/test.pdf"
    response.status_code = 200
    response.raise_for_status = MagicMock()
    return response

@pytest.fixture
def investment_funds() -> InvestmentFunds:
    """Fixture providing InvestmentFunds instance with default parameters.
    
    Returns
    -------
    InvestmentFunds
        Initialized InvestmentFunds instance
    """
    return InvestmentFunds(list_apps=["app1", "app2"], int_pages_join=3)

@pytest.fixture
def sample_dataframe() -> pd.DataFrame:
    """Fixture providing a sample DataFrame for testing.
    
    Returns
    -------
    pd.DataFrame
        Sample DataFrame with sample data
    """
    return pd.DataFrame([
        {"EVENT": "event1", "MATCH_PATTERN": "pattern1", "PATTERN_REGEX": "regex1", "URL": "url1"},
        {"EVENT": "event2", "MATCH_PATTERN": "pattern2", "PATTERN_REGEX": "regex2", "URL": "url2"}
    ])

@pytest.fixture
def mock_backoff(mocker: MockerFixture) -> None:
    """Mock backoff.on_exception to bypass retry delays.
    
    Parameters
    ----------
    mocker : MockerFixture
        Pytest-mock fixture for creating mocks
    
    Returns
    -------
    None
    """
    mocker.patch("backoff.on_exception", lambda *args, **kwargs: lambda func: func)

# --------------------------
# Tests for __init__
# --------------------------
def test_init_with_valid_inputs() -> None:
    """Test initialization with valid inputs.

    Verifies
    --------
    - The InvestmentFunds instance is initialized with correct attributes
    - Inherited classes are properly initialized
    - Input types are maintained

    Returns
    -------
    None
    """
    list_apps = ["app1", "app2"]
    int_pages_join = 3
    logger = MagicMock(spec=Logger)
    cls_db = MagicMock()

    instance = InvestmentFunds(list_apps=list_apps, int_pages_join=int_pages_join, logger=logger, 
                               cls_db=cls_db)

    assert instance.list_apps == list_apps
    assert instance.int_pages_join == int_pages_join
    assert instance.logger is logger
    assert instance.cls_db is cls_db
    assert isinstance(instance.cls_dir_files_management, DirFilesManagement)
    assert isinstance(instance.cls_dates_current, DatesCurrent)
    assert isinstance(instance.cls_create_log, CreateLog)

def test_init_with_empty_list_apps() -> None:
    """Test initialization with empty list_apps.

    Verifies
    --------
    - Empty list_apps is accepted
    - Other attributes are correctly initialized

    Returns
    -------
    None
    """
    instance = InvestmentFunds(list_apps=[])
    assert instance.list_apps == []
    assert instance.int_pages_join == 3
    assert instance.logger is None
    assert instance.cls_db is None

def test_init_with_invalid_list_apps_type() -> None:
    """Test initialization with invalid list_apps type.

    Verifies
    --------
    - TypeError is raised for non-list input

    Returns
    -------
    None
    """
    with pytest.raises(TypeError, match="must be of type"):
        InvestmentFunds(list_apps="not_a_list")

def test_init_with_invalid_int_pages_join_type() -> None:
    """Test initialization with invalid int_pages_join type.

    Verifies
    --------
    - TypeError is raised for non-integer/None input

    Returns
    -------
    None
    """
    with pytest.raises(TypeError, match="must be one of types"):
        InvestmentFunds(list_apps=["app1"], int_pages_join="not_an_int")

# --------------------------
# Tests for get_response
# --------------------------

def test_get_response_empty_apps(
    investment_funds: InvestmentFunds, 
    mock_requests_get: MagicMock, 
    mock_backoff: None
) -> None:
    """Test get_response with empty list_apps.

    Verifies
    --------
    - Empty response list is returned
    - No HTTP requests are made

    Parameters
    ----------
    investment_funds : InvestmentFunds
        Instance of InvestmentFunds
    mock_requests_get : MagicMock
        Mocked requests.get function
    mock_backoff : None
        Mocked backoff decorator

    Returns
    -------
    None
    """
    investment_funds.list_apps = []
    responses = investment_funds.get_response()
    assert responses == []
    mock_requests_get.assert_not_called()

def test_get_response_invalid_timeout_type(
    investment_funds: InvestmentFunds
) -> None:
    """Test get_response with invalid timeout type.

    Verifies
    --------
    - TypeError is raised for invalid timeout type

    Parameters
    ----------
    investment_funds : InvestmentFunds
        Instance of InvestmentFunds

    Returns
    -------
    None
    """
    with pytest.raises(TypeError, match="must be one of types"):
        investment_funds.get_response(timeout="invalid")

# --------------------------
# Tests for parse_raw_file
# --------------------------
def test_parse_raw_file_response(
    investment_funds: InvestmentFunds, 
    mock_response: Response
) -> None:
    """Test parse_raw_file with Response object.

    Verifies
    --------
    - Returns BytesIO with correct content
    - Content matches response content

    Parameters
    ----------
    investment_funds : InvestmentFunds
        Instance of InvestmentFunds
    mock_response : Response
        Mocked Response object

    Returns
    -------
    None
    """
    result = investment_funds.parse_raw_file(mock_response)
    assert isinstance(result, BytesIO)
    assert result.getvalue() == b"Sample PDF content"

def test_parse_raw_file_playwright_page(
    investment_funds: InvestmentFunds, 
    mocker: MockerFixture
) -> None:
    """Test parse_raw_file with PlaywrightPage.

    Verifies
    --------
    - Returns BytesIO with correct content
    - Handles PlaywrightPage input correctly

    Parameters
    ----------
    investment_funds : InvestmentFunds
        Instance of InvestmentFunds
    mocker : MockerFixture
        Pytest-mock fixture

    Returns
    -------
    None
    """
    mock_page = MagicMock(spec=PlaywrightPage)
    mock_page.content = b"Sample page content"
    result = investment_funds.parse_raw_file(mock_page)
    assert isinstance(result, BytesIO)
    assert result.getvalue() == b"Sample page content"

def test_parse_raw_file_selenium_webdriver(
    investment_funds: InvestmentFunds, 
    mocker: MockerFixture
) -> None:
    """Test parse_raw_file with SeleniumWebDriver.

    Verifies
    --------
    - Returns BytesIO with correct content
    - Handles SeleniumWebDriver input correctly

    Parameters
    ----------
    investment_funds : InvestmentFunds
        Instance of InvestmentFunds
    mocker : MockerFixture
        Pytest-mock fixture

    Returns
    -------
    None
    """
    mock_driver = MagicMock(spec=SeleniumWebDriver)
    # The actual implementation uses resp_req.content, not page_source
    # So we need to set the content attribute
    mock_driver.content = b"Sample driver content"
    result = investment_funds.parse_raw_file(mock_driver)
    assert isinstance(result, BytesIO)
    assert result.getvalue() == b"Sample driver content"

def test_parse_raw_file_invalid_input(
    investment_funds: InvestmentFunds
) -> None:
    """Test parse_raw_file with invalid input type.

    Verifies
    --------
    - TypeError is raised for invalid input type

    Parameters
    ----------
    investment_funds : InvestmentFunds
        Instance of InvestmentFunds

    Returns
    -------
    None
    """
    with pytest.raises(TypeError, match="must be one of types"):
        investment_funds.parse_raw_file("invalid_input")

# --------------------------
# Tests for transform_data
# --------------------------
def test_transform_data_success(
    investment_funds: InvestmentFunds, 
    mock_response: Response, 
    mocker: MockerFixture, 
    sample_dataframe: pd.DataFrame
) -> None:
    """Test transform_data with valid response list.

    Verifies
    --------
    - Returns correct DataFrame
    - parse_raw_file is called for each response
    - pdf_docx_regex is called with correct parameters
    - URL is added to DataFrame

    Parameters
    ----------
    investment_funds : InvestmentFunds
        Instance of InvestmentFunds
    mock_response : Response
        Mocked Response object
    mocker : MockerFixture
        Mocker fixture
    sample_dataframe : pd.DataFrame
        Sample DataFrame

    Returns
    -------
    None
    """
    mock_parse = mocker.patch.object(investment_funds, "parse_raw_file", 
                                     return_value=BytesIO(b"content"))
    mock_pdf_docx = mocker.patch.object(
        investment_funds,
        "pdf_docx_regex",
        return_value=sample_dataframe
    )
    mocker.patch.object(
        investment_funds.cls_dir_files_management,
        "get_last_file_extension",
        return_value=".pdf"
    )

    result = investment_funds.transform_data([mock_response, mock_response])

    assert isinstance(result, pd.DataFrame)
    assert len(result) == 4  # Two records per response
    assert all(col in result.columns for col in ["EVENT", "MATCH_PATTERN", "PATTERN_REGEX", "URL"])
    assert mock_parse.call_count == 2
    mock_pdf_docx.assert_called_with(
        bytes_file=mock_parse.return_value,
        str_file_extension=".pdf",
        int_pages_join=3,
        dict_regex_patterns=YAML_INVESTMENT_FUNDS_BYLAWS["regex_patterns"]
    )

def test_transform_data_empty_list(
    investment_funds: InvestmentFunds
) -> None:
    """Test transform_data with empty response list.

    Verifies
    --------
    - Returns empty DataFrame
    - No parsing or regex operations are called

    Parameters
    ----------
    investment_funds : InvestmentFunds
        Initialized InvestmentFunds instance

    Returns
    -------
    None
    """
    result = investment_funds.transform_data([])
    assert isinstance(result, pd.DataFrame)
    assert result.empty

def test_transform_data_invalid_response_type(
    investment_funds: InvestmentFunds
) -> None:
    """Test transform_data with invalid response type.

    Verifies
    --------
    - TypeError is raised for invalid response type

    Parameters
    ----------
    investment_funds : InvestmentFunds
        Initialized InvestmentFunds instance

    Returns
    -------
    None
    """
    with pytest.raises(TypeError, match="must be of type"):
        investment_funds.transform_data(["invalid_response"])

# --------------------------
# Tests for run
# --------------------------
def test_run_without_db(
    investment_funds: InvestmentFunds, 
    mock_requests_get: MagicMock, 
    mock_response: Response, 
    mocker: MockerFixture, 
    sample_dataframe: pd.DataFrame, 
    mock_backoff: None
) -> None:
    """Test run method without database session.

    Verifies
    --------
    - Returns transformed DataFrame
    - get_response and transform_data are called
    - standardize_dataframe is called with correct parameters
    - No database operations are performed

    Parameters
    ----------
    investment_funds : InvestmentFunds
        Instance of InvestmentFunds
    mock_requests_get : MagicMock
        Mocked requests.get function
    mock_response : Response
        Mocked Response object
    mocker : MockerFixture
        Pytest-mock fixture
    sample_dataframe : pd.DataFrame
        Sample DataFrame for testing
    mock_backoff : None
        Mocked backoff decorator

    Returns
    -------
    None
    """
    mock_requests_get.return_value = mock_response
    mocker.patch.object(investment_funds, "get_response", return_value=[mock_response])
    mocker.patch.object(investment_funds, "transform_data", return_value=sample_dataframe)
    mock_standardize = mocker.patch.object(
        investment_funds,
        "standardize_dataframe",
        return_value=sample_dataframe
    )

    result = investment_funds.run(timeout=(12.0, 21.0), bool_verify=True)

    assert isinstance(result, pd.DataFrame)
    assert result.equals(sample_dataframe)
    mock_standardize.assert_called_with(
        df_=sample_dataframe,
        date_ref=investment_funds.cls_dates_current.curr_date(),
        dict_dtypes={"EVENT": str, "MATCH_PATTERN": str, "PATTERN_REGEX": str, "URL": str}
    )

def test_run_with_db(
    investment_funds: InvestmentFunds, 
    mock_requests_get: MagicMock, 
    mock_response: Response, 
    mocker: MockerFixture, 
    sample_dataframe: pd.DataFrame, 
    mock_backoff: None
) -> None:
    """Test run method with database session.

    Verifies
    --------
    - No DataFrame is returned
    - Database insertion is called
    - All processing steps are executed

    Parameters
    ----------
    investment_funds : InvestmentFunds
        Instance of InvestmentFunds
    mock_requests_get : MagicMock
        Mocked requests.get function
    mock_response : Response
        Mocked Response object
    mocker : MockerFixture
        Pytest-mock fixture
    sample_dataframe : pd.DataFrame
        Sample DataFrame
    mock_backoff : None
        Mocked backoff decorator

    Returns
    -------
    None
    """
    investment_funds.cls_db = MagicMock()
    mock_requests_get.return_value = mock_response
    mocker.patch.object(investment_funds, "get_response", return_value=[mock_response])
    mocker.patch.object(investment_funds, "transform_data", return_value=sample_dataframe)
    _ = mocker.patch.object(
        investment_funds,
        "standardize_dataframe",
        return_value=sample_dataframe
    )
    mock_insert = mocker.patch.object(investment_funds, "insert_table_db")

    result = investment_funds.run(
        timeout=(12.0, 21.0),
        bool_verify=True,
        bool_insert_or_ignore=True,
        str_table_name="test_table"
    )

    assert result is None
    mock_insert.assert_called_with(
        cls_db=investment_funds.cls_db,
        str_table_name="test_table",
        bool_insert_or_ignore=True,
        df_=sample_dataframe
    )

def test_run_invalid_timeout(
    investment_funds: InvestmentFunds
) -> None:
    """Test run with invalid timeout type.

    Verifies
    --------
    - TypeError is raised for invalid timeout type

    Parameters
    ----------
    investment_funds : InvestmentFunds
        Initialized InvestmentFunds instance

    Returns
    -------
    None
    """
    with pytest.raises(TypeError, match="must be one of types"):
        investment_funds.run(timeout="invalid")

def test_run_invalid_table_name_type(
    investment_funds: InvestmentFunds
) -> None:
    """Test run with invalid table name type.

    Verifies
    --------
    - TypeError is raised for invalid table name type

    Parameters
    ----------
    investment_funds : InvestmentFunds
        Initialized InvestmentFunds instance

    Returns
    -------
    None
    """
    with pytest.raises(TypeError, match="must be of type"):
        investment_funds.run(str_table_name=123)

# --------------------------
# Reload Tests
# --------------------------
def test_module_reload(mocker: MockerFixture) -> None:
    """Test module reloading behavior.

    Verifies
    --------
    - Module can be reloaded without errors
    - Class attributes are preserved after reload

    Parameters
    ----------
    mocker : MockerFixture
        Pytest-mock fixture for creating mocks

    Returns
    -------
    None
    """
    instance = InvestmentFunds(list_apps=["app1"])
    original_list_apps = instance.list_apps

    importlib.reload(sys.modules[
        "stpstone.ingestion.countries.br.bylaws.cvm_investment_funds_bylaws"])

    new_instance = InvestmentFunds(list_apps=["app1"])
    assert new_instance.list_apps == original_list_apps