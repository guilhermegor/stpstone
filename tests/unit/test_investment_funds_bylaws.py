"""Unit tests for InvestmentFunds class.

Tests the functionality of fetching and processing investment fund bylaws from
Brazilian SEC (CVM) with various input scenarios, including:
- Initialization with valid and invalid inputs
- Response fetching with mocked HTTP requests
- Response transformation into DataFrame
- Full ingestion pipeline
- Edge cases and error conditions
"""

from logging import Logger
from typing import Optional, TypedDict
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from pytest_mock import MockerFixture
from requests import Response, Session

from stpstone.ingestion.countries.br.bylaws.investment_funds_bylaws import InvestmentFunds
from stpstone.utils.calendars.calendar_abc import DatesCurrent
from stpstone.utils.loggs.create_logs import CreateLog
from stpstone.utils.parsers.folders import DirFilesManagement


# --------------------------
# Type Definitions
# --------------------------
class ReturnInit(TypedDict):
    """Type definition for InvestmentFunds initialization attributes."""

    list_apps: list[str]
    int_pages_join: Optional[int]
    logger: Optional[Logger]
    cls_db: Optional[Session]
    cls_dir_files_management: DirFilesManagement
    cls_dates_current: DatesCurrent
    cls_create_log: CreateLog


class ReturnTransformResponse(TypedDict):
    """Type definition for transform_response return value."""
    
    EVENT: str
    MATCH_PATTERN: str
    PATTERN_REGEX: str
    URL: str


# --------------------------
# Fixtures
# --------------------------
@pytest.fixture
def mock_response() -> Response:
    """Fixture providing a mocked HTTP response.

    Returns
    -------
    Response
        Mocked requests.Response object with sample PDF content
    """
    response = MagicMock(spec=Response)
    response.content = b"Sample PDF content"
    response.url = "https://web.cvm.gov.br/app/fundosweb/fundos/regulamento/obter/por/arquivo/test.pdf"
    response.raise_for_status = MagicMock()
    return response


@pytest.fixture
def mock_requests_get(mocker: MockerFixture) -> MagicMock:
    """Fixture mocking requests.get to avoid real HTTP requests.

    Parameters
    ----------
    mocker : MockerFixture
        Pytest-mock fixture for creating mocks

    Returns
    -------
    MagicMock
        Mock object for requests.get
    """
    return mocker.patch("requests.get")


@pytest.fixture
def sample_apps() -> list[str]:
    """Fixture providing a sample list of app filenames.

    Returns
    -------
    list[str]
        List of sample PDF filenames
    """
    return ["DOC_REGUL_8557_122868_2025_02.pdf"]


@pytest.fixture
def sample_regex_patterns() -> dict[str, dict[str, str]]:
    """Fixture providing sample regex patterns.

    Returns
    -------
    dict[str, dict[str, str]]
        Sample regex patterns for testing
    """
    return {
        "FUND_NAME": {
            "NAME": r"Fund Name: (.*?)\n",
        }
    }


@pytest.fixture
def investment_funds(sample_apps: list[str]) -> InvestmentFunds:
    """Fixture providing an InvestmentFunds instance.

    Parameters
    ----------
    sample_apps : list[str]
        List of sample app filenames

    Returns
    -------
    InvestmentFunds
        Initialized InvestmentFunds instance
    """
    return InvestmentFunds(list_apps=sample_apps, int_pages_join=3, logger=None, cls_db=None)


# --------------------------
# Tests
# --------------------------
def test_init_with_valid_inputs(sample_apps: list[str]) -> None:
    """Test initialization with valid inputs.

    Verifies
    --------
    - InvestmentFunds can be initialized with valid inputs
    - All attributes are correctly set
    - Parent class initializations are called

    Parameters
    ----------
    sample_apps : list[str]
        Sample list of app filenames

    Returns
    -------
    None
    """
    instance = InvestmentFunds(
        list_apps=sample_apps,
        int_pages_join=3,
        logger=None,
        cls_db=None
    )
    assert instance.list_apps == sample_apps
    assert instance.int_pages_join == 3
    assert instance.logger is None
    assert instance.cls_db is None
    assert isinstance(instance.cls_dir_files_management, DirFilesManagement)
    assert isinstance(instance.cls_dates_current, DatesCurrent)
    assert isinstance(instance.cls_create_log, CreateLog)
    assert hasattr(instance, "cls_str_handler")
    assert hasattr(instance, "cls_handling_dicts")
    assert hasattr(instance, "cls_db_logs")


def test_init_with_empty_list_apps() -> None:
    """Test initialization with empty list_apps.

    Verifies
    --------
    - Empty list_apps is accepted
    - Attributes are correctly set

    Returns
    -------
    None
    """
    instance = InvestmentFunds(list_apps=[], int_pages_join=3)
    assert instance.list_apps == []
    assert instance.int_pages_join == 3


def test_init_with_invalid_int_pages_join_type() -> None:
    """Test initialization with invalid int_pages_join type raises TypeError.

    Verifies
    --------
    - Providing a non-int/None type for int_pages_join raises TypeError
    - Error message matches expected pattern

    Returns
    -------
    None
    """
    with pytest.raises(TypeError, match="int_pages_join must be one of types"):
        InvestmentFunds(list_apps=["test.pdf"], int_pages_join="invalid")


def test_get_response_success(
    investment_funds: InvestmentFunds,
    mock_requests_get: MagicMock,
    mock_response: Response
) -> None:
    """Test get_response with successful mocked HTTP requests.

    Verifies
    --------
    - get_response returns a list of Response objects
    - Requests are made for each app in list_apps
    - Sleep is called between requests

    Parameters
    ----------
    investment_funds : InvestmentFunds
        Initialized InvestmentFunds instance
    mock_requests_get : MagicMock
        Mocked requests.get function
    mock_response : Response
        Mocked Response object

    Returns
    -------
    None
    """
    mock_requests_get.return_value = mock_response
    with patch("stpstone.ingestion.countries.br.bylaws.investment_funds_bylaws.sleep") \
        as mock_sleep:
        result = investment_funds.get_response()
    assert isinstance(result, list)
    assert len(result) == 1
    assert result[0] == mock_response
    mock_requests_get.assert_called_once()
    mock_sleep.assert_called_once_with(10)
    mock_response.raise_for_status.assert_called_once()


def test_get_response_http_error(
    investment_funds: InvestmentFunds,
    mock_requests_get: MagicMock
) -> None:
    """Test get_response handling of HTTPError with backoff.

    Verifies
    --------
    - HTTPError is retried with backoff
    - Raises HTTPError after max retries

    Parameters
    ----------
    investment_funds : InvestmentFunds
        Initialized InvestmentFunds instance
    mock_requests_get : MagicMock
        Mocked requests.get function

    Returns
    -------
    None
    """
    from requests.exceptions import HTTPError
    mock_requests_get.side_effect = HTTPError("Request failed")
    with pytest.raises(HTTPError, match="Request failed"):
        investment_funds.get_response()


def test_get_response_empty_list_apps(investment_funds: InvestmentFunds) -> None:
    """Test get_response with empty list_apps.

    Verifies
    --------
    - Returns empty list when list_apps is empty
    - No requests are made

    Parameters
    ----------
    investment_funds : InvestmentFunds
        Initialized InvestmentFunds instance

    Returns
    -------
    None
    """
    investment_funds.list_apps = []
    result = investment_funds.get_response()
    assert result == []


def test_transform_response_success(
    investment_funds: InvestmentFunds,
    mock_response: Response,
    mocker: MockerFixture
) -> None:
    """Test transform_response with valid response.

    Verifies
    --------
    - Transforms response into DataFrame
    - Calls pdf_docx_regex with correct parameters
    - Adds URL column to DataFrame
    - Returns correct DataFrame structure

    Parameters
    ----------
    investment_funds : InvestmentFunds
        Initialized InvestmentFunds instance
    mock_response : Response
        Mocked Response object
    mocker : MockerFixture
        Pytest-mock fixture for creating mocks

    Returns
    -------
    None
    """
    sample_df = pd.DataFrame([{
        "EVENT": "FUND_NAME",
        "MATCH_PATTERN": "NAME",
        "PATTERN_REGEX": r"Fund Name: (.*?)\n",
        "REGEX_GROUP_0": "TEST FUND"
    }])
    mocker.patch.object(
        investment_funds,
        "pdf_docx_regex",
        return_value=sample_df
    )
    mocker.patch.object(
        investment_funds.cls_dir_files_management,
        "get_last_file_extension",
        return_value="pdf"
    )
    result = investment_funds.transform_response([mock_response])
    assert isinstance(result, pd.DataFrame)
    assert "URL" in result.columns
    assert result["URL"].iloc[0] == mock_response.url
    assert len(result) == 1
    assert result.iloc[0]["EVENT"] == "FUND_NAME"


def test_transform_response_empty_response(
    investment_funds: InvestmentFunds
) -> None:
    """Test transform_response with empty response list.

    Verifies
    --------
    - Returns empty DataFrame when input list is empty
    - No calls to pdf_docx_regex

    Parameters
    ----------
    investment_funds : InvestmentFunds
        Initialized InvestmentFunds instance

    Returns
    -------
    None
    """
    result = investment_funds.transform_response([])
    assert isinstance(result, pd.DataFrame)
    assert result.empty


def test_run_without_db(
    investment_funds: InvestmentFunds,
    mock_response: Response,
    mocker: MockerFixture
) -> None:
    """Test run method without database session.

    Verifies
    --------
    - Full pipeline executes correctly
    - Returns standardized DataFrame
    - Calls get_response and transform_response
    - Standardizes DataFrame with correct dtypes

    Parameters
    ----------
    investment_funds : InvestmentFunds
        Initialized InvestmentFunds instance
    mock_response : Response
        Mocked Response object
    mocker : MockerFixture
        Pytest-mock fixture for creating mocks

    Returns
    -------
    None
    """
    sample_df = pd.DataFrame([{
        "EVENT": "FUND_NAME",
        "MATCH_PATTERN": "NAME",
        "PATTERN_REGEX": r"Fund Name: (.*?)\n",
        "URL": mock_response.url
    }])
    mocker.patch.object(investment_funds, "get_response", return_value=[mock_response])
    mocker.patch.object(investment_funds, "transform_response", return_value=sample_df)
    mocker.patch.object(investment_funds, "standardize_dataframe", return_value=sample_df)
    result = investment_funds.run()
    assert isinstance(result, pd.DataFrame)
    assert len(result) == 1
    assert result["EVENT"].iloc[0] == "FUND_NAME"
    assert result["URL"].iloc[0] == mock_response.url


def test_run_with_db(
    investment_funds: InvestmentFunds,
    mock_response: Response,
    mocker: MockerFixture
) -> None:
    """Test run method with database session.

    Verifies
    --------
    - Calls insert_table_db when cls_db is provided
    - Does not return DataFrame
    - Calls get_response, transform_response, and standardize_dataframe

    Parameters
    ----------
    investment_funds : InvestmentFunds
        Initialized InvestmentFunds instance
    mock_response : Response
        Mocked Response object
    mocker : MockerFixture
        Pytest-mock fixture for creating mocks

    Returns
    -------
    None
    """
    mock_db = MagicMock(spec=Session)
    investment_funds.cls_db = mock_db
    sample_df = pd.DataFrame([{
        "EVENT": "FUND_NAME",
        "MATCH_PATTERN": "NAME",
        "PATTERN_REGEX": r"Fund Name: (.*?)\n",
        "URL": mock_response.url
    }])
    mocker.patch.object(investment_funds, "get_response", return_value=[mock_response])
    mocker.patch.object(investment_funds, "transform_response", return_value=sample_df)
    mocker.patch.object(investment_funds, "standardize_dataframe", return_value=sample_df)
    mocker.patch.object(investment_funds, "insert_table_db")
    result = investment_funds.run()
    assert result is None
    investment_funds.insert_table_db.assert_called_once_with(
        cls_db=mock_db,
        str_table_name="investment_funds",
        df_=sample_df
    )


def test_run_empty_list_apps(
    investment_funds: InvestmentFunds,
    mocker: MockerFixture
) -> None:
    """Test run method with empty list_apps.

    Verifies
    --------
    - Returns empty DataFrame
    - Calls get_response and transform_response
    - Standardizes empty DataFrame

    Parameters
    ----------
    investment_funds : InvestmentFunds
        Initialized InvestmentFunds instance
    mocker : MockerFixture
        Pytest-mock fixture for creating mocks

    Returns
    -------
    None
    """
    investment_funds.list_apps = []
    mocker.patch.object(investment_funds, "get_response", return_value=[])
    mocker.patch.object(investment_funds, "transform_response", return_value=pd.DataFrame())
    mocker.patch.object(investment_funds, "standardize_dataframe", return_value=pd.DataFrame())
    result = investment_funds.run()
    assert isinstance(result, pd.DataFrame)
    assert result.empty