"""Unit tests for Anbima550Listing class.

Tests the ingestion functionality with various scenarios including:
- Initialization with valid and invalid inputs
- HTTP response handling
- Data parsing and transformation
- Database insertion and fallback logic
"""

from datetime import date
from io import StringIO
from typing import Union
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from pytest_mock import MockerFixture
from requests import Response

from stpstone.ingestion.countries.br.exchange.anbima_550_listing import Anbima550Listing
from stpstone.utils.calendars.calendar_abc import DatesCurrent
from stpstone.utils.calendars.calendar_br import DatesBRAnbima
from stpstone.utils.loggs.create_logs import CreateLog
from stpstone.utils.parsers.folders import DirFilesManagement


# --------------------------
# Fixtures
# --------------------------
@pytest.fixture
def mock_response() -> Response:
    """Mock Response object with sample content.
    
    Returns
    -------
    Response
        Mocked Response object with predefined content
    """
    response = MagicMock(spec=Response)
    response.content = b"TITULO VENCIMENTO PRECO_UNITARIO PRECO_RETORNO POSICAO_CUSTODIA" \
        + b"\n1 01/01/2025 100.50 101.75 1000.0"
    response.status_code = 200
    response.raise_for_status = MagicMock()
    return response


@pytest.fixture
def mock_requests_get(mocker: MockerFixture) -> object:
    """Mock requests.get to prevent real HTTP calls.
    
    Parameters
    ----------
    mocker : MockerFixture
        Pytest-mock fixture for creating mocks
    
    Returns
    -------
    object
        Mocked requests.get object
    """
    return mocker.patch("requests.get")


@pytest.fixture
def mock_sleep(mocker: MockerFixture) -> object:
    """Mock time.sleep to eliminate delays.
    
    Parameters
    ----------
    mocker : MockerFixture
        Pytest-mock fixture for creating mocks
    
    Returns
    -------
    object
        Mocked time.sleep object
    """
    return mocker.patch("time.sleep")


@pytest.fixture
def mock_backoff(mocker: MockerFixture) -> object:
    """Mock backoff.on_exception to bypass retry delays.
    
    Parameters
    ----------
    mocker : MockerFixture
        Pytest-mock fixture for creating mocks
    
    Returns
    -------
    object
        Mocked backoff.on_exception object
    """
    return mocker.patch("backoff.on_exception", lambda *args, **kwargs: lambda func: func)


@pytest.fixture
def sample_date() -> date:
    """Provide a sample date for testing.
    
    Returns
    -------
    date
        Fixed date for consistent testing
    """
    return date(2025, 1, 1)


@pytest.fixture
def anbima_instance(sample_date: date) -> Anbima550Listing:
    """Fixture providing Anbima550Listing instance.
    
    Parameters
    ----------
    sample_date : date
        Sample date for initialization
    
    Returns
    -------
    Anbima550Listing
        Initialized Anbima550Listing instance
    """
    return Anbima550Listing(date_ref=sample_date)


@pytest.fixture
def sample_file() -> StringIO:
    """Provide sample file content for parsing.
    
    Returns
    -------
    StringIO
        Sample file content
    """
    content = (
        "header line 1\n"
        "header line 2\n"
        "1 01/01/2025 100,50 101,75 1000,0\n"
        "2 02/01/2025 102,25 103,50 2000,0\n"
        "3 03/01/2025 104,75 105,25 3000,0\n"
        "4 04/01/2025 106,50 107,75 4000,0\n"
        "5 05/01/2025 108,25 109,50 5000,0\n"
        "footer1\n"
        "footer2\n"
        "footer3\n"
        "footer4\n"
    )
    return StringIO(content)


# --------------------------
# Tests
# --------------------------
def test_init_with_valid_inputs(sample_date: date) -> None:
    """Test initialization with valid inputs.

    Verifies
    --------
    - The Anbima550Listing instance is initialized correctly
    - Attributes are set as expected
    - Date formatting works correctly

    Parameters
    ----------
    sample_date : date
        Sample date for initialization

    Returns
    -------
    None
    """
    instance = Anbima550Listing(date_ref=sample_date)
    assert instance.date_ref == sample_date
    assert instance.date_ref_yyyymmdd == "20250101"
    assert isinstance(instance.cls_dates_current, DatesCurrent)
    assert isinstance(instance.cls_dates_br, DatesBRAnbima)
    assert isinstance(instance.cls_create_log, CreateLog)
    assert isinstance(instance.cls_dir_files_management, DirFilesManagement)


def test_init_with_default_date() -> None:
    """Test initialization with default date.

    Verifies
    --------
    - Default date is set to previous working day
    - URL is correctly formatted

    Returns
    -------
    None
    """
    with patch.object(DatesBRAnbima, "add_working_days", return_value=date(2025, 1, 1)):
        instance = Anbima550Listing()
        assert instance.date_ref == date(2025, 1, 1)
        assert instance.date_ref_yyyymmdd == "20250101"
        assert instance.url.endswith("20250101_550.tex")


def test_get_response_success(
    anbima_instance: Anbima550Listing, 
    mock_requests_get: object, 
    mock_response: Response, 
    mock_backoff: object
) -> None:
    """Test successful HTTP response retrieval.

    Verifies
    --------
    - Successful HTTP request returns Response object
    - Correct parameters are passed to requests.get
    - No retries are attempted on success

    Parameters
    ----------
    anbima_instance : Anbima550Listing
        Instance of Anbima550Listing
    mock_requests_get : object
        Mocked requests.get function
    mock_response : Response
        Mocked Response object
    mock_backoff : object
        Mocked backoff decorator

    Returns
    -------
    None
    """
    mock_requests_get.return_value = mock_response
    result = anbima_instance.get_response(timeout=(12.0, 21.0), bool_verify=True)
    assert isinstance(result, Response)
    mock_requests_get.assert_called_once_with(anbima_instance.url, timeout=(12.0, 21.0), 
                                              verify=True)
    mock_response.raise_for_status.assert_called_once()


def test_parse_raw_file(anbima_instance: Anbima550Listing, mock_response: Response) -> None:
    """Test parsing of raw file content.

    Verifies
    --------
    - Response content is correctly parsed into StringIO
    - get_file method is called correctly

    Parameters
    ----------
    anbima_instance : Anbima550Listing
        Instance of Anbima550Listing
    mock_response : Response
        Mocked Response object

    Returns
    -------
    None
    """
    with patch.object(anbima_instance, "get_file", return_value=StringIO("test content")) \
        as mock_get_file:
        result = anbima_instance.parse_raw_file(mock_response)
        assert isinstance(result, StringIO)
        mock_get_file.assert_called_once_with(resp_req=mock_response)


def test_transform_data(anbima_instance: Anbima550Listing, sample_file: StringIO) -> None:
    """Test data transformation into DataFrame.

    Verifies
    --------
    - Input file is correctly transformed into DataFrame
    - Column names and data types are correct
    - Thousands and decimal separators are handled correctly

    Parameters
    ----------
    anbima_instance : Anbima550Listing
        Instance of Anbima550Listing
    sample_file : StringIO
        Sample file content

    Returns
    -------
    None
    """
    df_ = anbima_instance.transform_data(sample_file)
    assert isinstance(df_, pd.DataFrame)
    assert not df_.empty, "DataFrame should not be empty"
    assert list(df_.columns) == ["TITULO", "VENCIMENTO", "PRECO_UNITARIO", "PRECO_RETORNO", 
                                "POSICAO_CUSTODIA"]
    assert df_["TITULO"].iloc[0] == 1
    assert df_["PRECO_UNITARIO"].iloc[0] == pytest.approx(100.50, rel=1e-3)
    assert df_["POSICAO_CUSTODIA"].iloc[0] == pytest.approx(1000.0, rel=1e-3)


def test_run_without_db(
    anbima_instance: Anbima550Listing, 
    mock_requests_get: object, 
    mock_response: Response, 
    mock_backoff: object
) -> None:
    """Test run method without database session.

    Verifies
    --------
    - Full ingestion pipeline works without database
    - Returns transformed DataFrame
    - All intermediate methods are called correctly

    Parameters
    ----------
    anbima_instance : Anbima550Listing
        Instance of Anbima550Listing
    mock_requests_get : object
        Mocked requests.get function
    mock_response : Response
        Mocked Response object
    mock_backoff : object
        Mocked backoff decorator

    Returns
    -------
    None
    """
    mock_requests_get.return_value = mock_response
    with patch.object(anbima_instance, "parse_raw_file", return_value=StringIO("test content")) \
        as mock_parse, \
        patch.object(anbima_instance, "transform_data", 
                          return_value=pd.DataFrame({"TITULO": [1]})) as mock_transform, \
        patch.object(anbima_instance, "standardize_dataframe", 
                     return_value=pd.DataFrame({"TITULO": [1]})) as mock_standardize:
        result = anbima_instance.run()
        assert isinstance(result, pd.DataFrame)
        mock_parse.assert_called_once()
        mock_transform.assert_called_once()
        mock_standardize.assert_called_once()


def test_run_with_db(
    anbima_instance: Anbima550Listing, 
    mock_requests_get: object, 
    mock_response: Response, 
    mock_backoff: object
) -> None:
    """Test run method with database session.

    Verifies
    --------
    - Database insertion is called when cls_db is provided
    - No DataFrame is returned
    - All intermediate methods are called correctly

    Parameters
    ----------
    anbima_instance : Anbima550Listing
        Instance of Anbima550Listing
    mock_requests_get : object
        Mocked requests.get function
    mock_response : Response
        Mocked Response object
    mock_backoff : object
        Mocked backoff decorator

    Returns
    -------
    None
    """
    mock_db = MagicMock()
    anbima_instance.cls_db = mock_db
    mock_requests_get.return_value = mock_response
    with patch.object(anbima_instance, "parse_raw_file", 
                      return_value=StringIO("test content")) as mock_parse, \
        patch.object(anbima_instance, "transform_data", 
                     return_value=pd.DataFrame({"TITULO": [1]})) as mock_transform, \
        patch.object(anbima_instance, "standardize_dataframe", 
                     return_value=pd.DataFrame({"TITULO": [1]})) as mock_standardize, \
        patch.object(anbima_instance, "insert_table_db") as mock_insert:
        result = anbima_instance.run()
        assert result is None
        mock_parse.assert_called_once()
        mock_transform.assert_called_once()
        mock_standardize.assert_called_once()
        mock_insert.assert_called_once()


@pytest.mark.parametrize("timeout", [
    10,
    10.5,
    (5.0, 10.0),
    (5, 10),
])
def test_get_response_timeout_variations(
    anbima_instance: Anbima550Listing, 
    mock_requests_get: object, 
    mock_response: Response, 
    mock_backoff: object, 
    timeout: Union[int, float, tuple]
) -> None:
    """Test get_response with various timeout inputs.

    Verifies
    --------
    - Different timeout formats are handled correctly
    - Requests are made with correct timeout parameters

    Parameters
    ----------
    anbima_instance : Anbima550Listing
        Instance of Anbima550Listing
    mock_requests_get : object
        Mocked requests.get function
    mock_response : Response
        Mocked Response object
    mock_backoff : object
        Mocked backoff decorator
    timeout : Union[int, float, tuple]
        Timeout value or tuple to test

    Returns
    -------
    None
    """
    mock_requests_get.return_value = mock_response
    result = anbima_instance.get_response(timeout=timeout)
    assert isinstance(result, Response)
    mock_requests_get.assert_called_once_with(anbima_instance.url, timeout=timeout, verify=True)


@pytest.mark.parametrize("invalid_input,expected_error", [
    (None, "resp_req must be one of types: Response, Page, WebDriver, got NoneType"),
    ("invalid", "resp_req must be one of types: Response, Page, WebDriver, got str"),
    (123, "resp_req must be one of types: Response, Page, WebDriver, got int"),
    ([], "resp_req must be one of types: Response, Page, WebDriver, got list"),
])
def test_parse_raw_file_invalid_input(
    anbima_instance: Anbima550Listing, 
    invalid_input: Union[None, str, int, list], 
    expected_error: str
) -> None:
    """Test parse_raw_file with invalid inputs.

    Verifies
    --------
    - Invalid response types raise appropriate exceptions
    - Type checker enforces correct input types

    Parameters
    ----------
    anbima_instance : Anbima550Listing
        Instance of Anbima550Listing
    invalid_input : Union[None, str, int, list]
        Invalid input to test error handling
    expected_error : str
        Expected error message from type checker

    Returns
    -------
    None
    """
    with pytest.raises(TypeError, match=expected_error):
        anbima_instance.parse_raw_file(invalid_input)


def test_transform_data_empty_file(anbima_instance: Anbima550Listing) -> None:
    """Test transform_data with empty file.

    Verifies
    --------
    - Empty file input results in empty DataFrame
    - No errors are raised for empty input

    Parameters
    ----------
    anbima_instance : Anbima550Listing
        Instance of Anbima550Listing

    Returns
    -------
    None
    """
    empty_file = StringIO("")
    df_ = anbima_instance.transform_data(empty_file)
    assert isinstance(df_, pd.DataFrame)
    assert df_.empty


def test_reload_module() -> None:
    """Test module reloading behavior.

    Verifies
    --------
    - Module can be reloaded without errors
    - Instance attributes are preserved after reload

    Returns
    -------
    None
    """
    import importlib

    import stpstone.ingestion.countries.br.exchange.anbima_550_listing
    original_instance = Anbima550Listing(date_ref=date(2025, 1, 1))
    importlib.reload(stpstone.ingestion.countries.br.exchange.anbima_550_listing)
    new_instance = Anbima550Listing(date_ref=date(2025, 1, 1))
    assert new_instance.date_ref == original_instance.date_ref
    assert new_instance.url == original_instance.url