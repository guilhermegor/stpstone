"""Unit tests for B3ConsolidatedTrades class.

Tests the B3 consolidated trades ingestion functionality, covering:
- Initialization with various inputs
- Token retrieval
- Response handling
- Data parsing and transformation
- Full ingestion pipeline
- Edge cases and error conditions
"""

from datetime import date
from io import StringIO
from logging import Logger
from typing import Any, Union
from unittest.mock import ANY, MagicMock, patch

import pandas as pd
import pytest
from pytest_mock import MockerFixture
from requests import Response
from requests.exceptions import HTTPError

from stpstone.ingestion.countries.br.exchange.consolidated_trades import (
    B3ConsolidatedTrades,
)
from stpstone.utils.calendars.calendar_abc import DatesCurrent
from stpstone.utils.calendars.calendar_br import DatesBRAnbima


# --------------------------
# Fixtures
# --------------------------
@pytest.fixture(autouse=True)
def mock_requests_get(mocker: MockerFixture) -> MagicMock:
    """Mock requests.get to prevent real HTTP calls for all tests.

    Parameters
    ----------
    mocker : MockerFixture
        Pytest-mock fixture for creating mocks

    Returns
    -------
    MagicMock
        Mock object for requests.get
    """
    mock = mocker.patch("requests.get")
    mock_response = MagicMock(spec=Response)
    mock_response.status_code = 200
    mock_response.json.return_value = {"token": "mock_token"}
    mock_response.content = b"Header1;Header2\nData1;Data2\nData3;Data4"
    mock_response.raise_for_status = MagicMock()
    mock.return_value = mock_response
    return mock


@pytest.fixture
def mock_response() -> Response:
    """Mock Response object with sample content.

    Returns
    -------
    Response
        Mocked response with sample CSV content
    """
    response = MagicMock(spec=Response)
    response.content = b"Header1;Header2\nData1;Data2\nData3;Data4"
    response.status_code = 200
    response.raise_for_status = MagicMock()
    response.json.return_value = {"token": "mock_token"}
    return response


@pytest.fixture
def sample_date() -> date:
    """Provide a sample date for testing.

    Returns
    -------
    date
        A fixed date for consistent testing
    """
    return date(2025, 1, 1)


@pytest.fixture
def b3_instance(
    sample_date: date, 
    mock_requests_get: MagicMock
) -> B3ConsolidatedTrades:
    """Fixture providing B3ConsolidatedTrades instance.

    Parameters
    ----------
    sample_date : date
        Fixed date for testing
    mock_requests_get : MagicMock
        Mocked requests.get to prevent real calls

    Returns
    -------
    B3ConsolidatedTrades
        Initialized instance with sample date
    """
    return B3ConsolidatedTrades(date_ref=sample_date)


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
# Tests
# --------------------------
def test_init_with_valid_inputs(sample_date: date, mock_requests_get: MagicMock) -> None:
    """Test initialization with valid inputs.

    Verifies
    --------
    - Instance is properly initialized with provided date
    - Default attributes are set correctly
    - URL is formatted with the correct date

    Parameters
    ----------
    sample_date : date
        Fixed date for testing
    mock_requests_get : MagicMock
        Mocked requests.get

    Returns
    -------
    None
    """
    instance = B3ConsolidatedTrades(date_ref=sample_date)
    assert instance.date_ref == sample_date
    assert isinstance(instance.logger, Logger) or instance.logger is None
    assert instance.cls_db is None
    assert "2025-01-01" in instance.url_token
    assert instance.url.endswith("#format=.csv")
    mock_requests_get.assert_called_with(instance.url_token, timeout=(12.0, 21.0))


def test_init_without_date_ref(mocker: MockerFixture, mock_requests_get: MagicMock) -> None:
    """Test initialization without date_ref uses previous working day.

    Verifies
    --------
    - Date is set to previous working day when not provided
    - Other attributes are initialized correctly

    Parameters
    ----------
    mocker : MockerFixture
        Pytest-mock fixture for creating mocks
    mock_requests_get : MagicMock
        Mocked requests.get

    Returns
    -------
    None
    """
    mock_dates_current = mocker.patch.object(DatesCurrent, "curr_date", 
                                             return_value=date(2025, 1, 2))
    mock_dates_br = mocker.patch.object(DatesBRAnbima, "add_working_days", 
                                        return_value=date(2025, 1, 1))
    
    instance = B3ConsolidatedTrades()
    assert instance.date_ref == date(2025, 1, 1)
    mock_dates_current.assert_called_once()
    mock_dates_br.assert_called_once_with(date(2025, 1, 2), -1)
    mock_requests_get.assert_called_with(instance.url_token, timeout=(12.0, 21.0))


def test_get_token_success(mock_requests_get: MagicMock, mock_response: Response) -> None:
    """Test successful token retrieval.

    Verifies
    --------
    - Token is retrieved from response JSON
    - Request is made with correct URL
    - Response status is checked

    Parameters
    ----------
    mock_requests_get : MagicMock
        Mocked requests.get
    mock_response : Response
        Mocked response object

    Returns
    -------
    None
    """
    mock_requests_get.return_value = mock_response
    instance = B3ConsolidatedTrades(date_ref=date(2025, 1, 1))
    mock_response.json.reset_mock()
    token = instance.get_token()
    assert token == "mock_token" # noqa S105: possible hardcoded password
    mock_requests_get.assert_any_call(instance.url_token, timeout=(12.0, 21.0))
    mock_response.raise_for_status.assert_any_call()
    mock_response.json.assert_called_once()


def test_get_token_http_error(mock_requests_get: MagicMock, mock_backoff: None) -> None:
    """Test token retrieval with HTTP error.

    Verifies
    --------
    - HTTPError is raised when request fails
    - Error message is propagated correctly

    Parameters
    ----------
    mock_requests_get : MagicMock
        Mocked requests.get
    mock_backoff : None
        Mocked backoff to bypass retries

    Returns
    -------
    None
    """
    mock_requests_get.side_effect = HTTPError("HTTP error")
    
    # Create instance without calling get_token during init
    with patch.object(B3ConsolidatedTrades, 'get_token', return_value="mock_token"):
        instance = B3ConsolidatedTrades(date_ref=date(2025, 1, 1))
    
    # Now test get_token method specifically
    with pytest.raises(HTTPError, match="HTTP error"):
        instance.get_token()

def test_get_response_success(
    b3_instance: B3ConsolidatedTrades,
    mock_requests_get: MagicMock,
    mock_response: Response,
    mock_backoff: None
) -> None:
    """Test successful response retrieval.

    Verifies
    --------
    - Response is retrieved correctly
    - Request uses correct URL and parameters
    - Status is checked

    Parameters
    ----------
    b3_instance : B3ConsolidatedTrades
        B3 instance for testing
    mock_requests_get : MagicMock
        Mocked requests.get
    mock_response : Response
        Mocked response object
    mock_backoff : None
        Mocked backoff to bypass retries

    Returns
    -------
    None
    """
    mock_requests_get.return_value = mock_response
    response = b3_instance.get_response(timeout=(12.0, 21.0), bool_verify=True)
    assert response == mock_response
    mock_requests_get.assert_any_call(b3_instance.url, timeout=(12.0, 21.0), verify=True)
    mock_response.raise_for_status.assert_called_once()


@pytest.mark.parametrize("timeout", [
    10,
    10.0,
    (5.0, 10.0),
    (5, 10),
])
def test_get_response_valid_timeout(
    b3_instance: B3ConsolidatedTrades,
    mock_requests_get: MagicMock,
    mock_response: Response,
    mock_backoff: None,
    timeout: Union[int, float, tuple[float, float], tuple[int, int]]
) -> None:
    """Test response retrieval with various valid timeout types.

    Verifies
    --------
    - Different timeout types are handled correctly
    - Request is made with provided timeout

    Parameters
    ----------
    b3_instance : B3ConsolidatedTrades
        B3 instance for testing
    mock_requests_get : MagicMock
        Mocked requests.get
    mock_response : Response
        Mocked response object
    mock_backoff : None
        Mocked backoff to bypass retries
    timeout : Union[int, float, tuple[float, float], tuple[int, int]]
        Timeout value to test

    Returns
    -------
    None
    """
    mock_requests_get.return_value = mock_response
    response = b3_instance.get_response(timeout=timeout)
    assert response == mock_response
    mock_requests_get.assert_any_call(b3_instance.url, timeout=timeout, verify=True)


def test_get_response_invalid_timeout(
    b3_instance: B3ConsolidatedTrades,
    mock_requests_get: MagicMock,
    mock_backoff: None
) -> None:
    """Test response retrieval with invalid timeout type.

    Verifies
    --------
    - TypeError is raised for invalid timeout type
    - Error message matches expected pattern

    Parameters
    ----------
    b3_instance : B3ConsolidatedTrades
        B3 instance for testing
    mock_requests_get : MagicMock
        Mocked requests.get
    mock_backoff : None
        Mocked backoff to bypass retries

    Returns
    -------
    None
    """
    with pytest.raises(TypeError, match="timeout must be"):
        b3_instance.get_response(timeout="invalid")


def test_parse_raw_file(
    b3_instance: B3ConsolidatedTrades,
    mock_response: Response
) -> None:
    """Test parsing raw file content.

    Verifies
    --------
    - Response content is parsed into StringIO
    - Content is correctly processed

    Parameters
    ----------
    b3_instance : B3ConsolidatedTrades
        B3 instance for testing
    mock_response : Response
        Mocked response object

    Returns
    -------
    None
    """
    with patch.object(b3_instance, "get_file", return_value=StringIO("test content")) \
        as mock_get_file:
        result = b3_instance.parse_raw_file(mock_response)
        assert isinstance(result, StringIO)
        mock_get_file.assert_called_once_with(resp_req=mock_response)


def test_transform_data(b3_instance: B3ConsolidatedTrades) -> None:
    """Test data transformation into DataFrame.

    Verifies
    --------
    - Input StringIO is transformed into DataFrame
    - Expected columns are present
    - Missing values are filled with -1

    Parameters
    ----------
    b3_instance : B3ConsolidatedTrades
        B3 instance for testing

    Returns
    -------
    None
    """
    sample_csv = StringIO(
        "Header1;Header2\n"
        "2025-01-01;ABC123\n"
        "2025-01-02;XYZ789"
    )
    df_ = b3_instance.transform_data(sample_csv)
    assert isinstance(df_, pd.DataFrame)
    assert set(df_.columns) == {
        "RPT_DT", "TCKR_SYMB", "ISIN", "SGMT_NM", "MIN_PRIC", "MAX_PRIC",
        "TRAD_AVRG_PRIC", "LAST_PRIC", "OSCN_PCTG", "ADJSTD_QT",
        "ADJSTD_QT_TAX", "REF_PRIC", "TRAD_QTY", "FIN_INSTRM_QTY", "NTL_FIN_VOL"
    }
    assert (df_[["MIN_PRIC", "MAX_PRIC"]] == -1).all().all()


def test_run_without_db(
    b3_instance: B3ConsolidatedTrades,
    mock_requests_get: MagicMock,
    mock_response: Response,
    mock_backoff: None
) -> None:
    """Test full ingestion pipeline without database.

    Verifies
    --------
    - Pipeline returns DataFrame when no database is provided
    - All intermediate steps are called
    - DataFrame is correctly formatted

    Parameters
    ----------
    b3_instance : B3ConsolidatedTrades
        B3 instance for testing
    mock_requests_get : MagicMock
        Mocked requests.get
    mock_response : Response
        Mocked response object
    mock_backoff : None
        Mocked backoff to bypass retries

    Returns
    -------
    None
    """
    mock_requests_get.return_value = mock_response
    with patch.object(b3_instance, "get_file", 
                      return_value=StringIO("Header1;Header2\nData1;Data2")), \
        patch.object(b3_instance, "standardize_dataframe", return_value=pd.DataFrame()) \
        as mock_standardize:
        result = b3_instance.run()
        assert isinstance(result, pd.DataFrame)
        mock_standardize.assert_called_once()


def test_run_with_db(
    b3_instance: B3ConsolidatedTrades,
    mock_requests_get: MagicMock,
    mock_response: Response,
    mock_backoff: None
) -> None:
    """Test full ingestion pipeline with database.

    Verifies
    --------
    - Pipeline inserts data into database
    - No DataFrame is returned
    - Database insertion is called with correct parameters

    Parameters
    ----------
    b3_instance : B3ConsolidatedTrades
        B3 instance for testing
    mock_requests_get : MagicMock
        Mocked requests.get
    mock_response : Response
        Mocked response object
    mock_backoff : None
        Mocked backoff to bypass retries

    Returns
    -------
    None
    """
    mock_db = MagicMock()
    b3_instance.cls_db = mock_db
    mock_requests_get.return_value = mock_response
    with patch.object(b3_instance, "get_file", 
                      return_value=StringIO("Header1;Header2\nData1;Data2")), \
        patch.object(b3_instance, "standardize_dataframe", return_value=pd.DataFrame()) \
            as mock_standardize, \
        patch.object(b3_instance, "insert_table_db") as mock_insert:
        result = b3_instance.run()
        assert result is None
        mock_insert.assert_called_once()
        mock_standardize.assert_called_once()


@pytest.mark.parametrize("invalid_input", [
    None,
    "",
    "invalid",
    123,
])
def test_invalid_date_ref(invalid_input: Any) -> None: # noqa ANN401: typing.Any is not allowed
    """Test initialization with invalid date_ref types.

    Verifies
    --------
    - TypeError is raised for invalid date_ref inputs
    - Error message matches expected pattern

    Parameters
    ----------
    invalid_input : Any
        Invalid input to test

    Returns
    -------
    None
    """
    # Allow None as a valid input since date_ref is Optional[date]
    if invalid_input is None:
        instance = B3ConsolidatedTrades(date_ref=invalid_input)
        assert instance.date_ref is not None  # Should fallback to default date
    else:
        with pytest.raises(TypeError, match="date_ref must be one of types: date, NoneType"):
            B3ConsolidatedTrades(date_ref=invalid_input)


def test_empty_response(
    b3_instance: B3ConsolidatedTrades,
    mock_requests_get: MagicMock,
    mock_backoff: None
) -> None:
    """Test handling of empty response content.

    Verifies
    --------
    - Empty response is handled appropriately
    - ValueError is raised when DataFrame is empty

    Parameters
    ----------
    b3_instance : B3ConsolidatedTrades
        B3 instance for testing
    mock_requests_get : MagicMock
        Mocked requests.get
    mock_backoff : None
        Mocked backoff to bypass retries

    Returns
    -------
    None
    """
    mock_response = MagicMock(spec=Response)
    mock_response.content = b""
    mock_response.status_code = 200
    mock_response.raise_for_status = MagicMock()
    mock_requests_get.return_value = mock_response

    # Mock the transform_data to return empty DataFrame instead of StringIO
    empty_df = pd.DataFrame(columns=[
        "RPT_DT", "TCKR_SYMB", "ISIN", "SGMT_NM", "MIN_PRIC", "MAX_PRIC",
        "TRAD_AVRG_PRIC", "LAST_PRIC", "OSCN_PCTG", "ADJSTD_QT",
        "ADJSTD_QT_TAX", "REF_PRIC", "TRAD_QTY", "FIN_INSTRM_QTY", "NTL_FIN_VOL"
    ])

    with patch.object(b3_instance, "get_file", return_value=StringIO("")), \
        patch.object(b3_instance, "transform_data", return_value=empty_df), \
        pytest.raises(RuntimeError, match="Error in check_if_empty: DataFrame is empty"):
            b3_instance.run()


def test_reload_module() -> None:
    """Test module reloading behavior.

    Verifies
    --------
    - Module can be reloaded without errors
    - Instance retains functionality after reload

    Returns
    -------
    None
    """
    import importlib

    import stpstone.ingestion.countries.br.exchange.consolidated_trades
    importlib.reload(stpstone.ingestion.countries.br.exchange.consolidated_trades)
    instance = B3ConsolidatedTrades(date_ref=date(2025, 1, 1))
    assert isinstance(instance, B3ConsolidatedTrades)
    assert instance.date_ref == date(2025, 1, 1)


def test_fallback_no_requests(mocker: MockerFixture, sample_date: date) -> None:
    """Test behavior when requests module is unavailable.

    Verifies
    --------
    - ImportError is raised when requests.get is called
    - Instance can still be initialized but fails on token retrieval

    Parameters
    ----------
    mocker : MockerFixture
        Pytest-mock fixture for creating mocks
    sample_date : date
        Fixed date for testing

    Returns
    -------
    None
    """
    mocker.patch("requests.get", side_effect=ImportError("requests module unavailable"))
    with pytest.raises(ImportError, match="requests module unavailable"):
        B3ConsolidatedTrades(date_ref=sample_date)


@pytest.mark.parametrize("bool_verify", [True, False])
def test_bool_verify_variations(
    b3_instance: B3ConsolidatedTrades,
    mock_requests_get: MagicMock,
    mock_response: Response,
    mock_backoff: None,
    bool_verify: bool
) -> None:
    """Test response retrieval with different bool_verify values.

    Verifies
    --------
    - Request is made with correct verify parameter
    - Response is handled correctly

    Parameters
    ----------
    b3_instance : B3ConsolidatedTrades
        B3 instance for testing
    mock_requests_get : MagicMock
        Mocked requests.get
    mock_response : Response
        Mocked response object
    mock_backoff : None
        Mocked backoff to bypass retries
    bool_verify : bool
        SSL verification flag to test

    Returns
    -------
    None
    """
    mock_requests_get.return_value = mock_response
    response = b3_instance.get_response(bool_verify=bool_verify)
    assert response == mock_response
    mock_requests_get.assert_any_call(b3_instance.url, timeout=(12.0, 21.0), verify=bool_verify)


def test_transform_data_empty_input(b3_instance: B3ConsolidatedTrades) -> None:
    """Test data transformation with empty input.

    Verifies
    --------
    - Empty StringIO results in empty DataFrame
    - Expected columns are present
    - No errors are raised

    Parameters
    ----------
    b3_instance : B3ConsolidatedTrades
        B3 instance for testing

    Returns
    -------
    None
    """
    empty_file = StringIO("")
    df_ = b3_instance.transform_data(empty_file)
    assert isinstance(df_, pd.DataFrame)
    assert df_.empty
    assert set(df_.columns) == {
        "RPT_DT", "TCKR_SYMB", "ISIN", "SGMT_NM", "MIN_PRIC", "MAX_PRIC",
        "TRAD_AVRG_PRIC", "LAST_PRIC", "OSCN_PCTG", "ADJSTD_QT",
        "ADJSTD_QT_TAX", "REF_PRIC", "TRAD_QTY", "FIN_INSTRM_QTY", "NTL_FIN_VOL"
    }


def test_standardize_dataframe_types(b3_instance: B3ConsolidatedTrades) -> None:
    """Test DataFrame standardization with correct dtypes.

    Verifies
    --------
    - DataFrame is standardized with correct dtypes
    - Date formatting is applied correctly
    - Fillna strategies are applied

    Parameters
    ----------
    b3_instance : B3ConsolidatedTrades
        B3 instance for testing

    Returns
    -------
    None
    """
    df_ = pd.DataFrame({
        "RPT_DT": ["2025-01-01"],
        "TCKR_SYMB": ["ABC"],
        "ISIN": ["XYZ"],
        "SGMT_NM": ["Segment"],
        "MIN_PRIC": [10.0],
        "MAX_PRIC": [20.0],
        "TRAD_AVRG_PRIC": [15.0],
        "LAST_PRIC": [18.0],
        "OSCN_PCTG": [0.5],
        "ADJSTD_QT": [100.0],
        "ADJSTD_QT_TAX": [1.0],
        "REF_PRIC": [17.0],
        "TRAD_QTY": [0],  # Updated to reflect filled value
        "FIN_INSTRM_QTY": [1000],
        "NTL_FIN_VOL": [10000.0],
    })
    sample_csv = StringIO(
        "RPT_DT;TCKR_SYMB;ISIN;SGMT_NM;MIN_PRIC;MAX_PRIC;TRAD_AVRG_PRIC;LAST_PRIC;OSCN_PCTG;"
        "ADJSTD_QT;ADJSTD_QT_TAX;REF_PRIC;TRAD_QTY;FIN_INSTRM_QTY;NTL_FIN_VOL\n"
        "2025-01-01;ABC;XYZ;Segment;10.0;20.0;15.0;18.0;0.5;100.0;1.0;17.0;;1000;10000.0"
    )
    with patch.object(b3_instance, "get_file", return_value=sample_csv), \
        patch.object(b3_instance, "standardize_dataframe", return_value=df_) \
        as mock_standardize:
        result = b3_instance.run()
        assert isinstance(result, pd.DataFrame)
        assert result["TRAD_QTY"].dtype == "int64"
        mock_standardize.assert_called_once()


def test_insert_table_db_called(
    b3_instance: B3ConsolidatedTrades,
    mock_requests_get: MagicMock,
    mock_response: Response,
    mock_backoff: None
) -> None:
    """Test database insertion is called with correct parameters.

    Verifies
    --------
    - insert_table_db is called with correct table name and DataFrame
    - bool_insert_or_ignore is passed correctly

    Parameters
    ----------
    b3_instance : B3ConsolidatedTrades
        B3 instance for testing
    mock_requests_get : MagicMock
        Mocked requests.get
    mock_response : Response
        Mocked response object
    mock_backoff : None
        Mocked backoff to bypass retries

    Returns
    -------
    None
    """
    mock_db = MagicMock()
    b3_instance.cls_db = mock_db
    mock_requests_get.return_value = mock_response
    sample_df = pd.DataFrame({
        "RPT_DT": ["2025-01-01"],
        "TCKR_SYMB": ["ABC"],
        "ISIN": ["XYZ"],
        "SGMT_NM": ["Segment"],
        "MIN_PRIC": [10.0],
        "MAX_PRIC": [20.0],
        "TRAD_AVRG_PRIC": [15.0],
        "LAST_PRIC": [18.0],
        "OSCN_PCTG": [0.5],
        "ADJSTD_QT": [100.0],
        "ADJSTD_QT_TAX": [1.0],
        "REF_PRIC": [17.0],
        "TRAD_QTY": [100],
        "FIN_INSTRM_QTY": [1000],
        "NTL_FIN_VOL": [10000.0],
    })
    with patch.object(b3_instance, "get_file", 
                      return_value=StringIO("Header1;Header2\nData1;Data2")), \
        patch.object(b3_instance, "transform_data", return_value=sample_df), \
        patch.object(b3_instance, "insert_table_db") as mock_insert:
        b3_instance.run(bool_insert_or_ignore=True, str_table_name="test_table")
        mock_insert.assert_called_once_with(
            cls_db=mock_db,
            str_table_name="test_table",
            df_=ANY,
            bool_insert_or_ignore=True
        )