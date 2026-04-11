"""Unit tests for FredUSMacro ingestion class.

Tests the ingestion functionality with various scenarios including:
- Initialization with valid and default inputs
- HTTP response handling (success, HTTP error)
- Data parsing and transformation
- Database insertion and fallback logic
"""

from datetime import date
from typing import Union
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from pytest_mock import MockerFixture
import requests
from requests import Response

from stpstone.ingestion.countries.us.macroeconomics.fred import FredUSMacro
from stpstone.utils.calendars.calendar_abc import DatesCurrent
from stpstone.utils.calendars.calendar_br import DatesBRAnbima
from stpstone.utils.loggs.create_logs import CreateLog
from stpstone.utils.parsers.folders import DirFilesManagement


# --------------------------
# Fixtures
# --------------------------
@pytest.fixture
def sample_date_ref() -> date:
    """Provide a fixed reference date for deterministic testing.

    Returns
    -------
    date
        Fixed date for consistent testing.
    """
    return date(2025, 1, 10)


@pytest.fixture
def sample_date_start() -> date:
    """Provide a fixed start date for deterministic testing.

    Returns
    -------
    date
        Fixed start date for testing.
    """
    return date(2024, 10, 1)


@pytest.fixture
def sample_date_end() -> date:
    """Provide a fixed end date for deterministic testing.

    Returns
    -------
    date
        Fixed end date for testing.
    """
    return date(2025, 1, 9)


@pytest.fixture
def fred_instance(
    sample_date_ref: date, sample_date_start: date, sample_date_end: date
) -> FredUSMacro:
    """Fixture providing a FredUSMacro instance with fixed dates.

    Parameters
    ----------
    sample_date_ref : date
        Fixed reference date.
    sample_date_start : date
        Fixed start date.
    sample_date_end : date
        Fixed end date.

    Returns
    -------
    FredUSMacro
        Initialized FredUSMacro instance.
    """
    return FredUSMacro(
        api_key="test_api_key",
        date_ref=sample_date_ref,
        date_start=sample_date_start,
        date_end=sample_date_end,
    )


@pytest.fixture
def mock_response() -> Response:
    """Mock Response object with sample FRED JSON content.

    Returns
    -------
    Response
        Mocked Response object with predefined JSON content.
    """
    response = MagicMock(spec=Response)
    response.status_code = 200
    response.raise_for_status = MagicMock()
    response.json.return_value = {
        "seriess": [
            {
                "id": "GNPCA",
                "realtime_start": "2025-01-01",
                "realtime_end": "2025-01-09",
                "title": "Real Gross National Product",
                "observation_start": "1929-01-01",
                "observation_end": "2023-01-01",
                "frequency": "Annual",
                "frequency_short": "A",
                "units": "Billions of Chained 2017 Dollars",
                "units_short": "Bil. of Chn. 2017 $",
                "seasonal_adjustment": "Not Seasonally Adjusted",
                "seasonal_adjustment_short": "NSA",
                "last_updated": "2024-03-28 07:50:01-05",
                "popularity": 14,
                "notes": "GNP in chained dollars.",
            }
        ]
    }
    return response


@pytest.fixture
def mock_requests_get(mocker: MockerFixture) -> object:
    """Mock requests.get to prevent real HTTP calls.

    Parameters
    ----------
    mocker : MockerFixture
        Pytest-mock fixture for creating mocks.

    Returns
    -------
    object
        Mocked requests.get object.
    """
    return mocker.patch(
        "stpstone.ingestion.countries.us.macroeconomics.fred.requests.get"
    )


@pytest.fixture
def mock_backoff(mocker: MockerFixture) -> object:
    """Mock backoff.on_exception to bypass retry delays.

    Parameters
    ----------
    mocker : MockerFixture
        Pytest-mock fixture for creating mocks.

    Returns
    -------
    object
        Mocked backoff.on_exception object.
    """
    return mocker.patch("backoff.on_exception", lambda *args, **kwargs: lambda func: func)


# --------------------------
# Tests
# --------------------------
def test_init_with_explicit_dates(
    sample_date_ref: date, sample_date_start: date, sample_date_end: date
) -> None:
    """Test initialization with all dates explicitly provided.

    Verifies
    --------
    - Attributes are stored correctly.
    - Helper class instances are created.

    Parameters
    ----------
    sample_date_ref : date
        Fixed reference date.
    sample_date_start : date
        Fixed start date.
    sample_date_end : date
        Fixed end date.

    Returns
    -------
    None
    """
    instance = FredUSMacro(
        api_key="my_key",
        date_ref=sample_date_ref,
        date_start=sample_date_start,
        date_end=sample_date_end,
    )
    assert instance.api_key == "my_key"
    assert instance.date_ref == sample_date_ref
    assert instance.date_start == sample_date_start
    assert instance.date_end == sample_date_end
    assert isinstance(instance.cls_dates_current, DatesCurrent)
    assert isinstance(instance.cls_dates_br, DatesBRAnbima)
    assert isinstance(instance.cls_create_log, CreateLog)
    assert isinstance(instance.cls_dir_files_management, DirFilesManagement)


def test_init_default_dates() -> None:
    """Test initialization with default dates derived from the calendar.

    Verifies
    --------
    - date_ref, date_start, and date_end are set when not provided.
    - list_slugs defaults to the standard macro set.

    Returns
    -------
    None
    """
    with patch.object(DatesBRAnbima, "add_working_days", return_value=date(2025, 1, 9)):
        instance = FredUSMacro(api_key="key")
        assert instance.date_ref == date(2025, 1, 9)
        assert instance.list_slugs == FredUSMacro._DEFAULT_SLUGS


def test_init_custom_slugs(sample_date_ref: date) -> None:
    """Test initialization with a custom slug list.

    Verifies
    --------
    - Custom slugs replace the default list.

    Parameters
    ----------
    sample_date_ref : date
        Fixed reference date.

    Returns
    -------
    None
    """
    custom_slugs = ["DGS10", "DGS2"]
    instance = FredUSMacro(
        api_key="key",
        date_ref=sample_date_ref,
        list_slugs=custom_slugs,
    )
    assert instance.list_slugs == custom_slugs


def test_build_url(fred_instance: FredUSMacro) -> None:
    """Test URL construction for a given series slug.

    Verifies
    --------
    - URL contains the base host, slug, api_key, and date range.

    Parameters
    ----------
    fred_instance : FredUSMacro
        Instance of FredUSMacro.

    Returns
    -------
    None
    """
    url = fred_instance._build_url("GNPCA")
    assert "api.stlouisfed.org" in url
    assert "series_id=GNPCA" in url
    assert "api_key=test_api_key" in url
    assert "realtime_start=2024-10-01" in url
    assert "realtime_sup=2025-01-09" in url


def test_get_response_success(
    fred_instance: FredUSMacro,
    mock_requests_get: object,
    mock_response: Response,
    mock_backoff: object,
) -> None:
    """Test successful HTTP response retrieval.

    Verifies
    --------
    - Successful HTTP request returns the Response object.
    - raise_for_status is called.

    Parameters
    ----------
    fred_instance : FredUSMacro
        Instance of FredUSMacro.
    mock_requests_get : object
        Mocked requests.get function.
    mock_response : Response
        Mocked Response object.
    mock_backoff : object
        Mocked backoff decorator.

    Returns
    -------
    None
    """
    fred_instance.url = fred_instance._build_url("GNPCA")
    mock_requests_get.return_value = mock_response
    result = fred_instance.get_response()
    assert result is mock_response
    mock_response.raise_for_status.assert_called_once()


def test_get_response_http_error(
    fred_instance: FredUSMacro,
    mock_requests_get: object,
    mock_backoff: object,
) -> None:
    """Test that an HTTP error is raised and propagated.

    Verifies
    --------
    - requests.exceptions.HTTPError is raised when the server returns an error.

    Parameters
    ----------
    fred_instance : FredUSMacro
        Instance of FredUSMacro.
    mock_requests_get : object
        Mocked requests.get function.
    mock_backoff : object
        Mocked backoff decorator.

    Returns
    -------
    None
    """
    fred_instance.url = fred_instance._build_url("GNPCA")
    error_response = MagicMock(spec=Response)
    error_response.raise_for_status.side_effect = requests.exceptions.HTTPError("400 Bad Request")
    mock_requests_get.return_value = error_response
    with pytest.raises(requests.exceptions.HTTPError):
        fred_instance.get_response()


def test_parse_raw_file_passthrough(
    fred_instance: FredUSMacro, mock_response: Response
) -> None:
    """Test that parse_raw_file returns the response object unchanged.

    Verifies
    --------
    - The response is passed through without modification.

    Parameters
    ----------
    fred_instance : FredUSMacro
        Instance of FredUSMacro.
    mock_response : Response
        Mocked Response object.

    Returns
    -------
    None
    """
    result = fred_instance.parse_raw_file(mock_response)
    assert result is mock_response


def test_transform_data_returns_dataframe(
    fred_instance: FredUSMacro, mock_response: Response
) -> None:
    """Test that transform_data extracts the seriess key into a DataFrame.

    Verifies
    --------
    - Returns a non-empty DataFrame.
    - Contains expected columns from the FRED API response.

    Parameters
    ----------
    fred_instance : FredUSMacro
        Instance of FredUSMacro.
    mock_response : Response
        Mocked Response object with sample JSON.

    Returns
    -------
    None
    """
    df_ = fred_instance.transform_data(resp_req=mock_response)
    assert isinstance(df_, pd.DataFrame)
    assert not df_.empty
    assert "id" in df_.columns
    assert df_["id"].iloc[0] == "GNPCA"


def test_transform_data_empty_seriess(fred_instance: FredUSMacro) -> None:
    """Test transform_data with an empty seriess list.

    Verifies
    --------
    - Returns an empty DataFrame without errors.

    Parameters
    ----------
    fred_instance : FredUSMacro
        Instance of FredUSMacro.

    Returns
    -------
    None
    """
    empty_response = MagicMock(spec=Response)
    empty_response.json.return_value = {"seriess": []}
    df_ = fred_instance.transform_data(resp_req=empty_response)
    assert isinstance(df_, pd.DataFrame)
    assert df_.empty


def test_run_without_db(
    fred_instance: FredUSMacro,
    mock_requests_get: object,
    mock_response: Response,
    mock_backoff: object,
) -> None:
    """Test run method returns a DataFrame when no database session is set.

    Verifies
    --------
    - Returns a transformed DataFrame.
    - All intermediate pipeline methods are called.

    Parameters
    ----------
    fred_instance : FredUSMacro
        Instance of FredUSMacro.
    mock_requests_get : object
        Mocked requests.get function.
    mock_response : Response
        Mocked Response object.
    mock_backoff : object
        Mocked backoff decorator.

    Returns
    -------
    None
    """
    mock_requests_get.return_value = mock_response
    sample_df = pd.DataFrame({"id": ["GNPCA"]})
    with patch.object(fred_instance, "transform_data", return_value=sample_df) as mock_transform, \
        patch.object(
            fred_instance,
            "standardize_dataframe",
            return_value=sample_df,
        ) as mock_standardize:
        result = fred_instance.run()
        assert isinstance(result, pd.DataFrame)
        assert mock_transform.call_count == len(fred_instance.list_slugs)
        mock_standardize.assert_called_once()


def test_run_with_db(
    fred_instance: FredUSMacro,
    mock_requests_get: object,
    mock_response: Response,
    mock_backoff: object,
) -> None:
    """Test run method inserts into DB and returns None when cls_db is set.

    Verifies
    --------
    - insert_table_db is called once.
    - No DataFrame is returned.

    Parameters
    ----------
    fred_instance : FredUSMacro
        Instance of FredUSMacro.
    mock_requests_get : object
        Mocked requests.get function.
    mock_response : Response
        Mocked Response object.
    mock_backoff : object
        Mocked backoff decorator.

    Returns
    -------
    None
    """
    mock_db = MagicMock()
    fred_instance.cls_db = mock_db
    mock_requests_get.return_value = mock_response
    sample_df = pd.DataFrame({"id": ["GNPCA"]})
    with patch.object(fred_instance, "transform_data", return_value=sample_df), \
        patch.object(fred_instance, "standardize_dataframe", return_value=sample_df), \
        patch.object(fred_instance, "insert_table_db") as mock_insert:
        result = fred_instance.run()
        assert result is None
        mock_insert.assert_called_once()


@pytest.mark.parametrize(
    "timeout",
    [10, 10.5, (5.0, 10.0), (5, 10)],
)
def test_get_response_timeout_variations(
    fred_instance: FredUSMacro,
    mock_requests_get: object,
    mock_response: Response,
    mock_backoff: object,
    timeout: Union[int, float, tuple],
) -> None:
    """Test get_response with various timeout formats.

    Verifies
    --------
    - Different timeout types are forwarded correctly to requests.get.

    Parameters
    ----------
    fred_instance : FredUSMacro
        Instance of FredUSMacro.
    mock_requests_get : object
        Mocked requests.get function.
    mock_response : Response
        Mocked Response object.
    mock_backoff : object
        Mocked backoff decorator.
    timeout : Union[int, float, tuple]
        Timeout value or tuple to test.

    Returns
    -------
    None
    """
    fred_instance.url = fred_instance._build_url("GNPCA")
    mock_requests_get.return_value = mock_response
    result = fred_instance.get_response(timeout=timeout)
    assert result is mock_response
    mock_requests_get.assert_called_once_with(
        fred_instance.url, timeout=timeout, verify=False
    )


@pytest.mark.parametrize(
    "invalid_input,expected_error",
    [
        (None, "resp_req must be one of types: Response, Page, WebDriver, got NoneType"),
        ("invalid", "resp_req must be one of types: Response, Page, WebDriver, got str"),
        (123, "resp_req must be one of types: Response, Page, WebDriver, got int"),
    ],
)
def test_parse_raw_file_invalid_input(
    fred_instance: FredUSMacro,
    invalid_input: Union[None, str, int],
    expected_error: str,
) -> None:
    """Test parse_raw_file raises TypeError for invalid inputs.

    Verifies
    --------
    - The type checker rejects invalid resp_req types with a descriptive message.

    Parameters
    ----------
    fred_instance : FredUSMacro
        Instance of FredUSMacro.
    invalid_input : Union[None, str, int]
        Invalid input to test error handling.
    expected_error : str
        Expected error message from type checker.

    Returns
    -------
    None
    """
    with pytest.raises(TypeError, match=expected_error):
        fred_instance.parse_raw_file(invalid_input)


def test_reload_module() -> None:
    """Test module reloading does not break the class contract.

    Verifies
    --------
    - Module can be reloaded without errors.
    - New instance preserves api_key and default slug list.

    Returns
    -------
    None
    """
    import importlib

    import stpstone.ingestion.countries.us.macroeconomics.fred as fred_module

    original = FredUSMacro(
        api_key="reload_key",
        date_ref=date(2025, 1, 10),
        date_start=date(2024, 10, 1),
        date_end=date(2025, 1, 9),
    )
    importlib.reload(fred_module)
    reloaded = fred_module.FredUSMacro(
        api_key="reload_key",
        date_ref=date(2025, 1, 10),
        date_start=date(2024, 10, 1),
        date_end=date(2025, 1, 9),
    )
    assert reloaded.api_key == original.api_key
    assert reloaded.list_slugs == original.list_slugs
