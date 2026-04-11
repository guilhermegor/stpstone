"""Unit tests for CoinMarket class.

Tests the ingestion functionality with various scenarios including:
- Initialization with valid inputs
- HTTP response handling (success and error paths)
- Data parsing and transformation
- Database insertion and DataFrame fallback logic
"""

from datetime import date
from typing import Union
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from pytest_mock import MockerFixture
import requests
from requests import Response

from stpstone.ingestion.countries.ww.exchange.crypto.coinmarket import CoinMarket
from stpstone.utils.calendars.calendar_abc import DatesCurrent
from stpstone.utils.calendars.calendar_br import DatesBRAnbima
from stpstone.utils.loggs.create_logs import CreateLog
from stpstone.utils.parsers.folders import DirFilesManagement


# --------------------------
# Fixtures
# --------------------------
@pytest.fixture
def sample_api_key() -> str:
    """Provide a sample API key for testing.

    Returns
    -------
    str
        Fake API key for use in tests.
    """
    return "test-api-key-12345"


@pytest.fixture
def sample_date() -> date:
    """Provide a sample date for testing.

    Returns
    -------
    date
        Fixed date for consistent testing.
    """
    return date(2025, 1, 15)


@pytest.fixture
def coinmarket_instance(sample_api_key: str, sample_date: date) -> CoinMarket:
    """Fixture providing a CoinMarket instance.

    Parameters
    ----------
    sample_api_key : str
        API key fixture.
    sample_date : date
        Sample date fixture.

    Returns
    -------
    CoinMarket
        Initialized CoinMarket instance.
    """
    return CoinMarket(api_key=sample_api_key, date_ref=sample_date)


@pytest.fixture
def sample_json_payload() -> dict:
    """Provide a minimal but valid CoinMarketCap API JSON payload.

    Returns
    -------
    dict
        Fake API payload mirroring the real schema.
    """
    return {
        "data": [
            {
                "id": 1,
                "name": "Bitcoin",
                "symbol": "BTC",
                "slug": "bitcoin",
                "total_supply": 21000000.0,
                "cmc_rank": 1,
                "num_market_pairs": 10000,
                "last_updated": "2025-01-15T00:00:00.000Z",
                "quote": {
                    "USD": {
                        "price": 50000.0,
                        "market_cap": 950000000000.0,
                        "volume_24h": 30000000000.0,
                    }
                },
            },
            {
                "id": 1027,
                "name": "Ethereum",
                "symbol": "ETH",
                "slug": "ethereum",
                "total_supply": 120000000.0,
                "cmc_rank": 2,
                "num_market_pairs": 8000,
                "last_updated": "2025-01-15T00:00:00.000Z",
                "quote": {
                    "USD": {
                        "price": 3000.0,
                        "market_cap": 360000000000.0,
                        "volume_24h": 20000000000.0,
                    }
                },
            },
        ]
    }


@pytest.fixture
def mock_response(sample_json_payload: dict) -> Response:
    """Mock Response object with a valid JSON payload.

    Parameters
    ----------
    sample_json_payload : dict
        JSON payload fixture.

    Returns
    -------
    Response
        Mocked Response object.
    """
    response = MagicMock(spec=Response)
    response.status_code = 200
    response.raise_for_status = MagicMock()
    response.json.return_value = sample_json_payload
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
    return mocker.patch("requests.get")


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
def test_init_with_valid_inputs(sample_api_key: str, sample_date: date) -> None:
    """Test initialization with valid inputs.

    Verifies
    --------
    - All required attributes are set correctly.
    - Helper objects are instantiated.
    - URL is the expected CoinMarketCap endpoint.

    Parameters
    ----------
    sample_api_key : str
        API key for initialization.
    sample_date : date
        Date for initialization.

    Returns
    -------
    None
    """
    instance = CoinMarket(api_key=sample_api_key, date_ref=sample_date)
    assert instance.api_key == sample_api_key
    assert instance.date_ref == sample_date
    assert instance.url == "https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest"
    assert instance.headers["X-CMC_PRO_API_KEY"] == sample_api_key
    assert instance.headers["Accepts"] == "application/json"
    assert instance.params["convert"] == "USD"
    assert instance.params["limit"] == "5000"
    assert instance.params["start"] == "1"
    assert isinstance(instance.cls_dates_current, DatesCurrent)
    assert isinstance(instance.cls_dates_br, DatesBRAnbima)
    assert isinstance(instance.cls_create_log, CreateLog)
    assert isinstance(instance.cls_dir_files_management, DirFilesManagement)


def test_init_with_default_date(sample_api_key: str) -> None:
    """Test initialization without an explicit date_ref.

    Verifies
    --------
    - date_ref falls back to the previous working day when not provided.

    Parameters
    ----------
    sample_api_key : str
        API key for initialization.

    Returns
    -------
    None
    """
    with patch.object(DatesBRAnbima, "add_working_days", return_value=date(2025, 1, 14)):
        instance = CoinMarket(api_key=sample_api_key)
        assert instance.date_ref == date(2025, 1, 14)


def test_get_response_success(
    coinmarket_instance: CoinMarket,
    mock_requests_get: object,
    mock_response: Response,
    mock_backoff: object,
) -> None:
    """Test successful HTTP response retrieval.

    Verifies
    --------
    - requests.get is called with correct URL, headers, params, and timeout.
    - raise_for_status is invoked on success.
    - The Response object is returned.

    Parameters
    ----------
    coinmarket_instance : CoinMarket
        Instance under test.
    mock_requests_get : object
        Mocked requests.get.
    mock_response : Response
        Mocked Response object.
    mock_backoff : object
        Mocked backoff decorator.

    Returns
    -------
    None
    """
    mock_requests_get.return_value = mock_response
    result = coinmarket_instance.get_response(timeout=(12.0, 12.0), bool_verify=False)
    assert isinstance(result, Response)
    mock_requests_get.assert_called_once_with(
        coinmarket_instance.url,
        headers=coinmarket_instance.headers,
        params=coinmarket_instance.params,
        timeout=(12.0, 12.0),
        verify=False,
    )
    mock_response.raise_for_status.assert_called_once()


def test_get_response_http_error(
    coinmarket_instance: CoinMarket,
    mock_requests_get: object,
    mock_backoff: object,
) -> None:
    """Test that HTTP errors are propagated from get_response.

    Verifies
    --------
    - An HTTPError raised by raise_for_status bubbles up to the caller.

    Parameters
    ----------
    coinmarket_instance : CoinMarket
        Instance under test.
    mock_requests_get : object
        Mocked requests.get.
    mock_backoff : object
        Mocked backoff decorator.

    Returns
    -------
    None
    """
    error_response = MagicMock(spec=Response)
    error_response.raise_for_status.side_effect = requests.exceptions.HTTPError("401 Unauthorized")
    mock_requests_get.return_value = error_response
    with pytest.raises(requests.exceptions.HTTPError):
        coinmarket_instance.get_response()


def test_parse_raw_file_returns_response(
    coinmarket_instance: CoinMarket,
    mock_response: Response,
) -> None:
    """Test that parse_raw_file passes the response object through unchanged.

    Verifies
    --------
    - The same response object is returned without modification.

    Parameters
    ----------
    coinmarket_instance : CoinMarket
        Instance under test.
    mock_response : Response
        Mocked Response object.

    Returns
    -------
    None
    """
    result = coinmarket_instance.parse_raw_file(mock_response)
    assert result is mock_response


def test_transform_data_returns_dataframe(
    coinmarket_instance: CoinMarket,
    mock_response: Response,
    sample_json_payload: dict,
) -> None:
    """Test data transformation from a valid JSON response.

    Verifies
    --------
    - Output is a non-empty DataFrame.
    - All expected columns are present.
    - Values from the first record match the fixture payload.

    Parameters
    ----------
    coinmarket_instance : CoinMarket
        Instance under test.
    mock_response : Response
        Mocked Response object returning the sample payload.
    sample_json_payload : dict
        The JSON payload fixture.

    Returns
    -------
    None
    """
    df_ = coinmarket_instance.transform_data(mock_response)
    expected_columns = [
        "ID", "NAME", "SYMBOL", "PRICE", "MARKET_CAP", "VOLUME",
        "SLUG", "TOTAL_SUPPLY", "CMC_RANK", "NUM_MARKET_PAIRS", "LAST_UPDATE",
    ]
    assert isinstance(df_, pd.DataFrame)
    assert not df_.empty
    assert list(df_.columns) == expected_columns
    assert len(df_) == len(sample_json_payload["data"])
    assert df_["NAME"].iloc[0] == "Bitcoin"
    assert df_["SYMBOL"].iloc[0] == "BTC"
    assert df_["PRICE"].iloc[0] == pytest.approx(50000.0)
    assert df_["CMC_RANK"].iloc[0] == 1


def test_transform_data_empty_payload(coinmarket_instance: CoinMarket) -> None:
    """Test transform_data with an empty data list in the payload.

    Verifies
    --------
    - An empty data list results in an empty DataFrame.

    Parameters
    ----------
    coinmarket_instance : CoinMarket
        Instance under test.

    Returns
    -------
    None
    """
    empty_response = MagicMock(spec=Response)
    empty_response.json.return_value = {"data": []}
    df_ = coinmarket_instance.transform_data(empty_response)
    assert isinstance(df_, pd.DataFrame)
    assert df_.empty


def test_run_without_db(
    coinmarket_instance: CoinMarket,
    mock_requests_get: object,
    mock_response: Response,
    mock_backoff: object,
) -> None:
    """Test run method without a database session returns a DataFrame.

    Verifies
    --------
    - The full pipeline executes without errors.
    - A DataFrame is returned when cls_db is None.
    - All intermediate methods are called.

    Parameters
    ----------
    coinmarket_instance : CoinMarket
        Instance under test (cls_db is None by default).
    mock_requests_get : object
        Mocked requests.get.
    mock_response : Response
        Mocked Response object.
    mock_backoff : object
        Mocked backoff decorator.

    Returns
    -------
    None
    """
    mock_requests_get.return_value = mock_response
    with patch.object(
        coinmarket_instance, "parse_raw_file", return_value=mock_response
    ) as mock_parse, patch.object(
        coinmarket_instance, "transform_data", return_value=pd.DataFrame({"ID": ["1"]})
    ) as mock_transform, patch.object(
        coinmarket_instance, "standardize_dataframe", return_value=pd.DataFrame({"ID": ["1"]})
    ) as mock_standardize:
        result = coinmarket_instance.run()
        assert isinstance(result, pd.DataFrame)
        mock_parse.assert_called_once()
        mock_transform.assert_called_once()
        mock_standardize.assert_called_once()


def test_run_with_db(
    coinmarket_instance: CoinMarket,
    mock_requests_get: object,
    mock_response: Response,
    mock_backoff: object,
) -> None:
    """Test run method with a database session inserts data and returns None.

    Verifies
    --------
    - insert_table_db is called when cls_db is set.
    - run returns None (not a DataFrame) when a db session is present.

    Parameters
    ----------
    coinmarket_instance : CoinMarket
        Instance under test.
    mock_requests_get : object
        Mocked requests.get.
    mock_response : Response
        Mocked Response object.
    mock_backoff : object
        Mocked backoff decorator.

    Returns
    -------
    None
    """
    mock_db = MagicMock()
    coinmarket_instance.cls_db = mock_db
    mock_requests_get.return_value = mock_response
    with patch.object(
        coinmarket_instance, "parse_raw_file", return_value=mock_response
    ) as mock_parse, patch.object(
        coinmarket_instance, "transform_data", return_value=pd.DataFrame({"ID": ["1"]})
    ) as mock_transform, patch.object(
        coinmarket_instance, "standardize_dataframe", return_value=pd.DataFrame({"ID": ["1"]})
    ) as mock_standardize, patch.object(
        coinmarket_instance, "insert_table_db"
    ) as mock_insert:
        result = coinmarket_instance.run()
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
    coinmarket_instance: CoinMarket,
    mock_requests_get: object,
    mock_response: Response,
    mock_backoff: object,
    timeout: Union[int, float, tuple],
) -> None:
    """Test get_response with various timeout formats.

    Verifies
    --------
    - Different timeout shapes are forwarded correctly to requests.get.

    Parameters
    ----------
    coinmarket_instance : CoinMarket
        Instance under test.
    mock_requests_get : object
        Mocked requests.get.
    mock_response : Response
        Mocked Response object.
    mock_backoff : object
        Mocked backoff decorator.
    timeout : Union[int, float, tuple]
        Timeout value to test.

    Returns
    -------
    None
    """
    mock_requests_get.return_value = mock_response
    result = coinmarket_instance.get_response(timeout=timeout)
    assert isinstance(result, Response)
    mock_requests_get.assert_called_once_with(
        coinmarket_instance.url,
        headers=coinmarket_instance.headers,
        params=coinmarket_instance.params,
        timeout=timeout,
        verify=False,
    )


def test_reload_module(sample_api_key: str, sample_date: date) -> None:
    """Test module reloading behavior.

    Verifies
    --------
    - Module can be reloaded without errors.
    - Instance attributes are consistent after reload.

    Parameters
    ----------
    sample_api_key : str
        API key for initialization.
    sample_date : date
        Date for initialization.

    Returns
    -------
    None
    """
    import importlib

    import stpstone.ingestion.countries.ww.exchange.crypto.coinmarket

    original = CoinMarket(api_key=sample_api_key, date_ref=sample_date)
    importlib.reload(stpstone.ingestion.countries.ww.exchange.crypto.coinmarket)
    reloaded_cls = stpstone.ingestion.countries.ww.exchange.crypto.coinmarket.CoinMarket
    new_instance = reloaded_cls(api_key=sample_api_key, date_ref=sample_date)
    assert new_instance.date_ref == original.date_ref
    assert new_instance.url == original.url
