"""Unit tests for CoinCap ingestion class.

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
from requests import Response

from stpstone.ingestion.countries.ww.exchange.crypto.coincap import CoinCap
from stpstone.utils.calendars.calendar_abc import DatesCurrent
from stpstone.utils.calendars.calendar_br import DatesBRAnbima
from stpstone.utils.loggs.create_logs import CreateLog
from stpstone.utils.parsers.folders import DirFilesManagement


# --------------------------
# Fixtures
# --------------------------
@pytest.fixture
def sample_date() -> date:
	"""Provide a fixed sample date for deterministic tests.

	Returns
	-------
	date
		Fixed reference date.
	"""
	return date(2025, 1, 2)


@pytest.fixture
def coincap_instance(sample_date: date) -> CoinCap:
	"""Provide an initialized CoinCap instance.

	Parameters
	----------
	sample_date : date
		Reference date used during initialization.

	Returns
	-------
	CoinCap
		Initialized CoinCap instance.
	"""
	return CoinCap(date_ref=sample_date)


@pytest.fixture
def mock_asset_payload() -> dict:
	"""Provide a minimal CoinCap asset JSON payload.

	Returns
	-------
	dict
		Simulated API response payload.
	"""
	return {
		"data": {
			"id": "bitcoin",
			"rank": "1",
			"symbol": "BTC",
			"name": "Bitcoin",
			"supply": "19000000.0",
			"maxSupply": "21000000.0",
			"marketCapUsd": "500000000000.0",
			"volumeUsd24Hr": "20000000000.0",
			"priceUsd": "26315.79",
			"changePercent24Hr": "1.23",
			"vwap24Hr": "26200.00",
			"explorer": "https://blockchain.info/",
		}
	}


@pytest.fixture
def mock_response(mock_asset_payload: dict) -> Response:
	"""Provide a mocked Response returning the asset payload.

	Parameters
	----------
	mock_asset_payload : dict
		Payload returned by .json().

	Returns
	-------
	Response
		Mocked response object.
	"""
	response = MagicMock(spec=Response)
	response.status_code = 200
	response.json.return_value = mock_asset_payload
	response.raise_for_status = MagicMock()
	return response


@pytest.fixture
def mock_requests_get(mocker: MockerFixture) -> object:
	"""Mock requests.get to prevent real HTTP calls.

	Parameters
	----------
	mocker : MockerFixture
		Pytest-mock fixture.

	Returns
	-------
	object
		Mocked requests.get.
	"""
	return mocker.patch("stpstone.ingestion.countries.ww.exchange.crypto.coincap.requests.get")


@pytest.fixture
def mock_backoff(mocker: MockerFixture) -> object:
	"""Mock backoff.on_exception to bypass retry delays.

	Parameters
	----------
	mocker : MockerFixture
		Pytest-mock fixture.

	Returns
	-------
	object
		Mocked backoff decorator.
	"""
	return mocker.patch("backoff.on_exception", lambda *args, **kwargs: lambda func: func)


# --------------------------
# Tests
# --------------------------
def test_init_with_valid_inputs(sample_date: date) -> None:
	"""Test initialization with explicit inputs sets all attributes correctly.

	Verifies
	--------
	- date_ref is stored as provided.
	- Helper class instances are created.
	- base_url and slugs are set from inlined config.

	Parameters
	----------
	sample_date : date
		Reference date.

	Returns
	-------
	None
	"""
	instance = CoinCap(date_ref=sample_date)
	assert instance.date_ref == sample_date
	assert isinstance(instance.cls_dates_current, DatesCurrent)
	assert isinstance(instance.cls_dates_br, DatesBRAnbima)
	assert isinstance(instance.cls_create_log, CreateLog)
	assert isinstance(instance.cls_dir_files_management, DirFilesManagement)
	assert instance.base_url == "https://api.coincap.io/v2/"
	assert "bitcoin" in instance.slugs
	assert "ethereum" in instance.slugs


def test_init_default_date_falls_back_to_previous_working_day() -> None:
	"""Test that omitting date_ref uses the previous working day.

	Verifies
	--------
	- date_ref is derived from add_working_days(-1) when not supplied.

	Returns
	-------
	None
	"""
	with patch.object(
		DatesBRAnbima, "add_working_days", return_value=date(2025, 1, 2)
	) as mock_add:
		instance = CoinCap()
		assert instance.date_ref == date(2025, 1, 2)
		mock_add.assert_called_once()


def test_init_with_token_stores_token(sample_date: date) -> None:
	"""Test that a provided token is stored on the instance.

	Parameters
	----------
	sample_date : date
		Reference date.

	Returns
	-------
	None
	"""
	test_token = "mytoken"  # noqa: S105
	instance = CoinCap(date_ref=sample_date, token=test_token)
	assert instance.token == test_token


def test_get_response_success(
	coincap_instance: CoinCap,
	mock_requests_get: object,
	mock_response: Response,
	mock_backoff: object,
) -> None:
	"""Test get_response returns the Response when the request succeeds.

	Verifies
	--------
	- requests.get is called with the expected URL and headers.
	- raise_for_status is called exactly once.

	Parameters
	----------
	coincap_instance : CoinCap
		Initialized CoinCap instance.
	mock_requests_get : object
		Mocked requests.get.
	mock_response : Response
		Mocked successful response.
	mock_backoff : object
		Mocked backoff decorator.

	Returns
	-------
	None
	"""
	coincap_instance.url = "https://api.coincap.io/v2/assets/bitcoin"
	mock_requests_get.return_value = mock_response
	result = coincap_instance.get_response(timeout=(12.0, 12.0), bool_verify=False)
	assert result is mock_response
	mock_requests_get.assert_called_once_with(
		"https://api.coincap.io/v2/assets/bitcoin",
		headers={"Accept-Encoding": "gzip"},
		timeout=(12.0, 12.0),
		verify=False,
	)
	mock_response.raise_for_status.assert_called_once()


def test_get_response_includes_auth_header_when_token_set(
	sample_date: date,
	mock_requests_get: object,
	mock_response: Response,
	mock_backoff: object,
) -> None:
	"""Test that an Authorization header is sent when a token is provided.

	Parameters
	----------
	sample_date : date
		Reference date.
	mock_requests_get : object
		Mocked requests.get.
	mock_response : Response
		Mocked successful response.
	mock_backoff : object
		Mocked backoff decorator.

	Returns
	-------
	None
	"""
	test_token = "secret"  # noqa: S105
	instance = CoinCap(date_ref=sample_date, token=test_token)
	instance.url = "https://api.coincap.io/v2/assets/bitcoin"
	mock_requests_get.return_value = mock_response
	instance.get_response()
	_, kwargs = mock_requests_get.call_args
	assert kwargs["headers"]["Authorization"] == "Bearer secret"


def test_get_response_raises_on_http_error(
	coincap_instance: CoinCap,
	mock_requests_get: object,
	mock_backoff: object,
) -> None:
	"""Test that get_response propagates HTTPError from raise_for_status.

	Parameters
	----------
	coincap_instance : CoinCap
		Initialized CoinCap instance.
	mock_requests_get : object
		Mocked requests.get.
	mock_backoff : object
		Mocked backoff decorator.

	Returns
	-------
	None
	"""
	import requests as req_lib

	coincap_instance.url = "https://api.coincap.io/v2/assets/bitcoin"
	bad_response = MagicMock(spec=Response)
	bad_response.raise_for_status.side_effect = req_lib.exceptions.HTTPError("404")
	mock_requests_get.return_value = bad_response
	with pytest.raises(req_lib.exceptions.HTTPError):
		coincap_instance.get_response()


def test_parse_raw_file_returns_json(
	coincap_instance: CoinCap,
	mock_response: Response,
	mock_asset_payload: dict,
) -> None:
	"""Test that parse_raw_file delegates to resp_req.json().

	Parameters
	----------
	coincap_instance : CoinCap
		Initialized CoinCap instance.
	mock_response : Response
		Mocked response with a JSON payload.
	mock_asset_payload : dict
		Expected JSON payload.

	Returns
	-------
	None
	"""
	result = coincap_instance.parse_raw_file(mock_response)
	assert result == mock_asset_payload
	mock_response.json.assert_called_once()


def test_transform_data_returns_dataframe(
	coincap_instance: CoinCap,
	mock_asset_payload: dict,
) -> None:
	"""Test that transform_data wraps data in a single-row DataFrame.

	Parameters
	----------
	coincap_instance : CoinCap
		Initialized CoinCap instance.
	mock_asset_payload : dict
		Payload containing a 'data' key.

	Returns
	-------
	None
	"""
	df_ = coincap_instance.transform_data(mock_asset_payload)
	assert isinstance(df_, pd.DataFrame)
	assert len(df_) == 1
	assert "id" in df_.columns or "ID" in df_.columns


def test_transform_data_empty_data_key(coincap_instance: CoinCap) -> None:
	"""Test transform_data when 'data' is an empty dict.

	Parameters
	----------
	coincap_instance : CoinCap
		Initialized CoinCap instance.

	Returns
	-------
	None
	"""
	df_ = coincap_instance.transform_data({"data": {}})
	assert isinstance(df_, pd.DataFrame)


def test_run_without_db_returns_dataframe(
	coincap_instance: CoinCap,
	mock_requests_get: object,
	mock_response: Response,
	mock_backoff: object,
) -> None:
	"""Test that run returns a DataFrame when no database session is set.

	Verifies
	--------
	- All three pipeline methods are called for each slug.
	- The returned value is a DataFrame.

	Parameters
	----------
	coincap_instance : CoinCap
		Initialized CoinCap instance.
	mock_requests_get : object
		Mocked requests.get.
	mock_response : Response
		Mocked successful response.
	mock_backoff : object
		Mocked backoff decorator.

	Returns
	-------
	None
	"""
	mock_requests_get.return_value = mock_response
	single_row_df = pd.DataFrame([{"ID": "bitcoin", "RANK": 1}])
	with (
		patch.object(coincap_instance, "get_response", return_value=mock_response),
		patch.object(coincap_instance, "parse_raw_file", return_value={"data": {}}),
		patch.object(coincap_instance, "transform_data", return_value=single_row_df),
		patch.object(
			coincap_instance, "standardize_dataframe", return_value=single_row_df
		) as mock_std,
	):
		result = coincap_instance.run()
		assert isinstance(result, pd.DataFrame)
		assert len(result) == len(coincap_instance.slugs)
		assert mock_std.call_count == len(coincap_instance.slugs)


def test_run_with_db_calls_insert_and_returns_none(
	coincap_instance: CoinCap,
	mock_requests_get: object,
	mock_response: Response,
	mock_backoff: object,
) -> None:
	"""Test that run calls insert_table_db and returns None when cls_db is set.

	Parameters
	----------
	coincap_instance : CoinCap
		Initialized CoinCap instance.
	mock_requests_get : object
		Mocked requests.get.
	mock_response : Response
		Mocked successful response.
	mock_backoff : object
		Mocked backoff decorator.

	Returns
	-------
	None
	"""
	mock_db = MagicMock()
	coincap_instance.cls_db = mock_db
	single_row_df = pd.DataFrame([{"ID": "bitcoin", "RANK": 1}])
	with (
		patch.object(coincap_instance, "get_response", return_value=mock_response),
		patch.object(coincap_instance, "parse_raw_file", return_value={"data": {}}),
		patch.object(coincap_instance, "transform_data", return_value=single_row_df),
		patch.object(coincap_instance, "standardize_dataframe", return_value=single_row_df),
		patch.object(coincap_instance, "insert_table_db") as mock_insert,
	):
		result = coincap_instance.run()
		assert result is None
		mock_insert.assert_called_once()


@pytest.mark.parametrize(
	"timeout",
	[
		10,
		10.5,
		(5.0, 10.0),
		(5, 10),
	],
)
def test_get_response_timeout_variations(
	coincap_instance: CoinCap,
	mock_requests_get: object,
	mock_response: Response,
	mock_backoff: object,
	timeout: Union[int, float, tuple],
) -> None:
	"""Test get_response accepts all valid timeout types.

	Parameters
	----------
	coincap_instance : CoinCap
		Initialized CoinCap instance.
	mock_requests_get : object
		Mocked requests.get.
	mock_response : Response
		Mocked successful response.
	mock_backoff : object
		Mocked backoff decorator.
	timeout : Union[int, float, tuple]
		Timeout value to test.

	Returns
	-------
	None
	"""
	coincap_instance.url = "https://api.coincap.io/v2/assets/bitcoin"
	mock_requests_get.return_value = mock_response
	result = coincap_instance.get_response(timeout=timeout)
	assert result is mock_response
	mock_requests_get.assert_called_once_with(
		coincap_instance.url,
		headers={"Accept-Encoding": "gzip"},
		timeout=timeout,
		verify=False,
	)


def test_reload_module() -> None:
	"""Test module reloading preserves class contract.

	Verifies
	--------
	- Module can be reloaded without errors.
	- New instance has the same base_url and slugs.

	Returns
	-------
	None
	"""
	import importlib

	import stpstone.ingestion.countries.ww.exchange.crypto.coincap as mod

	original = CoinCap(date_ref=date(2025, 1, 2))
	importlib.reload(mod)
	fresh = mod.CoinCap(date_ref=date(2025, 1, 2))
	assert fresh.base_url == original.base_url
	assert fresh.slugs == original.slugs
