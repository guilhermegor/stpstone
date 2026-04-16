"""Unit tests for CoinPaprika ingestion class."""

from datetime import date
from io import StringIO
import json
from typing import Union
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from pytest_mock import MockerFixture
from requests import Response

from stpstone.ingestion.countries.ww.exchange.crypto.coinpaprika import CoinPaprika
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
def coinpaprika_instance(sample_date: date) -> CoinPaprika:
	"""Provide an initialized CoinPaprika instance.

	Parameters
	----------
	sample_date : date
		Reference date used during initialization.

	Returns
	-------
	CoinPaprika
		Initialized CoinPaprika instance.
	"""
	return CoinPaprika(date_ref=sample_date)


_SAMPLE_OHLCV = [
	{
		"time_open": "2025-01-01T00:00:00Z",
		"time_close": "2025-01-01T23:59:59Z",
		"open": 93000.0,
		"high": 95000.0,
		"low": 92000.0,
		"close": 94000.0,
		"volume": 12345678.9,
		"market_cap": 1800000000000.0,
	}
]


@pytest.fixture
def mock_response() -> Response:
	"""Provide a mocked Response with a sample OHLCV payload.

	Returns
	-------
	Response
		Mocked response object.
	"""
	response = MagicMock(spec=Response)
	response.status_code = 200
	response.raise_for_status = MagicMock()
	response.text = json.dumps(_SAMPLE_OHLCV)
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
	return mocker.patch("stpstone.ingestion.countries.ww.exchange.crypto.coinpaprika.requests.get")


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

	Parameters
	----------
	sample_date : date
		Reference date.

	Returns
	-------
	None
	"""
	instance = CoinPaprika(date_ref=sample_date)
	assert instance.date_ref == sample_date
	assert isinstance(instance.cls_dates_current, DatesCurrent)
	assert isinstance(instance.cls_dates_br, DatesBRAnbima)
	assert isinstance(instance.cls_create_log, CreateLog)
	assert isinstance(instance.cls_dir_files_management, DirFilesManagement)
	assert "btc-bitcoin" in instance.list_slugs
	assert instance.url == "https://api.coinpaprika.com/v1/"


def test_init_default_date_falls_back_to_previous_working_day() -> None:
	"""Test that omitting date_ref uses the previous working day.

	Returns
	-------
	None
	"""
	with patch.object(
		DatesBRAnbima, "add_working_days", return_value=date(2025, 1, 2)
	) as mock_add:
		instance = CoinPaprika()
		assert instance.date_ref == date(2025, 1, 2)
		mock_add.assert_called_once()


def test_init_custom_slugs(sample_date: date) -> None:
	"""Test initialization with a custom slug list.

	Parameters
	----------
	sample_date : date
		Reference date.

	Returns
	-------
	None
	"""
	instance = CoinPaprika(date_ref=sample_date, list_slugs=["eth-ethereum"])
	assert instance.list_slugs == ["eth-ethereum"]


def test_get_response_success(
	coinpaprika_instance: CoinPaprika,
	mock_requests_get: object,
	mock_response: Response,
	mock_backoff: object,
) -> None:
	"""Test get_response returns the Response on success.

	Parameters
	----------
	coinpaprika_instance : CoinPaprika
		Initialized instance.
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
	url = "https://api.coinpaprika.com/v1/coins/btc-bitcoin/ohlcv/latest"
	mock_requests_get.return_value = mock_response
	result = coinpaprika_instance.get_response(url=url)
	assert result is mock_response
	mock_response.raise_for_status.assert_called_once()


def test_get_response_raises_on_http_error(
	coinpaprika_instance: CoinPaprika,
	mock_requests_get: object,
	mock_backoff: object,
) -> None:
	"""Test that get_response propagates HTTPError.

	Parameters
	----------
	coinpaprika_instance : CoinPaprika
		Initialized instance.
	mock_requests_get : object
		Mocked requests.get.
	mock_backoff : object
		Mocked backoff decorator.

	Returns
	-------
	None
	"""
	import requests as req_lib

	url = "https://api.coinpaprika.com/v1/coins/btc-bitcoin/ohlcv/latest"
	bad_response = MagicMock(spec=Response)
	bad_response.raise_for_status.side_effect = req_lib.exceptions.HTTPError("404")
	mock_requests_get.return_value = bad_response
	with pytest.raises(req_lib.exceptions.HTTPError):
		coinpaprika_instance.get_response(url=url)


def test_parse_raw_file_returns_stringio(
	coinpaprika_instance: CoinPaprika, mock_response: Response
) -> None:
	"""Test that parse_raw_file returns a StringIO buffer.

	Parameters
	----------
	coinpaprika_instance : CoinPaprika
		Initialized instance.
	mock_response : Response
		Mocked response.

	Returns
	-------
	None
	"""
	result = coinpaprika_instance.parse_raw_file(mock_response)
	assert isinstance(result, StringIO)


def test_transform_data_returns_dataframe(coinpaprika_instance: CoinPaprika) -> None:
	"""Test transform_data builds a DataFrame from valid OHLCV JSON.

	Parameters
	----------
	coinpaprika_instance : CoinPaprika
		Initialized instance.

	Returns
	-------
	None
	"""
	file = StringIO(json.dumps(_SAMPLE_OHLCV))
	df_ = coinpaprika_instance.transform_data(file=file, slug="btc-bitcoin")
	assert isinstance(df_, pd.DataFrame)
	assert not df_.empty
	assert "SLUG" in df_.columns
	assert df_["SLUG"].iloc[0] == "btc-bitcoin"
	assert "CLOSE" in df_.columns


def test_transform_data_empty_payload(coinpaprika_instance: CoinPaprika) -> None:
	"""Test transform_data returns None when payload is an empty list.

	Parameters
	----------
	coinpaprika_instance : CoinPaprika
		Initialized instance.

	Returns
	-------
	None
	"""
	file = StringIO("[]")
	result = coinpaprika_instance.transform_data(file=file, slug="btc-bitcoin")
	assert result is None


def test_transform_data_invalid_json(coinpaprika_instance: CoinPaprika) -> None:
	"""Test transform_data returns None on malformed JSON.

	Parameters
	----------
	coinpaprika_instance : CoinPaprika
		Initialized instance.

	Returns
	-------
	None
	"""
	file = StringIO("not-json{{{")
	result = coinpaprika_instance.transform_data(file=file, slug="btc-bitcoin")
	assert result is None


def test_run_without_db_returns_dataframe(
	coinpaprika_instance: CoinPaprika,
	mock_requests_get: object,
	mock_response: Response,
	mock_backoff: object,
) -> None:
	"""Test run returns a DataFrame when no cls_db is set.

	Parameters
	----------
	coinpaprika_instance : CoinPaprika
		Initialized instance.
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
	single_row = pd.DataFrame([{"SLUG": "btc-bitcoin", "CLOSE": 94000.0}])
	with (
		patch.object(coinpaprika_instance, "get_response", return_value=mock_response),
		patch.object(coinpaprika_instance, "parse_raw_file", return_value=StringIO("[]")),
		patch.object(coinpaprika_instance, "transform_data", return_value=single_row),
		patch.object(coinpaprika_instance, "standardize_dataframe", return_value=single_row),
	):
		result = coinpaprika_instance.run()
	assert isinstance(result, pd.DataFrame)


def test_run_with_db_calls_insert_and_returns_none(
	coinpaprika_instance: CoinPaprika,
	mock_requests_get: object,
	mock_response: Response,
	mock_backoff: object,
) -> None:
	"""Test run calls insert_table_db and returns None when cls_db is set.

	Parameters
	----------
	coinpaprika_instance : CoinPaprika
		Initialized instance.
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
	coinpaprika_instance.cls_db = mock_db
	single_row = pd.DataFrame([{"SLUG": "btc-bitcoin", "CLOSE": 94000.0}])
	with (
		patch.object(coinpaprika_instance, "get_response", return_value=mock_response),
		patch.object(coinpaprika_instance, "parse_raw_file", return_value=StringIO("[]")),
		patch.object(coinpaprika_instance, "transform_data", return_value=single_row),
		patch.object(coinpaprika_instance, "standardize_dataframe", return_value=single_row),
		patch.object(coinpaprika_instance, "insert_table_db") as mock_insert,
	):
		result = coinpaprika_instance.run()
	assert result is None
	mock_insert.assert_called_once()


@pytest.mark.parametrize("timeout", [10, 10.5, (5.0, 10.0), (5, 10)])
def test_get_response_timeout_variations(
	coinpaprika_instance: CoinPaprika,
	mock_requests_get: object,
	mock_response: Response,
	mock_backoff: object,
	timeout: Union[int, float, tuple],
) -> None:
	"""Test get_response accepts all valid timeout types.

	Parameters
	----------
	coinpaprika_instance : CoinPaprika
		Initialized instance.
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
	url = "https://api.coinpaprika.com/v1/coins/btc-bitcoin/ohlcv/latest"
	mock_requests_get.return_value = mock_response
	result = coinpaprika_instance.get_response(url=url, timeout=timeout)
	assert result is mock_response


def test_reload_module() -> None:
	"""Test module reloading preserves class contract.

	Returns
	-------
	None
	"""
	import importlib

	import stpstone.ingestion.countries.ww.exchange.crypto.coinpaprika as mod

	original = CoinPaprika(date_ref=date(2025, 1, 2))
	importlib.reload(mod)
	fresh = mod.CoinPaprika(date_ref=date(2025, 1, 2))
	assert fresh.url == original.url
	assert fresh.list_slugs == original.list_slugs
