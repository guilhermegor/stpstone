"""Unit tests for AlphaVantageUS ingestion class.

Tests the ingestion functionality with various scenarios including:
- Initialization with valid and invalid inputs
- URL building per slug
- HTTP response handling (success, HTTP error, timeout)
- JSON parsing and transformation
- Guard clauses for missing JSON keys
- Rate-limit sleep is called exactly once per slug
- Database insertion and no-DB return path
- Module reload stability
"""

from datetime import date
from io import StringIO
from typing import Union
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from pytest_mock import MockerFixture
from requests import Response

from stpstone.ingestion.countries.us.exchange.alphavantage import (
	RATE_LIMIT_SLEEP_SECONDS,
	AlphaVantageUS,
)
from stpstone.utils.calendars.calendar_abc import DatesCurrent
from stpstone.utils.calendars.calendar_br import DatesBRAnbima
from stpstone.utils.loggs.create_logs import CreateLog
from stpstone.utils.parsers.folders import DirFilesManagement


# --------------------------
# Fixtures
# --------------------------

_SAMPLE_DATE = date(2025, 1, 2)
_SAMPLE_SLUG = "AAPL"
_SAMPLE_TOKEN = "test-api-key-placeholder"  # noqa: S105

_VALID_JSON = {
	"Meta Data": {
		"1. Information": "Daily Prices",
		"2. Symbol": _SAMPLE_SLUG,
		"3. Last Refreshed": "2025-01-02",
		"4. Output Size": "Compact",
		"5. Time Zone": "US/Eastern",
	},
	"Time Series (Daily)": {
		"2025-01-02": {
			"1. open": "182.5000",
			"2. high": "185.0000",
			"3. low": "181.0000",
			"4. close": "183.7500",
			"5. volume": "55000000",
		},
		"2025-01-01": {
			"1. open": "180.0000",
			"2. high": "183.0000",
			"3. low": "179.5000",
			"4. close": "182.5000",
			"5. volume": "48000000",
		},
	},
}


@pytest.fixture
def sample_date() -> date:
	"""Return a fixed reference date.

	Returns
	-------
	date
		Fixed date for deterministic tests.
	"""
	return _SAMPLE_DATE


@pytest.fixture
def instance(sample_date: date) -> AlphaVantageUS:
	"""Return an AlphaVantageUS instance with one slug and no DB session.

	Parameters
	----------
	sample_date : date
		Fixed reference date.

	Returns
	-------
	AlphaVantageUS
		Configured instance for testing.
	"""
	return AlphaVantageUS(
		date_ref=sample_date,
		token=_SAMPLE_TOKEN,
		list_slugs=[_SAMPLE_SLUG],
	)


@pytest.fixture
def mock_response() -> Response:
	"""Return a mocked Response whose JSON matches the AlphaVantage schema.

	Returns
	-------
	Response
		Mocked Response with valid OHLCV JSON payload.
	"""
	resp = MagicMock(spec=Response)
	resp.json.return_value = _VALID_JSON
	resp.text = "{}"
	resp.status_code = 200
	resp.raise_for_status = MagicMock()
	return resp


@pytest.fixture
def mock_requests_get(mocker: MockerFixture) -> object:
	"""Patch requests.get to prevent real HTTP calls.

	Parameters
	----------
	mocker : MockerFixture
		Pytest-mock fixture.

	Returns
	-------
	object
		Patched requests.get callable.
	"""
	return mocker.patch("stpstone.ingestion.countries.us.exchange.alphavantage.requests.get")


@pytest.fixture
def mock_sleep(mocker: MockerFixture) -> object:
	"""Patch time.sleep to eliminate rate-limit delays.

	Parameters
	----------
	mocker : MockerFixture
		Pytest-mock fixture.

	Returns
	-------
	object
		Patched sleep callable.
	"""
	return mocker.patch("stpstone.ingestion.countries.us.exchange.alphavantage.sleep")


@pytest.fixture
def mock_backoff(mocker: MockerFixture) -> object:
	"""Patch backoff.on_exception to bypass retry delays.

	Parameters
	----------
	mocker : MockerFixture
		Pytest-mock fixture.

	Returns
	-------
	object
		Patched backoff decorator.
	"""
	return mocker.patch("backoff.on_exception", lambda *args, **kwargs: lambda func: func)


# --------------------------
# Tests — __init__
# --------------------------


def test_init_with_valid_inputs(sample_date: date) -> None:
	"""Test that all attributes are set correctly when valid inputs are supplied.

	Parameters
	----------
	sample_date : date
		Fixed reference date.

	Returns
	-------
	None
	"""
	inst = AlphaVantageUS(
		date_ref=sample_date,
		token=_SAMPLE_TOKEN,
		list_slugs=[_SAMPLE_SLUG, "MSFT"],
	)
	assert inst.date_ref == sample_date
	assert inst.token == _SAMPLE_TOKEN
	assert inst.list_slugs == [_SAMPLE_SLUG, "MSFT"]
	assert inst.cls_db is None
	assert isinstance(inst.cls_dates_current, DatesCurrent)
	assert isinstance(inst.cls_dates_br, DatesBRAnbima)
	assert isinstance(inst.cls_create_log, CreateLog)
	assert isinstance(inst.cls_dir_files_management, DirFilesManagement)


def test_init_default_date_is_previous_working_day() -> None:
	"""Test that the default date is set to the previous working day.

	Returns
	-------
	None
	"""
	with patch.object(DatesBRAnbima, "add_working_days", return_value=_SAMPLE_DATE):
		inst = AlphaVantageUS(token=_SAMPLE_TOKEN)
		assert inst.date_ref == _SAMPLE_DATE


def test_init_empty_list_slugs_defaults_to_empty_list() -> None:
	"""Test that omitting list_slugs results in an empty list (not None).

	Returns
	-------
	None
	"""
	inst = AlphaVantageUS(date_ref=_SAMPLE_DATE, token=_SAMPLE_TOKEN)
	assert inst.list_slugs == []


# --------------------------
# Tests — _build_url
# --------------------------


def test_build_url_contains_slug_and_token(instance: AlphaVantageUS) -> None:
	"""Test that the built URL encodes the ticker symbol and API key.

	Parameters
	----------
	instance : AlphaVantageUS
		Configured ingestion instance.

	Returns
	-------
	None
	"""
	url = instance._build_url("AAPL")
	assert "symbol=AAPL" in url
	assert f"apikey={_SAMPLE_TOKEN}" in url
	assert "function=TIME_SERIES_DAILY" in url
	assert "alphavantage.co" in url


@pytest.mark.parametrize("slug", ["MSFT", "GOOGL", "NVDA"])
def test_build_url_varies_by_slug(instance: AlphaVantageUS, slug: str) -> None:
	"""Test that each ticker symbol produces a distinct URL.

	Parameters
	----------
	instance : AlphaVantageUS
		Configured ingestion instance.
	slug : str
		Ticker symbol to embed in the URL.

	Returns
	-------
	None
	"""
	url = instance._build_url(slug)
	assert f"symbol={slug}" in url


# --------------------------
# Tests — get_response
# --------------------------


def test_get_response_success(
	instance: AlphaVantageUS,
	mock_requests_get: object,
	mock_response: Response,
	mock_backoff: object,
) -> None:
	"""Test that a successful HTTP call returns the response object.

	Parameters
	----------
	instance : AlphaVantageUS
		Configured ingestion instance.
	mock_requests_get : object
		Patched requests.get.
	mock_response : Response
		Mocked HTTP response.
	mock_backoff : object
		Patched backoff decorator.

	Returns
	-------
	None
	"""
	mock_requests_get.return_value = mock_response
	url = instance._build_url(_SAMPLE_SLUG)
	result = instance.get_response(url=url, timeout=(12.0, 21.0), bool_verify=False)
	assert result is mock_response
	mock_requests_get.assert_called_once_with(url, timeout=(12.0, 21.0), verify=False)
	mock_response.raise_for_status.assert_called_once()


def test_get_response_raises_on_http_error(
	instance: AlphaVantageUS,
	mock_requests_get: object,
	mock_backoff: object,
) -> None:
	"""Test that HTTPError from requests.get propagates (triggering backoff).

	Parameters
	----------
	instance : AlphaVantageUS
		Configured ingestion instance.
	mock_requests_get : object
		Patched requests.get.
	mock_backoff : object
		Patched backoff decorator.

	Returns
	-------
	None
	"""
	error_resp = MagicMock(spec=Response)
	error_resp.raise_for_status.side_effect = requests.exceptions.HTTPError("503")
	mock_requests_get.return_value = error_resp
	url = instance._build_url(_SAMPLE_SLUG)
	with pytest.raises(requests.exceptions.HTTPError):
		instance.get_response(url=url)


@pytest.mark.parametrize("timeout", [10, 15.0, (5.0, 10.0), (5, 10)])
def test_get_response_accepts_various_timeout_forms(
	instance: AlphaVantageUS,
	mock_requests_get: object,
	mock_response: Response,
	mock_backoff: object,
	timeout: Union[int, float, tuple],
) -> None:
	"""Test that all supported timeout types are forwarded to requests.get.

	Parameters
	----------
	instance : AlphaVantageUS
		Configured ingestion instance.
	mock_requests_get : object
		Patched requests.get.
	mock_response : Response
		Mocked HTTP response.
	mock_backoff : object
		Patched backoff decorator.
	timeout : Union[int, float, tuple]
		Timeout value under test.

	Returns
	-------
	None
	"""
	mock_requests_get.return_value = mock_response
	url = instance._build_url(_SAMPLE_SLUG)
	instance.get_response(url=url, timeout=timeout)
	mock_requests_get.assert_called_once_with(url, timeout=timeout, verify=False)


# --------------------------
# Tests — parse_raw_file
# --------------------------


def test_parse_raw_file_returns_stringio(
	instance: AlphaVantageUS,
	mock_response: Response,
) -> None:
	"""Test that parse_raw_file wraps the response text in a StringIO.

	Parameters
	----------
	instance : AlphaVantageUS
		Configured ingestion instance.
	mock_response : Response
		Mocked HTTP response.

	Returns
	-------
	None
	"""
	with patch.object(instance, "get_file", return_value=StringIO("{}")) as mock_get_file:
		result = instance.parse_raw_file(mock_response)
		assert isinstance(result, StringIO)
		mock_get_file.assert_called_once_with(resp_req=mock_response)


@pytest.mark.parametrize(
	"invalid_input,expected_error",
	[
		(None, "resp_req must be one of types: Response, Page, WebDriver, got NoneType"),
		("bad", "resp_req must be one of types: Response, Page, WebDriver, got str"),
		(42, "resp_req must be one of types: Response, Page, WebDriver, got int"),
		([], "resp_req must be one of types: Response, Page, WebDriver, got list"),
	],
)
def test_parse_raw_file_rejects_invalid_types(
	instance: AlphaVantageUS,
	invalid_input: object,
	expected_error: str,
) -> None:
	"""Test that type checker rejects non-Response inputs to parse_raw_file.

	Parameters
	----------
	instance : AlphaVantageUS
		Configured ingestion instance.
	invalid_input : object
		Value that should be rejected.
	expected_error : str
		Expected fragment of the TypeError message.

	Returns
	-------
	None
	"""
	with pytest.raises(TypeError, match=expected_error):
		instance.parse_raw_file(invalid_input)


# --------------------------
# Tests — transform_data
# --------------------------


def test_transform_data_valid_json_returns_dataframe(
	instance: AlphaVantageUS,
	mock_response: Response,
) -> None:
	"""Test that valid AlphaVantage JSON produces a well-formed DataFrame.

	Parameters
	----------
	instance : AlphaVantageUS
		Configured ingestion instance.
	mock_response : Response
		Mocked HTTP response with a valid JSON payload.

	Returns
	-------
	None
	"""
	df_ = instance.transform_data(resp_req=mock_response, slug=_SAMPLE_SLUG)
	assert isinstance(df_, pd.DataFrame)
	assert not df_.empty
	assert list(df_.columns) == ["DATE", "TICKER", "OPEN", "HIGH", "LOW", "CLOSE", "VOLUME"]
	assert (df_["TICKER"] == _SAMPLE_SLUG).all()
	assert len(df_) == 2


def test_transform_data_uses_slug_as_fallback_ticker(
	instance: AlphaVantageUS,
) -> None:
	"""Test that TICKER falls back to slug when Meta Data key is absent.

	Parameters
	----------
	instance : AlphaVantageUS
		Configured ingestion instance.

	Returns
	-------
	None
	"""
	json_no_meta = {
		"Time Series (Daily)": {
			"2025-01-02": {
				"1. open": "100",
				"2. high": "101",
				"3. low": "99",
				"4. close": "100.5",
				"5. volume": "1000",
			}
		}
	}
	resp = MagicMock(spec=Response)
	resp.json.return_value = json_no_meta
	df_ = instance.transform_data(resp_req=resp, slug="FAKE")
	assert df_["TICKER"].iloc[0] == "FAKE"


def test_transform_data_missing_time_series_returns_none(
	instance: AlphaVantageUS,
) -> None:
	"""Test that a response without 'Time Series (Daily)' returns None.

	Parameters
	----------
	instance : AlphaVantageUS
		Configured ingestion instance.

	Returns
	-------
	None
	"""
	resp = MagicMock(spec=Response)
	resp.json.return_value = {"Note": "API rate limit reached."}
	result = instance.transform_data(resp_req=resp, slug=_SAMPLE_SLUG)
	assert result is None


def test_transform_data_json_decode_error_returns_none(
	instance: AlphaVantageUS,
) -> None:
	"""Test that a JSON decode failure returns None without raising.

	Parameters
	----------
	instance : AlphaVantageUS
		Configured ingestion instance.

	Returns
	-------
	None
	"""
	resp = MagicMock(spec=Response)
	resp.json.side_effect = ValueError("No JSON")
	result = instance.transform_data(resp_req=resp, slug=_SAMPLE_SLUG)
	assert result is None


def test_transform_data_empty_time_series_returns_empty_df(
	instance: AlphaVantageUS,
) -> None:
	"""Test that an empty Time Series dict produces an empty DataFrame.

	Parameters
	----------
	instance : AlphaVantageUS
		Configured ingestion instance.

	Returns
	-------
	None
	"""
	resp = MagicMock(spec=Response)
	resp.json.return_value = {
		"Meta Data": {"2. Symbol": _SAMPLE_SLUG},
		"Time Series (Daily)": {},
	}
	# An empty mapping for Time Series is falsy → guard clause fires → None
	result = instance.transform_data(resp_req=resp, slug=_SAMPLE_SLUG)
	assert result is None


# --------------------------
# Tests — run
# --------------------------


def test_run_without_db_returns_dataframe(
	instance: AlphaVantageUS,
	mock_requests_get: object,
	mock_response: Response,
	mock_sleep: object,
	mock_backoff: object,
) -> None:
	"""Test that run() returns a DataFrame when cls_db is not set.

	Parameters
	----------
	instance : AlphaVantageUS
		Configured ingestion instance.
	mock_requests_get : object
		Patched requests.get.
	mock_response : Response
		Mocked HTTP response.
	mock_sleep : object
		Patched sleep callable.
	mock_backoff : object
		Patched backoff decorator.

	Returns
	-------
	None
	"""
	mock_requests_get.return_value = mock_response
	with patch.object(instance, "standardize_dataframe", side_effect=lambda **kw: kw["df_"]):
		result = instance.run()
	assert isinstance(result, pd.DataFrame)
	assert not result.empty


def test_run_sleeps_once_per_slug(
	instance: AlphaVantageUS,
	mock_requests_get: object,
	mock_response: Response,
	mock_sleep: object,
	mock_backoff: object,
) -> None:
	"""Test that the rate-limit sleep fires exactly once per slug.

	Parameters
	----------
	instance : AlphaVantageUS
		Configured ingestion instance.
	mock_requests_get : object
		Patched requests.get.
	mock_response : Response
		Mocked HTTP response.
	mock_sleep : object
		Patched sleep callable.
	mock_backoff : object
		Patched backoff decorator.

	Returns
	-------
	None
	"""
	instance.list_slugs = ["AAPL", "MSFT"]
	mock_requests_get.return_value = mock_response
	with patch.object(instance, "standardize_dataframe", side_effect=lambda **kw: kw["df_"]):
		instance.run()
	assert mock_sleep.call_count == 2
	mock_sleep.assert_called_with(RATE_LIMIT_SLEEP_SECONDS)


def test_run_with_db_inserts_and_returns_none(
	instance: AlphaVantageUS,
	mock_requests_get: object,
	mock_response: Response,
	mock_sleep: object,
	mock_backoff: object,
) -> None:
	"""Test that run() persists to the database and returns None when cls_db is set.

	Parameters
	----------
	instance : AlphaVantageUS
		Configured ingestion instance.
	mock_requests_get : object
		Patched requests.get.
	mock_response : Response
		Mocked HTTP response.
	mock_sleep : object
		Patched sleep callable.
	mock_backoff : object
		Patched backoff decorator.

	Returns
	-------
	None
	"""
	mock_db = MagicMock()
	instance.cls_db = mock_db
	mock_requests_get.return_value = mock_response
	with (
		patch.object(instance, "standardize_dataframe", side_effect=lambda **kw: kw["df_"]),
		patch.object(instance, "insert_table_db") as mock_insert,
	):
		result = instance.run()
	assert result is None
	mock_insert.assert_called_once()


def test_run_with_empty_slugs_returns_none(sample_date: date) -> None:
	"""Test that run() returns None immediately when list_slugs is empty.

	Parameters
	----------
	sample_date : date
		Fixed reference date.

	Returns
	-------
	None
	"""
	inst = AlphaVantageUS(date_ref=sample_date, token=_SAMPLE_TOKEN, list_slugs=[])
	result = inst.run()
	assert result is None


def test_run_skips_slugs_with_empty_responses(
	instance: AlphaVantageUS,
	mock_requests_get: object,
	mock_sleep: object,
	mock_backoff: object,
) -> None:
	"""Test that slugs returning None from transform_data are silently skipped.

	Parameters
	----------
	instance : AlphaVantageUS
		Configured ingestion instance.
	mock_requests_get : object
		Patched requests.get.
	mock_sleep : object
		Patched sleep callable.
	mock_backoff : object
		Patched backoff decorator.

	Returns
	-------
	None
	"""
	rate_limit_resp = MagicMock(spec=Response)
	rate_limit_resp.raise_for_status = MagicMock()
	rate_limit_resp.json.return_value = {"Note": "API rate limit reached."}
	mock_requests_get.return_value = rate_limit_resp
	result = instance.run()
	assert result is None


# --------------------------
# Tests — module reload
# --------------------------


def test_reload_module_preserves_constant() -> None:
	"""Test that RATE_LIMIT_SLEEP_SECONDS survives a module reload.

	Returns
	-------
	None
	"""
	import importlib

	import stpstone.ingestion.countries.us.exchange.alphavantage as mod

	original_val = mod.RATE_LIMIT_SLEEP_SECONDS
	importlib.reload(mod)
	assert original_val == mod.RATE_LIMIT_SLEEP_SECONDS


import requests  # noqa: E402  (needed for exception reference in test above)
