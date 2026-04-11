"""Unit tests for TiingoUS ingestion class.

Tests the ingestion functionality with various scenarios including:
- Initialization with valid and invalid inputs
- URL building per slug
- HTTP response handling (success, HTTP error, timeout variants)
- JSON parsing and transformation
- Guard clauses for missing / empty JSON payloads
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
import requests
from requests import Response

from stpstone.ingestion.countries.us.exchange.tiingo import (
	RATE_LIMIT_SLEEP_SECONDS,
	TiingoUS,
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

_VALID_JSON = [
	{
		"date": "2025-01-02T00:00:00+00:00",
		"close": 183.75,
		"high": 185.00,
		"low": 181.00,
		"open": 182.50,
		"volume": 55000000.0,
		"adjClose": 183.75,
		"adjHigh": 185.00,
		"adjLow": 181.00,
		"adjOpen": 182.50,
		"adjVolume": 55000000.0,
		"divCash": 0.0,
		"splitFactor": 1.0,
	},
	{
		"date": "2025-01-01T00:00:00+00:00",
		"close": 182.50,
		"high": 183.00,
		"low": 179.50,
		"open": 180.00,
		"volume": 48000000.0,
		"adjClose": 182.50,
		"adjHigh": 183.00,
		"adjLow": 179.50,
		"adjOpen": 180.00,
		"adjVolume": 48000000.0,
		"divCash": 0.0,
		"splitFactor": 1.0,
	},
]


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
def instance(sample_date: date) -> TiingoUS:
	"""Return a TiingoUS instance with one slug and no DB session.

	Parameters
	----------
	sample_date : date
		Fixed reference date.

	Returns
	-------
	TiingoUS
		Configured instance for testing.
	"""
	return TiingoUS(
		date_ref=sample_date,
		token=_SAMPLE_TOKEN,
		list_slugs=[_SAMPLE_SLUG],
	)


@pytest.fixture
def mock_response() -> Response:
	"""Return a mocked Response whose JSON matches the Tiingo schema.

	Returns
	-------
	Response
		Mocked Response with valid adjusted OHLCV JSON payload.
	"""
	resp = MagicMock(spec=Response)
	resp.json.return_value = _VALID_JSON
	resp.text = "[]"
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
	return mocker.patch(
		"stpstone.ingestion.countries.us.exchange.tiingo.requests.get"
	)


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
	return mocker.patch(
		"stpstone.ingestion.countries.us.exchange.tiingo.sleep"
	)


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
	return mocker.patch(
		"backoff.on_exception", lambda *args, **kwargs: lambda func: func
	)


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
	inst = TiingoUS(
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
	with patch.object(
		DatesBRAnbima, "add_working_days", return_value=_SAMPLE_DATE
	):
		inst = TiingoUS(token=_SAMPLE_TOKEN)
		assert inst.date_ref == _SAMPLE_DATE


def test_init_empty_list_slugs_defaults_to_empty_list() -> None:
	"""Test that omitting list_slugs results in an empty list (not None).

	Returns
	-------
	None
	"""
	inst = TiingoUS(date_ref=_SAMPLE_DATE, token=_SAMPLE_TOKEN)
	assert inst.list_slugs == []


def test_init_explicit_date_start_and_end() -> None:
	"""Test that explicit date_start and date_end override the computed defaults.

	Returns
	-------
	None
	"""
	start = date(2024, 6, 1)
	end = date(2024, 12, 31)
	inst = TiingoUS(date_ref=_SAMPLE_DATE, date_start=start, date_end=end)
	assert inst.date_start == start
	assert inst.date_end == end


# --------------------------
# Tests — _build_url
# --------------------------


def test_build_url_contains_slug_dates_and_base(instance: TiingoUS) -> None:
	"""Test that the built URL encodes the ticker symbol and date range.

	Parameters
	----------
	instance : TiingoUS
		Configured ingestion instance.

	Returns
	-------
	None
	"""
	url = instance._build_url("AAPL")
	assert "tiingo.com" in url
	assert "/daily/AAPL/prices" in url
	assert "startDate=" in url
	assert "endDate=" in url
	assert "format=json" in url


@pytest.mark.parametrize("slug", ["MSFT", "GOOGL", "NVDA"])
def test_build_url_varies_by_slug(instance: TiingoUS, slug: str) -> None:
	"""Test that each ticker symbol produces a distinct URL.

	Parameters
	----------
	instance : TiingoUS
		Configured ingestion instance.
	slug : str
		Ticker symbol to embed in the URL.

	Returns
	-------
	None
	"""
	url = instance._build_url(slug)
	assert f"/daily/{slug}/prices" in url


# --------------------------
# Tests — get_response
# --------------------------


def test_get_response_success(
	instance: TiingoUS,
	mock_requests_get: object,
	mock_response: Response,
	mock_backoff: object,
) -> None:
	"""Test that a successful HTTP call returns the response object.

	Parameters
	----------
	instance : TiingoUS
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
	mock_response.raise_for_status.assert_called_once()


def test_get_response_raises_on_http_error(
	instance: TiingoUS,
	mock_requests_get: object,
	mock_backoff: object,
) -> None:
	"""Test that HTTPError from requests.get propagates (triggering backoff).

	Parameters
	----------
	instance : TiingoUS
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
	error_resp.raise_for_status.side_effect = requests.exceptions.HTTPError("403")
	mock_requests_get.return_value = error_resp
	url = instance._build_url(_SAMPLE_SLUG)
	with pytest.raises(requests.exceptions.HTTPError):
		instance.get_response(url=url)


@pytest.mark.parametrize("timeout", [10, 15.0, (5.0, 10.0), (5, 10)])
def test_get_response_accepts_various_timeout_forms(
	instance: TiingoUS,
	mock_requests_get: object,
	mock_response: Response,
	mock_backoff: object,
	timeout: Union[int, float, tuple],
) -> None:
	"""Test that all supported timeout types are forwarded to requests.get.

	Parameters
	----------
	instance : TiingoUS
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
	mock_requests_get.assert_called_once()
	_, call_kwargs = mock_requests_get.call_args
	assert call_kwargs["timeout"] == timeout


# --------------------------
# Tests — parse_raw_file
# --------------------------


def test_parse_raw_file_returns_stringio(
	instance: TiingoUS,
	mock_response: Response,
) -> None:
	"""Test that parse_raw_file wraps the response text in a StringIO.

	Parameters
	----------
	instance : TiingoUS
		Configured ingestion instance.
	mock_response : Response
		Mocked HTTP response.

	Returns
	-------
	None
	"""
	with patch.object(
		instance, "get_file", return_value=StringIO("[]")
	) as mock_get_file:
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
	instance: TiingoUS,
	invalid_input: object,
	expected_error: str,
) -> None:
	"""Test that type checker rejects non-Response inputs to parse_raw_file.

	Parameters
	----------
	instance : TiingoUS
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
	instance: TiingoUS,
	mock_response: Response,
) -> None:
	"""Test that a valid Tiingo JSON list produces a well-formed DataFrame.

	Parameters
	----------
	instance : TiingoUS
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
	expected_cols = [
		"DATE", "TICKER", "CLOSE", "HIGH", "LOW", "OPEN", "VOLUME",
		"ADJ_CLOSE", "ADJ_HIGH", "ADJ_LOW", "ADJ_OPEN", "ADJ_VOLUME",
		"DIV_CASH", "SPLIT_FACTOR",
	]
	assert list(df_.columns) == expected_cols
	assert (df_["TICKER"] == _SAMPLE_SLUG).all()
	assert len(df_) == 2


def test_transform_data_empty_payload_returns_none(
	instance: TiingoUS,
) -> None:
	"""Test that an empty list payload returns None.

	Parameters
	----------
	instance : TiingoUS
		Configured ingestion instance.

	Returns
	-------
	None
	"""
	resp = MagicMock(spec=Response)
	resp.json.return_value = []
	result = instance.transform_data(resp_req=resp, slug=_SAMPLE_SLUG)
	assert result is None


def test_transform_data_json_decode_error_returns_none(
	instance: TiingoUS,
) -> None:
	"""Test that a JSON decode failure returns None without raising.

	Parameters
	----------
	instance : TiingoUS
		Configured ingestion instance.

	Returns
	-------
	None
	"""
	resp = MagicMock(spec=Response)
	resp.json.side_effect = ValueError("No JSON")
	result = instance.transform_data(resp_req=resp, slug=_SAMPLE_SLUG)
	assert result is None


def test_transform_data_ticker_matches_slug(
	instance: TiingoUS,
) -> None:
	"""Test that the TICKER column always reflects the supplied slug argument.

	Parameters
	----------
	instance : TiingoUS
		Configured ingestion instance.

	Returns
	-------
	None
	"""
	resp = MagicMock(spec=Response)
	resp.json.return_value = [
		{
			"date": "2025-01-02T00:00:00+00:00",
			"close": 100.0, "high": 101.0, "low": 99.0, "open": 100.0,
			"volume": 1000.0, "adjClose": 100.0, "adjHigh": 101.0,
			"adjLow": 99.0, "adjOpen": 100.0, "adjVolume": 1000.0,
			"divCash": 0.0, "splitFactor": 1.0,
		}
	]
	df_ = instance.transform_data(resp_req=resp, slug="FAKE")
	assert df_["TICKER"].iloc[0] == "FAKE"


# --------------------------
# Tests — run
# --------------------------


def test_run_without_db_returns_dataframe(
	instance: TiingoUS,
	mock_requests_get: object,
	mock_response: Response,
	mock_sleep: object,
	mock_backoff: object,
) -> None:
	"""Test that run() returns a DataFrame when cls_db is not set.

	Parameters
	----------
	instance : TiingoUS
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
	with patch.object(
		instance, "standardize_dataframe", side_effect=lambda **kw: kw["df_"]
	):
		result = instance.run()
	assert isinstance(result, pd.DataFrame)
	assert not result.empty


def test_run_sleeps_once_per_slug(
	instance: TiingoUS,
	mock_requests_get: object,
	mock_response: Response,
	mock_sleep: object,
	mock_backoff: object,
) -> None:
	"""Test that the rate-limit sleep fires exactly once per slug.

	Parameters
	----------
	instance : TiingoUS
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
	with patch.object(
		instance, "standardize_dataframe", side_effect=lambda **kw: kw["df_"]
	):
		instance.run()
	assert mock_sleep.call_count == 2
	mock_sleep.assert_called_with(RATE_LIMIT_SLEEP_SECONDS)


def test_run_with_db_inserts_and_returns_none(
	instance: TiingoUS,
	mock_requests_get: object,
	mock_response: Response,
	mock_sleep: object,
	mock_backoff: object,
) -> None:
	"""Test that run() persists to the database and returns None when cls_db is set.

	Parameters
	----------
	instance : TiingoUS
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
	with patch.object(
		instance, "standardize_dataframe", side_effect=lambda **kw: kw["df_"]
	), patch.object(instance, "insert_table_db") as mock_insert:
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
	inst = TiingoUS(date_ref=sample_date, token=_SAMPLE_TOKEN, list_slugs=[])
	result = inst.run()
	assert result is None


def test_run_skips_slugs_with_empty_responses(
	instance: TiingoUS,
	mock_requests_get: object,
	mock_sleep: object,
	mock_backoff: object,
) -> None:
	"""Test that slugs returning None from transform_data are silently skipped.

	Parameters
	----------
	instance : TiingoUS
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
	empty_resp = MagicMock(spec=Response)
	empty_resp.raise_for_status = MagicMock()
	empty_resp.json.return_value = []
	mock_requests_get.return_value = empty_resp
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

	import stpstone.ingestion.countries.us.exchange.tiingo as mod

	original_val = mod.RATE_LIMIT_SLEEP_SECONDS
	importlib.reload(mod)
	assert original_val == mod.RATE_LIMIT_SLEEP_SECONDS
