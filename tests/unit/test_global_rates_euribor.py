"""Unit tests for GlobalRatesEuribor class."""

from datetime import date
from typing import Union
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from pytest_mock import MockerFixture
from requests import Response

from stpstone.ingestion.countries.ww.macroeconomics.global_rates_euribor import GlobalRatesEuribor
from stpstone.utils.calendars.calendar_abc import DatesCurrent
from stpstone.utils.calendars.calendar_br import DatesBRAnbima
from stpstone.utils.loggs.create_logs import CreateLog
from stpstone.utils.parsers.folders import DirFilesManagement


# --------------------------
# Fixtures
# --------------------------
@pytest.fixture
def sample_date() -> date:
	"""Provide a fixed date for testing.

	Returns
	-------
	date
		Fixed reference date.
	"""
	return date(2025, 1, 1)


@pytest.fixture
def mock_response() -> Response:
	"""Mock HTTP Response with minimal HTML content.

	Returns
	-------
	Response
		Mocked Response object.
	"""
	response = MagicMock(spec=Response)
	response.status_code = 200
	response.raise_for_status = MagicMock()
	response.text = "<html><body><table><th>DATE</th><td>01-01-2025</td></table></body></html>"
	return response


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
		Mocked requests.get.
	"""
	return mocker.patch("requests.get")


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
		Mocked backoff decorator.
	"""
	return mocker.patch("backoff.on_exception", lambda *args, **kwargs: lambda func: func)


@pytest.fixture
def instance(sample_date: date) -> GlobalRatesEuribor:
	"""Provide a GlobalRatesEuribor instance for testing.

	Parameters
	----------
	sample_date : date
		Fixed reference date.

	Returns
	-------
	GlobalRatesEuribor
		Initialized instance.
	"""
	return GlobalRatesEuribor(date_ref=sample_date)


# --------------------------
# Tests
# --------------------------
def test_init_with_valid_date(sample_date: date) -> None:
	"""Test initialization sets expected attributes.

	Parameters
	----------
	sample_date : date
		Fixed reference date.

	Returns
	-------
	None
	"""
	obj = GlobalRatesEuribor(date_ref=sample_date)
	assert obj.date_ref == sample_date
	assert isinstance(obj.cls_dates_current, DatesCurrent)
	assert isinstance(obj.cls_dates_br, DatesBRAnbima)
	assert isinstance(obj.cls_create_log, CreateLog)
	assert isinstance(obj.cls_dir_files_management, DirFilesManagement)
	assert "global-rates.com" in obj.url
	assert "euribor" in obj.url


def test_init_with_default_date() -> None:
	"""Test initialization without a date_ref uses the previous working day.

	Returns
	-------
	None
	"""
	with patch.object(DatesBRAnbima, "add_working_days", return_value=date(2025, 1, 1)):
		obj = GlobalRatesEuribor()
		assert obj.date_ref == date(2025, 1, 1)


def test_get_response_success(
	instance: GlobalRatesEuribor,
	mock_requests_get: object,
	mock_response: Response,
	mock_backoff: object,
) -> None:
	"""Test get_response returns the mocked Response on success.

	Parameters
	----------
	instance : GlobalRatesEuribor
		Ingestion instance.
	mock_requests_get : object
		Mocked requests.get.
	mock_response : Response
		Mocked Response.
	mock_backoff : object
		Mocked backoff decorator.

	Returns
	-------
	None
	"""
	mock_requests_get.return_value = mock_response
	result = instance.get_response(timeout=(12.0, 21.0), bool_verify=False)
	assert result is mock_response
	mock_response.raise_for_status.assert_called_once()


def test_get_response_http_error(
	instance: GlobalRatesEuribor,
	mock_requests_get: object,
	mock_backoff: object,
) -> None:
	"""Test get_response propagates HTTPError after retries.

	Parameters
	----------
	instance : GlobalRatesEuribor
		Ingestion instance.
	mock_requests_get : object
		Mocked requests.get.
	mock_backoff : object
		Mocked backoff decorator.

	Returns
	-------
	None
	"""
	import requests as req_lib

	mock_requests_get.side_effect = req_lib.exceptions.HTTPError("500 Server Error")
	with pytest.raises(req_lib.exceptions.HTTPError):
		instance.get_response()


@pytest.mark.parametrize("timeout", [10, 10.5, (5.0, 10.0), (5, 10)])
def test_get_response_timeout_variations(
	instance: GlobalRatesEuribor,
	mock_requests_get: object,
	mock_response: Response,
	mock_backoff: object,
	timeout: Union[int, float, tuple],
) -> None:
	"""Test get_response accepts various timeout formats.

	Parameters
	----------
	instance : GlobalRatesEuribor
		Ingestion instance.
	mock_requests_get : object
		Mocked requests.get.
	mock_response : Response
		Mocked Response.
	mock_backoff : object
		Mocked backoff decorator.
	timeout : Union[int, float, tuple]
		Timeout value to test.

	Returns
	-------
	None
	"""
	mock_requests_get.return_value = mock_response
	result = instance.get_response(timeout=timeout)
	assert result is mock_response


def test_parse_raw_file_passthrough(
	instance: GlobalRatesEuribor,
	mock_response: Response,
) -> None:
	"""Test parse_raw_file returns the same response object unchanged.

	Parameters
	----------
	instance : GlobalRatesEuribor
		Ingestion instance.
	mock_response : Response
		Mocked Response.

	Returns
	-------
	None
	"""
	result = instance.parse_raw_file(mock_response)
	assert result is mock_response


def test_transform_data_returns_dataframe(instance: GlobalRatesEuribor) -> None:
	"""Test transform_data returns a DataFrame when HTML is parseable.

	Parameters
	----------
	instance : GlobalRatesEuribor
		Ingestion instance.

	Returns
	-------
	None
	"""
	mock_resp = MagicMock(spec=Response)
	mock_resp.text = (
		"<html><body><table>"
		"<tr><th>DATE</th><th>RATE_NAME</th><th>RATE_VALUE</th></tr>"
		"<tr><td>01-01-2025</td><td>EURIBOR_1W</td><td>3.5%</td></tr>"
		"</table></body></html>"
	)
	with patch(
		"stpstone.ingestion.countries.ww.macroeconomics._global_rates_base.HtmlHandler"
	) as mock_html_handler:
		from bs4 import BeautifulSoup

		soup = BeautifulSoup(mock_resp.text, "html.parser")
		mock_html_handler.return_value.bs_parser.return_value = soup
		with patch.object(instance, "_td_th_parser", return_value=pd.DataFrame({"A": [1]})):
			df_ = instance.transform_data(resp_req=mock_resp)
	assert isinstance(df_, pd.DataFrame)


def test_run_without_db(
	instance: GlobalRatesEuribor,
	mock_requests_get: object,
	mock_response: Response,
	mock_backoff: object,
) -> None:
	"""Test run returns a DataFrame when no database session is provided.

	Parameters
	----------
	instance : GlobalRatesEuribor
		Ingestion instance.
	mock_requests_get : object
		Mocked requests.get.
	mock_response : Response
		Mocked Response.
	mock_backoff : object
		Mocked backoff decorator.

	Returns
	-------
	None
	"""
	mock_requests_get.return_value = mock_response
	with (
		patch.object(instance, "get_response", return_value=mock_response),
		patch.object(
			instance, "transform_data", return_value=pd.DataFrame({"DATE": ["01-01-2025"]})
		),
		patch.object(
			instance, "standardize_dataframe", return_value=pd.DataFrame({"DATE": ["01-01-2025"]})
		),
	):
		result = instance.run()
	assert isinstance(result, pd.DataFrame)


def test_run_with_db(
	instance: GlobalRatesEuribor,
	mock_requests_get: object,
	mock_response: Response,
	mock_backoff: object,
) -> None:
	"""Test run calls insert_table_db and returns None when cls_db is set.

	Parameters
	----------
	instance : GlobalRatesEuribor
		Ingestion instance.
	mock_requests_get : object
		Mocked requests.get.
	mock_response : Response
		Mocked Response.
	mock_backoff : object
		Mocked backoff decorator.

	Returns
	-------
	None
	"""
	instance.cls_db = MagicMock()
	with (
		patch.object(instance, "get_response", return_value=mock_response),
		patch.object(
			instance, "transform_data", return_value=pd.DataFrame({"DATE": ["01-01-2025"]})
		),
		patch.object(
			instance, "standardize_dataframe", return_value=pd.DataFrame({"DATE": ["01-01-2025"]})
		),
		patch.object(instance, "insert_table_db") as mock_insert,
	):
		result = instance.run()
	assert result is None
	mock_insert.assert_called_once()


def test_reload_module() -> None:
	"""Test that the module can be reloaded without errors.

	Returns
	-------
	None
	"""
	import importlib

	import stpstone.ingestion.countries.ww.macroeconomics.global_rates_euribor as mod

	importlib.reload(mod)
	obj = mod.GlobalRatesEuribor(date_ref=date(2025, 1, 1))
	assert obj.date_ref == date(2025, 1, 1)
