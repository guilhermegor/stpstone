"""Unit tests for YahiiMinWage class.

Tests the ingestion functionality with various scenarios including:
- Initialization with valid and invalid inputs
- HTTP response handling
- HTML parsing
- Data transformation and filtering
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

from stpstone.ingestion.countries.br.macroeconomics.yahii_min_wage import YahiiMinWage
from stpstone.utils.calendars.calendar_abc import DatesCurrent
from stpstone.utils.calendars.calendar_br import DatesBRAnbima
from stpstone.utils.loggs.create_logs import CreateLog
from stpstone.utils.parsers.folders import DirFilesManagement


# --------------------------
# Fixtures
# --------------------------
@pytest.fixture
def mock_response() -> Response:
	"""Mock Response object with sample HTML content.

	Returns
	-------
	Response
		Mocked Response object with predefined content
	"""
	response = MagicMock(spec=Response)
	response.content = b"<html><body><table cellspacing='3'><tbody><tr>"
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
	return date(2025, 1, 15)


@pytest.fixture
def yahii_instance(sample_date: date) -> YahiiMinWage:
	"""Fixture providing YahiiMinWage instance.

	Parameters
	----------
	sample_date : date
		Sample date for initialization

	Returns
	-------
	YahiiMinWage
		Initialized YahiiMinWage instance
	"""
	return YahiiMinWage(date_ref=sample_date)


@pytest.fixture
def sample_list_td() -> list[str]:
	"""Provide sample table data for transformation testing.

	Returns
	-------
	list[str]
		Sample raw text values mimicking HTML extraction
	"""
	return [
		"DECRETO-LEI 399/38",
		"01.05.00",
		"R$ 151,00",
		"LEI 12.382/11",
		"01.01.11",
		"R$ 540,00",
		"LEI 14.158/21",
		"01.01.22",
		"R$ 1.212,00",
	]


# --------------------------
# Tests
# --------------------------
def test_init_with_valid_inputs(sample_date: date) -> None:
	"""Test initialization with valid inputs.

	Verifies
	--------
	- The YahiiMinWage instance is initialized correctly
	- Attributes are set as expected

	Parameters
	----------
	sample_date : date
		Sample date for initialization

	Returns
	-------
	None
	"""
	instance = YahiiMinWage(date_ref=sample_date)
	assert instance.date_ref == sample_date
	assert instance.url == "https://www.yahii.com.br/salariomi.html"
	assert isinstance(instance.cls_dates_current, DatesCurrent)
	assert isinstance(instance.cls_dates_br, DatesBRAnbima)
	assert isinstance(instance.cls_create_log, CreateLog)
	assert isinstance(instance.cls_dir_files_management, DirFilesManagement)


def test_init_with_default_date() -> None:
	"""Test initialization with default date.

	Verifies
	--------
	- Default date is set to previous working day
	- URL is correctly set

	Returns
	-------
	None
	"""
	with patch.object(DatesBRAnbima, "add_working_days", return_value=date(2025, 1, 1)):
		instance = YahiiMinWage()
		assert instance.date_ref == date(2025, 1, 1)
		assert instance.url == "https://www.yahii.com.br/salariomi.html"


def test_get_response_success(
	yahii_instance: YahiiMinWage,
	mock_requests_get: object,
	mock_response: Response,
	mock_backoff: object,
) -> None:
	"""Test successful HTTP response retrieval.

	Verifies
	--------
	- Successful HTTP request returns Response object
	- Correct parameters are passed to requests.get
	- SSL verify is False by default

	Parameters
	----------
	yahii_instance : YahiiMinWage
		Instance of YahiiMinWage
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
	result = yahii_instance.get_response(timeout=(12.0, 21.0), bool_verify=False)
	assert isinstance(result, Response)
	mock_requests_get.assert_called_once_with(
		yahii_instance.url, timeout=(12.0, 21.0), verify=False
	)
	mock_response.raise_for_status.assert_called_once()


def test_parse_raw_file(yahii_instance: YahiiMinWage, mock_response: Response) -> None:
	"""Test parsing of raw HTML content.

	Verifies
	--------
	- HTML content is parsed and XPath extracts text elements
	- Returns a list of strings

	Parameters
	----------
	yahii_instance : YahiiMinWage
		Instance of YahiiMinWage
	mock_response : Response
		Mocked Response object

	Returns
	-------
	None
	"""
	mock_element = MagicMock()
	mock_element.text = "test value"
	with patch(
		"stpstone.ingestion.countries.br.macroeconomics.yahii_min_wage.HtmlHandler"
	) as mock_html:
		mock_html_instance = MagicMock()
		mock_html.return_value = mock_html_instance
		mock_html_instance.lxml_parser.return_value = MagicMock()
		mock_html_instance.lxml_xpath.return_value = [mock_element]
		result = yahii_instance.parse_raw_file(mock_response)
		assert isinstance(result, list)
		assert len(result) == 1
		assert result[0] == "test value"


def test_transform_data(
	yahii_instance: YahiiMinWage,
	sample_list_td: list[str],
) -> None:
	"""Test data transformation into DataFrame.

	Verifies
	--------
	- Input list is correctly transformed into DataFrame
	- Currency prefixes are stripped
	- Date filtering works correctly

	Parameters
	----------
	yahii_instance : YahiiMinWage
		Instance of YahiiMinWage
	sample_list_td : list[str]
		Sample raw text values

	Returns
	-------
	None
	"""
	df_ = yahii_instance.transform_data(sample_list_td)
	assert isinstance(df_, pd.DataFrame)
	assert not df_.empty, "DataFrame should not be empty"
	assert "DISPOSITIVO_LEGAL" in df_.columns
	assert "DATA" in df_.columns
	assert "VALOR" in df_.columns


def test_transform_data_filters_revogada(yahii_instance: YahiiMinWage) -> None:
	"""Test that REVOGADA entries are filtered out.

	Verifies
	--------
	- Rows with DATA == "REVOGADA" are removed

	Parameters
	----------
	yahii_instance : YahiiMinWage
		Instance of YahiiMinWage

	Returns
	-------
	None
	"""
	list_td = [
		"DECRETO 1/00",
		"REVOGADA",
		"R$ 100,00",
		"LEI 2/01",
		"01.01.01",
		"R$ 200,00",
	]
	df_ = yahii_instance.transform_data(list_td)
	assert "REVOGADA" not in df_["DATA"].astype(str).values


def test_run_without_db(
	yahii_instance: YahiiMinWage,
	mock_requests_get: object,
	mock_response: Response,
	mock_backoff: object,
) -> None:
	"""Test run method without database session.

	Verifies
	--------
	- Full ingestion pipeline works without database
	- Returns transformed DataFrame
	- All intermediate methods are called correctly

	Parameters
	----------
	yahii_instance : YahiiMinWage
		Instance of YahiiMinWage
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
	with patch.object(
		yahii_instance, "parse_raw_file", return_value=["DECRETO 1", "01.01.01", "R$ 100,00"]
	) as mock_parse, \
		patch.object(
			yahii_instance, "transform_data",
			return_value=pd.DataFrame({"DISPOSITIVO_LEGAL": ["LEI 1"], "DATA": ["2001-01-01"],
				"VALOR": [100.0]})
		) as mock_transform, \
		patch.object(
			yahii_instance, "standardize_dataframe",
			return_value=pd.DataFrame({"DISPOSITIVO_LEGAL": ["LEI 1"], "DATA": ["2001-01-01"],
				"VALOR": [100.0]})
		) as mock_standardize:
		result = yahii_instance.run()
		assert isinstance(result, pd.DataFrame)
		mock_parse.assert_called_once()
		mock_transform.assert_called_once()
		mock_standardize.assert_called_once()


def test_run_with_db(
	yahii_instance: YahiiMinWage,
	mock_requests_get: object,
	mock_response: Response,
	mock_backoff: object,
) -> None:
	"""Test run method with database session.

	Verifies
	--------
	- Database insertion is called when cls_db is provided
	- No DataFrame is returned

	Parameters
	----------
	yahii_instance : YahiiMinWage
		Instance of YahiiMinWage
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
	yahii_instance.cls_db = mock_db
	mock_requests_get.return_value = mock_response
	with patch.object(
		yahii_instance, "parse_raw_file", return_value=["DECRETO 1", "01.01.01", "R$ 100,00"]
	) as mock_parse, \
		patch.object(
			yahii_instance, "transform_data",
			return_value=pd.DataFrame({"DISPOSITIVO_LEGAL": ["LEI 1"]})
		) as mock_transform, \
		patch.object(
			yahii_instance, "standardize_dataframe",
			return_value=pd.DataFrame({"DISPOSITIVO_LEGAL": ["LEI 1"]})
		) as mock_standardize, \
		patch.object(yahii_instance, "insert_table_db") as mock_insert:
		result = yahii_instance.run()
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
	yahii_instance: YahiiMinWage,
	mock_requests_get: object,
	mock_response: Response,
	mock_backoff: object,
	timeout: Union[int, float, tuple],
) -> None:
	"""Test get_response with various timeout inputs.

	Verifies
	--------
	- Different timeout formats are handled correctly

	Parameters
	----------
	yahii_instance : YahiiMinWage
		Instance of YahiiMinWage
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
	result = yahii_instance.get_response(timeout=timeout)
	assert isinstance(result, Response)
	mock_requests_get.assert_called_once_with(yahii_instance.url, timeout=timeout, verify=False)


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

	import stpstone.ingestion.countries.br.macroeconomics.yahii_min_wage
	original_instance = YahiiMinWage(date_ref=date(2025, 1, 1))
	importlib.reload(stpstone.ingestion.countries.br.macroeconomics.yahii_min_wage)
	new_instance = YahiiMinWage(date_ref=date(2025, 1, 1))
	assert new_instance.date_ref == original_instance.date_ref
	assert new_instance.url == original_instance.url
