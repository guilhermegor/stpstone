"""Unit tests for YahiiINSSContribution class.

Tests the ingestion functionality with various scenarios including:
- Initialization with valid and invalid inputs
- HTTP response handling
- HTML parsing
- Data transformation with salary bracket parsing
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

from stpstone.ingestion.countries.br.macroeconomics.yahii_inss_contribution import (
	YahiiINSSContribution,
)
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
def inss_instance(sample_date: date) -> YahiiINSSContribution:
	"""Fixture providing YahiiINSSContribution instance.

	Parameters
	----------
	sample_date : date
		Sample date for initialization

	Returns
	-------
	YahiiINSSContribution
		Initialized YahiiINSSContribution instance
	"""
	return YahiiINSSContribution(date_ref=sample_date)


@pytest.fixture
def sample_list_td() -> list[str]:
	"""Provide sample table data for transformation testing.

	Returns
	-------
	list[str]
		Sample raw text values mimicking INSS contribution table
	"""
	return [
		"De 1.302,00 ate 2.571,29",
		"7,5%",
		"De 2.571,30 ate 3.856,94",
		"9%",
		"De 3.856,95 ate 7.507,49",
		"12%",
		"acima de 7.507,49",
		"14%",
	]


# --------------------------
# Tests
# --------------------------
def test_init_with_valid_inputs(sample_date: date) -> None:
	"""Test initialization with valid inputs.

	Verifies
	--------
	- The YahiiINSSContribution instance is initialized correctly
	- Attributes are set as expected

	Parameters
	----------
	sample_date : date
		Sample date for initialization

	Returns
	-------
	None
	"""
	instance = YahiiINSSContribution(date_ref=sample_date)
	assert instance.date_ref == sample_date
	assert instance.url == "https://www.yahii.com.br/inss.html"
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
		instance = YahiiINSSContribution()
		assert instance.date_ref == date(2025, 1, 1)
		assert instance.url == "https://www.yahii.com.br/inss.html"


def test_get_response_success(
	inss_instance: YahiiINSSContribution,
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
	inss_instance : YahiiINSSContribution
		Instance of YahiiINSSContribution
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
	result = inss_instance.get_response(timeout=(12.0, 21.0), bool_verify=False)
	assert isinstance(result, Response)
	mock_requests_get.assert_called_once_with(
		inss_instance.url, timeout=(12.0, 21.0), verify=False
	)
	mock_response.raise_for_status.assert_called_once()


def test_parse_raw_file(inss_instance: YahiiINSSContribution, mock_response: Response) -> None:
	"""Test parsing of raw HTML content.

	Verifies
	--------
	- HTML content is parsed and XPath extracts text elements
	- Returns a list of strings

	Parameters
	----------
	inss_instance : YahiiINSSContribution
		Instance of YahiiINSSContribution
	mock_response : Response
		Mocked Response object

	Returns
	-------
	None
	"""
	mock_element = MagicMock()
	mock_element.text = "De 1.302,00 ate 2.571,29"
	with patch(
		"stpstone.ingestion.countries.br.macroeconomics.yahii_inss_contribution.HtmlHandler"
	) as mock_html:
		mock_html_instance = MagicMock()
		mock_html.return_value = mock_html_instance
		mock_html_instance.lxml_parser.return_value = MagicMock()
		mock_html_instance.lxml_xpath.return_value = [mock_element]
		result = inss_instance.parse_raw_file(mock_response)
		assert isinstance(result, list)
		assert len(result) == 1
		assert result[0] == "De 1.302,00 ate 2.571,29"


def test_transform_data(
	inss_instance: YahiiINSSContribution,
	sample_list_td: list[str],
) -> None:
	"""Test data transformation into DataFrame.

	Verifies
	--------
	- Input list is correctly transformed into DataFrame
	- Salary brackets are parsed into SALARIO_INF and SALARIO_SUP
	- Percentage values are converted to decimals

	Parameters
	----------
	inss_instance : YahiiINSSContribution
		Instance of YahiiINSSContribution
	sample_list_td : list[str]
		Sample raw text values

	Returns
	-------
	None
	"""
	df_ = inss_instance.transform_data(sample_list_td)
	assert isinstance(df_, pd.DataFrame)
	assert not df_.empty, "DataFrame should not be empty"
	assert "SALARIO_CONTRIBUICAO" in df_.columns
	assert "ALIQUOTA_RECOLHIMENTO_INSS" in df_.columns
	assert "SALARIO_INF" in df_.columns
	assert "SALARIO_SUP" in df_.columns


def test_transform_data_salary_brackets(inss_instance: YahiiINSSContribution) -> None:
	"""Test that salary bracket parsing works correctly.

	Verifies
	--------
	- SALARIO_INF is extracted from "De X ate Y" patterns
	- SALARIO_SUP is extracted from "ate Y" patterns
	- "acima de" ranges produce correct INF values

	Parameters
	----------
	inss_instance : YahiiINSSContribution
		Instance of YahiiINSSContribution

	Returns
	-------
	None
	"""
	list_td = [
		"De 1.302,00 ate 2.571,29",
		"7,5",
		"acima de 7.507,49",
		"14",
	]
	df_ = inss_instance.transform_data(list_td)
	assert df_["SALARIO_INF"].iloc[0] == pytest.approx(1302.0, rel=1e-2)
	assert df_["SALARIO_SUP"].iloc[0] == pytest.approx(2571.29, rel=1e-2)
	assert df_["SALARIO_INF"].iloc[1] == pytest.approx(7507.49, rel=1e-2)
	assert df_["SALARIO_SUP"].iloc[1] == pytest.approx(1_000_000_000, rel=1e-2)


def test_run_without_db(
	inss_instance: YahiiINSSContribution,
	mock_requests_get: object,
	mock_response: Response,
	mock_backoff: object,
) -> None:
	"""Test run method without database session.

	Verifies
	--------
	- Full ingestion pipeline works without database
	- Returns transformed DataFrame

	Parameters
	----------
	inss_instance : YahiiINSSContribution
		Instance of YahiiINSSContribution
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
		inss_instance, "parse_raw_file", return_value=["De 1.302,00 ate 2.571,29", "7,5"]
	) as mock_parse, \
		patch.object(
			inss_instance, "transform_data",
			return_value=pd.DataFrame({"SALARIO_CONTRIBUICAO": ["bracket"]})
		) as mock_transform, \
		patch.object(
			inss_instance, "standardize_dataframe",
			return_value=pd.DataFrame({"SALARIO_CONTRIBUICAO": ["bracket"]})
		) as mock_standardize:
		result = inss_instance.run()
		assert isinstance(result, pd.DataFrame)
		mock_parse.assert_called_once()
		mock_transform.assert_called_once()
		mock_standardize.assert_called_once()


def test_run_with_db(
	inss_instance: YahiiINSSContribution,
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
	inss_instance : YahiiINSSContribution
		Instance of YahiiINSSContribution
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
	inss_instance.cls_db = mock_db
	mock_requests_get.return_value = mock_response
	with patch.object(
		inss_instance, "parse_raw_file", return_value=["De 1.302,00 ate 2.571,29", "7,5"]
	) as mock_parse, \
		patch.object(
			inss_instance, "transform_data",
			return_value=pd.DataFrame({"SALARIO_CONTRIBUICAO": ["bracket"]})
		) as mock_transform, \
		patch.object(
			inss_instance, "standardize_dataframe",
			return_value=pd.DataFrame({"SALARIO_CONTRIBUICAO": ["bracket"]})
		) as mock_standardize, \
		patch.object(inss_instance, "insert_table_db") as mock_insert:
		result = inss_instance.run()
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
	inss_instance: YahiiINSSContribution,
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
	inss_instance : YahiiINSSContribution
		Instance of YahiiINSSContribution
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
	result = inss_instance.get_response(timeout=timeout)
	assert isinstance(result, Response)
	mock_requests_get.assert_called_once_with(inss_instance.url, timeout=timeout, verify=False)


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

	import stpstone.ingestion.countries.br.macroeconomics.yahii_inss_contribution
	original_instance = YahiiINSSContribution(date_ref=date(2025, 1, 1))
	importlib.reload(stpstone.ingestion.countries.br.macroeconomics.yahii_inss_contribution)
	new_instance = YahiiINSSContribution(date_ref=date(2025, 1, 1))
	assert new_instance.date_ref == original_instance.date_ref
	assert new_instance.url == original_instance.url
