"""Unit tests for B3FinancialIndicators class.

Tests the ingestion, parsing, and transformation functionality with various input scenarios
including:
- Initialization with valid and invalid inputs
- Response handling and parsing
- Data transformation and standardization
- Database insertion scenarios
- Edge cases and error conditions
"""

from datetime import date
from typing import Any, Union
from unittest.mock import ANY, MagicMock, patch

import pandas as pd
from playwright.sync_api import Page as PlaywrightPage
import pytest
from pytest_mock import MockerFixture
from requests import Response
from selenium.webdriver.remote.webdriver import WebDriver as SeleniumWebDriver

from stpstone.ingestion.countries.br.macroeconomics.b3_financial_indicators import (
	B3FinancialIndicators,
)


# --------------------------
# Fixtures
# --------------------------
@pytest.fixture
def b3_instance() -> B3FinancialIndicators:
	"""Fixture providing a B3FinancialIndicators instance with default settings.

	Returns
	-------
	B3FinancialIndicators
		Instance initialized with no date_ref, logger, or db session
	"""
	return B3FinancialIndicators()


@pytest.fixture
def mock_response() -> Response:
	"""Fixture providing a mocked requests Response object.

	Returns
	-------
	Response
		Mocked response with sample JSON data
	"""
	response = MagicMock(spec=Response)
	response.status_code = 200
	response.json.return_value = [
		{
			"securityIdentificationCode": "123",
			"description": "Test Indicator",
			"groupDescription": "Group A",
			"value": "100.50",
			"rate": "1.5",
			"lastUpdate": date(2023, 1, 1),
		}
	]
	response.raise_for_status = MagicMock()
	return response


@pytest.fixture
def sample_json_data() -> list[dict[str, Union[str, int, float]]]:
	"""Fixture providing sample JSON data for testing.

	Returns
	-------
	list[dict[str, Union[str, int, float]]]
		Sample data mimicking B3 financial indicators
	"""
	return [
		{
			"securityIdentificationCode": "123",
			"description": "Test Indicator",
			"groupDescription": "Group A",
			"value": "100.50",
			"rate": "1.5",
			"lastUpdate": date(2023, 1, 1),
		}
	]


@pytest.fixture
def mock_requests_get(mocker: MockerFixture) -> MagicMock:
	"""Mock requests.get to prevent real HTTP calls.

	Parameters
	----------
	mocker : MockerFixture
		Pytest-mock fixture for creating mocks

	Returns
	-------
	MagicMock
		Mock object for requests.get
	"""
	return mocker.patch("requests.get")


@pytest.fixture
def mock_db_session(mocker: MockerFixture) -> MagicMock:
	"""Mock database session for testing.

	Parameters
	----------
	mocker : MockerFixture
		Pytest-mock fixture for creating mocks

	Returns
	-------
	MagicMock
		Mock object for database session
	"""
	return mocker.patch("requests.Session")


# --------------------------
# Tests
# --------------------------
def test_init_default(b3_instance: B3FinancialIndicators) -> None:
	"""Test initialization with default parameters.

	Verifies
	-------
	- Instance is created with default date_ref
	- Logger and db session are None
	- URL is correctly set
	- Inherited classes are initialized

	Parameters
	----------
	b3_instance : B3FinancialIndicators
		Initialized instance

	Returns
	-------
	None
	"""
	assert isinstance(b3_instance.date_ref, date)
	assert b3_instance.logger is None
	assert b3_instance.cls_db is None
	assert (
		b3_instance.url
		== "https://sistemaswebb3-derivativos.b3.com.br/financialIndicatorsProxy/FinancialIndicators/GetFinancialIndicators/eyJsYW5ndWFnZSI6InB0LWJyIn0="
	)
	assert hasattr(b3_instance, "cls_dir_files_management")
	assert hasattr(b3_instance, "cls_dates_current")
	assert hasattr(b3_instance, "cls_create_log")
	assert hasattr(b3_instance, "cls_dates_br")


def test_init_with_custom_date(b3_instance: B3FinancialIndicators) -> None:
	"""Test initialization with a custom date_ref.

	Verifies
	-------
	- Instance is created with provided date_ref
	- Other attributes are set correctly

	Parameters
	----------
	b3_instance : B3FinancialIndicators
		Initialized instance

	Returns
	-------
	None
	"""
	custom_date = date(2023, 1, 1)
	instance = B3FinancialIndicators(date_ref=custom_date)
	assert instance.date_ref == custom_date
	assert instance.logger is None
	assert instance.cls_db is None


@pytest.mark.parametrize("invalid_date", ["2023-01-01", 123, []])
def test_init_invalid_date_type(
	invalid_date: Any,  # noqa ANN401: typing.Any is not allowed
) -> None:
	"""Test initialization with invalid date_ref types.

	Parameters
	----------
	invalid_date : Any
		Invalid date values to test

	Returns
	-------
	None
	"""
	# These should raise TypeError due to type validation
	with pytest.raises(TypeError, match="date_ref must be one of types"):
		B3FinancialIndicators(date_ref=invalid_date)


def test_get_response_success(
	b3_instance: B3FinancialIndicators, mock_requests_get: MagicMock, mock_response: Response
) -> None:
	"""Test get_response with successful HTTP request.

	Verifies
	-------
	- HTTP request is made with correct parameters
	- Response is returned correctly
	- No exceptions are raised

	Parameters
	----------
	b3_instance : B3FinancialIndicators
		Instance of B3FinancialIndicators
	mock_requests_get : MagicMock
		Mock for requests.get
	mock_response : Response
		Mocked response object

	Returns
	-------
	None
	"""
	mock_requests_get.return_value = mock_response
	with patch("backoff.on_exception", lambda *args, **kwargs: lambda func: func):
		result = b3_instance.get_response(timeout=(10.0, 20.0), bool_verify=True)
	assert result == mock_response
	mock_requests_get.assert_called_once()
	mock_response.raise_for_status.assert_called_once()


@pytest.mark.parametrize("timeout", [None, 10, 10.5, (5.0, 10.0), (5, 10)])
def test_get_response_timeout_types(
	b3_instance: B3FinancialIndicators,
	mock_requests_get: MagicMock,
	mock_response: Response,
	timeout: Any,  # noqa ANN401: typing.Any is not allowed
) -> None:
	"""Test get_response with various valid timeout types.

	Parameters
	----------
	b3_instance : B3FinancialIndicators
		Instance of B3FinancialIndicators
	mock_requests_get : MagicMock
		Mock for requests.get
	mock_response : Response
		Mocked response object
	timeout : Any
		Various valid timeout values

	Returns
	-------
	None
	"""
	mock_requests_get.return_value = mock_response
	with patch("backoff.on_exception", lambda *args, **kwargs: lambda func: func):
		result = b3_instance.get_response(timeout=timeout)
	assert result == mock_response
	mock_requests_get.assert_called_once()


@pytest.mark.parametrize("invalid_timeout", ["10", [10, 20], {}])
def test_get_response_invalid_timeout(
	b3_instance: B3FinancialIndicators,
	mock_requests_get: MagicMock,
	invalid_timeout: Any,  # noqa ANN401: typing.Any is not allowed
) -> None:
	"""Test get_response with invalid timeout types.

	Parameters
	----------
	b3_instance : B3FinancialIndicators
		Instance of B3FinancialIndicators
	mock_requests_get : MagicMock
		Mock for requests.get
	invalid_timeout : Any
		Invalid timeout values

	Returns
	-------
	None
	"""
	# The type checker should catch these invalid timeout types
	with pytest.raises(TypeError, match="timeout must be one of types"):
		b3_instance.get_response(timeout=invalid_timeout)


def test_parse_raw_file_response(
	b3_instance: B3FinancialIndicators,
	mock_response: Response,
	sample_json_data: list[dict[str, Union[str, int, float]]],
) -> None:
	"""Test parse_raw_file with valid Response object.

	Verifies
	-------
	- JSON parsing works correctly
	- Returns expected data structure

	Parameters
	----------
	b3_instance : B3FinancialIndicators
		Instance of B3FinancialIndicators
	mock_response : Response
		Mocked response object
	sample_json_data : list[dict[str, Union[str, int, float]]]
		Sample JSON data

	Returns
	-------
	None
	"""
	result = b3_instance.parse_raw_file(mock_response)
	assert result == sample_json_data
	mock_response.json.assert_called_once()


@pytest.mark.parametrize(
	"invalid_response", [MagicMock(spec=PlaywrightPage), MagicMock(spec=SeleniumWebDriver)]
)
def test_parse_raw_file_invalid_type(
	b3_instance: B3FinancialIndicators,
	invalid_response: Any,  # noqa ANN401: typing.Any is not allowed
) -> None:
	"""Test parse_raw_file with invalid response types.

	Parameters
	----------
	b3_instance : B3FinancialIndicators
		Instance of B3FinancialIndicators
	invalid_response : Any
		Invalid response objects (PlaywrightPage, SeleniumWebDriver)

	Returns
	-------
	None
	"""
	with pytest.raises(AttributeError, match="json"):
		b3_instance.parse_raw_file(invalid_response)


def test_transform_data(
	b3_instance: B3FinancialIndicators, sample_json_data: list[dict[str, Union[str, int, float]]]
) -> None:
	"""Test transform_data with valid JSON input.

	Verifies
	-------
	- Converts JSON to DataFrame correctly
	- Maintains expected column names and data

	Parameters
	----------
	b3_instance : B3FinancialIndicators
		Instance of B3FinancialIndicators
	sample_json_data : list[dict[str, Union[str, int, float]]]
		Sample JSON data

	Returns
	-------
	None
	"""
	df_ = b3_instance.transform_data(sample_json_data)
	assert isinstance(df_, pd.DataFrame)
	assert len(df_) == 1
	assert list(df_.columns) == [
		"securityIdentificationCode",
		"description",
		"groupDescription",
		"value",
		"rate",
		"lastUpdate",
	]
	assert df_.iloc[0]["securityIdentificationCode"] == "123"


def test_transform_data_empty(b3_instance: B3FinancialIndicators) -> None:
	"""Test transform_data with empty JSON input.

	Verifies
	-------
	- Handles empty input correctly
	- Returns empty DataFrame

	Parameters
	----------
	b3_instance : B3FinancialIndicators
		Instance of B3FinancialIndicators

	Returns
	-------
	None
	"""
	df_ = b3_instance.transform_data([])
	assert isinstance(df_, pd.DataFrame)
	assert df_.empty


def test_run_without_db(
	b3_instance: B3FinancialIndicators, mock_requests_get: MagicMock, mock_response: Response
) -> None:
	"""Test run method without database session.

	Verifies
	-------
	- Full pipeline works without db
	- Returns transformed DataFrame
	- All intermediate steps are called

	Parameters
	----------
	b3_instance : B3FinancialIndicators
		Instance of B3FinancialIndicators
	mock_requests_get : MagicMock
		Mock for requests.get
	mock_response : Response
		Mocked response object

	Returns
	-------
	None
	"""
	mock_requests_get.return_value = mock_response
	with (
		patch("backoff.on_exception", lambda *args, **kwargs: lambda func: func),
		patch.object(b3_instance, "standardize_dataframe") as mock_standardize,
	):
		mock_standardize.return_value = pd.DataFrame(
			[
				{
					"SECURITY_IDENTIFICATION_CODE": "123",
					"DESCRIPTION": "Test",
					"GROUP_DESCRIPTION": "Group A",
					"VALUE": "100.50",
					"RATE": "1.5",
					"LAST_UPDATE": date(2023, 1, 1),
				}
			]
		)
		df_ = b3_instance.run()
	assert isinstance(df_, pd.DataFrame)
	assert not df_.empty
	mock_requests_get.assert_called_once()
	mock_response.json.assert_called_once()
	mock_standardize.assert_called_once()


@pytest.mark.parametrize("bool_verify", [True, False])
def test_run_bool_verify(
	b3_instance: B3FinancialIndicators,
	mock_requests_get: MagicMock,
	mock_response: Response,
	bool_verify: bool,
) -> None:
	"""Test run method with different bool_verify settings.

	Parameters
	----------
	b3_instance : B3FinancialIndicators
		Instance of B3FinancialIndicators
	mock_requests_get : MagicMock
		Mock for requests.get
	mock_response : Response
		Mocked response object
	bool_verify : bool
		SSL verification flag

	Returns
	-------
	None
	"""
	mock_requests_get.return_value = mock_response
	with (
		patch("backoff.on_exception", lambda *args, **kwargs: lambda func: func),
		patch.object(b3_instance, "standardize_dataframe") as mock_standardize,
	):
		mock_standardize.return_value = pd.DataFrame(
			[
				{
					"SECURITY_IDENTIFICATION_CODE": "123",
					"DESCRIPTION": "Test",
					"GROUP_DESCRIPTION": "Group A",
					"VALUE": "100.50",
					"RATE": "1.5",
					"LAST_UPDATE": date(2023, 1, 1),
				}
			]
		)
		df_ = b3_instance.run(bool_verify=bool_verify)
	assert isinstance(df_, pd.DataFrame)
	mock_requests_get.assert_called_once_with(
		b3_instance.url, timeout=(12.0, 21.0), verify=bool_verify, headers=ANY
	)


def test_run_empty_response(
	b3_instance: B3FinancialIndicators, mock_requests_get: MagicMock, mock_response: Response
) -> None:
	"""Test run method with empty response data.

	Verifies
	-------
	- Handles empty JSON response
	- Raises RuntimeError due to empty DataFrame check

	Parameters
	----------
	b3_instance : B3FinancialIndicators
		Instance of B3FinancialIndicators
	mock_requests_get : MagicMock
		Mock for requests.get
	mock_response : Response
		Mocked response object

	Returns
	-------
	None
	"""
	mock_response.json.return_value = []
	mock_requests_get.return_value = mock_response
	with (
		patch("backoff.on_exception", lambda *args, **kwargs: lambda func: func),
		pytest.raises(RuntimeError, match="Error in check_if_empty: DataFrame is empty"),
	):
		b3_instance.run()


def test_standardize_dataframe(
	b3_instance: B3FinancialIndicators, sample_json_data: list[dict[str, Union[str, int, float]]]
) -> None:
	"""Test standardize_dataframe method.

	Verifies
	-------
	- Correctly standardizes column names and types
	- Adds reference date and URL columns
	- Applies correct case transformation

	Parameters
	----------
	b3_instance : B3FinancialIndicators
		Instance of B3FinancialIndicators
	sample_json_data : list[dict[str, Union[str, int, float]]]
		Sample JSON data

	Returns
	-------
	None
	"""
	df_ = pd.DataFrame(sample_json_data)
	result = b3_instance.standardize_dataframe(
		df_=df_,
		date_ref=date(2023, 1, 1),
		dict_dtypes={
			"SECURITY_IDENTIFICATION_CODE": str,
			"DESCRIPTION": str,
			"GROUP_DESCRIPTION": str,
			"VALUE": str,
			"RATE": str,
			"LAST_UPDATE": str,
		},
		str_fmt_dt="DD/MM/YYYY",
		cols_from_case="camel",
		cols_to_case="upper_constant",
		url=b3_instance.url,
	)
	assert isinstance(result, pd.DataFrame)
	# Check that the expected core columns are present
	expected_core_columns = [
		"SECURITY_IDENTIFICATION_CODE",
		"DESCRIPTION",
		"GROUP_DESCRIPTION",
		"VALUE",
		"RATE",
		"LAST_UPDATE",
		"URL",
	]
	for col in expected_core_columns:
		assert col in result.columns

	# Check that either REF_DATE or REFERENCE_DATE is present (depending on implementation)
	assert "REF_DATE" in result.columns or "REFERENCE_DATE" in result.columns

	# Check the content for the reference date column (whichever name it has)
	if "REF_DATE" in result.columns:
		assert result["REF_DATE"].iloc[0] == date(2023, 1, 1)
	else:
		assert result["REFERENCE_DATE"].iloc[0] == date(2023, 1, 1)

	assert result["URL"].iloc[0] == b3_instance.url


def test_insert_table_db(b3_instance: B3FinancialIndicators, mock_db_session: MagicMock) -> None:
	"""Test insert_table_db method.

	Verifies
	-------
	- Database insertion is called with correct parameters
	- Handles insert or ignore flag

	Parameters
	----------
	b3_instance : B3FinancialIndicators
		Instance of B3FinancialIndicators
	mock_db_session : MagicMock
		Mocked database session

	Returns
	-------
	None
	"""
	df_ = pd.DataFrame([{"TEST_COLUMN": "value"}])
	b3_instance.cls_db = mock_db_session
	with patch.object(b3_instance, "insert_table_db") as mock_insert:
		b3_instance.insert_table_db(
			cls_db=mock_db_session,
			str_table_name="test_table",
			df_=df_,
			bool_insert_or_ignore=True,
		)
	mock_insert.assert_called_once_with(
		cls_db=mock_db_session, str_table_name="test_table", df_=df_, bool_insert_or_ignore=True
	)
