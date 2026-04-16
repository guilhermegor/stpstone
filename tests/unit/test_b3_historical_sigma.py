"""Unit tests for B3HistoricalSigma class.

Tests the ingestion functionality with various scenarios including:
- Initialization with valid and invalid inputs
- HTTP response handling with mocked requests
- Data parsing and transformation
- Database operations
- Edge cases and error conditions
"""

from collections.abc import Callable
from datetime import date
import importlib
from typing import Any, Optional, Union
from unittest.mock import ANY, MagicMock

import pandas as pd
import pytest
from pytest_mock import MockerFixture
from requests import Response

from stpstone.ingestion.countries.br.exchange.b3_historical_sigma import B3HistoricalSigma
from stpstone.utils.calendars.calendar_br import DatesBRAnbima


# --------------------------
# Fixtures
# --------------------------
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
def mock_response() -> Response:
	"""Mock Response object with sample content.

	Returns
	-------
	Response
		Mocked Response object with JSON data
	"""
	response = MagicMock(spec=Response)
	response.json.return_value = {
		"results": [
			{
				"CODE": "TEST1",
				"TRADING_NAME": "Test Asset",
				"SERIE": "A",  # codespell:ignore
				"STANDARD_DEVIATION_1": 0.1,
				"ANNUALIZED_VOLATILITY_1": 1.5,
			}
		],
		"page": 1,
	}
	response.url = "https://example.com/test"
	response.status_code = 200
	response.raise_for_status = MagicMock()
	return response


@pytest.fixture
def sample_date() -> date:
	"""Provide a sample date for testing.

	Returns
	-------
	date
		A fixed date for consistent testing
	"""
	return date(2025, 9, 8)


@pytest.fixture
def b3_instance(sample_date: date) -> B3HistoricalSigma:
	"""Fixture providing B3HistoricalSigma instance.

	Parameters
	----------
	sample_date : date
		A fixed date for testing

	Returns
	-------
	B3HistoricalSigma
		Instance initialized with sample date
	"""
	return B3HistoricalSigma(date_ref=sample_date)


@pytest.fixture
def mock_db_session(mocker: MockerFixture) -> MagicMock:
	"""Mock database session.

	Parameters
	----------
	mocker : MockerFixture
		Pytest-mock fixture for creating mocks

	Returns
	-------
	MagicMock
		Mocked database session
	"""
	return mocker.patch("sqlalchemy.orm.Session")


# --------------------------
# Tests for Initialization
# --------------------------
def test_init_with_valid_date(sample_date: date) -> None:
	"""Test initialization with valid date input.

	Verifies
	--------
	- Instance is created correctly
	- date_ref is set as expected
	- Inherited classes are initialized

	Parameters
	----------
	sample_date : date
		A fixed date for testing

	Returns
	-------
	None
	"""
	instance = B3HistoricalSigma(date_ref=sample_date)
	assert instance.date_ref == sample_date
	assert isinstance(instance.cls_dates_br, DatesBRAnbima)
	assert instance.cls_db is None
	assert instance.logger is None


def test_init_without_date(mocker: MockerFixture) -> None:
	"""Test initialization without date input, using fallback.

	Verifies
	--------
	- date_ref is set to previous working day
	- Inherited classes are initialized

	Parameters
	----------
	mocker : MockerFixture
		Pytest-mock fixture for creating mocks

	Returns
	-------
	None
	"""
	mock_prev_day = mocker.patch(
		"stpstone.utils.calendars.calendar_br.DatesBRAnbima.add_working_days",
		return_value=date(2025, 9, 7),
	)
	instance = B3HistoricalSigma()
	assert instance.date_ref == date(2025, 9, 7)
	mock_prev_day.assert_called_once()


# --------------------------
# Tests for get_response
# --------------------------
def test_get_response_success(
	b3_instance: B3HistoricalSigma, mock_requests_get: MagicMock, mock_response: Response
) -> None:
	"""Test get_response with successful HTTP calls.

	Verifies
	--------
	- Returns list of Response objects
	- Calls requests.get with correct parameters
	- Handles multiple app codes

	Parameters
	----------
	b3_instance : B3HistoricalSigma
		Instance of B3HistoricalSigma
	mock_requests_get : MagicMock
		Mocked requests.get
	mock_response : Response
		Mocked Response object

	Returns
	-------
	None
	"""
	mock_requests_get.return_value = mock_response
	result = b3_instance.get_response(timeout=(12.0, 21.0), bool_verify=True)
	assert len(result) == 8  # 8 app codes
	assert all(isinstance(r, Response) for r in result)
	assert mock_requests_get.call_count == 8


def test_get_response_timeout_types(
	b3_instance: B3HistoricalSigma, mock_requests_get: MagicMock, mock_response: Response
) -> None:
	"""Test get_response with different timeout types.

	Verifies
	--------
	- Handles int, float, tuple timeouts
	- Calls requests.get with correct timeout

	Parameters
	----------
	b3_instance : B3HistoricalSigma
		Instance of B3HistoricalSigma
	mock_requests_get : MagicMock
		Mocked requests.get
	mock_response : Response
		Mocked Response object

	Returns
	-------
	None
	"""
	mock_requests_get.return_value = mock_response
	timeouts = [10, 10.5, (12.0, 21.0), (12, 21)]

	for timeout in timeouts:
		mock_requests_get.reset_mock()  # Reset call history
		result = b3_instance.get_response(timeout=timeout, bool_verify=True)
		assert len(result) == 8
		# Check that requests.get was called 8 times with correct timeout
		assert mock_requests_get.call_count == 8
		# Verify the timeout parameter in any of the calls
		calls = mock_requests_get.call_args_list
		assert all(call.kwargs["timeout"] == timeout for call in calls)


# --------------------------
# Tests for get_individual_response
# --------------------------
def test_get_individual_response_invalid_url_type(b3_instance: B3HistoricalSigma) -> None:
	"""Test get_individual_response with invalid URL type.

	Verifies
	--------
	- Raises TypeError for non-string URL
	- Error message matches expected pattern

	Parameters
	----------
	b3_instance : B3HistoricalSigma
		Instance of B3HistoricalSigma

	Returns
	-------
	None
	"""
	with pytest.raises(TypeError, match="fstr_url must be"):
		b3_instance.get_individual_response(fstr_url=123, list_apps=["E"])


def test_get_individual_response_empty_apps(b3_instance: B3HistoricalSigma) -> None:
	"""Test get_individual_response with empty app list.

	Verifies
	--------
	- Returns empty list when app list is empty
	- No HTTP requests are made

	Parameters
	----------
	b3_instance : B3HistoricalSigma
		Instance of B3HistoricalSigma

	Returns
	-------
	None
	"""
	result = b3_instance.get_individual_response(
		fstr_url=b3_instance.fstr_url_1, list_apps=[], timeout=(12.0, 21.0), bool_verify=True
	)
	assert result == []


# --------------------------
# Tests for parse_raw_file
# --------------------------
def test_parse_raw_file_valid_response(
	b3_instance: B3HistoricalSigma, mock_response: Response, mocker: MockerFixture
) -> None:
	"""Test parse_raw_file with valid response.

	Verifies
	--------
	- Correctly parses JSON data
	- Adds page and URL keys to dictionaries
	- Returns list of dictionaries

	Parameters
	----------
	b3_instance : B3HistoricalSigma
		Instance of B3HistoricalSigma
	mock_response : Response
		Mocked Response object
	mocker : MockerFixture
		Pytest-mock fixture for creating mocks

	Returns
	-------
	None
	"""
	# Mock the add_key_value_to_dicts method to return expected results
	_ = mocker.patch.object(
		b3_instance.cls_dicts_handler,
		"add_key_value_to_dicts",
		side_effect=[
			[
				{
					"CODE": "TEST1",
					"TRADING_NAME": "Test Asset",
					"SERIE": "A",  # codespell:ignore
					"STANDARD_DEVIATION_1": 0.1,
					"ANNUALIZED_VOLATILITY_1": 1.5,
					"page": 1,
				}
			],
			[
				{
					"CODE": "TEST1",
					"TRADING_NAME": "Test Asset",
					"SERIE": "A",  # codespell:ignore
					"STANDARD_DEVIATION_1": 0.1,
					"ANNUALIZED_VOLATILITY_1": 1.5,
					"page": 1,
					"URL": "https://example.com/test",
				}
			],  # Second call adds URL
		],
	)

	result = b3_instance.parse_raw_file([mock_response])
	assert len(result) == 1
	assert result[0]["CODE"] == "TEST1"
	assert result[0]["page"] == 1
	assert result[0]["URL"] == "https://example.com/test"


def test_parse_raw_file_empty_response(b3_instance: B3HistoricalSigma) -> None:
	"""Test parse_raw_file with empty response list.

	Verifies
	--------
	- Returns empty list for empty input
	- No errors raised

	Parameters
	----------
	b3_instance : B3HistoricalSigma
		Instance of B3HistoricalSigma

	Returns
	-------
	None
	"""
	result = b3_instance.parse_raw_file([])
	assert result == []


def test_parse_raw_file_invalid_response_type(b3_instance: B3HistoricalSigma) -> None:
	"""Test parse_raw_file with invalid response type.

	Verifies
	--------
	- Raises AttributeError for non-Response objects
	- Error message indicates JSON parsing failure

	Parameters
	----------
	b3_instance : B3HistoricalSigma
		Instance of B3HistoricalSigma

	Returns
	-------
	None
	"""
	with pytest.raises(AttributeError):
		# Create a proper mock object that doesn't have json method
		invalid_response = MagicMock()
		del invalid_response.json  # Remove json method to trigger AttributeError
		b3_instance.parse_raw_file([invalid_response])


# --------------------------
# Tests for transform_data
# --------------------------
def test_transform_data_valid_input(b3_instance: B3HistoricalSigma) -> None:
	"""Test transform_data with valid input.

	Verifies
	--------
	- Creates DataFrame from list of dictionaries
	- Removes duplicates
	- Returns correct DataFrame structure

	Parameters
	----------
	b3_instance : B3HistoricalSigma
		Instance of B3HistoricalSigma

	Returns
	-------
	None
	"""
	input_data = [
		{"CODE": "TEST1", "VALUE": 1.0},
		{"CODE": "TEST1", "VALUE": 1.0},
		{"CODE": "TEST2", "VALUE": 2.0},
	]
	result = b3_instance.transform_data(input_data)
	assert isinstance(result, pd.DataFrame)
	assert len(result) == 2
	assert result["CODE"].tolist() == ["TEST1", "TEST2"]


def test_transform_data_empty_input(b3_instance: B3HistoricalSigma) -> None:
	"""Test transform_data with empty input.

	Verifies
	--------
	- Returns empty DataFrame
	- No errors raised

	Parameters
	----------
	b3_instance : B3HistoricalSigma
		Instance of B3HistoricalSigma

	Returns
	-------
	None
	"""
	result = b3_instance.transform_data([])
	assert isinstance(result, pd.DataFrame)
	assert result.empty


# --------------------------
# Tests for run
# --------------------------
def test_run_without_db(
	b3_instance: B3HistoricalSigma,
	mock_requests_get: MagicMock,
	mock_response: Response,
	mocker: MockerFixture,
) -> None:
	"""Test run method without database session.

	Verifies
	--------
	- Returns DataFrame
	- Processes responses correctly
	- Applies standardization

	Parameters
	----------
	b3_instance : B3HistoricalSigma
		Instance of B3HistoricalSigma
	mock_requests_get : MagicMock
		Mocked requests.get
	mock_response : Response
		Mocked Response object
	mocker : MockerFixture
		Pytest-mock fixture for creating mocks

	Returns
	-------
	None
	"""
	mock_requests_get.return_value = mock_response
	mocker.patch("backoff.on_exception", lambda *args, **kwargs: lambda func: func)

	# Create realistic return values for each call in parse_raw_file
	_ = {
		"CODE": "TEST1",
		"TRADING_NAME": "Test Asset",
		"SERIE": "A",  # codespell:ignore
		"STANDARD_DEVIATION_1": 0.1,
		"ANNUALIZED_VOLATILITY_1": 1.5,
		"page": 1,
	}
	_ = {
		"CODE": "TEST1",
		"TRADING_NAME": "Test Asset",
		"SERIE": "A",  # codespell:ignore
		"STANDARD_DEVIATION_1": 0.1,
		"ANNUALIZED_VOLATILITY_1": 1.5,
		"page": 1,
		"URL": "https://example.com/test",
	}

	# Mock the add_key_value_to_dicts method with a callable that cycles through responses
	def mock_add_key_value_side_effect(
		list_ser: list[dict[str, Union[str, int, float]]],
		key: Union[str, list[dict[str, Union[str, int, float]]]],
		value: Optional[
			Union[Callable[..., Union[str, int, float]], Union[str, int, float]]
		] = None,
	) -> list[dict[str, Union[str, int, float]]]:
		"""Mock add_key_value_to_dicts method.

		Parameters
		----------
		list_ser : list[dict[str, Union[str, int, float]]]
			The list of dictionaries to add key-value pairs to.
		key : Union[str, list[dict[str, Union[str, int, float]]]]
			The key to add to the dictionaries.
		value : Optional[Union[Callable[..., Union[str, int, float]], Union[str, int, float]]]
			The value to add to the dictionaries, by default None

		Returns
		-------
		list[dict[str, Union[str, int, float]]]
			The modified list of dictionaries.
		"""
		if isinstance(key, list):  # Adding page number
			return [dict(item, page=key[0]) for item in list_ser]
		else:  # Adding URL
			return [dict(item, URL=value) for item in list_ser]

	mocker.patch.object(
		b3_instance.cls_dicts_handler,
		"add_key_value_to_dicts",
		side_effect=mock_add_key_value_side_effect,
	)

	mocker.patch.object(
		b3_instance, "standardize_dataframe", return_value=pd.DataFrame([{"CODE": "TEST1"}])
	)
	result = b3_instance.run()
	assert isinstance(result, pd.DataFrame)
	assert len(result) == 1
	assert result["CODE"].iloc[0] == "TEST1"


def test_run_with_db(
	b3_instance: B3HistoricalSigma,
	mock_requests_get: MagicMock,
	mock_response: Response,
	mock_db_session: MagicMock,
	mocker: MockerFixture,
) -> None:
	"""Test run method with database session.

	Verifies
	--------
	- Calls insert_table_db
	- Does not return DataFrame
	- Processes responses correctly

	Parameters
	----------
	b3_instance : B3HistoricalSigma
		Instance of B3HistoricalSigma
	mock_requests_get : MagicMock
		Mocked requests.get
	mock_response : Response
		Mocked Response object
	mock_db_session : MagicMock
		Mocked database session
	mocker : MockerFixture
		Pytest-mock fixture for creating mocks

	Returns
	-------
	None
	"""
	b3_instance.cls_db = mock_db_session
	mock_requests_get.return_value = mock_response
	mocker.patch("backoff.on_exception", lambda *args, **kwargs: lambda func: func)

	# Mock the add_key_value_to_dicts method with a callable
	def mock_add_key_value_side_effect(
		list_ser: list[dict[str, Union[str, int, float]]],
		key: Union[str, list[dict[str, Union[str, int, float]]]],
		value: Optional[
			Union[Callable[..., Union[str, int, float]], Union[str, int, float]]
		] = None,
	) -> list[dict[str, Union[str, int, float]]]:
		"""Mock add_key_value_to_dicts method.

		Parameters
		----------
		list_ser : list[dict[str, Union[str, int, float]]]
			The list of dictionaries to add key-value pairs to.
		key : Union[str, list[dict[str, Union[str, int, float]]]]
			The key to add to the dictionaries.
		value : Optional[Union[Callable[..., Union[str, int, float]], Union[str, int, float]]]
			The value to add to the dictionaries, by default None

		Returns
		-------
		list[dict[str, Union[str, int, float]]]
			The modified list of dictionaries.
		"""
		if isinstance(key, list):  # Adding page number
			return [dict(item, page=key[0]) for item in list_ser]
		else:  # Adding URL
			return [dict(item, URL=value) for item in list_ser]

	mocker.patch.object(
		b3_instance.cls_dicts_handler,
		"add_key_value_to_dicts",
		side_effect=mock_add_key_value_side_effect,
	)

	mocker.patch.object(b3_instance, "standardize_dataframe", return_value=pd.DataFrame())
	mocker.patch.object(b3_instance, "insert_table_db")
	result = b3_instance.run(bool_insert_or_ignore=True, str_table_name="TEST_TABLE")
	assert result is None
	b3_instance.insert_table_db.assert_called_once()


# --------------------------
# Edge Cases and Error Conditions
# --------------------------
@pytest.mark.parametrize("invalid_timeout", ["invalid", [12.0, 21.0], {12.0, 21.0}])
def test_get_response_invalid_timeout(
	b3_instance: B3HistoricalSigma,
	invalid_timeout: Any,  # noqa ANN401: typing.Any is not allowed
) -> None:
	"""Test get_response with invalid timeout types.

	Verifies
	--------
	- Raises TypeError for invalid timeout types
	- Error message matches expected pattern

	Parameters
	----------
	b3_instance : B3HistoricalSigma
		Instance of B3HistoricalSigma
	invalid_timeout : Any
		Invalid timeout value to test

	Returns
	-------
	None
	"""
	with pytest.raises(TypeError, match="timeout must be one of types"):
		b3_instance.get_response(timeout=invalid_timeout)


def test_run_reload_module(
	mocker: MockerFixture, mock_requests_get: MagicMock, mock_response: Response
) -> None:
	"""Test module reload behavior.

	Verifies
	--------
	- Module can be reloaded
	- Instance maintains functionality after reload
	- Mocks are applied correctly

	Parameters
	----------
	mocker : MockerFixture
		Pytest-mock fixture for creating mocks
	mock_requests_get : MagicMock
		Mocked requests.get
	mock_response : Response
		Mocked Response object

	Returns
	-------
	None
	"""
	mock_requests_get.return_value = mock_response
	mocker.patch("backoff.on_exception", lambda *args, **kwargs: lambda func: func)

	# Mock the calendar initialization to prevent API calls
	mock_calendar_init = mocker.patch(
		"stpstone.utils.calendars.calendar_br.DatesBRAnbima.__init__", return_value=None
	)
	_ = mocker.patch(
		"stpstone.utils.calendars.calendar_br.DatesBRAnbima.add_working_days",
		return_value=date(2025, 9, 7),
	)

	# Import the module first to ensure it exists
	from stpstone.ingestion.countries.br.exchange import b3_historical_sigma

	importlib.reload(b3_historical_sigma)

	# Mock the add_key_value_to_dicts method for the new instance
	instance = B3HistoricalSigma(date_ref=date(2025, 9, 8))  # avoid calendar calls

	def mock_add_key_value_side_effect(
		list_ser: list[dict[str, Union[str, int, float]]],
		key: Union[str, list[dict[str, Union[str, int, float]]]],
		value: Optional[
			Union[Callable[..., Union[str, int, float]], Union[str, int, float]]
		] = None,
	) -> list[dict[str, Union[str, int, float]]]:
		"""Mock add_key_value_to_dicts method.

		Parameters
		----------
		list_ser : list[dict[str, Union[str, int, float]]]
			The list of dictionaries to add key-value pairs to.
		key : Union[str, list[dict[str, Union[str, int, float]]]]
			The key to add to the dictionaries.
		value : Optional[Union[Callable[..., Union[str, int, float]], Union[str, int, float]]]
			The value to add to the dictionaries, by default None

		Returns
		-------
		list[dict[str, Union[str, int, float]]]
			The modified list of dictionaries.
		"""
		if isinstance(key, list):  # Adding page number
			return [dict(item, page=key[0]) for item in list_ser]
		else:  # Adding URL
			return [dict(item, URL=value) for item in list_ser]

	mocker.patch.object(
		instance.cls_dicts_handler,
		"add_key_value_to_dicts",
		side_effect=mock_add_key_value_side_effect,
	)
	mocker.patch.object(
		instance, "standardize_dataframe", return_value=pd.DataFrame([{"CODE": "TEST1"}])
	)

	result = instance.run()
	assert isinstance(result, pd.DataFrame)

	# Verify that mocks were called
	mock_calendar_init.assert_called()
	# Note: mock_add_working_days won't be called since we provide explicit date_ref


# --------------------------
# Type Validation
# --------------------------
def test_transform_data_invalid_input_type(b3_instance: B3HistoricalSigma) -> None:
	"""Test transform_data with invalid input type.

	Verifies
	--------
	- Raises TypeError for non-list input
	- Error message matches expected pattern

	Parameters
	----------
	b3_instance : B3HistoricalSigma
		Instance of B3HistoricalSigma

	Returns
	-------
	None
	"""
	with pytest.raises(TypeError, match="list_ser must be"):
		b3_instance.transform_data("not a list")


# --------------------------
# Fallback Logic
# --------------------------
def test_get_response_fallback_no_verify(
	b3_instance: B3HistoricalSigma, mock_requests_get: MagicMock, mock_response: Response
) -> None:
	"""Test get_response with SSL verification disabled.

	Verifies
	--------
	- Handles bool_verify=False correctly
	- Makes requests with verify=False
	- Returns valid responses

	Parameters
	----------
	b3_instance : B3HistoricalSigma
		Instance of B3HistoricalSigma
	mock_requests_get : MagicMock
		Mocked requests.get
	mock_response : Response
		Mocked Response object

	Returns
	-------
	None
	"""
	mock_requests_get.return_value = mock_response
	result = b3_instance.get_response(bool_verify=False)
	assert len(result) == 8
	mock_requests_get.assert_called_with(ANY, timeout=(12.0, 21.0), verify=False, headers=ANY)
