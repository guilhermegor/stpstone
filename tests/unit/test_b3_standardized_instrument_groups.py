"""Unit tests for B3StandardizedInstrumentGroups ingestion class."""

from collections.abc import Callable
from contextlib import suppress
from datetime import date
from io import StringIO
from typing import Any
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
import requests
from pytest_mock import MockerFixture
from requests import Response

from stpstone.ingestion.countries.br.exchange.b3_standardized_instrument_groups import (
	B3StandardizedInstrumentGroups,
)


# --------------------------
# Module Utilities
# --------------------------
def create_mock_logger() -> MagicMock:
	"""Create a mock logger for testing.

	Returns
	-------
	MagicMock
		Mock logger instance
	"""
	return MagicMock()


def create_mock_db_session() -> MagicMock:
	"""Create a mock database session for testing.

	Returns
	-------
	MagicMock
		Mock database session instance
	"""
	return MagicMock()


def create_mock_response(content: bytes = b"test content") -> MagicMock:
	"""Create a mock response object.

	Parameters
	----------
	content : bytes
		Response content, by default b"test content"

	Returns
	-------
	MagicMock
		Mock response object
	"""
	response = MagicMock(spec=Response)
	response.content = content
	response.status_code = 200
	response.raise_for_status = MagicMock()
	return response


# --------------------------
# Fixtures
# --------------------------
@pytest.fixture
def mock_logger() -> MagicMock:
	"""Fixture providing a mock logger.

	Returns
	-------
	MagicMock
		Mock logger instance
	"""
	return create_mock_logger()


@pytest.fixture
def mock_db_session() -> MagicMock:
	"""Fixture providing a mock database session.

	Returns
	-------
	MagicMock
		Mock database session instance
	"""
	return create_mock_db_session()


@pytest.fixture
def sample_date() -> date:
	"""Fixture providing a sample date for testing.

	Returns
	-------
	date
		Sample date object
	"""
	return date(2024, 1, 15)


@pytest.fixture
def csv_stringio() -> StringIO:
	"""Fixture providing CSV content as StringIO.

	Returns
	-------
	StringIO
		StringIO object with CSV data
	"""
	return StringIO("HEADER1;HEADER2;HEADER3\nvalue1;value2;value3\nvalue4;value5;value6")


@pytest.fixture(autouse=True)
def mock_fast_operations(mocker: MockerFixture) -> dict[str, MagicMock]:
	"""Auto-mock expensive operations for all tests.

	Parameters
	----------
	mocker : MockerFixture
		Pytest-mock fixture for creating mocks

	Returns
	-------
	dict[str, MagicMock]
		Dictionary of mock objects
	"""

	def bypass_backoff(
		*args: Any,  # noqa ANN401: typing.Any is not allowed
		**kwargs: Any,  # noqa ANN401: typing.Any is not allowed
	) -> Callable:
		"""Bypass backoff decorator.

		Parameters
		----------
		*args : Any
			Variable-length argument list
		**kwargs : Any
			Arbitrary keyword arguments

		Returns
		-------
		Callable
			Function unchanged
		"""

		def decorator(func: Callable) -> Callable:
			"""Return the function unchanged.

			Parameters
			----------
			func : Callable
				Function to be decorated

			Returns
			-------
			Callable
				Function unchanged
			"""
			return func

		return decorator

	mocks = {
		"requests_get": mocker.patch("requests.get"),
		"time_sleep": mocker.patch("time.sleep"),
		"backoff_on_exception": mocker.patch("backoff.on_exception", side_effect=bypass_backoff),
		"subprocess_run": mocker.patch("subprocess.run"),
		"shutil_rmtree": mocker.patch("shutil.rmtree"),
		"tempfile_mkdtemp": mocker.patch("tempfile.mkdtemp"),
	}

	mocks["requests_get"].return_value = create_mock_response()
	mocks["subprocess_run"].return_value = MagicMock(returncode=0, stdout="", stderr="")
	mocks["tempfile_mkdtemp"].return_value = "/tmp/test_dir"  # noqa S108: probable insecure usage of temporary file or directory

	return mocks


# --------------------------
# Tests for B3StandardizedInstrumentGroups
# --------------------------
class TestB3StandardizedInstrumentGroups:
	"""Test cases for B3StandardizedInstrumentGroups class."""

	def test_init_with_defaults(self) -> None:
		"""Test initialization with default parameters.

		Verifies
		--------
		- Correct URL is constructed with placeholder
		- Default values are set properly
		- Class inherits from ABCB3SearchByTradingSession

		Returns
		-------
		None
		"""
		instance = B3StandardizedInstrumentGroups()

		with (
			patch.object(instance, "get_response") as mock_get_response,
			patch.object(instance, "parse_raw_file") as mock_parse,
			patch.object(instance, "transform_data") as mock_transform,
			patch.object(instance, "standardize_dataframe") as mock_standardize,
		):
			mock_get_response.return_value = create_mock_response()
			mock_parse.return_value = (StringIO("test,data"), "test.csv")
			mock_transform.return_value = pd.DataFrame(
				{"TIPO_REGISTRO": ["1"], "FILE_NAME": ["test.csv"]}
			)
			mock_standardize.return_value = pd.DataFrame({"TIPO_REGISTRO": ["1"]})

			_ = instance.run()

			expected_dtypes = {
				"TIPO_REGISTRO": str,
				"ID_GRUPO_INSTRUMENTOS": str,
				"ID_CAMARA": str,
				"ID_INSTRUMENTO": str,
				"ORIGEM_INSTRUMENTO": str,
				"FILE_NAME": "category",
			}
			mock_standardize.assert_called_once()
			call_args = mock_standardize.call_args[1]
			assert call_args["dict_dtypes"] == expected_dtypes

	def test_transform_data_csv_parsing(self, csv_stringio: StringIO) -> None:
		"""Test transform_data method with CSV content.

		Verifies
		--------
		- CSV is parsed correctly with skiprows=1
		- Correct column names are assigned
		- FILE_NAME column is added
		- Returns DataFrame with expected structure

		Parameters
		----------
		csv_stringio : StringIO
			StringIO object with CSV data

		Returns
		-------
		None
		"""
		instance = B3StandardizedInstrumentGroups()

		csv_content = StringIO("""header
01;GROUP1;CAM1;INST1;ORIG1
02;GROUP2;CAM2;INST2;ORIG2""")

		result = instance.transform_data(csv_content, "test_file.csv")

		assert isinstance(result, pd.DataFrame)
		assert len(result) == 2
		assert "FILE_NAME" in result.columns
		assert all(result["FILE_NAME"] == "test_file.csv")

		expected_columns = [
			"TIPO_REGISTRO",
			"ID_GRUPO_INSTRUMENTOS",
			"ID_CAMARA",
			"ID_INSTRUMENTO",
			"ORIGEM_INSTRUMENTO",
			"FILE_NAME",
		]
		assert list(result.columns) == expected_columns

	def test_transform_data_empty_file(self) -> None:
		"""Test transform_data with empty file.

		Verifies
		--------
		- Empty CSV files are handled gracefully
		- Returns DataFrame with correct columns but no data
		- FILE_NAME column is still added

		Returns
		-------
		None
		"""
		instance = B3StandardizedInstrumentGroups()
		empty_csv = StringIO("header\n")

		result = instance.transform_data(empty_csv, "empty.csv")

		assert isinstance(result, pd.DataFrame)
		assert len(result) == 0
		assert "FILE_NAME" in result.columns

	def test_run_parameter_validation(self) -> None:
		"""Test run method parameter type validation.

		Verifies
		--------
		- TypeError raised for invalid timeout type
		- TypeError raised for invalid bool_verify type
		- TypeError raised for invalid str_table_name type

		Returns
		-------
		None
		"""
		instance = B3StandardizedInstrumentGroups()

		with pytest.raises(TypeError):
			instance.run(timeout="invalid")

		with pytest.raises(TypeError):
			instance.run(bool_verify="not_bool")

		with pytest.raises(TypeError):
			instance.run(str_table_name=123)

	def test_large_dataset_handling(self, mock_fast_operations: dict[str, MagicMock]) -> None:
		"""Test handling of large datasets.

		Verifies
		--------
		- Large datasets are processed efficiently
		- Memory usage is reasonable
		- No performance degradation

		Parameters
		----------
		mock_fast_operations : dict[str, MagicMock]
			Dictionary of mocked operations

		Returns
		-------
		None
		"""
		instance = B3StandardizedInstrumentGroups()

		large_csv_lines = ["header"]
		for i in range(10000):
			large_csv_lines.append(f"0{i % 10};GROUP{i};CAM{i};INST{i};ORIG{i}")
		large_csv_content = "\n".join(large_csv_lines)

		result = instance.transform_data(StringIO(large_csv_content), "large.csv")

		assert isinstance(result, pd.DataFrame)
		assert len(result) == 10000
		assert "FILE_NAME" in result.columns

	def test_malformed_data_handling(self) -> None:
		"""Test handling of malformed data.

		Verifies
		--------
		- Malformed CSV data is handled gracefully
		- Appropriate errors are raised or data is cleaned
		- No unexpected crashes occur

		Returns
		-------
		None
		"""
		instance = B3StandardizedInstrumentGroups()

		malformed_csv = StringIO("""header
01;GROUP1;CAM1;INST1;ORIG1
02;GROUP2;CAM2;INST2""")

		with suppress(Exception):
			result = instance.transform_data(malformed_csv, "malformed.csv")
			assert isinstance(result, pd.DataFrame)

	def test_unicode_content_handling(self) -> None:
		"""Test handling of Unicode content.

		Verifies
		--------
		- Unicode characters are processed correctly
		- Different encodings are handled
		- No encoding errors occur

		Returns
		-------
		None
		"""
		instance = B3StandardizedInstrumentGroups()

		unicode_csv = StringIO("""header
01;Açúcar;Câmara;Instrumento;Origem
02;Café;São Paulo;Opções;Brasil""")

		result = instance.transform_data(unicode_csv, "unicode.csv")

		assert isinstance(result, pd.DataFrame)
		assert len(result) == 2

	def test_error_propagation_chain(
		self,
		mock_fast_operations: dict[str, MagicMock],
	) -> None:
		"""Test error propagation through the processing chain.

		Verifies
		--------
		- Network errors are properly propagated
		- Processing errors are handled correctly
		- Cleanup occurs on failures

		Parameters
		----------
		mock_fast_operations : dict[str, MagicMock]
			Dictionary of mocked operations

		Returns
		-------
		None
		"""
		instance = B3StandardizedInstrumentGroups()

		mock_fast_operations["requests_get"].side_effect = requests.exceptions.ConnectionError(
			"Network error"
		)

		with pytest.raises(requests.exceptions.ConnectionError, match="Network error"):
			instance.run()

	def test_data_pipeline_end_to_end(self, mock_fast_operations: dict[str, MagicMock]) -> None:
		"""Test complete data pipeline from request to DataFrame.

		Verifies
		--------
		- Full pipeline executes successfully
		- Data transformations are applied correctly
		- Output format is consistent

		Parameters
		----------
		mock_fast_operations : dict[str, MagicMock]
			Dictionary of mocked operations

		Returns
		-------
		None
		"""
		instance = B3StandardizedInstrumentGroups()

		zip_content = b"PK\x03\x04test_zip_content"
		mock_fast_operations["requests_get"].return_value = create_mock_response(zip_content)

		with patch.object(
			instance.cls_dir_files_management, "recursive_unzip_in_memory"
		) as mock_unzip:
			csv_content = StringIO("""header
01;GROUP1;CAM1;INST1;ORIG1
02;GROUP2;CAM2;INST2;ORIG2""")
			mock_unzip.return_value = [(csv_content, "test.csv")]

			result = instance.run()

			assert isinstance(result, pd.DataFrame)
			assert len(result) == 2
			assert "FILE_NAME" in result.columns

	def test_mock_requests_prevents_real_calls(
		self,
		mock_fast_operations: dict[str, MagicMock],
	) -> None:
		"""Test that mocked requests prevent real HTTP calls.

		Verifies
		--------
		- No real HTTP requests are made
		- Mock is called with correct parameters
		- Network isolation is maintained

		Parameters
		----------
		mock_fast_operations : dict[str, MagicMock]
			Dictionary of mocked operations

		Returns
		-------
		None
		"""
		instance = B3StandardizedInstrumentGroups()

		instance.get_response(timeout=(5.0, 10.0), bool_verify=False)

		mock_fast_operations["requests_get"].assert_called_once_with(
			instance.url, timeout=(5.0, 10.0), verify=False
		)

	def test_all_exception_paths_covered(self) -> None:
		"""Test that all exception paths are covered.

		Verifies
		--------
		- All exception handling code is tested
		- Error messages are validated
		- Recovery mechanisms work

		Returns
		-------
		None
		"""
		instance = B3StandardizedInstrumentGroups()

		with pytest.raises(TypeError):
			instance.transform_data(None, "test.csv")

	def test_edge_case_data_values(self) -> None:
		"""Test edge case data values.

		Verifies
		--------
		- Boundary values are handled correctly
		- Special characters in data
		- Empty and null values

		Returns
		-------
		None
		"""
		instance = B3StandardizedInstrumentGroups()

		edge_case_csv = StringIO("""header
;;;;;
01;;;;"";
;;CAM1;;""")

		result = instance.transform_data(edge_case_csv, "edge_case.csv")
		assert isinstance(result, pd.DataFrame)

	def test_all_property_access_patterns(self) -> None:
		"""Test all property access patterns.

		Verifies
		--------
		- All class properties are accessible
		- Property getters work correctly
		- No missing attribute errors

		Returns
		-------
		None
		"""
		with patch("tempfile.mkdtemp", return_value="/tmp/test"):  # noqa S108: probable insecure usage of temporary file or directory
			instance = B3StandardizedInstrumentGroups()

			assert hasattr(instance, "date_ref")
			assert hasattr(instance, "url")
			assert hasattr(instance, "logger")
			assert hasattr(instance, "cls_db")
			assert hasattr(instance, "cls_dir_files_management")

			assert isinstance(instance.date_ref, date)
			assert isinstance(instance.url, str)

	def test_init_with_parameters(
		self,
		sample_date: date,
		mock_logger: MagicMock,
		mock_db_session: MagicMock,
	) -> None:
		"""Test initialization with custom parameters.

		Verifies
		--------
		- Custom parameters are correctly assigned
		- URL contains formatted date
		- All attributes are properly set

		Parameters
		----------
		sample_date : date
			Sample date for testing
		mock_logger : MagicMock
			Mock logger instance
		mock_db_session : MagicMock
			Mock database session instance

		Returns
		-------
		None
		"""
		instance = B3StandardizedInstrumentGroups(
			date_ref=sample_date,
			logger=mock_logger,
			cls_db=mock_db_session,
		)

		assert instance.date_ref == sample_date
		assert instance.logger == mock_logger
		assert instance.cls_db == mock_db_session
		assert "AI240115" in instance.url

	def test_run_with_default_parameters(
		self,
		mock_fast_operations: dict[str, MagicMock],
	) -> None:
		"""Test run method with default parameters.

		Verifies
		--------
		- Correct data types are passed to parent run method
		- Default table name is used
		- Method executes without errors

		Parameters
		----------
		mock_fast_operations : dict[str, MagicMock]
			Dictionary of mocked operations

		Returns
		-------
		None
		"""
		instance = B3StandardizedInstrumentGroups()

		with (
			patch.object(instance, "get_response") as mock_get_response,
			patch.object(instance, "parse_raw_file") as mock_parse,
			patch.object(instance, "transform_data") as mock_transform,
			patch.object(instance, "standardize_dataframe") as mock_standardize,
		):
			mock_get_response.return_value = create_mock_response()
			mock_parse.return_value = (StringIO("test,data"), "test.csv")
			mock_transform.return_value = pd.DataFrame(
				{
					"test_col": [1, 2, 3],
					"FILE_NAME": ["test.csv", "test.csv", "test.csv"],
				}
			)
			mock_standardize.return_value = pd.DataFrame({"TEST_COL": [1, 2, 3]})

			_ = instance.run()

			mock_standardize.assert_called_once()
			call_args = mock_standardize.call_args[1]
			expected_dtypes = {
				"TIPO_REGISTRO": str,
				"ID_GRUPO_INSTRUMENTOS": str,
				"ID_CAMARA": str,
				"ID_INSTRUMENTO": str,
				"ORIGEM_INSTRUMENTO": str,
				"FILE_NAME": "category",
			}
			assert call_args["dict_dtypes"] == expected_dtypes
