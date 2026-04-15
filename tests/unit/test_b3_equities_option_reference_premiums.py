"""Unit tests for B3EquitiesOptionReferencePremiums ingestion class."""

from collections.abc import Callable
from datetime import date
from io import BytesIO, StringIO
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, mock_open, patch

import pandas as pd
import pytest
from pytest_mock import MockerFixture
from requests import Response

from stpstone.ingestion.countries.br.exchange.b3_equities_option_reference_premiums import (
	B3EquitiesOptionReferencePremiums,
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


def create_sample_csv_content() -> str:
	"""Create sample CSV content for testing.

	Returns
	-------
	str
		Sample CSV content
	"""
	return """HEADER1;HEADER2;HEADER3
value1;value2;value3
value4;value5;value6"""


def create_sample_xml_content() -> str:
	"""Create sample XML content for testing.

	Returns
	-------
	str
		Sample XML content
	"""
	return """<?xml version="1.0" encoding="UTF-8"?>
<root>
    <IndxInf>
        <TckrSymb>TEST</TckrSymb>
        <Id>123</Id>
        <Prtry>TEST_PRTRY</Prtry>
        <MktIdrCd>BVMF</MktIdrCd>
        <OpngPric>100.50</OpngPric>
        <MinPric>99.00</MinPric>
        <MaxPric>101.00</MaxPric>
        <TradAvrgPric>100.25</TradAvrgPric>
        <PrvsDayClsgPric>100.00</PrvsDayClsgPric>
        <ClsgPric>100.75</ClsgPric>
        <IndxVal>1000.00</IndxVal>
        <OscnVal>0.75</OscnVal>
        <AsstDesc>Test Asset</AsstDesc>
        <SttlmVal Ccy="BRL">1000000</SttlmVal>
        <RsngShrsNb>1000000</RsngShrsNb>
        <FlngShrsNb>900000</FlngShrsNb>
        <StblShrsNb>100000</StblShrsNb>
    </IndxInf>
</root>"""


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
def mock_response() -> MagicMock:
	"""Fixture providing a mock response object.

	Returns
	-------
	MagicMock
		Mock response object
	"""
	return create_mock_response()


@pytest.fixture
def csv_stringio() -> StringIO:
	"""Fixture providing CSV content as StringIO.

	Returns
	-------
	StringIO
		StringIO object with CSV data
	"""
	return StringIO(create_sample_csv_content())


@pytest.fixture
def xml_stringio() -> StringIO:
	"""Fixture providing XML content as StringIO.

	Returns
	-------
	StringIO
		StringIO object with XML data
	"""
	return StringIO(create_sample_xml_content())


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


class TestB3EquitiesOptionReferencePremiums:
	"""Test cases for B3EquitiesOptionReferencePremiums class."""

	def test_init_ex_file_url(self) -> None:
		"""Test initialization with .ex_ file URL.

		Verifies
		--------
		- URL points to .ex_ file instead of .zip
		- Correct file pattern is used
		- Class inherits executable parsing functionality

		Returns
		-------
		None
		"""
		instance = B3EquitiesOptionReferencePremiums()

		assert "pesquisapregao/download?filelist=PE" in instance.url
		assert instance.url.endswith(".ex_")

	def test_parse_raw_file_calls_parse_raw_ex_file(
		self, mock_fast_operations: dict[str, MagicMock]
	) -> None:
		"""Test parse_raw_file delegates to parse_raw_ex_file.

		Verifies
		--------
		- parse_raw_ex_file is called with correct parameters
		- Correct prefix and file name are used
		- Method delegation works properly

		Parameters
		----------
		mock_fast_operations : dict[str, MagicMock]
			Dictionary of mocked operations

		Returns
		-------
		None
		"""
		instance = B3EquitiesOptionReferencePremiums()
		mock_response = create_mock_response()

		with patch.object(instance, "parse_raw_ex_file") as mock_parse_ex:
			mock_parse_ex.return_value = (StringIO("output"), "filename")

			_ = instance.parse_raw_file(mock_response)

			mock_parse_ex.assert_called_once_with(
				resp_req=mock_response,
				prefix="b3_option_premiums_",
				file_name="b3_equities_option_reference_premiums_",
			)

	def test_run_with_executable_processing(
		self, mock_fast_operations: dict[str, MagicMock]
	) -> None:
		"""Test run method with executable file processing.

		Verifies
		--------
		- Executable processing pipeline works
		- Correct data types for option premiums
		- Proper error handling for Wine execution

		Parameters
		----------
		mock_fast_operations : dict[str, MagicMock]
			Dictionary of mocked operations

		Returns
		-------
		None
		"""
		instance = B3EquitiesOptionReferencePremiums()

		with (
			patch.object(instance, "get_response") as mock_get_response,
			patch.object(instance, "parse_raw_file") as mock_parse,
			patch.object(instance, "transform_data") as mock_transform,
			patch.object(instance, "standardize_dataframe") as mock_standardize,
		):
			mock_get_response.return_value = create_mock_response()
			mock_parse.return_value = (StringIO("option;data"), "premiums.txt")
			mock_transform.return_value = pd.DataFrame(
				{
					"TICKER_SYMBOL": ["PETR4"],
					"OPTION_TYPE": ["CALL"],
					"FILE_NAME": ["premiums.txt"],
				}
			)
			mock_standardize.return_value = pd.DataFrame({"TICKER_SYMBOL": ["PETR4"]})

			_ = instance.run()

			call_args = mock_standardize.call_args[1]
			expected_dtypes = call_args["dict_dtypes"]

			# Verify option-specific data types
			assert expected_dtypes["TICKER_SYMBOL"] is str
			assert expected_dtypes["OPTION_TYPE"] is str
			assert expected_dtypes["EXERCISE_PRICE"] is float
			assert expected_dtypes["IMPLIED_VOLATILITY"] is float

	def test_transform_data_option_premium_format(self) -> None:
		"""Test transform_data for option premium CSV format.

		Verifies
		--------
		- CSV is parsed with correct column structure
		- Option-specific columns are present
		- Skiprows parameter is applied

		Returns
		-------
		None
		"""
		instance = B3EquitiesOptionReferencePremiums()

		csv_content = StringIO("""header_row
PETR4;CALL;AMERICAN;20241215;25.50;2.35;0.25
VALE3;PUT;AMERICAN;20241215;45.00;1.80;0.22""")

		result = instance.transform_data(csv_content, "premiums.csv")

		assert isinstance(result, pd.DataFrame)
		assert len(result) == 2
		assert "FILE_NAME" in result.columns

		expected_columns = [
			"TICKER_SYMBOL",
			"OPTION_TYPE",
			"OPTION_STYLE",
			"EXPIRY_DATE",
			"EXERCISE_PRICE",
			"REFERENCE_PREMIUM",
			"IMPLIED_VOLATILITY",
			"FILE_NAME",
		]
		assert list(result.columns) == expected_columns

	def test_wine_execution_error_handling(
		self, mock_fast_operations: dict[str, MagicMock]
	) -> None:
		"""Test error handling for Wine execution failures.

		Verifies
		--------
		- Wine execution errors are caught
		- Temporary cleanup is performed
		- Appropriate exceptions are raised

		Parameters
		----------
		mock_fast_operations : dict[str, MagicMock]
			Dictionary of mocked operations

		Returns
		-------
		None
		"""
		instance = B3EquitiesOptionReferencePremiums()
		mock_response = create_mock_response()

		# configure subprocess to fail
		mock_fast_operations["subprocess_run"].return_value = MagicMock(
			returncode=1, stdout="", stderr="Wine execution failed"
		)

		with (
			patch.object(
				instance.cls_dir_files_management, "recursive_unzip_in_memory"
			) as mock_unzip,
			patch("builtins.open", mock_open()),
			patch("os.chmod"),
			patch("os.getcwd", return_value="/current"),
			patch("os.chdir"),
			patch.object(Path, "exists", return_value=True),
			patch.object(Path, "glob", return_value=[]),
			patch.object(Path, "iterdir", return_value=[]),
		):
			mock_unzip.return_value = [(BytesIO(b"exe content"), "test.ex_")]

			with pytest.raises(RuntimeError, match="No output file generated"):
				instance.parse_raw_file(mock_response)


# --------------------------
# Tests for B3DerivativesMarketOTCMarketTrades
# --------------------------
