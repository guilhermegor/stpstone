"""Unit tests for B3FixedIncome ingestion class."""

from collections.abc import Callable
from datetime import date
from io import StringIO
from typing import Any
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from pytest_mock import MockerFixture
from requests import Response

from stpstone.ingestion.countries.br.exchange.b3_fixed_income import (
	B3FixedIncome,
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
	content : bytes, optional
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


class TestB3FixedIncome:
	"""Test cases for B3FixedIncome class."""

	def test_init_fixed_income_url(self) -> None:
		"""Test initialization with fixed income URL pattern.

		Verifies
		--------
		- Correct URL pattern for fixed income
		- RF prefix is used in URL
		- .ex_ file extension

		Returns
		-------
		None
		"""
		instance = B3FixedIncome()

		assert "pesquisapregao/download?filelist=RF" in instance.url
		assert instance.url.endswith(".ex_")

	def test_run_with_fixed_income_dtypes(
		self, mock_fast_operations: dict[str, MagicMock]
	) -> None:
		"""Test run method with fixed income data types.

		Verifies
		--------
		- YYYYMMDD date format
		- Rate and price precision
		- Ticker symbol handling

		Parameters
		----------
		mock_fast_operations : dict[str, MagicMock]
			Dictionary of mocked operations

		Returns
		-------
		None
		"""
		instance = B3FixedIncome()

		with (
			patch.object(instance, "get_response") as mock_get_response,
			patch.object(instance, "parse_raw_file") as mock_parse,
			patch.object(instance, "transform_data") as mock_transform,
			patch.object(instance, "standardize_dataframe") as mock_standardize,
		):
			mock_get_response.return_value = create_mock_response()
			mock_parse.return_value = (StringIO("fixed income data"), "fixed_income.txt")
			mock_transform.return_value = pd.DataFrame(
				{
					"TICKER": ["LTN"],
					"PRECO_UNITARIO": ["985.50"],
					"FILE_NAME": ["fixed_income.txt"],
				}
			)
			mock_standardize.return_value = pd.DataFrame({"TICKER": ["LTN"]})

			_ = instance.run()

			call_args = mock_standardize.call_args[1]
			assert call_args["str_fmt_dt"] == "YYYYMMDD"

			expected_dtypes = call_args["dict_dtypes"]
			assert expected_dtypes["TICKER"] is str
			assert expected_dtypes["PRECO_UNITARIO"] is str
			assert expected_dtypes["TAXA_COMPRA"] is str

	def test_transform_data_csv_format(self) -> None:
		"""Test transform_data for CSV format.

		Verifies
		--------
		- CSV parsing with semicolon separator
		- Rate columns are handled
		- Price precision is maintained

		Returns
		-------
		None
		"""
		instance = B3FixedIncome()

		csv_content = StringIO("""header
LTN;20240115;252;252;985.50;10.25;10.35;10.30
NTN-B;20240115;1825;1825;3250.75;6.15;6.25;6.20""")

		result = instance.transform_data(csv_content, "fixed_income.csv")

		assert isinstance(result, pd.DataFrame)
		assert len(result) == 2
		assert "FILE_NAME" in result.columns

		expected_columns = [
			"TICKER",
			"DATE",
			"PRAZO_DIAS_UTEIS",
			"PRAZO_DIAS_CORRIDOS",
			"PRECO_UNITARIO",
			"TAXA_COMPRA",
			"TAXA_VENDA",
			"TAXA_INDICATIVA",
			"FILE_NAME",
		]
		assert list(result.columns) == expected_columns


# --------------------------
# Tests for B3InstrumentGroupParameters
# --------------------------
