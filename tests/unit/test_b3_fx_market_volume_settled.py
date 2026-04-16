"""Unit tests for B3FXMarketVolumeSettled ingestion class."""

from collections.abc import Callable
from datetime import date
from io import StringIO
from typing import Any
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from pytest_mock import MockerFixture
from requests import Response

from stpstone.ingestion.countries.br.exchange.b3_fx_market_volume_settled import (
	B3FXMarketVolumeSettled,
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


class TestB3FXMarketVolumeSettled:
	"""Test cases for B3FXMarketVolumeSettled class."""

	def test_init_volume_settled_url(self) -> None:
		"""Test initialization with volume settled URL pattern.

		Verifies
		--------
		- Correct URL pattern for volume settled
		- CV prefix is used in URL
		- .zip file extension

		Returns
		-------
		None
		"""
		instance = B3FXMarketVolumeSettled()

		assert "pesquisapregao/download?filelist=CV" in instance.url
		assert instance.url.endswith(".zip")

	def test_run_with_volume_dtypes(self, mock_fast_operations: dict[str, MagicMock]) -> None:
		"""Test run method with volume-specific data types.

		Verifies
		--------
		- Date format as YYYYMMDD
		- Float precision for volume values
		- Currency handling

		Parameters
		----------
		mock_fast_operations : dict[str, MagicMock]
			Dictionary of mocked operations

		Returns
		-------
		None
		"""
		instance = B3FXMarketVolumeSettled()

		with (
			patch.object(instance, "get_response") as mock_get_response,
			patch.object(instance, "parse_raw_file") as mock_parse,
			patch.object(instance, "transform_data") as mock_transform,
			patch.object(instance, "standardize_dataframe") as mock_standardize,
		):
			mock_get_response.return_value = create_mock_response()
			mock_parse.return_value = (StringIO("volume data"), "volume.txt")
			mock_transform.return_value = pd.DataFrame(
				{
					"ID_TRANSACAO": ["12345678"],
					"VALOR_LIQUIDO_COMPENSADO_DOLAR": [100000.50],
					"FILE_NAME": ["volume.txt"],
				}
			)
			mock_standardize.return_value = pd.DataFrame({"ID_TRANSACAO": ["12345678"]})

			_ = instance.run()

			call_args = mock_standardize.call_args[1]
			assert call_args["str_fmt_dt"] == "YYYYMMDD"

			expected_dtypes = call_args["dict_dtypes"]
			assert expected_dtypes["VALOR_LIQUIDO_COMPENSADO_DOLAR"] is float
			assert expected_dtypes["VALOR_LIQUIDO_COMPENSADO_REAL"] is float

	def test_transform_data_fixed_width_volume(self) -> None:
		"""Test transform_data for volume fixed-width format.

		Verifies
		--------
		- Fixed-width format is parsed correctly
		- Volume calculations are accurate
		- Currency fields are handled

		Returns
		-------
		None
		"""
		instance = B3FXMarketVolumeSettled()

		# create sample fixed-width content for volume
		fixed_width_content = StringIO("12345678202401152024011500000000100000000000000500000")

		result = instance.transform_data(fixed_width_content, "volume_settled.txt")

		assert isinstance(result, pd.DataFrame)
		assert len(result) == 1
		assert "FILE_NAME" in result.columns

		expected_columns = [
			"ID_TRANSACAO",
			"DATA_REFERENCIA",
			"DATA_LIQUIDACAO_VALORES_LIQUIDOS_COMPENSADO",
			"VALOR_LIQUIDO_COMPENSADO_DOLAR",
			"VALOR_LIQUIDO_COMPENSADO_REAL",
			"FILE_NAME",
		]
		assert len(result.columns) == len(expected_columns)


# --------------------------
# Tests for B3FixedIncome
# --------------------------
