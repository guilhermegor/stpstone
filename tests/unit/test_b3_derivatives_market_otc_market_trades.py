"""Unit tests for B3DerivativesMarketOTCMarketTrades ingestion class."""

from collections.abc import Callable
from datetime import date
from io import StringIO
from typing import Any
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from pytest_mock import MockerFixture
from requests import Response

from stpstone.ingestion.countries.br.exchange.b3_derivatives_market_otc_market_trades import (
	B3DerivativesMarketOTCMarketTrades,
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


class TestB3DerivativesMarketOTCMarketTrades:
	"""Test cases for B3DerivativesMarketOTCMarketTrades class."""

	def test_init_derivatives_url_pattern(self) -> None:
		"""Test initialization with derivatives OTC URL pattern.

		Verifies
		--------
		- Correct URL pattern for OTC trades
		- .ex_ file extension is used
		- BE prefix is in URL

		Returns
		-------
		None
		"""
		instance = B3DerivativesMarketOTCMarketTrades()

		assert "pesquisapregao/download?filelist=BE" in instance.url
		assert instance.url.endswith(".ex_")

	def test_run_with_fixed_width_format_dtypes(
		self, mock_fast_operations: dict[str, MagicMock]
	) -> None:
		"""Test run method with fixed-width format data types.

		Verifies
		--------
		- Complex data types for OTC trades
		- Date and numeric columns are properly typed
		- Float precision is maintained

		Parameters
		----------
		mock_fast_operations : dict[str, MagicMock]
			Dictionary of mocked operations

		Returns
		-------
		None
		"""
		instance = B3DerivativesMarketOTCMarketTrades()

		with (
			patch.object(instance, "get_response") as mock_get_response,
			patch.object(instance, "parse_raw_file") as mock_parse,
			patch.object(instance, "transform_data") as mock_transform,
			patch.object(instance, "standardize_dataframe") as mock_standardize,
		):
			mock_get_response.return_value = create_mock_response()
			mock_parse.return_value = (StringIO("fixed width data"), "otc.txt")
			mock_transform.return_value = pd.DataFrame(
				{
					"ID_TRANSACAO": ["123456"],
					"VOLUME_DIA_BRL": [100000.50],
					"FILE_NAME": ["otc.txt"],
				}
			)
			mock_standardize.return_value = pd.DataFrame({"ID_TRANSACAO": ["123456"]})

			_ = instance.run()

			call_args = mock_standardize.call_args[1]
			expected_dtypes = call_args["dict_dtypes"]

			# verify OTC-specific data types
			assert expected_dtypes["ID_TRANSACAO"] is str
			assert expected_dtypes["VOLUME_DIA_BRL"] is float
			assert expected_dtypes["QTD_CONTRATOS_ABERTO_APOS_LIQUIDACAO"] is float
			assert expected_dtypes["SINAL_TAXA_MEDIA_PREMIO_MEDIO"] is str

	def test_transform_data_fixed_width_parsing(self) -> None:
		"""Test transform_data for fixed-width file format.

		Verifies
		--------
		- Fixed-width format is parsed correctly
		- Column specifications are accurate
		- All expected columns are present

		Returns
		-------
		None
		"""
		instance = B3DerivativesMarketOTCMarketTrades()

		fixed_width_content = StringIO(
			"12345601202401151001DOL 20240115000001000000000010000000100000001000000010000000100000001000000010000000100000001+00000000000000000010000000000000100000000000010000000"  # noqa E501: line too long
		)

		result = instance.transform_data(fixed_width_content, "otc_trades.txt")

		assert isinstance(result, pd.DataFrame)
		assert len(result) == 1
		assert "FILE_NAME" in result.columns

		expected_columns = [
			"ID_TRANSACAO",
			"COMPLEMENTO_TRANSACAO",
			"TIPO_REGISTRO",
			"DATA_GERACAO_ARQUIVO",
			"TIPO_NEGOCIACAO",
			"CODIGO_MERCADORIA",
			"CODIGO_MERCADO",
			"DATA_BASE",
			"DATA_VENCIMENTO",
			"VOLUME_DIA_BRL",
			"VOLUME_DIA_USD",
			"QTD_CONTRATOS_ABERTO_APOS_LIQUIDACAO",
			"QTD_NEGOCIOS_EFETUADOS",
			"QTD_CONTRATOS_NEGOCIADOS",
			"QTD_CONTRATOS_ABERTOS_ANTES_LIQUIDACAO",
			"QTD_CONTRATOS_LIQUIDADOS",
			"QTD_CONTRATOS_ABERTO_FINAL",
			"TAXA_MEDIA_SWAP_PREMIO_MEIO_OPC_FLEX",
			"SINAL_TAXA_MEDIA_PREMIO_MEDIO",
			"TIPO_TAXA_MEDIA",
			"PRECO_EXERCICIO_MEDIO_OPC_FLEX",
			"SINAL_PRECO_MEDIO_EXERCICIO",
			"VOLUME_ABERTO_BRL",
			"VOLUME_ABERTO_USD",
			"FILE_NAME",
		]
		assert len(result.columns) == len(expected_columns)

	def test_transform_data_empty_fixed_width_file(self) -> None:
		"""Test transform_data with empty fixed-width file.

		Verifies
		--------
		- Empty files are handled gracefully
		- DataFrame structure is maintained
		- No exceptions are raised

		Returns
		-------
		None
		"""
		instance = B3DerivativesMarketOTCMarketTrades()
		empty_content = StringIO("")

		result = instance.transform_data(empty_content, "empty.txt")

		assert isinstance(result, pd.DataFrame)
		assert len(result) == 0
		assert "FILE_NAME" in result.columns


# --------------------------
# Tests for B3VariableFees
# --------------------------
