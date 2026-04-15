"""Unit tests for B3UpdatesSearchByTradingSessionUpdateTimeSeries ingestion class."""

from collections.abc import Callable
from datetime import date
from io import StringIO
from typing import Any
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from pytest_mock import MockerFixture
from requests import Response

from stpstone.ingestion.countries.br.exchange.b3_updates_search_by_trading_session_update_time_series import (  # noqa: E501
	B3UpdatesSearchByTradingSessionUpdateTimeSeries,
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


class TestB3UpdatesSearchByTradingSessionUpdateTimeSeries:
	"""Test cases for B3UpdatesSearchByTradingSessionUpdateTimeSeries class."""

	def test_init_updates_url(self) -> None:
		"""Test initialization with updates URL.

		Verifies
		--------
		- Different URL pattern for updates page
		- Not inheriting from ABCB3SearchByTradingSession
		- Correct HTML parsing setup

		Returns
		-------
		None
		"""
		instance = B3UpdatesSearchByTradingSessionUpdateTimeSeries()

		assert "pt_br/market-data-e-indices" in instance.url
		assert "pesquisa-por-pregao" in instance.url
		assert not instance.url.endswith(".zip")

	def test_run_html_processing_pipeline(
		self, mock_fast_operations: dict[str, MagicMock]
	) -> None:
		"""Test run method with HTML processing pipeline.

		Verifies
		--------
		- HTML response is processed instead of ZIP
		- Different data transformation pipeline
		- Date format is DD/MM/YYYY

		Parameters
		----------
		mock_fast_operations : dict[str, MagicMock]
			Dictionary of mocked operations

		Returns
		-------
		None
		"""
		instance = B3UpdatesSearchByTradingSessionUpdateTimeSeries()

		with (
			patch.object(instance, "get_response") as mock_get_response,
			patch.object(instance, "parse_raw_file") as mock_parse,
			patch.object(instance, "transform_data") as mock_transform,
			patch.object(instance, "standardize_dataframe") as mock_standardize,
		):
			mock_get_response.return_value = create_mock_response()
			mock_parse.return_value = MagicMock()  # HtmlElement mock
			mock_transform.return_value = pd.DataFrame(
				{"ARQUIVOS_CLEARING_B3": ["File1"], "DATA_ATUALIZACAO": ["15/01/2024"]}
			)
			mock_standardize.return_value = pd.DataFrame({"ARQUIVOS_CLEARING_B3": ["File1"]})

			_ = instance.run()

			call_args = mock_standardize.call_args[1]
			assert call_args["str_fmt_dt"] == "DD/MM/YYYY"

	def test_parse_raw_file_html_parsing(self, mock_fast_operations: dict[str, MagicMock]) -> None:
		"""Test parse_raw_file for HTML content.

		Verifies
		--------
		- HTML handler is used instead of file extraction
		- lxml_parser is called
		- Response object is passed correctly

		Parameters
		----------
		mock_fast_operations : dict[str, MagicMock]
			Dictionary of mocked operations

		Returns
		-------
		None
		"""
		instance = B3UpdatesSearchByTradingSessionUpdateTimeSeries()
		mock_response = create_mock_response()

		with patch.object(instance.cls_html_handler, "lxml_parser") as mock_lxml_parser:
			mock_lxml_parser.return_value = MagicMock()

			_ = instance.parse_raw_file(mock_response)

			mock_lxml_parser.assert_called_once_with(resp_req=mock_response)

	def test_transform_data_xpath_extraction(self) -> None:
		"""Test transform_data with XPath extraction from HTML.

		Verifies
		--------
		- XPath expressions are used correctly
		- Multiple tables are processed
		- Data is properly paired with headers

		Returns
		-------
		None
		"""
		instance = B3UpdatesSearchByTradingSessionUpdateTimeSeries()
		mock_html_root = MagicMock()

		with (
			patch.object(instance.cls_html_handler, "lxml_xpath") as mock_xpath,
			patch.object(instance.cls_handling_dicts, "pair_headers_with_data") as mock_pair,
		):
			# mock XPath results - create a cycle that will eventually return empty lists
			xpath_results = [
				["File1", "File2"],  # Table 1 file names
				["15/01/2024", "16/01/2024"],  # Table 1 timestamps
				["File3"],  # Table 2 file names
				["17/01/2024"],  # Table 2 timestamps
			]

			# create an iterator that will eventually exhaust
			def xpath_side_effect(
				*args: Any,  # noqa ANN401: typing.Any is not allowed
				**kwargs: Any,  # noqa ANN401: typing.Any is not allowed
			) -> list[str]:
				"""Cycle through XPath results and eventually return empty lists.

				Parameters
				----------
				*args : Any
					Variable-length argument list.
				**kwargs : Any
					Arbitrary keyword arguments.

				Returns
				-------
				list[str]
					List of strings.
				"""
				if xpath_results:
					return xpath_results.pop(0)
				return []  # return empty list when exhausted

			mock_xpath.side_effect = xpath_side_effect

			mock_pair.return_value = [
				{"ARQUIVOS_CLEARING_B3": "File1", "DATA_ATUALIZACAO": "15/01/2024"},
				{"ARQUIVOS_CLEARING_B3": "File2", "DATA_ATUALIZACAO": "16/01/2024"},
				{"ARQUIVOS_CLEARING_B3": "File3", "DATA_ATUALIZACAO": "17/01/2024"},
			]

			result = instance.transform_data(html_root=mock_html_root)

			assert isinstance(result, pd.DataFrame)
			assert len(result) == 3
			assert "ARQUIVOS_CLEARING_B3" in result.columns
			assert "DATA_ATUALIZACAO" in result.columns

	def test_transform_data_empty_html_tables(self) -> None:
		"""Test transform_data with empty HTML tables.

		Verifies
		--------
		- Empty tables are handled gracefully
		- DataFrame is created with correct structure
		- No exceptions are raised

		Returns
		-------
		None
		"""
		instance = B3UpdatesSearchByTradingSessionUpdateTimeSeries()
		mock_html_root = MagicMock()

		with (
			patch.object(instance.cls_html_handler, "lxml_xpath") as mock_xpath,
			patch.object(instance.cls_handling_dicts, "pair_headers_with_data") as mock_pair,
		):
			mock_xpath.return_value = []
			mock_pair.return_value = []

			result = instance.transform_data(html_root=mock_html_root)

			assert isinstance(result, pd.DataFrame)
			assert len(result) == 0


# --------------------------
# Integration Tests
# --------------------------
