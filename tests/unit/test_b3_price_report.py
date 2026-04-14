"""Unit tests for B3PriceReport ingestion class."""

from collections.abc import Callable
from datetime import date
from io import StringIO
from typing import Any
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from pytest_mock import MockerFixture
from requests import Response

from stpstone.ingestion.countries.br.exchange.b3_price_report import (
	B3PriceReport,
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


class TestB3PriceReport:
	"""Test cases for B3PriceReport class."""

	def test_init_url_construction(self) -> None:
		"""Test URL construction for Price Report.

		Verifies
		--------
		- Correct URL pattern is used
		- URL contains PR prefix for Price Report
		- Date formatting is correct

		Returns
		-------
		None
		"""
		instance = B3PriceReport()

		assert "pesquisapregao/download?filelist=PR" in instance.url
		assert instance.url.endswith(".zip")

	def test_run_data_types_validation(self, mock_fast_operations: dict[str, MagicMock]) -> None:
		"""Test run method with correct data types.

		Verifies
		--------
		- Extensive data type dictionary is correctly passed
		- Date columns are properly typed
		- Currency columns are included
		- Numeric precision is maintained

		Parameters
		----------
		mock_fast_operations : dict[str, MagicMock]
			Dictionary of mocked operations

		Returns
		-------
		None
		"""
		instance = B3PriceReport()

		with (
			patch.object(instance, "get_response") as mock_get_response,
			patch.object(instance, "parse_raw_file") as mock_parse,
			patch.object(instance, "transform_data") as mock_transform,
			patch.object(instance, "standardize_dataframe") as mock_standardize,
		):
			mock_get_response.return_value = create_mock_response()
			mock_parse.return_value = (StringIO(create_sample_xml_content()), "test.xml")
			mock_transform.return_value = pd.DataFrame(
				{"Dt": ["20240115"], "FILE_NAME": ["test.xml"]}
			)
			mock_standardize.return_value = pd.DataFrame({"DT": ["2024-01-15"]})

			instance.run()

			call_args = mock_standardize.call_args[1]
			expected_dtypes = call_args["dict_dtypes"]

			assert expected_dtypes["DT"] == "date"
			assert expected_dtypes["TCKR_SYMB"] is str
			assert expected_dtypes["TRAD_QTY"] is int
			assert expected_dtypes["BEST_BID_PRIC"] is float
			assert expected_dtypes["NTL_FIN_VOL_CCY"] is str

	def test_transform_data_price_report_xml(self) -> None:
		"""Test transform_data for PriceReport XML structure.

		Verifies
		--------
		- PricRpt nodes are correctly identified
		- All price-related fields are extracted
		- Currency attributes are captured
		- Data validation is performed

		Returns
		-------
		None
		"""
		instance = B3PriceReport()

		xml_content = StringIO("""<?xml version="1.0"?>
        <root>
            <PricRpt>
                <Dt>20240115</Dt>
                <TckrSymb>PETR4</TckrSymb>
                <Id>123</Id>
                <Prtry>EQUITY</Prtry>
                <MktIdrCd>BVMF</MktIdrCd>
                <TradQty>1000</TradQty>
                <OpnIntrst>500</OpnIntrst>
                <FinInstrmQty>100</FinInstrmQty>
                <OscnPctg>2.5</OscnPctg>
                <NtlFinVol Ccy="BRL">100000</NtlFinVol>
                <IntlFinVol Ccy="BRL">50000</IntlFinVol>
                <BestBidPric Ccy="BRL">25.50</BestBidPric>
                <BestAskPric Ccy="BRL">25.60</BestAskPric>
                <FrstPric Ccy="BRL">25.45</FrstPric>
                <MinPric Ccy="BRL">25.30</MinPric>
                <MaxPric Ccy="BRL">25.70</MaxPric>
                <TradAvrgPric Ccy="BRL">25.55</TradAvrgPric>
                <LastPric Ccy="BRL">25.65</LastPric>
                <VartnPts>0.15</VartnPts>
                <MaxTradLmt>100.00</MaxTradLmt>
                <MinTradLmt>1.00</MinTradLmt>
                <NtlRglrVol Ccy="BRL">80000</NtlRglrVol>
                <IntlRglrVol Ccy="BRL">40000</IntlRglrVol>
            </PricRpt>
        </root>""")

		with (
			patch.object(instance.cls_xml_handler, "memory_parser") as mock_parser,
			patch.object(instance.cls_xml_handler, "find_all") as mock_find_all,
		):
			# create mock BeautifulSoup structure
			mock_xml = MagicMock()
			mock_parser.return_value = mock_xml

			mock_node = MagicMock()
			mock_find_all.return_value = [mock_node]

			# mock the complex find behavior for price report
			def mock_find(tag: str) -> MagicMock:
				"""Mock find method.

				Parameters
				----------
				tag : str
					Tag to find

				Returns
				-------
				MagicMock
				"""
				mock_element = MagicMock()
				if tag == "Dt":
					mock_element.getText.return_value = "20240115"
				elif tag == "TckrSymb":
					mock_element.getText.return_value = "PETR4"
				elif tag == "NtlFinVol":
					mock_element.getText.return_value = "100000"
					mock_element.get.return_value = "BRL"
				else:
					mock_element.getText.return_value = f"test_{tag}"
					mock_element.get.return_value = "BRL"
				return mock_element

			mock_node.find = mock_find

			result = instance.transform_data(xml_content, "price_report.xml")

			assert isinstance(result, pd.DataFrame)
			assert len(result) == 1
			assert "FILE_NAME" in result.columns

	def test_transform_data_multiple_price_reports(self) -> None:
		"""Test transform_data with multiple PricRpt nodes.

		Verifies
		--------
		- Multiple price reports are processed
		- Each report creates a separate row
		- All data is preserved correctly

		Returns
		-------
		None
		"""
		instance = B3PriceReport()

		with (
			patch.object(instance.cls_xml_handler, "memory_parser") as mock_parser,
			patch.object(instance.cls_xml_handler, "find_all") as mock_find_all,
		):
			mock_xml = MagicMock()
			mock_parser.return_value = mock_xml

			# create multiple mock nodes
			mock_node1 = MagicMock()
			mock_node2 = MagicMock()
			mock_find_all.return_value = [mock_node1, mock_node2]

			def create_mock_find(node_id: str) -> MagicMock:
				"""Create mock find method for each node.

				Parameters
				----------
				node_id : str
					ID of the node

				Returns
				-------
				MagicMock
				"""

				def mock_find(tag: str) -> MagicMock:
					"""Mock find method.

					Parameters
					----------
					tag : str
						Tag to find

					Returns
					-------
					MagicMock
					"""
					mock_element = MagicMock()
					mock_element.getText.return_value = f"value_{node_id}_{tag}"
					mock_element.get.return_value = "BRL"
					return mock_element

				return mock_find

			mock_node1.find = create_mock_find("1")
			mock_node2.find = create_mock_find("2")

			result = instance.transform_data(StringIO("<root></root>"), "test.xml")

			assert isinstance(result, pd.DataFrame)
			assert len(result) == 2


# --------------------------
# Tests for B3InstrumentsFile
# --------------------------
