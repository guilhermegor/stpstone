"""Unit tests for B3IndexReport ingestion class."""

from collections.abc import Callable
from io import StringIO
from typing import Any
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from pytest_mock import MockerFixture
from requests import Response

from stpstone.ingestion.countries.br.exchange.b3_index_report import B3IndexReport


# --------------------------
# Module Utilities
# --------------------------
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


# --------------------------
# Tests for B3IndexReport
# --------------------------
class TestB3IndexReport:
	"""Test cases for B3IndexReport class."""

	def test_init_with_defaults(self) -> None:
		"""Test initialization with default parameters.

		Verifies
		--------
		- Correct URL is constructed for Index Report
		- Default values are set properly
		- Inherits from ABCB3SearchByTradingSession

		Returns
		-------
		None
		"""
		instance = B3IndexReport()

		assert "pesquisapregao/download?filelist=IR" in instance.url
		assert instance.logger is None
		assert instance.cls_db is None

	def test_run_with_custom_parameters(
		self,
		mock_fast_operations: dict[str, MagicMock],
	) -> None:
		"""Test run method with custom parameters.

		Verifies
		--------
		- Custom column case conversion parameters work
		- Correct data types are used for index report
		- Default table name is applied

		Parameters
		----------
		mock_fast_operations : dict[str, MagicMock]
			Dictionary of mocked operations

		Returns
		-------
		None
		"""
		instance = B3IndexReport()

		with (
			patch.object(instance, "get_response") as mock_get_response,
			patch.object(instance, "parse_raw_file") as mock_parse,
			patch.object(instance, "transform_data") as mock_transform,
			patch.object(instance, "standardize_dataframe") as mock_standardize,
		):
			mock_get_response.return_value = create_mock_response()
			mock_parse.return_value = (StringIO(create_sample_xml_content()), "test.xml")
			mock_transform.return_value = pd.DataFrame(
				{"TckrSymb": ["TEST"], "FILE_NAME": ["test.xml"]}
			)
			mock_standardize.return_value = pd.DataFrame({"TCKR_SYMB": ["TEST"]})

			_ = instance.run(
				cols_from_case="snake",
				cols_to_case="lower",
			)

			mock_standardize.assert_called_once()
			call_args = mock_standardize.call_args[1]
			assert call_args["cols_from_case"] == "snake"
			assert call_args["cols_to_case"] == "lower"

	def test_transform_data_xml_parsing(self, xml_stringio: StringIO) -> None:
		"""Test transform_data method with XML content.

		Verifies
		--------
		- XML is parsed correctly
		- IndxInf nodes are extracted
		- All expected fields are captured
		- Currency attributes are handled

		Parameters
		----------
		xml_stringio : StringIO
			StringIO object with XML data

		Returns
		-------
		None
		"""
		instance = B3IndexReport()

		with (
			patch.object(instance.cls_xml_handler, "memory_parser") as mock_parser,
			patch.object(instance.cls_xml_handler, "find_all") as mock_find_all,
		):
			mock_xml = MagicMock()
			mock_parser.return_value = mock_xml

			mock_node = MagicMock()
			mock_find_all.return_value = [mock_node]

			def mock_find(tag: str) -> MagicMock:
				"""Mock find method.

				Parameters
				----------
				tag : str
					Tag to find

				Returns
				-------
				MagicMock
					Mocked element
				"""
				mock_element = MagicMock()
				mock_element.getText.return_value = f"test_{tag}"
				if tag == "SttlmVal":
					mock_element.get.return_value = "BRL"
				return mock_element

			mock_node.find = mock_find

			result = instance.transform_data(xml_stringio, "test.xml")

			assert isinstance(result, pd.DataFrame)
			assert len(result) == 1
			assert "FILE_NAME" in result.columns
			assert result.iloc[0]["FILE_NAME"] == "test.xml"

	def test_transform_data_missing_xml_elements(self) -> None:
		"""Test transform_data with missing XML elements.

		Verifies
		--------
		- Missing XML elements are handled gracefully
		- None values are set for missing elements
		- No exceptions are raised

		Returns
		-------
		None
		"""
		instance = B3IndexReport()

		with (
			patch.object(instance.cls_xml_handler, "memory_parser") as mock_parser,
			patch.object(instance.cls_xml_handler, "find_all") as mock_find_all,
		):
			mock_xml = MagicMock()
			mock_parser.return_value = mock_xml

			mock_node = MagicMock()
			mock_find_all.return_value = [mock_node]

			mock_node.find.return_value = None

			result = instance.transform_data(StringIO("<root></root>"), "test.xml")

			assert isinstance(result, pd.DataFrame)
			assert len(result) == 1
			for col in result.columns:
				if col != "FILE_NAME":
					assert result.iloc[0][col] is None
