"""Unit tests for B3VariableFees ingestion class."""

from collections.abc import Callable
from datetime import date
from io import StringIO
from typing import Any
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from pytest_mock import MockerFixture
from requests import Response

from stpstone.ingestion.countries.br.exchange.b3_variable_fees import (
	B3VariableFees,
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


class TestB3VariableFees:
	"""Test cases for B3VariableFees class."""

	def test_init_variable_fees_url(self) -> None:
		"""Test initialization with variable fees URL pattern.

		Verifies
		--------
		- Correct URL pattern for variable fees
		- VA prefix is used in URL
		- .zip file extension

		Returns
		-------
		None
		"""
		instance = B3VariableFees()

		assert "pesquisapregao/download?filelist=VA" in instance.url
		assert instance.url.endswith(".zip")

	def test_run_with_xml_processing(self, mock_fast_operations: dict[str, MagicMock]) -> None:
		"""Test run method with XML processing for variable fees.

		Verifies
		--------
		- XML processing pipeline works
		- Pascal to upper constant case conversion
		- Complex XML structure is handled

		Parameters
		----------
		mock_fast_operations : dict[str, MagicMock]
			Dictionary of mocked operations

		Returns
		-------
		None
		"""
		instance = B3VariableFees()

		with (
			patch.object(instance, "get_response") as mock_get_response,
			patch.object(instance, "parse_raw_file") as mock_parse,
			patch.object(instance, "transform_data") as mock_transform,
			patch.object(instance, "standardize_dataframe") as mock_standardize,
		):
			mock_get_response.return_value = create_mock_response()
			mock_parse.return_value = (StringIO(create_sample_xml_content()), "fees.xml")
			mock_transform.return_value = pd.DataFrame(
				{"Frqcy": ["DAILY"], "Id": ["123"], "FILE_NAME": ["fees.xml"]}
			)
			mock_standardize.return_value = pd.DataFrame({"FRQCY": ["DAILY"]})

			_ = instance.run()

			call_args = mock_standardize.call_args[1]
			assert call_args["cols_from_case"] == "pascal"
			assert call_args["cols_to_case"] == "upper_constant"

	def test_instrument_indicator_node_xml_extraction(self) -> None:
		"""Test _instrument_indicator_node method for XML extraction.

		Verifies
		--------
		- Complex XML structure is navigated correctly
		- Nested elements are extracted
		- All required fields are captured

		Returns
		-------
		None
		"""
		instance = B3VariableFees()

		mock_soup_parent = MagicMock()

		mock_rpt_params = MagicMock()
		mock_validity_period = MagicMock()
		mock_fee_inf = MagicMock()
		mock_fin_instrm_attrbts = MagicMock()
		mock_othr_fee_qtn_inf = MagicMock()
		mock_convs_ind = MagicMock()
		mock_othr_id = MagicMock()
		mock_tp = MagicMock()
		mock_plc_of_listg = MagicMock()

		mock_soup_parent.find.side_effect = lambda tag: {
			"RptParams": mock_rpt_params,
			"VldtyPrd": mock_validity_period,
			"FeeInf": mock_fee_inf,
		}.get(tag)

		mock_fee_inf.find.side_effect = lambda tag: {
			"FinInstrmAttrbts": mock_fin_instrm_attrbts,
			"OthrFeeQtnInf": mock_othr_fee_qtn_inf,
			"ConvsInd": mock_convs_ind,
		}.get(tag)

		def create_text_mock(text: str) -> MagicMock:
			"""Create mock for text extraction.

			Parameters
			----------
			text : str
				Text to mock.

			Returns
			-------
			MagicMock
				Mock object for text extraction
			"""
			mock = MagicMock()
			mock.text = text
			return mock

		mock_rpt_params.find.side_effect = lambda tag: {
			"Frqcy": create_text_mock("DAILY"),
			"RptNb": create_text_mock("123"),
			"RptDtAndTm": MagicMock(),
		}.get(tag)

		mock_rpt_params.find("RptDtAndTm").find.return_value = create_text_mock("20240115")

		mock_validity_period.find.side_effect = lambda tag: {
			"FrDtTm": create_text_mock("20240101"),
			"ToDtTm": create_text_mock("20241231"),
		}.get(tag)

		mock_fin_instrm_attrbts.find.side_effect = lambda tag: {
			"Sgmt": create_text_mock("EQUITY"),
			"Asst": create_text_mock("STOCK"),
		}.get(tag)

		mock_othr_fee_qtn_inf.find.side_effect = lambda tag: {
			"RefDt": create_text_mock("20240115"),
			"ConvsIndVal": create_text_mock("1.0"),
			"PlcOfListg": mock_plc_of_listg,
		}.get(tag)

		mock_convs_ind.find.return_value = mock_othr_id
		mock_othr_id.find.side_effect = lambda tag: {
			"Id": create_text_mock("IND123"),
			"Tp": mock_tp,
		}.get(tag)

		mock_tp.find.return_value = create_text_mock("CUSTOM")
		mock_plc_of_listg.find.return_value = create_text_mock("BVMF")

		result = instance._instrument_indicator_node(mock_soup_parent)

		assert isinstance(result, dict)
		assert result["Frqcy"] == "DAILY"
		assert result["RptNb"] == "123"
		assert result["Sgmt"] == "EQUITY"
		assert result["ConvsIndVal"] == "1.0"

	def test_transform_data_variable_fees_xml(self) -> None:
		"""Test transform_data for variable fees XML structure.

		Verifies
		--------
		- FeeVarblInf nodes are processed
		- Multiple fee records are handled
		- FILE_NAME is added to results

		Returns
		-------
		None
		"""
		instance = B3VariableFees()

		with (
			patch.object(instance.cls_xml_handler, "memory_parser") as mock_parser,
			patch.object(instance.cls_xml_handler, "find_all") as mock_find_all,
			patch.object(instance, "_instrument_indicator_node") as mock_node_method,
		):
			mock_xml = MagicMock()
			mock_parser.return_value = mock_xml

			mock_node1 = MagicMock()
			mock_node2 = MagicMock()
			mock_find_all.return_value = [mock_node1, mock_node2]

			mock_node_method.side_effect = [
				{"Frqcy": "DAILY", "Id": "123"},
				{"Frqcy": "WEEKLY", "Id": "456"},
			]

			result = instance.transform_data(StringIO("<root></root>"), "fees.xml")

			assert isinstance(result, pd.DataFrame)
			assert len(result) == 2
			assert "FILE_NAME" in result.columns
			assert all(result["FILE_NAME"] == "fees.xml")


# --------------------------
# Tests for B3UpdatesSearchByTradingSessionUpdateTimeSeries
# --------------------------
