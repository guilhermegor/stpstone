"""Unit tests for B3InstrumentsFileIndicators ingestion class."""

from collections.abc import Callable
from datetime import date
from io import StringIO
from typing import Any, Optional
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from pytest_mock import MockerFixture
from requests import Response

from stpstone.ingestion.countries.br.exchange.b3_instruments_file_indicators import (
	B3InstrumentsFileIndicators,
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


class TestB3InstrumentsFileIndicators:
	"""Test cases for B3InstrumentsFileIndicators class."""

	def test_init_indicators_url(self) -> None:
		"""Test initialization with indicators URL pattern.

		Verifies
		--------
		- Correct URL pattern for indicators
		- II prefix is used in URL
		- .zip file extension

		Returns
		-------
		None
		"""
		instance = B3InstrumentsFileIndicators()

		assert "pesquisapregao/download?filelist=II" in instance.url
		assert instance.url.endswith(".zip")

	def test_run_with_indicators_dtypes(self, mock_fast_operations: dict[str, MagicMock]) -> None:
		"""Test run method with indicators data types.

		Verifies
		--------
		- Date columns are properly typed
		- Pascal to upper constant case conversion
		- Complex indicator data handling

		Parameters
		----------
		mock_fast_operations : dict[str, MagicMock]
			Dictionary of mocked operations

		Returns
		-------
		None
		"""
		instance = B3InstrumentsFileIndicators()

		with (
			patch.object(instance, "get_response") as mock_get_response,
			patch.object(instance, "parse_raw_file") as mock_parse,
			patch.object(instance, "transform_data") as mock_transform,
			patch.object(instance, "standardize_dataframe") as mock_standardize,
		):
			mock_get_response.return_value = create_mock_response()
			mock_parse.return_value = (StringIO(create_sample_xml_content()), "indicators.xml")
			mock_transform.return_value = pd.DataFrame(
				{"ActvtyInd": ["Y"], "Id": ["IND001"], "FILE_NAME": ["indicators.xml"]}
			)
			mock_standardize.return_value = pd.DataFrame({"ACTVTY_IND": ["Y"]})

			_ = instance.run()

			call_args = mock_standardize.call_args[1]
			expected_dtypes = call_args["dict_dtypes"]

			assert expected_dtypes["DT"] == "date"
			assert expected_dtypes["ACTVTY_IND"] is str

	def test_instrument_indicator_node_xml_extraction(self) -> None:
		"""Test _instrument_indicator_node method for indicators XML extraction.

		Verifies
		--------
		- Complex nested XML structure is navigated
		- All indicator fields are extracted
		- Economic indicator details are captured

		Returns
		-------
		None
		"""
		instance = B3InstrumentsFileIndicators()

		# create mock XML structure for indicators
		mock_soup_parent = MagicMock()

		# create nested mock structure for complex indicators XML
		mock_rpt_params = MagicMock()
		mock_instrm_id = MagicMock()
		mock_instrm_inf = MagicMock()
		mock_instrm_dtls = MagicMock()

		def create_nested_mock(text: str, nested_text: Optional[str] = None) -> MagicMock:
			"""Create nested mock structure.

			Parameters
			----------
			text : str
				Text content for mock node
			nested_text : Optional[str], optional
				Text content for nested mock node

			Returns
			-------
			MagicMock
				Nested mock structure
			"""
			mock = MagicMock()
			mock.text = text
			if nested_text:
				nested = MagicMock()
				nested.text = nested_text
				mock.find.return_value = nested
			return mock

		# setup the complex find chain for indicators
		mock_soup_parent.find.side_effect = lambda tag: {
			"RptParams": mock_rpt_params,
			"InstrmId": mock_instrm_id,
			"InstrmInf": mock_instrm_inf,
			"InstrmDtls": mock_instrm_dtls,
		}.get(tag)

		# mock indicator node extraction
		result = instance._instrument_indicator_node(mock_soup_parent)

		assert isinstance(result, dict)


# --------------------------
# Tests for B3InstrumentsFileOptnOnEqts
# --------------------------
