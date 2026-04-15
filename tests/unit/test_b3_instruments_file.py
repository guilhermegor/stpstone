"""Unit tests for B3InstrumentsFile ingestion class."""

from collections.abc import Callable
from datetime import date
from io import StringIO
from typing import Any
from unittest.mock import MagicMock, mock_open, patch

import pandas as pd
import pytest
from pytest_mock import MockerFixture
from requests import Response

from stpstone.ingestion.countries.br.exchange.b3_instruments_file import (
	B3InstrumentsFile,
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


class TestB3InstrumentsFile:
	"""Test cases for B3InstrumentsFile class with caching."""

	def test_init_creates_temp_directory(self, mock_fast_operations: dict[str, MagicMock]) -> None:
		"""Test initialization creates temporary directory.

		Verifies
		--------
		- Temporary directory is created on initialization
		- Directory path is stored correctly
		- Correct URL pattern is used

		Parameters
		----------
		mock_fast_operations : dict[str, MagicMock]
			Dictionary of mocked operations

		Returns
		-------
		None
		"""
		temp_path = "/tmp/b3_instruments_test"  # noqa S108: probable insecure usage of temporary file or directory
		mock_fast_operations["tempfile_mkdtemp"].return_value = temp_path

		instance = B3InstrumentsFile()

		assert str(instance.temp_dir) == temp_path
		assert "pesquisapregao/download?filelist=IN" in instance.url
		mock_fast_operations["tempfile_mkdtemp"].assert_called_once()

	def test_get_cached_or_fetch_cache_hit(
		self, mock_fast_operations: dict[str, MagicMock]
	) -> None:
		"""Test get_cached_or_fetch with cache hit.

		Verifies
		--------
		- Cache is checked first
		- Cached content is returned when available
		- No network request is made

		Parameters
		----------
		mock_fast_operations : dict[str, MagicMock]
			Dictionary of mocked operations

		Returns
		-------
		None
		"""
		instance = B3InstrumentsFile()

		with patch.object(instance, "_load_from_cache") as mock_load_cache:
			mock_load_cache.return_value = StringIO("cached content")

			result = instance.get_cached_or_fetch()

			assert isinstance(result, StringIO)
			mock_load_cache.assert_called_once()

	def test_get_cached_or_fetch_cache_miss(
		self, mock_fast_operations: dict[str, MagicMock]
	) -> None:
		"""Test get_cached_or_fetch with cache miss.

		Verifies
		--------
		- Cache miss triggers network fetch
		- get_response and parse_raw_file are called
		- Result is returned from network

		Parameters
		----------
		mock_fast_operations : dict[str, MagicMock]
			Dictionary of mocked operations

		Returns
		-------
		None
		"""
		instance = B3InstrumentsFile()

		with (
			patch.object(instance, "_load_from_cache") as mock_load_cache,
			patch.object(instance, "get_response") as mock_get_response,
			patch.object(instance, "parse_raw_file") as mock_parse,
		):
			mock_load_cache.side_effect = ValueError("Cache miss")
			mock_get_response.return_value = create_mock_response()
			mock_parse.return_value = (StringIO("fresh content"), "test.xml")

			_ = instance.get_cached_or_fetch()

			mock_get_response.assert_called_once()
			mock_parse.assert_called_once()

	def test_load_from_cache_success(self, mock_fast_operations: dict[str, MagicMock]) -> None:
		"""Test successful cache loading.

		Verifies
		--------
		- Cache file is read correctly
		- Content is returned as StringIO
		- File operations work properly

		Parameters
		----------
		mock_fast_operations : dict[str, MagicMock]
			Dictionary of mocked operations

		Returns
		-------
		None
		"""
		instance = B3InstrumentsFile()

		with (
			patch.object(instance, "_get_cached_file_path") as mock_get_path,
			patch("builtins.open", mock_open(read_data="cached xml content")),
		):
			cache_path = MagicMock()
			cache_path.exists.return_value = True
			mock_get_path.return_value = cache_path

			result = instance._load_from_cache()

			assert isinstance(result, StringIO)
			assert result.read() == "cached xml content"

	def test_load_from_cache_file_not_found(
		self, mock_fast_operations: dict[str, MagicMock]
	) -> None:
		"""Test cache loading when file doesn't exist.

		Verifies
		--------
		- ValueError is raised when cache file missing
		- Error message is descriptive

		Parameters
		----------
		mock_fast_operations : dict[str, MagicMock]
			Dictionary of mocked operations

		Returns
		-------
		None
		"""
		instance = B3InstrumentsFile()

		with patch.object(instance, "_get_cached_file_path") as mock_get_path:
			cache_path = MagicMock()
			cache_path.exists.return_value = False
			mock_get_path.return_value = cache_path

			with pytest.raises(ValueError, match="Cache file not found"):
				instance._load_from_cache()

	def test_save_to_cache_success(self, mock_fast_operations: dict[str, MagicMock]) -> None:
		"""Test successful cache saving.

		Verifies
		--------
		- Cache directory is created if needed
		- XML content is written to file
		- No exceptions are raised

		Parameters
		----------
		mock_fast_operations : dict[str, MagicMock]
			Dictionary of mocked operations

		Returns
		-------
		None
		"""
		instance = B3InstrumentsFile()

		with (
			patch.object(instance, "_get_cached_file_path") as mock_get_path,
			patch("builtins.open", mock_open()) as mock_file,
		):
			cache_path = MagicMock()
			cache_path.parent.mkdir = MagicMock()
			mock_get_path.return_value = cache_path

			instance._save_to_cache("test xml content")

			cache_path.parent.mkdir.assert_called_once_with(parents=True, exist_ok=True)
			mock_file.assert_called_once()

	def test_get_cached_file_path(
		self, mock_fast_operations: dict[str, MagicMock], sample_date: date
	) -> None:
		"""Test cached file path generation.

		Verifies
		--------
		- Correct filename format is used
		- Date is formatted properly in filename
		- Path is within temp directory

		Parameters
		----------
		mock_fast_operations : dict[str, MagicMock]
			Dictionary of mocked operations
		sample_date : date
			Sample date for testing

		Returns
		-------
		None
		"""
		instance = B3InstrumentsFile(date_ref=sample_date)

		cache_path = instance._get_cached_file_path()

		assert cache_path.name == "instruments_240115.xml"
		assert str(instance.temp_dir) in str(cache_path)

	def test_cleanup_cache_success(self, mock_fast_operations: dict[str, MagicMock]) -> None:
		"""Test successful cache cleanup.

		Verifies
		--------
		- Temporary directory is removed
		- shutil.rmtree is called with correct path
		- No exceptions are raised

		Parameters
		----------
		mock_fast_operations : dict[str, MagicMock]
			Dictionary of mocked operations

		Returns
		-------
		None
		"""
		instance = B3InstrumentsFile()

		# Mock the existing method on the instance's temp_dir
		with patch("pathlib.Path.exists", return_value=True):
			instance.cleanup_cache()

			mock_fast_operations["shutil_rmtree"].assert_called_once_with(instance.temp_dir)

	def test_cleanup_cache_failure(self, mock_fast_operations: dict[str, MagicMock]) -> None:
		"""Test cache cleanup with failure.

		Verifies
		--------
		- Cleanup failures are handled gracefully
		- Warning is logged but no exception raised
		- Method completes successfully

		Parameters
		----------
		mock_fast_operations : dict[str, MagicMock]
			Dictionary of mocked operations

		Returns
		-------
		None
		"""
		instance = B3InstrumentsFile()

		mock_fast_operations["shutil_rmtree"].side_effect = OSError("Permission denied")

		with patch("pathlib.Path.exists", return_value=True):
			# should not raise exception
			instance.cleanup_cache()

			mock_fast_operations["shutil_rmtree"].assert_called_once()

	def test_get_node_info_basic_structure(self) -> None:
		"""Test _get_node_info method with basic XML structure.

		Verifies
		--------
		- XML nodes are processed correctly
		- Child tags are extracted
		- Attributes are handled when provided
		- List of dictionaries is returned

		Returns
		-------
		None
		"""
		instance = B3InstrumentsFile()

		# create mock BeautifulSoup structure
		mock_soup = MagicMock()
		mock_node = MagicMock()

		with patch.object(instance.cls_xml_handler, "find_all") as mock_find_all:
			mock_find_all.return_value = [mock_node]

			def mock_find(tag: str) -> MagicMock:
				"""Mock find method.

				Parameters
				----------
				tag : str
					Tag to search for

				Returns
				-------
				MagicMock
				"""
				mock_element = MagicMock()
				mock_element.getText.return_value = f"text_{tag}"
				mock_element.get.return_value = f"attr_{tag}"
				return mock_element

			mock_node.find = mock_find

			result = instance._get_node_info(
				soup_xml=mock_soup,
				tag_parent="TestParent",
				list_tags_children=["tag1", "tag2"],
				list_tups_attributes=[("tag1", "attr1")],
			)

			assert isinstance(result, list)
			assert len(result) == 1
			assert "tag1" in result[0]
			assert "tag2" in result[0]
			assert "tag1attr1" in result[0]

	def test_get_node_info_no_attributes(self) -> None:
		"""Test _get_node_info with no attributes.

		Verifies
		--------
		- Method works without attribute extraction
		- Only child tags are processed
		- Empty attribute list is handled

		Returns
		-------
		None
		"""
		instance = B3InstrumentsFile()

		mock_soup = MagicMock()
		mock_node = MagicMock()

		with patch.object(instance.cls_xml_handler, "find_all") as mock_find_all:
			mock_find_all.return_value = [mock_node]

			def mock_find(tag: str) -> MagicMock:
				"""Mock find method.

				Parameters
				----------
				tag : str
					Tag to search for

				Returns
				-------
				MagicMock
				"""
				mock_element = MagicMock()
				mock_element.getText.return_value = f"text_{tag}"
				return mock_element

			mock_node.find = mock_find

			result = instance._get_node_info(
				soup_xml=mock_soup,
				tag_parent="TestParent",
				list_tags_children=["tag1", "tag2"],
				list_tups_attributes=[],
			)

			assert isinstance(result, list)
			assert len(result) == 1
			assert len(result[0]) == 2  # only child tags, no attributes

	def test_run_with_caching_integration(
		self, mock_fast_operations: dict[str, MagicMock]
	) -> None:
		"""Test run method with caching integration.

		Verifies
		--------
		- Caching is integrated into run method
		- Transform data is called with cached content
		- Standard pipeline is followed

		Parameters
		----------
		mock_fast_operations : dict[str, MagicMock]
			Dictionary of mocked operations

		Returns
		-------
		None
		"""
		instance = B3InstrumentsFile()

		with (
			patch.object(instance, "get_cached_or_fetch") as mock_cached_fetch,
			patch.object(instance, "transform_data") as mock_transform,
			patch.object(instance, "standardize_dataframe") as mock_standardize,
		):
			mock_cached_fetch.return_value = (StringIO("cached xml"), "cached.xml")
			mock_transform.return_value = pd.DataFrame({"test": [1], "FILE_NAME": ["cached.xml"]})
			mock_standardize.return_value = pd.DataFrame({"TEST": [1]})

			result = instance.run(dict_dtypes={"test": int})

			mock_cached_fetch.assert_called_once()
			mock_transform.assert_called_once()
			assert isinstance(result, pd.DataFrame)


# --------------------------
# Tests for B3InstrumentsFileEqty
# --------------------------
