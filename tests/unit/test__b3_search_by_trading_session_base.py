"""Unit tests for ABCB3SearchByTradingSession base class."""

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

from stpstone.ingestion.countries.br.exchange._b3_search_by_trading_session_base import (
	ABCB3SearchByTradingSession,
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
# Tests for ABCB3SearchByTradingSession
# --------------------------
class TestABCB3SearchByTradingSession:
	"""Test cases for ABCB3SearchByTradingSession abstract base class."""

	def test_init_with_default_values(
		self,
		mock_logger: MagicMock,
		mock_db_session: MagicMock,
	) -> None:
		"""Test initialization with default values.

		Verifies
		--------
		- The class can be initialized with default parameters
		- Default date is set correctly when not provided
		- URL is formatted correctly with date

		Parameters
		----------
		mock_logger : MagicMock
			Mock logger instance
		mock_db_session : MagicMock
			Mock database session instance

		Returns
		-------
		None
		"""

		class ConcreteB3Class(ABCB3SearchByTradingSession):
			"""Concrete subclass for testing."""

			def transform_data(
				self,
				file: StringIO,
				file_name: str,
			) -> pd.DataFrame:
				"""Transform file content into a DataFrame.

				Parameters
				----------
				file : StringIO
					The file content.
				file_name : str
					The file name

				Returns
				-------
				pd.DataFrame
					The transformed DataFrame.
				"""
				return pd.DataFrame({"test": [1, 2, 3]})

		instance = ConcreteB3Class(
			logger=mock_logger,
			cls_db=mock_db_session,
			url="https://example.com/{}",
		)

		assert instance.logger == mock_logger
		assert instance.cls_db == mock_db_session
		assert instance.date_ref is not None
		assert "https://example.com/" in instance.url

	def test_init_with_custom_date(
		self,
		mock_logger: MagicMock,
		mock_db_session: MagicMock,
		sample_date: date,
	) -> None:
		"""Test initialization with custom date reference.

		Verifies
		--------
		- The class accepts custom date reference
		- URL is formatted with provided date
		- All attributes are set correctly

		Parameters
		----------
		mock_logger : MagicMock
			Mock logger instance
		mock_db_session : MagicMock
			Mock database session instance
		sample_date : date
			Sample date for testing

		Returns
		-------
		None
		"""

		class ConcreteB3Class(ABCB3SearchByTradingSession):
			"""Concrete subclass for testing."""

			def transform_data(self, file: StringIO, file_name: str) -> pd.DataFrame:
				"""Transform file content into a DataFrame.

				Parameters
				----------
				file : StringIO
					The file content.
				file_name : str
					The file name

				Returns
				-------
				pd.DataFrame
					The transformed DataFrame.
				"""
				return pd.DataFrame({"test": [1, 2, 3]})

		instance = ConcreteB3Class(
			date_ref=sample_date,
			logger=mock_logger,
			cls_db=mock_db_session,
			url="https://example.com/{}",
		)

		assert instance.date_ref == sample_date
		assert instance.logger == mock_logger
		assert instance.cls_db == mock_db_session
		assert "240115" in instance.url  # YYMMDD format

	def test_init_with_none_values(self) -> None:
		"""Test initialization with None values.

		Verifies
		--------
		- The class handles None values gracefully
		- Default date is calculated when date_ref is None
		- Logger and db session can be None

		Returns
		-------
		None
		"""

		class ConcreteB3Class(ABCB3SearchByTradingSession):
			"""Concrete subclass for testing."""

			def transform_data(self, file: StringIO, file_name: str) -> pd.DataFrame:
				"""Transform file content into a DataFrame.

				Parameters
				----------
				file : StringIO
					The file content.
				file_name : str
					The file name

				Returns
				-------
				pd.DataFrame
					The transformed DataFrame.
				"""
				return pd.DataFrame({"test": [1, 2, 3]})

		instance = ConcreteB3Class(url="https://example.com/{}")

		assert instance.date_ref is not None
		assert instance.logger is None
		assert instance.cls_db is None

	def test_get_response_success(self, mock_fast_operations: dict[str, MagicMock]) -> None:
		"""Test successful HTTP response.

		Verifies
		--------
		- HTTP request is made successfully
		- Response status is checked
		- Timeout and verify parameters are passed correctly

		Parameters
		----------
		mock_fast_operations : dict[str, MagicMock]
			Dictionary of mocked operations

		Returns
		-------
		None
		"""

		class ConcreteB3Class(ABCB3SearchByTradingSession):
			"""Concrete subclass for testing."""

			def transform_data(self, file: StringIO, file_name: str) -> pd.DataFrame:
				"""Transform file content into a DataFrame.

				Parameters
				----------
				file : StringIO
					The file content.
				file_name : str
					The file name

				Returns
				-------
				pd.DataFrame
					The transformed DataFrame.
				"""
				return pd.DataFrame({"test": [1, 2, 3]})

		instance = ConcreteB3Class(url="https://example.com/{}")

		result = instance.get_response(timeout=(10.0, 20.0), bool_verify=False)

		mock_fast_operations["requests_get"].assert_called_once_with(
			instance.url, timeout=(10.0, 20.0), verify=False
		)
		assert result == mock_fast_operations["requests_get"].return_value

	def test_parse_raw_file_with_zip_content(self) -> None:
		"""Test parsing raw file with ZIP content.

		Verifies
		--------
		- ZIP files are properly extracted
		- Content is decoded with correct encoding
		- StringIO object is returned

		Returns
		-------
		None
		"""

		class ConcreteB3Class(ABCB3SearchByTradingSession):
			"""Concrete subclass for testing."""

			def transform_data(self, file: StringIO, file_name: str) -> pd.DataFrame:
				"""Transform file content into a DataFrame.

				Parameters
				----------
				file : StringIO
					The file content.
				file_name : str
					The file name

				Returns
				-------
				pd.DataFrame
					The transformed DataFrame.
				"""
				return pd.DataFrame({"test": [1, 2, 3]})

		instance = ConcreteB3Class(url="https://example.com/{}")
		mock_response = create_mock_response()

		with patch.object(
			instance.cls_dir_files_management, "recursive_unzip_in_memory"
		) as mock_unzip:
			mock_unzip.return_value = [(StringIO("test,content"), "test.csv")]

			result_file, result_filename = instance.parse_raw_file(mock_response)

			assert isinstance(result_file, StringIO)
			assert result_filename == "test.csv"
			mock_unzip.assert_called_once()

	def test_parse_raw_file_no_files_found(self) -> None:
		"""Test parse_raw_file when no files are found.

		Verifies
		--------
		- ValueError is raised when no files found
		- Error message is descriptive

		Returns
		-------
		None
		"""

		class ConcreteB3Class(ABCB3SearchByTradingSession):
			"""Concrete subclass for testing."""

			def transform_data(self, file: StringIO, file_name: str) -> pd.DataFrame:
				"""Transform file content into a DataFrame.

				Parameters
				----------
				file : StringIO
					The file content.
				file_name : str
					The file name

				Returns
				-------
				pd.DataFrame
					The transformed DataFrame.
				"""
				return pd.DataFrame({"test": [1, 2, 3]})

		instance = ConcreteB3Class(url="https://example.com/{}")
		mock_response = create_mock_response()

		with patch.object(
			instance.cls_dir_files_management, "recursive_unzip_in_memory"
		) as mock_unzip:
			mock_unzip.return_value = []

			with pytest.raises(ValueError, match="No files found in the downloaded content"):
				instance.parse_raw_file(mock_response)

	def test_parse_raw_file_encoding_fallback(self) -> None:
		"""Test parse_raw_file with encoding fallback.

		Verifies
		--------
		- UTF-8 decoding is tried first
		- Falls back to latin-1 encoding
		- Falls back to cp1252 encoding
		- Finally uses error replacement

		Returns
		-------
		None
		"""

		class ConcreteB3Class(ABCB3SearchByTradingSession):
			"""Concrete subclass for testing."""

			def transform_data(self, file: StringIO, file_name: str) -> pd.DataFrame:
				"""Transform file content into a DataFrame.

				Parameters
				----------
				file : StringIO
					The file content.
				file_name : str
					The file name

				Returns
				-------
				pd.DataFrame
					The transformed DataFrame.
				"""
				return pd.DataFrame({"test": [1, 2, 3]})

		instance = ConcreteB3Class(url="https://example.com/{}")
		mock_response = create_mock_response()

		invalid_utf8 = BytesIO(b"\xff\xfe\x00invalid\xff")

		with patch.object(
			instance.cls_dir_files_management, "recursive_unzip_in_memory"
		) as mock_unzip:
			mock_unzip.return_value = [(invalid_utf8, "test.txt")]

			result_file, result_filename = instance.parse_raw_file(mock_response)

			assert isinstance(result_file, StringIO)
			assert result_filename == "test.txt"

	def test_parse_raw_ex_file_success(self, mock_fast_operations: dict[str, MagicMock]) -> None:
		"""Test successful parsing of .ex_ file with Wine.

		Verifies
		--------
		- Temporary directory is created
		- .ex_ file is extracted and saved
		- Wine is executed successfully
		- Output file is found and read
		- Cleanup is performed

		Parameters
		----------
		mock_fast_operations : dict[str, MagicMock]
			Dictionary of mocked operations

		Returns
		-------
		None
		"""

		class ConcreteB3Class(ABCB3SearchByTradingSession):
			"""Concrete subclass for testing."""

			def transform_data(self, file: StringIO, file_name: str) -> pd.DataFrame:
				"""Transform file content into a DataFrame.

				Parameters
				----------
				file : StringIO
					The file content.
				file_name : str
					The file name

				Returns
				-------
				pd.DataFrame
					The transformed DataFrame.
				"""
				return pd.DataFrame({"test": [1, 2, 3]})

		instance = ConcreteB3Class(url="https://example.com/{}")
		mock_response = create_mock_response()

		temp_dir = Path("/tmp/test_dir")  # noqa S108: probable insecure usage of temporary file or directory
		mock_fast_operations["tempfile_mkdtemp"].return_value = str(temp_dir)

		with (
			patch.object(
				instance.cls_dir_files_management, "recursive_unzip_in_memory"
			) as mock_unzip,
			patch("builtins.open", mock_open(read_data="output content")),
			patch("os.chmod"),
			patch("os.getcwd", return_value="/current"),
			patch("os.chdir"),
			patch.object(Path, "exists", return_value=True),
			patch.object(Path, "glob") as mock_glob,
			patch.object(Path, "stat") as mock_stat,
		):
			mock_unzip.return_value = [(BytesIO(b"exe content"), "test.ex_")]
			mock_glob.return_value = [Path("/tmp/test_dir/output.txt")]  # noqa S108: probable insecure usage of temporary file or directory
			mock_stat.return_value.st_size = 1000

			result_file, result_filename = instance.parse_raw_ex_file(
				mock_response, "test_prefix_", "test_file"
			)

			assert isinstance(result_file, StringIO)
			assert result_filename == "test.ex_"
			mock_fast_operations["subprocess_run"].assert_called_once()

	def test_parse_raw_ex_file_no_ex_file(self) -> None:
		"""Test parse_raw_ex_file when no .ex_ file is found.

		Verifies
		--------
		- ValueError is raised when no .ex_ file found
		- Error message is descriptive

		Returns
		-------
		None
		"""

		class ConcreteB3Class(ABCB3SearchByTradingSession):
			"""Concrete subclass for testing."""

			def transform_data(self, file: StringIO, file_name: str) -> pd.DataFrame:
				"""Transform file content into a DataFrame.

				Parameters
				----------
				file : StringIO
					The file content.
				file_name : str
					The file name

				Returns
				-------
				pd.DataFrame
					The transformed DataFrame.
				"""
				return pd.DataFrame({"test": [1, 2, 3]})

		instance = ConcreteB3Class(url="https://example.com/{}")
		mock_response = create_mock_response()

		with patch.object(
			instance.cls_dir_files_management, "recursive_unzip_in_memory"
		) as mock_unzip:
			mock_unzip.return_value = [(BytesIO(b"content"), "test.txt")]

			with pytest.raises(ValueError, match="No .ex_ file found in the downloaded ZIP"):
				instance.parse_raw_ex_file(mock_response, "prefix_", "filename")

	def test_parse_raw_ex_file_wine_execution_failure(
		self,
		mock_fast_operations: dict[str, MagicMock],
	) -> None:
		"""Test parse_raw_ex_file when Wine execution fails.

		Verifies
		--------
		- Wine execution failure is handled
		- RuntimeError is raised when no output files
		- Cleanup is performed even on failure

		Parameters
		----------
		mock_fast_operations : dict[str, MagicMock]
			Dictionary of mocked operations

		Returns
		-------
		None
		"""

		class ConcreteB3Class(ABCB3SearchByTradingSession):
			"""Concrete subclass for testing."""

			def transform_data(self, file: StringIO, file_name: str) -> pd.DataFrame:
				"""Transform file content into a DataFrame.

				Parameters
				----------
				file : StringIO
					The file content.
				file_name : str
					The file name

				Returns
				-------
				pd.DataFrame
					The transformed DataFrame.
				"""
				return pd.DataFrame({"test": [1, 2, 3]})

		instance = ConcreteB3Class(url="https://example.com/{}")
		mock_response = create_mock_response()

		temp_dir = Path("/tmp/test_dir")  # noqa S108: probable insecure usage of temporary file or directory
		mock_fast_operations["tempfile_mkdtemp"].return_value = str(temp_dir)
		mock_fast_operations["subprocess_run"].return_value = MagicMock(
			returncode=1,
			stdout="",
			stderr="Wine error",
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

			with pytest.raises(
				RuntimeError, match="No output file generated after Wine execution"
			):
				instance.parse_raw_ex_file(mock_response, "prefix_", "filename")

	def test_run_with_database_session(
		self,
		mock_db_session: MagicMock,
		mock_fast_operations: dict[str, MagicMock],
	) -> None:
		"""Test run method with database session provided.

		Verifies
		--------
		- Data is processed through full pipeline
		- Database insertion is called
		- No DataFrame is returned when db session provided

		Parameters
		----------
		mock_db_session : MagicMock
			Mock database session
		mock_fast_operations : dict[str, MagicMock]
			Dictionary of mocked operations

		Returns
		-------
		None
		"""

		class ConcreteB3Class(ABCB3SearchByTradingSession):
			"""Concrete subclass for testing."""

			def transform_data(self, file: StringIO, file_name: str) -> pd.DataFrame:
				"""Transform file content into a DataFrame.

				Parameters
				----------
				file : StringIO
					The file content.
				file_name : str
					The file name

				Returns
				-------
				pd.DataFrame
					The transformed DataFrame.
				"""
				return pd.DataFrame(
					{
						"test_col": [1, 2, 3],
						"FILE_NAME": [file_name, file_name, file_name],
					}
				)

		instance = ConcreteB3Class(cls_db=mock_db_session, url="https://example.com/{}")

		with (
			patch.object(instance, "get_response") as mock_get_response,
			patch.object(instance, "parse_raw_file") as mock_parse,
			patch.object(instance, "standardize_dataframe") as mock_standardize,
			patch.object(instance, "insert_table_db") as mock_insert,
		):
			mock_get_response.return_value = create_mock_response()
			mock_parse.return_value = (StringIO("test,data"), "test.csv")
			mock_standardize.return_value = pd.DataFrame({"test_col": [1, 2, 3]})

			result = instance.run(
				dict_dtypes={"test_col": int},
				str_table_name="test_table",
			)

			assert result is None
			mock_insert.assert_called_once()

	def test_run_without_database_session(
		self,
		mock_fast_operations: dict[str, MagicMock],
	) -> None:
		"""Test run method without database session.

		Verifies
		--------
		- Data is processed through pipeline
		- DataFrame is returned when no db session
		- Database insertion is not called

		Parameters
		----------
		mock_fast_operations : dict[str, MagicMock]
			Dictionary of mocked operations

		Returns
		-------
		None
		"""

		class ConcreteB3Class(ABCB3SearchByTradingSession):
			def transform_data(self, file: StringIO, file_name: str) -> pd.DataFrame:
				"""Transform file content into a DataFrame.

				Parameters
				----------
				file : StringIO
					The file content.
				file_name : str
					The file name

				Returns
				-------
				pd.DataFrame
					The transformed DataFrame.
				"""
				return pd.DataFrame(
					{
						"test_col": [1, 2, 3],
						"FILE_NAME": [file_name, file_name, file_name],
					}
				)

		instance = ConcreteB3Class(url="https://example.com/{}")

		with (
			patch.object(instance, "get_response") as mock_get_response,
			patch.object(instance, "parse_raw_file") as mock_parse,
			patch.object(instance, "standardize_dataframe") as mock_standardize,
		):
			mock_get_response.return_value = create_mock_response()
			mock_parse.return_value = (StringIO("test,data"), "test.csv")
			mock_standardize.return_value = pd.DataFrame({"test_col": [1, 2, 3]})

			result = instance.run(
				dict_dtypes={"test_col": int},
				str_table_name="test_table",
			)

			assert isinstance(result, pd.DataFrame)
			assert len(result) == 3

	def test_run_parameter_validation(self) -> None:
		"""Test run method parameter validation.

		Verifies
		--------
		- TypeError is raised for invalid timeout values
		- TypeError is raised for invalid boolean values
		- TypeError is raised for non-string table names

		Returns
		-------
		None
		"""

		class ConcreteB3Class(ABCB3SearchByTradingSession):
			"""Concrete subclass for testing."""

			def transform_data(self, file: StringIO, file_name: str) -> pd.DataFrame:
				"""Transform file content into a DataFrame.

				Parameters
				----------
				file : StringIO
					The file content.
				file_name : str
					The file name

				Returns
				-------
				pd.DataFrame
					The transformed DataFrame.
				"""
				return pd.DataFrame({"test": [1, 2, 3]})

		instance = ConcreteB3Class(url="https://example.com/{}")

		with pytest.raises(TypeError):
			instance.run(dict_dtypes={}, timeout="invalid")

		with pytest.raises(TypeError):
			instance.run(dict_dtypes={}, bool_verify="invalid")

		with pytest.raises(TypeError):
			instance.run(dict_dtypes={}, str_table_name=123)
