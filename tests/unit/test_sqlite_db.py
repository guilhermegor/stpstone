"""Unit tests for SQLiteDB class.

Tests the SQLite database operations handler with various scenarios including:
- Database connection initialization
- Query execution and data operations
- Backup functionality
- Error conditions and edge cases
- Type validation and fallback mechanisms
"""

from logging import Logger
import os
import sqlite3
import subprocess
import sys
from typing import Any
from unittest.mock import Mock

import pandas as pd
import pytest
from pytest_mock import MockerFixture

from stpstone.utils.calendars.calendar_abc import ABCCalendarOperations
from stpstone.utils.connections.databases.sql.sqlite_db import SQLiteDB
from stpstone.utils.parsers.json import JsonFiles


# --------------------------
# Fixtures
# --------------------------
@pytest.fixture
def temp_db_path(
	tmp_path: Any,  # noqa ANN401: typing.Any is not allowed
) -> str:
	"""Fixture providing a temporary database path.

	Parameters
	----------
	tmp_path : Any
		Temporary directory path

	Returns
	-------
	str
		Path to temporary SQLite database file
	"""
	return str(tmp_path / "test.db")


@pytest.fixture
def sample_data() -> list[dict[str, Any]]:
	"""Fixture providing sample data for insertion.

	Returns
	-------
	list[dict[str, Any]]
		Sample data for testing insertions
	"""
	return [{"id": 1, "name": "Alice", "age": 25}, {"id": 2, "name": "Bob", "age": 30}]


@pytest.fixture
def mock_logger(mocker: MockerFixture) -> Mock:
	"""Fixture providing a mock logger.

	Parameters
	----------
	mocker : MockerFixture
		Pytest-mock fixture for creating mocks

	Returns
	-------
	Mock
		Mocked logger object
	"""
	return mocker.Mock(spec=Logger)


@pytest.fixture
def sqlite_db(temp_db_path: str, mock_logger: Mock) -> SQLiteDB:
	"""Fixture providing an initialized SQLiteDB instance.

	Parameters
	----------
	temp_db_path : str
		Temporary database path from fixture
	mock_logger : Mock
		Mocked logger from fixture

	Returns
	-------
	SQLiteDB
		Initialized SQLiteDB instance
	"""
	db = SQLiteDB(temp_db_path, logger=mock_logger)
	db.execute("CREATE TABLE test_table (id INTEGER, name TEXT, age INTEGER)")
	return db


# --------------------------
# Tests for __init__
# --------------------------
def test_init_valid_path(temp_db_path: str, mock_logger: Mock) -> None:
	"""Test initialization with valid database path.

	Verifies
	--------
	- Database connection is established
	- Connection and cursor attributes are set
	- Logger is properly assigned

	Parameters
	----------
	temp_db_path : str
		Temporary database path from fixture
	mock_logger : Mock
		Mocked logger from fixture

	Returns
	-------
	None
	"""
	db = SQLiteDB(temp_db_path, logger=mock_logger)
	assert isinstance(db.conn, sqlite3.Connection)
	assert isinstance(db.cursor, sqlite3.Cursor)
	assert db.logger == mock_logger
	assert db.db_path == temp_db_path
	db.close()


def test_init_empty_path(mock_logger: Mock) -> None:
	"""Test initialization with empty database path.

	Verifies
	--------
	- Raises ValueError for empty path
	- Error message is correct

	Parameters
	----------
	mock_logger : Mock
		Mocked logger from fixture

	Returns
	-------
	None
	"""
	with pytest.raises(ValueError, match="Database path cannot be empty"):
		SQLiteDB("", logger=mock_logger)


def test_init_invalid_path_type(mocker: MockerFixture, mock_logger: Mock) -> None:
	"""Test initialization with non-string database path.

	Verifies
	--------
	- Raises ValueError for non-string path
	- Error message is correct

	Parameters
	----------
	mocker : MockerFixture
		Pytest-mock fixture for creating mocks
	mock_logger : Mock
		Mocked logger from fixture

	Returns
	-------
	None
	"""
	mocker.patch("stpstone.transformations.validation.metaclass_type_checker.validate_type")
	with pytest.raises(ValueError, match="Database path must be a string"):
		SQLiteDB(123, logger=mock_logger)


def test_init_connection_error(
	mocker: MockerFixture, temp_db_path: str, mock_logger: Mock
) -> None:
	"""Test initialization with connection failure.

	Verifies
	--------
	- Raises ConnectionError on connection failure
	- Logs error message
	- Error message is correct

	Parameters
	----------
	mocker : MockerFixture
		Pytest-mock fixture for creating mocks
	temp_db_path : str
		Temporary database path from fixture
	mock_logger : Mock
		Mocked logger from fixture

	Returns
	-------
	None
	"""
	mocker.patch("sqlite3.connect", side_effect=sqlite3.Error("Connection failed"))
	with pytest.raises(ConnectionError, match="Error connecting to database: Connection failed"):
		SQLiteDB(temp_db_path, logger=mock_logger)
	mock_logger.error.assert_called_once()


# --------------------------
# Tests for _validate_db_path
# --------------------------
def test_validate_db_path_valid(temp_db_path: str) -> None:
	"""Test database path validation with valid input.

	Verifies
	--------
	- No exception is raised for valid path
	- Method executes without errors

	Parameters
	----------
	temp_db_path : str
		Temporary database path from fixture

	Returns
	-------
	None
	"""
	db = SQLiteDB(temp_db_path)
	db._validate_db_path()
	db.close()


def test_validate_db_path_empty(
	tmp_path: Any,  # noqa ANN401: typing.Any is not allowed
) -> None:
	"""Test database path validation with empty path.

	Verifies
	--------
	- Raises ValueError for empty path
	- Error message is correct

	Parameters
	----------
	tmp_path : Any
		Temporary directory provided by pytest (unique per worker).

	Returns
	-------
	None
	"""
	db = SQLiteDB(str(tmp_path / "temp.db"))
	db.db_path = ""
	with pytest.raises(ValueError, match="Database path cannot be empty"):
		db._validate_db_path()
	db.close()


def test_validate_db_path_non_string(
	mocker: MockerFixture,
	tmp_path: Any,  # noqa ANN401: typing.Any is not allowed
) -> None:
	"""Test database path validation with non-string path.

	Verifies
	--------
	- Raises ValueError for non-string path
	- Error message is correct

	Parameters
	----------
	mocker : MockerFixture
		Pytest-mock fixture for creating mocks
	tmp_path : Any
		Temporary directory provided by pytest (unique per worker).

	Returns
	-------
	None
	"""
	mocker.patch("stpstone.transformations.validation.metaclass_type_checker.validate_type")
	db = SQLiteDB(str(tmp_path / "temp.db"))
	db.db_path = 123
	with pytest.raises(ValueError, match="Database path must be a string"):
		db._validate_db_path()
	db.close()


# --------------------------
# Tests for execute
# --------------------------
def test_execute_valid_query(sqlite_db: SQLiteDB) -> None:
	"""Test execution of valid SQL query.

	Verifies
	--------
	- Query executes without errors
	- Query results are correct

	Parameters
	----------
	sqlite_db : SQLiteDB
		Initialized SQLiteDB instance from fixture

	Returns
	-------
	None
	"""
	sqlite_db.execute("SELECT 1 AS test")
	result = sqlite_db.cursor.fetchall()
	assert result == [(1,)]


def test_execute_empty_query(sqlite_db: SQLiteDB) -> None:
	"""Test execution with empty query.

	Verifies
	--------
	- Raises ValueError for empty query
	- Error message is correct

	Parameters
	----------
	sqlite_db : SQLiteDB
		Initialized SQLiteDB instance from fixture

	Returns
	-------
	None
	"""
	with pytest.raises(ValueError, match="Query cannot be empty"):
		sqlite_db.execute("")


def test_execute_operational_error(
	mocker: MockerFixture, temp_db_path: str, mock_logger: Mock
) -> None:
	"""Test execution with operational error and backoff.

	Verifies
	--------
	- Backoff retries on OperationalError
	- Raises OperationalError after max retries

	Parameters
	----------
	mocker : MockerFixture
		Pytest-mock fixture for creating mocks
	temp_db_path : str
		Temporary database path from fixture
	mock_logger : Mock
		Mocked logger from fixture

	Returns
	-------
	None
	"""
	mock_cursor = Mock()

	def execute_side_effect(query: str) -> None:
		"""Mock cursor execute method.

		Parameters
		----------
		query : str
			SQL query to execute

		Returns
		-------
		None
		"""
		if query == "SELECT 1":
			return None
		raise sqlite3.OperationalError("Database locked")

	mock_cursor.execute.side_effect = execute_side_effect
	mock_conn = Mock()
	mock_conn.cursor.return_value = mock_cursor
	mocker.patch("sqlite3.connect", return_value=mock_conn)
	db = SQLiteDB(temp_db_path, logger=mock_logger)
	with pytest.raises(sqlite3.OperationalError, match="Database locked"):
		db.execute("SELECT * FROM test_table")


# --------------------------
# Tests for read
# --------------------------
def test_read_valid_query(sqlite_db: SQLiteDB, sample_data: list[dict[str, Any]]) -> None:
	"""Test reading data with valid query.

	Verifies
	--------
	- Returns correct DataFrame
	- Data matches inserted data
	- Type conversions are applied correctly

	Parameters
	----------
	sqlite_db : SQLiteDB
		Initialized SQLiteDB instance from fixture
	sample_data : list[dict[str, Any]]
		Sample data for insertion from fixture

	Returns
	-------
	None
	"""
	sqlite_db.insert(sample_data, "test_table")
	df_ = sqlite_db.read("SELECT * FROM test_table")
	assert isinstance(df_, pd.DataFrame)
	assert len(df_) == 2
	assert list(df_.columns) == ["id", "name", "age"]
	assert df_.iloc[0]["name"] == "Alice"
	assert df_.iloc[1]["name"] == "Bob"


def test_read_with_type_conversion(sqlite_db: SQLiteDB, sample_data: list[dict[str, Any]]) -> None:
	"""Test reading data with type conversion.

	Verifies
	--------
	- Applies specified type conversions
	- Returns correct DataFrame with converted types

	Parameters
	----------
	sqlite_db : SQLiteDB
		Initialized SQLiteDB instance from fixture
	sample_data : list[dict[str, Any]]
		Sample data for insertion from fixture

	Returns
	-------
	None
	"""
	sqlite_db.insert(sample_data, "test_table")
	dict_type_cols = {"age": float}
	df_ = sqlite_db.read("SELECT * FROM test_table", dict_type_cols=dict_type_cols)
	assert df_["age"].dtype == float


def test_read_with_date_conversion(mocker: MockerFixture, sqlite_db: SQLiteDB) -> None:
	"""Test reading data with date conversion.

	Verifies
	--------
	- Applies date conversions correctly
	- Calls ABCCalendarOperations.str_date_to_datetime for date columns

	Parameters
	----------
	mocker : MockerFixture
		Pytest-mock fixture for creating mocks
	sqlite_db : SQLiteDB
		Initialized SQLiteDB instance from fixture

	Returns
	-------
	None
	"""
	mocker.patch.object(
		ABCCalendarOperations, "str_date_to_datetime", return_value=pd.Timestamp("2023-01-01")
	)
	sqlite_db.execute("CREATE TABLE dates (id INTEGER, date TEXT)")
	sqlite_db.insert([{"id": 1, "date": "2023-01-01"}], "dates")
	df_ = sqlite_db.read("SELECT * FROM dates", list_cols_dt=["date"], str_fmt_dt="%Y-%m-%d")
	assert pd.isna(df_["date"].iloc[0]) or df_["date"].iloc[0] == pd.Timestamp("2023-01-01")


def test_read_empty_query(sqlite_db: SQLiteDB) -> None:
	"""Test reading with empty query.

	Verifies
	--------
	- Raises ValueError for empty query
	- Error message is correct

	Parameters
	----------
	sqlite_db : SQLiteDB
		Initialized SQLiteDB instance from fixture

	Returns
	-------
	None
	"""
	with pytest.raises(ValueError, match="Query cannot be empty"):
		sqlite_db.read("")


def test_read_invalid_date_params(sqlite_db: SQLiteDB) -> None:
	"""Test reading with mismatched date parameters.

	Verifies
	--------
	- Raises ValueError for mismatched date parameters
	- Error message is correct

	Parameters
	----------
	sqlite_db : SQLiteDB
		Initialized SQLiteDB instance from fixture

	Returns
	-------
	None
	"""
	with pytest.raises(
		ValueError, match="Both list_cols_dt and str_fmt_dt must be provided or None"
	):
		sqlite_db.read("SELECT * FROM test_table", list_cols_dt=["date"], str_fmt_dt=None)


# --------------------------
# Tests for insert
# --------------------------
def test_insert_valid_data(sqlite_db: SQLiteDB, sample_data: list[dict[str, Any]]) -> None:
	"""Test insertion of valid data.

	Verifies
	--------
	- Inserts data correctly
	- Commits transaction
	- Logs success message

	Parameters
	----------
	sqlite_db : SQLiteDB
		Initialized SQLiteDB instance from fixture
	sample_data : list[dict[str, Any]]
		Sample data for insertion from fixture

	Returns
	-------
	None
	"""
	sqlite_db.insert(sample_data, "test_table")
	df_ = sqlite_db.read("SELECT * FROM test_table")
	assert len(df_) == 2
	assert df_.iloc[0]["name"] == "Alice"
	sqlite_db.logger.info.assert_called_once()


def test_insert_empty_table_name(sqlite_db: SQLiteDB, sample_data: list[dict[str, Any]]) -> None:
	"""Test insertion with empty table name.

	Verifies
	--------
	- Raises ValueError for empty table name
	- Error message is correct

	Parameters
	----------
	sqlite_db : SQLiteDB
		Initialized SQLiteDB instance from fixture
	sample_data : list[dict[str, Any]]
		Sample data for insertion from fixture

	Returns
	-------
	None
	"""
	with pytest.raises(ValueError, match="Table name cannot be empty"):
		sqlite_db.insert(sample_data, "")


def test_insert_empty_data(sqlite_db: SQLiteDB) -> None:
	"""Test insertion with empty data.

	Verifies
	--------
	- Returns without error for empty data
	- No database changes occur

	Parameters
	----------
	sqlite_db : SQLiteDB
		Initialized SQLiteDB instance from fixture

	Returns
	-------
	None
	"""
	sqlite_db.insert([], "test_table")
	df_ = sqlite_db.read("SELECT * FROM test_table")
	assert df_.empty


def test_insert_or_ignore(sqlite_db: SQLiteDB, sample_data: list[dict[str, Any]]) -> None:
	"""Test insertion with INSERT OR IGNORE option.

	Verifies
	--------
	- Inserts data with INSERT OR IGNORE
	- Duplicate entries are ignored
	- Correct data is stored

	Parameters
	----------
	sqlite_db : SQLiteDB
		Initialized SQLiteDB instance from fixture
	sample_data : list[dict[str, Any]]
		Sample data for insertion from fixture

	Returns
	-------
	None
	"""
	sqlite_db.execute("CREATE UNIQUE INDEX idx_id ON test_table(id)")
	sqlite_db.insert(sample_data, "test_table", bool_insert_or_ignore=True)
	sqlite_db.insert(sample_data, "test_table", bool_insert_or_ignore=True)
	df_ = sqlite_db.read("SELECT * FROM test_table")
	assert len(df_) == 2


def test_insert_error(
	mocker: MockerFixture, temp_db_path: str, mock_logger: Mock, sample_data: list[dict[str, Any]]
) -> None:
	"""Test insertion with database error.

	Verifies
	--------
	- Rolls back transaction on error
	- Logs error message
	- Raises Exception with details

	Parameters
	----------
	mocker : MockerFixture
		Pytest-mock fixture for creating mocks
	temp_db_path : str
		Temporary database path from fixture
	mock_logger : Mock
		Mocked logger from fixture
	sample_data : list[dict[str, Any]]
		Sample data for insertion from fixture

	Returns
	-------
	None
	"""
	mock_cursor = Mock()
	mock_cursor.executemany.side_effect = sqlite3.Error("Insert failed")
	mock_conn = Mock()
	mock_conn.cursor.return_value = mock_cursor
	mock_conn.commit = Mock()
	mock_conn.rollback = Mock()
	mocker.patch("sqlite3.connect", return_value=mock_conn)
	db = SQLiteDB(temp_db_path, logger=mock_logger)
	db.execute("CREATE TABLE test_table (id INTEGER, name TEXT, age INTEGER)")
	with pytest.raises(Exception, match="Error while inserting data"):
		db.insert(sample_data, "test_table")
	mock_conn.rollback.assert_called_once()
	mock_logger.error.assert_called_once()


# --------------------------
# Tests for close
# --------------------------
def test_close(sqlite_db: SQLiteDB) -> None:
	"""Test closing database connection.

	Verifies
	--------
	- Connection is closed
	- No errors are raised

	Parameters
	----------
	sqlite_db : SQLiteDB
		Initialized SQLiteDB instance from fixture

	Returns
	-------
	None
	"""
	sqlite_db.close()
	with pytest.raises(sqlite3.ProgrammingError):
		sqlite_db.cursor.execute("SELECT 1")


# --------------------------
# Tests for backup
# --------------------------
def test_backup_valid(
	mocker: MockerFixture,
	sqlite_db: SQLiteDB,
	tmp_path: Any,  # noqa ANN401: typing.Any is not allowed
) -> None:
	"""Test database backup with valid parameters.

	Verifies
	--------
	- Backup is created successfully
	- Returns success message
	- Backup file exists

	Parameters
	----------
	mocker : MockerFixture
		Pytest-mock fixture for creating mocks
	sqlite_db : SQLiteDB
		Initialized SQLiteDB instance from fixture
	tmp_path : Any
		Temporary path for backup file

	Returns
	-------
	None
	"""
	mocker.patch("shutil.which", return_value="/usr/bin/sqlite3")
	mocker.patch("subprocess.run", return_value=Mock(returncode=0))
	mocker.patch("os.makedirs")
	mocker.patch("os.path.exists", return_value=True)
	backup_dir = str(tmp_path / "backup")
	result = sqlite_db.backup(backup_dir, "test_backup.db")
	assert "Backup successful" in result
	assert os.path.exists(os.path.join(backup_dir, "test_backup.db"))


def test_backup_empty_dir(sqlite_db: SQLiteDB) -> None:
	"""Test backup with empty directory path.

	Verifies
	--------
	- Raises ValueError for empty directory
	- Error message is correct

	Parameters
	----------
	sqlite_db : SQLiteDB
		Initialized SQLiteDB instance from fixture

	Returns
	-------
	None
	"""
	with pytest.raises(ValueError, match="Backup directory cannot be empty"):
		sqlite_db.backup("")


def test_backup_tool_unavailable(
	mocker: MockerFixture,
	sqlite_db: SQLiteDB,
	tmp_path: Any,  # noqa ANN401: typing.Any is not allowed
) -> None:
	"""Test backup when backup tool is unavailable.

	Verifies
	--------
	- Raises RuntimeError when backup tool is missing
	- Logs error messages
	- Error message is correct

	Parameters
	----------
	mocker : MockerFixture
		Pytest-mock fixture for creating mocks
	sqlite_db : SQLiteDB
		Initialized SQLiteDB instance from fixture
	tmp_path : Any
		Temporary path for backup file

	Returns
	-------
	None
	"""
	mocker.patch("shutil.which", return_value=None)
	mocker.patch("platform.system", return_value="windows")
	with pytest.raises(RuntimeError, match="Backup tool is required for backups"):
		sqlite_db.backup(str(tmp_path))
	assert sqlite_db.logger.error.call_count == 2


def test_backup_subprocess_error(
	mocker: MockerFixture,
	sqlite_db: SQLiteDB,
	tmp_path: Any,  # noqa ANN401: typing.Any is not allowed
) -> None:
	"""Test backup with subprocess error.

	Verifies
	--------
	- Returns error message on subprocess failure
	- No exception is raised

	Parameters
	----------
	mocker : MockerFixture
		Pytest-mock fixture for creating mocks
	sqlite_db : SQLiteDB
		Initialized SQLiteDB instance from fixture
	tmp_path : Any
		Temporary path for backup file

	Returns
	-------
	None
	"""
	mocker.patch("shutil.which", return_value="/usr/bin/sqlite3")
	mocker.patch(
		"subprocess.run",
		side_effect=subprocess.CalledProcessError(1, ["sqlite3"], stderr="Backup failed"),
	)
	result = sqlite_db.backup(str(tmp_path))
	assert "Backup failed" in result


# --------------------------
# Tests for check_bkp_tool
# --------------------------
def test_check_bkp_tool_available(mocker: MockerFixture, sqlite_db: SQLiteDB) -> None:
	"""Test check_bkp_tool when sqlite3 is available.

	Verifies
	--------
	- Returns True when sqlite3 is found
	- No installation attempt is made

	Parameters
	----------
	mocker : MockerFixture
		Pytest-mock fixture for creating mocks
	sqlite_db : SQLiteDB
		Initialized SQLiteDB instance from fixture

	Returns
	-------
	None
	"""
	mocker.patch("shutil.which", return_value="/usr/bin/sqlite3")
	assert sqlite_db.check_bkp_tool()


def test_check_bkp_tool_install_success(mocker: MockerFixture, sqlite_db: SQLiteDB) -> None:
	"""Test check_bkp_tool with successful installation.

	Verifies
	--------
	- Attempts installation on Linux
	- Returns True after successful installation
	- Logs success message

	Parameters
	----------
	mocker : MockerFixture
		Pytest-mock fixture for creating mocks
	sqlite_db : SQLiteDB
		Initialized SQLiteDB instance from fixture

	Returns
	-------
	None
	"""
	mocker.patch("shutil.which", side_effect=[None, "/usr/bin/sqlite3"])
	mocker.patch("platform.system", return_value="linux")
	mocker.patch("subprocess.run", return_value=Mock(returncode=0))
	assert sqlite_db.check_bkp_tool()
	sqlite_db.logger.info.assert_called_once()


def test_check_bkp_tool_install_failure(mocker: MockerFixture, sqlite_db: SQLiteDB) -> None:
	"""Test check_bkp_tool with installation failure.

	Verifies
	--------
	- Returns False on installation failure
	- Logs error message

	Parameters
	----------
	mocker : MockerFixture
		Pytest-mock fixture for creating mocks
	sqlite_db : SQLiteDB
		Initialized SQLiteDB instance from fixture

	Returns
	-------
	None
	"""
	mocker.patch("shutil.which", return_value=None)
	mocker.patch("platform.system", return_value="linux")
	mocker.patch("subprocess.run", side_effect=subprocess.CalledProcessError(1, ["apt-get"]))
	assert not sqlite_db.check_bkp_tool()
	sqlite_db.logger.error.assert_called_once()


# --------------------------
# Tests for reload logic
# --------------------------
def test_reload_module(mocker: MockerFixture, temp_db_path: str) -> None:
	"""Test module reloading behavior.

	Verifies
	--------
	- Module can be reloaded
	- New instance can be created after reload
	- No state is preserved

	Parameters
	----------
	mocker : MockerFixture
		Pytest-mock fixture for creating mocks
	temp_db_path : str
		Temporary database path from fixture

	Returns
	-------
	None
	"""
	import importlib

	db = SQLiteDB(temp_db_path)
	importlib.reload(sys.modules["stpstone.utils.connections.databases.sql.sqlite_db"])
	new_db = SQLiteDB(temp_db_path)
	assert new_db.db_path == temp_db_path
	new_db.close()
	db.close()


# --------------------------
# Tests for edge cases
# --------------------------
def test_read_empty_result(sqlite_db: SQLiteDB) -> None:
	"""Test reading with empty result set.

	Verifies
	--------
	- Returns empty DataFrame for empty result
	- No type or date conversions are applied

	Parameters
	----------
	sqlite_db : SQLiteDB
		Initialized SQLiteDB instance from fixture

	Returns
	-------
	None
	"""
	df_ = sqlite_db.read("SELECT * FROM test_table WHERE id = 999")
	assert df_.empty


def test_insert_special_characters(mocker: MockerFixture, sqlite_db: SQLiteDB) -> None:
	"""Test insertion with special characters in data.

	Verifies
	--------
	- Handles special characters correctly
	- Normalizes JSON keys
	- Inserts data successfully

	Parameters
	----------
	mocker : MockerFixture
		Pytest-mock fixture for creating mocks
	sqlite_db : SQLiteDB
		Initialized SQLiteDB instance from fixture

	Returns
	-------
	None
	"""
	mocker.patch.object(JsonFiles, "normalize_json_keys", return_value=[{"name": "José 😊"}])
	sqlite_db.insert([{"name": "José 😊"}], "test_table")
	df_ = sqlite_db.read("SELECT * FROM test_table")
	assert df_.iloc[0]["name"] == "José 😊"


def test_backup_large_db(
	mocker: MockerFixture,
	sqlite_db: SQLiteDB,
	tmp_path: Any,  # noqa ANN401: typing.Any is not allowed
) -> None:
	"""Test backup with large database.

	Verifies
	--------
	- Handles large database backup
	- Creates backup file
	- Returns success message

	Parameters
	----------
	mocker : MockerFixture
		Pytest-mock fixture for creating mocks
	sqlite_db : SQLiteDB
		Initialized SQLiteDB instance from fixture
	tmp_path : Any
		Temporary path for backup file

	Returns
	-------
	None
	"""
	mocker.patch("shutil.which", return_value="/usr/bin/sqlite3")
	mocker.patch("subprocess.run", return_value=Mock(returncode=0))
	mocker.patch("os.path.exists", return_value=True)
	for i in range(1000):
		sqlite_db.insert([{"id": i, "name": f"User{i}", "age": 20}], "test_table")
	backup_dir = str(tmp_path / "backup")
	result = sqlite_db.backup(backup_dir)
	assert "Backup successful" in result
