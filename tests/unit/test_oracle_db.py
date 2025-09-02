"""Unit tests for OracleDB class.

Tests the Oracle database operations handler, covering connection management,
query execution, data insertion, reading, backup functionality, and error handling.
Ensures robust type validation and edge cases are properly handled.
"""

import logging
from typing import Union
from unittest.mock import Mock

import oracledb
import pandas as pd
import pytest
from pytest_mock import MockerFixture

from stpstone.utils.connections.databases.sql.oracle_db import OracleDB
from stpstone.utils.loggs.create_logs import CreateLog


# --------------------------
# Fixtures
# --------------------------
@pytest.fixture
def mock_oracledb(mocker: MockerFixture) -> Mock:
    """Mock oracledb module for database operations.

    Parameters
    ----------
    mocker : MockerFixture
        Pytest-mock fixture for creating mocks

    Returns
    -------
    Mock
        Mocked oracledb module
    """
    mock = mocker.patch("stpstone.utils.connections.databases.sql.oracle_db.oracledb")
    mock_conn = Mock(spec=oracledb.Connection)
    mock_cursor = Mock(spec=oracledb.Cursor)
    mock_cursor.description = [("col1",), ("col2",)]
    mock_cursor.fetchall.return_value = [(1, "test"), (2, "data")]
    mock_conn.cursor.return_value = mock_cursor
    mock.connect.return_value = mock_conn
    
    # Mock the error hierarchy properly
    mock.Error = oracledb.Error
    mock.DatabaseError = oracledb.DatabaseError
    
    mocker.patch("stpstone.utils.loggs.create_logs.CreateLog.log_message")
    return mock

@pytest.fixture
def valid_db_params() -> dict[str, Union[str, int]]:
    """Fixture providing valid database parameters.

    Returns
    -------
    dict[str, Union[str, int]]
        Dictionary with valid database connection parameters
    """
    return {
        "dbname": "orcl",
        "user": "testuser",
        "password": "testpass",
        "host": "localhost",
        "port": 1521,
        "str_schema": "test_schema",
    }


@pytest.fixture
def sample_data() -> list[dict[str, Union[int, str]]]:
    """Fixture providing sample data for insertion.

    Returns
    -------
    list[dict[str, Union[int, str]]]
        List of dictionaries with sample data
    """
    return [
        {"id": 1, "name": "Alice"},
        {"id": 2, "name": "Bob"},
    ]


@pytest.fixture
def logger() -> logging.Logger:
    """Fixture providing a logger instance.

    Returns
    -------
    logging.Logger
        Configured logger instance
    """
    return logging.getLogger("test_oracle_db")


# --------------------------
# Tests
# --------------------------
def test_init_valid_params(
    mock_oracledb: Mock, valid_db_params: dict[str, Union[str, int]]
) -> None:
    """Test initialization with valid parameters.

    Verifies
    --------
    - Successful connection with valid parameters
    - Correct setting of instance attributes
    - Proper execution of initial queries

    Parameters
    ----------
    mock_oracledb : Mock
        Mocked oracledb module
    valid_db_params : dict[str, Union[str, int]]
        Valid database connection parameters

    Returns
    -------
    None
    """
    db = OracleDB(**valid_db_params)
    assert db.dbname == valid_db_params["dbname"]
    assert db.user == valid_db_params["user"]
    assert db.password == valid_db_params["password"]
    assert db.host == valid_db_params["host"]
    assert db.port == valid_db_params["port"]
    assert db.str_schema == valid_db_params["str_schema"]
    mock_oracledb.init_oracle_client.assert_called_once()
    mock_oracledb.connect.assert_called_once_with(
        user=valid_db_params["user"],
        password=valid_db_params["password"],
        dsn=f"{valid_db_params['host']}:{valid_db_params['port']}/{valid_db_params['dbname']}",
    )


def test_init_invalid_params(mock_oracledb: Mock) -> None:
    """Test initialization with invalid parameters.

    Verifies
    --------
    - Raises ValueError for empty or invalid parameters
    - Validates all required parameters

    Parameters
    ----------
    mock_oracledb : Mock
        Mocked oracledb module

    Returns
    -------
    None
    """
    invalid_params = [
        {"dbname": "", "user": "user", "password": "pass", "host": "localhost", "port": 1521},
        {"dbname": "orcl", "user": "", "password": "pass", "host": "localhost", "port": 1521},
        {"dbname": "orcl", "user": "user", "password": "", "host": "localhost", "port": 1521},
        {"dbname": "orcl", "user": "user", "password": "pass", "host": "", "port": 1521},
        {"dbname": "orcl", "user": "user", "password": "pass", "host": "localhost", "port": 0},
        {"dbname": "orcl", "user": "user", "password": "pass", "host": "localhost", "port": -1},
        {"dbname": "orcl", "user": "user", "password": "pass", "host": "localhost", "port": 1521, 
         "str_schema": ""},
    ]
    for params in invalid_params:
        with pytest.raises(ValueError):
            OracleDB(**params)


def test_init_connection_error(
    mock_oracledb: Mock, 
    valid_db_params: dict[str, Union[str, int]], 
    logger: logging.Logger
) -> None:
    """Test handling of connection errors during initialization.

    Verifies
    --------
    - Raises ConnectionError on connection failure
    - Logs appropriate error message

    Parameters
    ----------
    mock_oracledb : Mock
        Mocked oracledb module
    valid_db_params : dict[str, Union[str, int]]
        Valid database connection parameters
    logger : logging.Logger
        Logger instance

    Returns
    -------
    None
    """
    mock_oracledb.DatabaseError = oracledb.DatabaseError
    mock_oracledb.connect.side_effect = oracledb.DatabaseError("Connection failed")
    with pytest.raises(ConnectionError, match="Error connecting to Oracle database"):
        OracleDB(**valid_db_params, logger=logger)
    mock_log_message = CreateLog().log_message
    mock_log_message.assert_called_with(
        logger, "Error connecting to Oracle database: Connection failed", "error"
    )

def test_execute_valid_query(
    mock_oracledb: Mock, 
    valid_db_params: dict[str, Union[str, int]]
) -> None:
    """Test execution of valid SQL query.

    Verifies
    --------
    - Successful query execution
    - Cursor execute method called with correct query

    Parameters
    ----------
    mock_oracledb : Mock
        Mocked oracledb module
    valid_db_params : dict[str, Union[str, int]]
        Valid database connection parameters

    Returns
    -------
    None
    """
    db = OracleDB(**valid_db_params)
    query = "SELECT * FROM test_table"
    db.execute(query)
    db.cursor.execute.assert_called_with(query)


def test_execute_empty_query(
    mock_oracledb: Mock, 
    valid_db_params: dict[str, Union[str, int]]
) -> None:
    """Test execution with empty query.

    Verifies
    --------
    - Raises ValueError for empty query
    - Validates query parameter

    Parameters
    ----------
    mock_oracledb : Mock
        Mocked oracledb module
    valid_db_params : dict[str, Union[str, int]]
        Valid database connection parameters

    Returns
    -------
    None
    """
    db = OracleDB(**valid_db_params)
    with pytest.raises(ValueError, match="SQL query cannot be empty"):
        db.execute("")


def test_read_valid_query(
    mock_oracledb: Mock, 
    valid_db_params: dict[str, Union[str, int]]
) -> None:
    """Test reading data from valid query.

    Verifies
    --------
    - Returns correct DataFrame with query results
    - Proper column name handling
    - Correct data type conversion

    Parameters
    ----------
    mock_oracledb : Mock
        Mocked oracledb module
    valid_db_params : dict[str, Union[str, int]]
        Valid database connection parameters

    Returns
    -------
    None
    """
    db = OracleDB(**valid_db_params)
    query = "SELECT * FROM test_table"
    result = db.read(query)
    expected_df = pd.DataFrame([(1, "test"), (2, "data")], columns=["col1", "col2"])
    pd.testing.assert_frame_equal(result, expected_df)
    db.cursor.execute.assert_called_with(query)


def test_read_with_date_conversion(
    mock_oracledb: Mock, 
    valid_db_params: dict[str, Union[str, int]], 
    mocker: MockerFixture
) -> None:
    """Test reading with date column conversion.

    Verifies
    --------
    - Correct date format conversion
    - Proper handling of date columns
    - Returns correct DataFrame

    Parameters
    ----------
    mock_oracledb : Mock
        Mocked oracledb module
    valid_db_params : dict[str, Union[str, int]]
        Valid database connection parameters
    mocker : MockerFixture
        Pytest-mock fixture for creating mocks

    Returns
    -------
    None
    """
    mock_dates = mocker.patch(
        "stpstone.utils.calendars.calendar_abc.ABCCalendarOperations.str_date_to_datetime")
    mock_dates.return_value = pd.Timestamp("2023-01-01")
    db = OracleDB(**valid_db_params)
    query = "SELECT * FROM test_table"
    result = db.read(query, list_cols_dt=["col1"], str_fmt_dt="%Y-%m-%d")
    assert mock_dates.called
    assert len(result["col1"]) == 2
    assert all(result["col1"] == pd.Timestamp("2023-01-01"))


def test_read_invalid_date_params(
    mock_oracledb: Mock, 
    valid_db_params: dict[str, Union[str, int]]
) -> None:
    """Test reading with invalid date parameters.

    Verifies
    --------
    - Raises ValueError when date parameters are inconsistent
    - Validates list_cols_dt and str_fmt_dt pairing

    Parameters
    ----------
    mock_oracledb : Mock
        Mocked oracledb module
    valid_db_params : dict[str, Union[str, int]]
        Valid database connection parameters

    Returns
    -------
    None
    """
    db = OracleDB(**valid_db_params)
    query = "SELECT * FROM test_table"
    with pytest.raises(
        ValueError, 
        match="Both list_cols_dt and str_fmt_dt must be provided or None"
    ):
        db.read(query, list_cols_dt=["col1"], str_fmt_dt=None)
    with pytest.raises(
        ValueError, 
        match="Both list_cols_dt and str_fmt_dt must be provided or None"
    ):
        db.read(query, list_cols_dt=None, str_fmt_dt="%Y-%m-%d")


def test_insert_valid_data(
    mock_oracledb: Mock, 
    valid_db_params: dict[str, Union[str, int]], 
    sample_data: list[dict[str, Union[int, str]]]
) -> None:
    """Test insertion of valid data.

    Verifies
    --------
    - Successful data insertion
    - Proper commit call
    - Correct query formation

    Parameters
    ----------
    mock_oracledb : Mock
        Mocked oracledb module
    valid_db_params : dict[str, Union[str, int]]
        Valid database connection parameters
    sample_data : list[dict[str, Union[int, str]]]
        Sample data for insertion

    Returns
    -------
    None
    """
    db = OracleDB(**valid_db_params)
    db.insert(sample_data, "test_table")
    db.cursor.executemany.assert_called_once()
    db.conn.commit.assert_called_once()


def test_insert_empty_data(
    mock_oracledb: Mock, 
    valid_db_params: dict[str, Union[str, int]]
) -> None:
    """Test insertion with empty data.

    Verifies
    --------
    - Returns without error for empty data
    - No database operations performed

    Parameters
    ----------
    mock_oracledb : Mock
        Mocked oracledb module
    valid_db_params : dict[str, Union[str, int]]
        Valid database connection parameters

    Returns
    -------
    None
    """
    db = OracleDB(**valid_db_params)
    with pytest.raises(ValueError, match="Insert data cannot be empty"):
        db.insert([], "test_table")
    db.cursor.executemany.assert_not_called()


def test_insert_error(
    mock_oracledb: Mock, 
    valid_db_params: dict[str, Union[str, int]], 
    sample_data: list[dict[str, Union[int, str]]], 
    logger: logging.Logger
) -> None:
    """Test handling of insertion errors.

    Verifies
    --------
    - Raises Exception on insertion failure
    - Performs rollback
    - Logs error message

    Parameters
    ----------
    mock_oracledb : Mock
        Mocked oracledb module
    valid_db_params : dict[str, Union[str, int]]
        Valid database connection parameters
    sample_data : list[dict[str, Union[int, str]]]
        Sample data for insertion
    logger : logging.Logger
        Logger instance

    Returns
    -------
    None
    """
    db = OracleDB(**valid_db_params, logger=logger)
    mock_oracledb.DatabaseError = oracledb.DatabaseError
    db.cursor.executemany.side_effect = oracledb.DatabaseError("Insert failed")
    with pytest.raises(ValueError, match="Error while inserting data"):
        db.insert(sample_data, "test_table")
    db.conn.rollback.assert_called_once()
    db.conn.close.assert_called_once()
    mock_log_message = CreateLog().log_message
    mock_log_message.assert_called_with(
        logger, 
        f"Error while inserting data\nDB_CONFIG: {db.dict_db_config}\n"
        + f"TABLE_NAME: test_table\nJSON_DATA: {sample_data}\nERROR_MESSAGE: Insert failed",
        "error"
    )

def test_close(
    mock_oracledb: Mock, 
    valid_db_params: dict[str, Union[str, int]]
) -> None:
    """Test closing database connection.

    Verifies
    --------
    - Successful closing of cursor and connection
    - Proper method calls

    Parameters
    ----------
    mock_oracledb : Mock
        Mocked oracledb module
    valid_db_params : dict[str, Union[str, int]]
        Valid database connection parameters

    Returns
    -------
    None
    """
    db = OracleDB(**valid_db_params)
    db.close()
    db.cursor.close.assert_called_once()
    db.conn.close.assert_called_once()


def test_close_error(
    mock_oracledb: Mock, 
    valid_db_params: dict[str, Union[str, int]], 
    logger: logging.Logger
) -> None:
    """Test handling of errors during connection close.

    Verifies
    --------
    - Raises oracledb.Error on close failure
    - Logs error message

    Parameters
    ----------
    mock_oracledb : Mock
        Mocked oracledb module
    valid_db_params : dict[str, Union[str, int]]
        Valid database connection parameters
    logger : logging.Logger
        Logger instance

    Returns
    -------
    None
    """
    db = OracleDB(**valid_db_params, logger=logger)
    mock_oracledb.DatabaseError = oracledb.DatabaseError
    db.cursor.close.side_effect = oracledb.DatabaseError("Close failed")
    with pytest.raises(oracledb.DatabaseError, match="Close failed"):
        db.close()
    mock_log_message = CreateLog().log_message
    mock_log_message.assert_called_with(
        logger, "Error closing connection: Close failed", "error"
    )

def test_backup_valid(
    mock_oracledb: Mock, 
    valid_db_params: dict[str, Union[str, int]], 
    mocker: MockerFixture
) -> None:
    """Test successful database backup.

    Verifies
    --------
    - Successful backup creation
    - Correct file path handling
    - Proper subprocess call

    Parameters
    ----------
    mock_oracledb : Mock
        Mocked oracledb module
    valid_db_params : dict[str, Union[str, int]]
        Valid database connection parameters
    mocker : MockerFixture
        Pytest-mock fixture for creating mocks

    Returns
    -------
    None
    """
    mocker.patch("shutil.which", return_value="/usr/bin/expdp")
    mocker.patch("os.makedirs")
    mocker.patch("subprocess.run")
    db = OracleDB(**valid_db_params)
    result = db.backup("/backups")
    assert result.startswith("Backup successful! File saved at: /backups/orcl_backup.dmp")


def test_backup_tool_unavailable(
    mock_oracledb: Mock, 
    valid_db_params: dict[str, Union[str, int]], 
    mocker: MockerFixture
) -> None:
    """Test backup when expdp tool is unavailable.

    Verifies
    --------
    - Raises RuntimeError when expdp is not found
    - Logs appropriate error message

    Parameters
    ----------
    mock_oracledb : Mock
        Mocked oracledb module
    valid_db_params : dict[str, Union[str, int]]
        Valid database connection parameters
    mocker : MockerFixture
        Pytest-mock fixture for creating mocks

    Returns
    -------
    None
    """
    mocker.patch("shutil.which", return_value=None)
    mocker.patch("platform.system", return_value="linux")
    mocker.patch("os.path.exists", return_value=False)
    db = OracleDB(**valid_db_params)
    with pytest.raises(RuntimeError, match="Oracle Data Pump"):
        db.backup("/backups")


def test_check_bkp_tool_unavailable_darwin(
    mock_oracledb: Mock, 
    valid_db_params: dict[str, Union[str, int]], 
    mocker: MockerFixture
) -> None:
    """Test check_bkp_tool on macOS where expdp is unsupported.

    Verifies
    --------
    - Returns False on macOS
    - Logs appropriate error message

    Parameters
    ----------
    mock_oracledb : Mock
        Mocked oracledb module
    valid_db_params : dict[str, Union[str, int]]
        Valid database connection parameters
    mocker : MockerFixture
        Pytest-mock fixture for creating mocks

    Returns
    -------
    None
    """
    mocker.patch("shutil.which", return_value=None)
    mocker.patch("platform.system", return_value="darwin")
    mock_log_message = mocker.patch("stpstone.utils.loggs.create_logs.CreateLog.log_message")
    db = OracleDB(**valid_db_params)
    assert not db.check_bkp_tool()
    mock_log_message.assert_called_with(
        db.logger, 
        "Oracle Data Pump is not supported on macOS. Please install manually.", 
        "error"
    )

def test_context_manager(
    mock_oracledb: Mock, 
    valid_db_params: dict[str, Union[str, int]]
) -> None:
    """Test context manager protocol implementation.

    Verifies
    --------
    - Proper entry and exit handling
    - Connection closure on exit
    - Exception propagation

    Parameters
    ----------
    mock_oracledb : Mock
        Mocked oracledb module
    valid_db_params : dict[str, Union[str, int]]
        Valid database connection parameters

    Returns
    -------
    None
    """
    with OracleDB(**valid_db_params) as db:
        assert isinstance(db, OracleDB)
        db.execute("SELECT 1 FROM DUAL")
    db.cursor.close.assert_called_once()
    db.conn.close.assert_called_once()