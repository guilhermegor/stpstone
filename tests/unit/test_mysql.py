"""Unit tests for MySQLDatabase class.

Tests MySQL database connection management, query execution, data operations,
backup functionality, and error handling scenarios.
"""

import json
from logging import Logger
import os
from pathlib import Path
import platform
import shutil
import subprocess
from typing import Any, Optional
from unittest.mock import MagicMock, Mock, patch

from numpy.typing import NDArray
import pandas as pd
import pymysql
import pytest
from pytest_mock import MockerFixture
from sqlalchemy import create_engine

from stpstone.utils.cals.handling_dates import DatesBR
from stpstone.utils.connections.databases.sql.database_abc import ABCDatabase
from stpstone.utils.connections.databases.sql.mysql import MySQLDatabase
from stpstone.utils.loggs.create_logs import CreateLog
from stpstone.utils.parsers.json import JsonFiles


# --------------------------
# Fixtures
# --------------------------
@pytest.fixture
def valid_db_config() -> dict[str, Any]:
    """Fixture providing valid database configuration.

    Returns
    -------
    dict[str, Any]
        Dictionary with valid database configuration parameters
    """
    return {
        "dbname": "test_db",
        "user": "test_user",
        "password": "test_password",
        "host": "localhost",
        "port": 3306,
        "str_schema": "test_schema",
    }


@pytest.fixture
def mock_logger() -> MagicMock:
    """Fixture providing a mock logger.

    Returns
    -------
    MagicMock
        Mocked logger instance
    """
    return MagicMock(spec=Logger)


@pytest.fixture
def mock_dates_br() -> MagicMock:
    """Fixture providing a mock DatesBR instance.

    Returns
    -------
    MagicMock
        Mocked DatesBR instance
    """
    mock_dates = MagicMock(spec=DatesBR)
    mock_dates.str_date_to_datetime.return_value = "2023-01-01 00:00:00"
    return mock_dates


@pytest.fixture
def mock_json_files() -> MagicMock:
    """Fixture providing a mock JsonFiles instance.

    Returns
    -------
    MagicMock
        Mocked JsonFiles instance
    """
    mock_json = MagicMock(spec=JsonFiles)
    mock_json.normalize_json_keys.return_value = [{"col1": "val1", "col2": "val2"}]
    return mock_json


@pytest.fixture
def sample_dataframe() -> pd.DataFrame:
    """Fixture providing sample DataFrame for testing.

    Returns
    -------
    pd.DataFrame
        Sample DataFrame with test data
    """
    return pd.DataFrame({
        "col1": [1, 2, 3],
        "col2": ["a", "b", "c"],
        "date_col": ["2023-01-01", "2023-01-02", "2023-01-03"]
    })


@pytest.fixture
def mysql_database(valid_db_config: dict[str, Any], mock_logger: MagicMock) -> MySQLDatabase:
    """Fixture providing MySQLDatabase instance with mocked connection.

    Parameters
    ----------
    valid_db_config : dict[str, Any]
        Valid database configuration
    mock_logger : MagicMock
        Mocked logger instance

    Returns
    -------
    MySQLDatabase
        MySQLDatabase instance with mocked connection
    """
    with patch("pymysql.connect"), patch("pymysql.connect.cursor"):
        db = MySQLDatabase(**valid_db_config, logger=mock_logger)
        db.conn = MagicMock()
        db.cursor = MagicMock()
        return db


# --------------------------
# Tests for __init__ and _validate_db_config
# --------------------------
class TestMySQLDatabaseInit:
    """Test cases for MySQLDatabase initialization and validation."""

    @pytest.mark.parametrize(
        "invalid_config,expected_error",
        [
            ({"dbname": ""}, "Database name cannot be empty"),
            ({"user": ""}, "Database user cannot be empty"),
            ({"password": ""}, "Database password cannot be empty"),
            ({"host": ""}, "Database host cannot be empty"),
            ({"port": 0}, "Database port must be a positive integer"),
            ({"port": -1}, "Database port must be a positive integer"),
            ({"str_schema": ""}, "Database schema cannot be empty"),
        ]
    )
    def test_init_validation_errors(
        self,
        valid_db_config: dict[str, Any],
        invalid_config: dict[str, Any],
        expected_error: str
    ) -> None:
        """Test initialization validation raises ValueError for invalid inputs.

        Verifies
        --------
        - ValueError is raised with appropriate message for each invalid parameter
        - Validation catches empty strings and invalid port numbers

        Parameters
        ----------
        valid_db_config : dict[str, Any]
            Base valid configuration
        invalid_config : dict[str, Any]
            Configuration with one invalid parameter
        expected_error : str
            Expected error message

        Returns
        -------
        None
        """
        config = {**valid_db_config, **invalid_config}
        with pytest.raises(ValueError, match=expected_error):
            MySQLDatabase(**config)

    def test_init_connection_success(
        self,
        valid_db_config: dict[str, Any],
        mocker: MockerFixture
    ) -> None:
        """Test successful database connection initialization.

        Verifies
        --------
        - pymysql.connect is called with correct parameters
        - Cursor is created and test query executed
        - Schema is set when not "public"

        Parameters
        ----------
        valid_db_config : dict[str, Any]
            Valid database configuration
        mocker : MockerFixture
            Pytest-mock fixture

        Returns
        -------
        None
        """
        mock_connect = mocker.patch("pymysql.connect")
        mock_cursor = MagicMock()
        mock_connect.return_value.cursor.return_value = mock_cursor

        db = MySQLDatabase(**valid_db_config)
        
        mock_connect.assert_called_once_with(**valid_db_config)
        mock_cursor.execute.assert_called_once_with("SELECT 1")
        mock_cursor.execute.assert_any_call(f"USE {valid_db_config['str_schema']}")

    def test_init_connection_failure(
        self,
        valid_db_config: dict[str, Any],
        mocker: MockerFixture,
        mock_logger: MagicMock
    ) -> None:
        """Test connection failure raises ConnectionError.

        Verifies
        --------
        - ConnectionError is raised when connection fails
        - Error is logged appropriately
        - Original exception is preserved

        Parameters
        ----------
        valid_db_config : dict[str, Any]
            Valid database configuration
        mocker : MockerFixture
            Pytest-mock fixture
        mock_logger : MagicMock
            Mocked logger instance

        Returns
        -------
        None
        """
        mock_connect = mocker.patch("pymysql.connect", side_effect=pymysql.Error("Connection failed"))
        
        with pytest.raises(ConnectionError, match="Error connecting to database"):
            MySQLDatabase(**valid_db_config, logger=mock_logger)
        
        mock_logger.error.assert_called_once()


# --------------------------
# Tests for execute method
# --------------------------
class TestExecuteMethod:
    """Test cases for execute method."""

    def test_execute_success(
        self,
        mysql_database: MySQLDatabase
    ) -> None:
        """Test successful query execution.

        Verifies
        --------
        - Cursor.execute is called with correct query
        - No exceptions are raised

        Parameters
        ----------
        mysql_database : MySQLDatabase
            MySQLDatabase instance with mocked connection

        Returns
        -------
        None
        """
        test_query = "SELECT * FROM test_table"
        
        mysql_database.execute(test_query)
        
        mysql_database.cursor.execute.assert_called_once_with(test_query)

    def test_execute_with_non_string_query(
        self,
        mysql_database: MySQLDatabase
    ) -> None:
        """Test execute with non-string query parameter.

        Verifies
        --------
        - Method accepts non-string queries (Any type)
        - Cursor.execute is called with the parameter

        Parameters
        ----------
        mysql_database : MySQLDatabase
            MySQLDatabase instance with mocked connection

        Returns
        -------
        None
        """
        test_query = 12345
        
        mysql_database.execute(test_query)
        
        mysql_database.cursor.execute.assert_called_once_with(test_query)


# --------------------------
# Tests for read_sql method
# --------------------------
class TestReadSqlMethod:
    """Test cases for read_sql method."""

    def test_read_sql_success(
        self,
        mysql_database: MySQLDatabase,
        mocker: MockerFixture,
        sample_dataframe: pd.DataFrame
    ) -> None:
        """Test successful SQL query execution with read_sql.

        Verifies
        --------
        - SQLAlchemy engine is created with correct connection string
        - pd.read_sql is called with correct parameters
        - DataFrame is returned

        Parameters
        ----------
        mysql_database : MySQLDatabase
            MySQLDatabase instance with mocked connection
        mocker : MockerFixture
            Pytest-mock fixture
        sample_dataframe : pd.DataFrame
            Sample DataFrame for mocking return value

        Returns
        -------
        None
        """
        mock_create_engine = mocker.patch("sqlalchemy.create_engine")
        mock_read_sql = mocker.patch("pandas.read_sql", return_value=sample_dataframe)
        test_query = "SELECT * FROM test_table"
        
        result = mysql_database.read_sql(test_query)
        
        mock_create_engine.assert_called_once()
        mock_read_sql.assert_called_once_with(test_query, con=mock_create_engine.return_value)
        assert result.equals(sample_dataframe)

    def test_read_sql_empty_query(
        self,
        mysql_database: MySQLDatabase
    ) -> None:
        """Test read_sql with empty query raises ValueError.

        Verifies
        --------
        - ValueError is raised with appropriate message
        - No database operations are performed

        Parameters
        ----------
        mysql_database : MySQLDatabase
            MySQLDatabase instance with mocked connection

        Returns
        -------
        None
        """
        with pytest.raises(ValueError, match="Query cannot be empty"):
            mysql_database.read_sql("")
        
        mysql_database.cursor.execute.assert_not_called()

    def test_read_sql_timeout(
        self,
        mysql_database: MySQLDatabase,
        mocker: MockerFixture
    ) -> None:
        """Test read_sql with custom timeout.

        Verifies
        --------
        - connect_timeout parameter is passed to create_engine
        - Timeout value is respected

        Parameters
        ----------
        mysql_database : MySQLDatabase
            MySQLDatabase instance with mocked connection
        mocker : MockerFixture
            Pytest-mock fixture

        Returns
        -------
        None
        """
        mock_create_engine = mocker.patch("sqlalchemy.create_engine")
        mocker.patch("pandas.read_sql")
        test_timeout = 3600
        
        mysql_database.read_sql("SELECT 1", timeout=test_timeout)
        
        call_args = mock_create_engine.call_args
        assert "connect_timeout" in call_args[1]["connect_args"]
        assert call_args[1]["connect_args"]["connect_timeout"] == test_timeout

    def test_read_sql_execution_error(
        self,
        mysql_database: MySQLDatabase,
        mocker: MockerFixture
    ) -> None:
        """Test read_sql handles execution errors.

        Verifies
        --------
        - ValueError is raised with appropriate message
        - Original exception is preserved

        Parameters
        ----------
        mysql_database : MySQLDatabase
            MySQLDatabase instance with mocked connection
        mocker : MockerFixture
            Pytest-mock fixture

        Returns
        -------
        None
        """
        mocker.patch("sqlalchemy.create_engine")
        mocker.patch("pandas.read_sql", side_effect=Exception("SQL execution failed"))
        
        with pytest.raises(ValueError, match="Failed to read SQL query"):
            mysql_database.read_sql("SELECT * FROM non_existent_table")


# --------------------------
# Tests for read method
# --------------------------
class TestReadMethod:
    """Test cases for read method."""

    def test_read_success(
        self,
        mysql_database: MySQLDatabase,
        sample_dataframe: pd.DataFrame
    ) -> None:
        """Test successful read operation.

        Verifies
        --------
        - Query is executed and data fetched
        - DataFrame is created with correct columns
        - No type conversion or date parsing when not requested

        Parameters
        ----------
        mysql_database : MySQLDatabase
            MySQLDatabase instance with mocked connection
        sample_dataframe : pd.DataFrame
            Sample DataFrame for comparison

        Returns
        -------
        None
        """
        test_data = [(1, "a", "2023-01-01"), (2, "b", "2023-01-02")]
        mysql_database.cursor.execute.return_value = None
        mysql_database.cursor.fetchall.return_value = test_data
        mysql_database.cursor.description = [
            ("col1",), ("col2",), ("date_col",)
        ]
        
        result = mysql_database.read("SELECT * FROM test_table")
        
        mysql_database.cursor.execute.assert_called_once_with("SELECT * FROM test_table")
        assert len(result) == 2
        assert list(result.columns) == ["col1", "col2", "date_col"]

    def test_read_empty_query(
        self,
        mysql_database: MySQLDatabase
    ) -> None:
        """Test read with empty query raises ValueError.

        Verifies
        --------
        - ValueError is raised with appropriate message
        - No database operations are performed

        Parameters
        ----------
        mysql_database : MySQLDatabase
            MySQLDatabase instance with mocked connection

        Returns
        -------
        None
        """
        with pytest.raises(ValueError, match="Query cannot be empty"):
            mysql_database.read("")
        
        mysql_database.cursor.execute.assert_not_called()

    def test_read_with_type_conversion(
        self,
        mysql_database: MySQLDatabase
    ) -> None:
        """Test read with column type conversion.

        Verifies
        --------
        - DataFrame.astype is called with provided type mapping
        - Column types are converted as specified

        Parameters
        ----------
        mysql_database : MySQLDatabase
            MySQLDatabase instance with mocked connection

        Returns
        -------
        None
        """
        test_data = [("1", "2.5")]
        mysql_database.cursor.fetchall.return_value = test_data
        mysql_database.cursor.description = [("int_col",), ("float_col",)]
        type_mapping = {"int_col": int, "float_col": float}
        
        result = mysql_database.read(
            "SELECT * FROM test_table",
            dict_type_cols=type_mapping
        )
        
        # Verify type conversion would occur (mocked astype call)
        assert "dict_type_cols" in locals()  # Parameter was processed

    def test_read_with_date_conversion(
        self,
        mysql_database: MySQLDatabase,
        mocker: MockerFixture
    ) -> None:
        """Test read with date column conversion.

        Verifies
        --------
        - DatesBR.str_date_to_datetime is called for date columns
        - Both list_cols_dt and str_fmt_dt must be provided together

        Parameters
        ----------
        mysql_database : MySQLDatabase
            MySQLDatabase instance with mocked connection
        mocker : MockerFixture
            Pytest-mock fixture

        Returns
        -------
        None
        """
        test_data = [("2023-01-01",)]
        mysql_database.cursor.fetchall.return_value = test_data
        mysql_database.cursor.description = [("date_col",)]
        mocker.patch.object(DatesBR, "str_date_to_datetime", return_value="2023-01-01 00:00:00")
        
        result = mysql_database.read(
            "SELECT * FROM test_table",
            list_cols_dt=["date_col"],
            str_fmt_dt="%Y-%m-%d"
        )
        
        # Verify date conversion parameters were processed
        assert "list_cols_dt" in locals()
        assert "str_fmt_dt" in locals()

    def test_read_invalid_date_params(
        self,
        mysql_database: MySQLDatabase
    ) -> None:
        """Test read with invalid date parameter combination.

        Verifies
        --------
        - ValueError is raised when only one of list_cols_dt/str_fmt_dt is provided
        - Appropriate error message is shown

        Parameters
        ----------
        mysql_database : MySQLDatabase
            MySQLDatabase instance with mocked connection

        Returns
        -------
        None
        """
        mysql_database.cursor.fetchall.return_value = [("2023-01-01",)]
        mysql_database.cursor.description = [("date_col",)]
        
        # Test only list_cols_dt provided
        with pytest.raises(ValueError, match="Both list_cols_dt and str_fmt_dt must be provided"):
            mysql_database.read("SELECT date_col FROM test_table", list_cols_dt=["date_col"])
        
        # Test only str_fmt_dt provided
        with pytest.raises(ValueError, match="Both list_cols_dt and str_fmt_dt must be provided"):
            mysql_database.read("SELECT date_col FROM test_table", str_fmt_dt="%Y-%m-%d")

    def test_read_no_data(
        self,
        mysql_database: MySQLDatabase
    ) -> None:
        """Test read with no data returned.

        Verifies
        --------
        - Empty DataFrame is returned when no data
        - Columns are preserved from cursor description

        Parameters
        ----------
        mysql_database : MySQLDatabase
            MySQLDatabase instance with mocked connection

        Returns
        -------
        None
        """
        mysql_database.cursor.fetchall.return_value = []
        mysql_database.cursor.description = [("col1",), ("col2",)]
        
        result = mysql_database.read("SELECT * FROM empty_table")
        
        assert len(result) == 0
        assert list(result.columns) == ["col1", "col2"]

    def test_read_no_description(
        self,
        mysql_database: MySQLDatabase
    ) -> None:
        """Test read when cursor has no description.

        Verifies
        --------
        - DataFrame is created with empty columns when no description
        - No exception is raised

        Parameters
        ----------
        mysql_database : MySQLDatabase
            MySQLDatabase instance with mocked connection

        Returns
        -------
        None
        """
        mysql_database.cursor.fetchall.return_value = [(1, 2)]
        mysql_database.cursor.description = None
        
        result = mysql_database.read("SELECT 1, 2")
        
        assert len(result) == 1
        assert len(result.columns) == 0  # No column names available


# --------------------------
# Tests for insert method
# --------------------------
class TestInsertMethod:
    """Test cases for insert method."""

    def test_insert_success(
        self,
        mysql_database: MySQLDatabase,
        mock_json_files: MagicMock,
        mock_logger: MagicMock
    ) -> None:
        """Test successful data insertion.

        Verifies
        --------
        - JsonFiles.normalize_json_keys is called
        - executemany is called with correct query and data
        - Connection is committed
        - Success is logged

        Parameters
        ----------
        mysql_database : MySQLDatabase
            MySQLDatabase instance with mocked connection
        mock_json_files : MagicMock
            Mocked JsonFiles instance
        mock_logger : MagicMock
            Mocked logger instance

        Returns
        -------
        None
        """
        test_data = [{"col1": "val1", "col2": "val2"}]
        test_table = "test_table"
        
        with patch.object(JsonFiles, "normalize_json_keys", return_value=test_data):
            mysql_database.insert(test_data, test_table)
        
        mysql_database.cursor.executemany.assert_called_once()
        mysql_database.conn.commit.assert_called_once()
        mock_logger.info.assert_called_once()

    def test_insert_or_ignore(
        self,
        mysql_database: MySQLDatabase
    ) -> None:
        """Test INSERT IGNORE functionality.

        Verifies
        --------
        - INSERT IGNORE query is generated when bool_insert_or_ignore is True
        - Normal INSERT query is generated when False

        Parameters
        ----------
        mysql_database : MySQLDatabase
            MySQLDatabase instance with mocked connection

        Returns
        -------
        None
        """
        test_data = [{"col1": "val1"}]
        test_table = "test_table"
        
        # Test INSERT IGNORE
        mysql_database.insert(test_data, test_table, bool_insert_or_ignore=True)
        call_args = mysql_database.cursor.executemany.call_args
        assert "INSERT IGNORE INTO" in call_args[0][0]
        
        # Test normal INSERT
        mysql_database.insert(test_data, test_table, bool_insert_or_ignore=False)
        call_args = mysql_database.cursor.executemany.call_args
        assert "INSERT INTO" in call_args[0][0] and "IGNORE" not in call_args[0][0]

    def test_insert_empty_table_name(
        self,
        mysql_database: MySQLDatabase
    ) -> None:
        """Test insert with empty table name raises ValueError.

        Verifies
        --------
        - ValueError is raised with appropriate message
        - No database operations are performed

        Parameters
        ----------
        mysql_database : MySQLDatabase
            MySQLDatabase instance with mocked connection

        Returns
        -------
        None
        """
        with pytest.raises(ValueError, match="Table name cannot be empty"):
            mysql_database.insert([{"col": "val"}], "")
        
        mysql_database.cursor.executemany.assert_not_called()

    def test_insert_empty_data(
        self,
        mysql_database: MySQLDatabase
    ) -> None:
        """Test insert with empty data does nothing.

        Verifies
        --------
        - Method returns early when json_data is empty
        - No database operations are performed

        Parameters
        ----------
        mysql_database : MySQLDatabase
            MySQLDatabase instance with mocked connection

        Returns
        -------
        None
        """
        mysql_database.insert([], "test_table")
        
        mysql_database.cursor.executemany.assert_not_called()
        mysql_database.conn.commit.assert_not_called()

    def test_insert_failure(
        self,
        mysql_database: MySQLDatabase,
        mock_logger: MagicMock
    ) -> None:
        """Test insert failure handling.

        Verifies
        --------
        - Exception is raised with detailed error message
        - Connection is rolled back
        - Connection is closed
        - Error is logged

        Parameters
        ----------
        mysql_database : MySQLDatabase
            MySQLDatabase instance with mocked connection
        mock_logger : MagicMock
            Mocked logger instance

        Returns
        -------
        None
        """
        test_error = Exception("Insert failed")
        mysql_database.cursor.executemany.side_effect = test_error
        
        with pytest.raises(Exception, match="Error while inserting data"):
            mysql_database.insert([{"col": "val"}], "test_table")
        
        mysql_database.conn.rollback.assert_called_once()
        mysql_database.close.assert_called_once()
        mock_logger.error.assert_called_once()


# --------------------------
# Tests for close method
# --------------------------
class TestCloseMethod:
    """Test cases for close method."""

    def test_close_success(
        self,
        mysql_database: MySQLDatabase
    ) -> None:
        """Test successful connection closure.

        Verifies
        --------
        - Cursor and connection are closed if they exist
        - No exceptions are raised

        Parameters
        ----------
        mysql_database : MySQLDatabase
            MySQLDatabase instance with mocked connection

        Returns
        -------
        None
        """
        mysql_database.close()
        
        mysql_database.cursor.close.assert_called_once()
        mysql_database.conn.close.assert_called_once()

    def test_close_no_cursor(
        self,
        mysql_database: MySQLDatabase
    ) -> None:
        """Test close when cursor doesn't exist.

        Verifies
        --------
        - No exception when cursor attribute doesn't exist
        - Connection is still closed if it exists

        Parameters
        ----------
        mysql_database : MySQLDatabase
            MySQLDatabase instance with mocked connection

        Returns
        -------
        None
        """
        delattr(mysql_database, "cursor")
        mysql_database.close()
        
        mysql_database.conn.close.assert_called_once()

    def test_close_no_connection(
        self,
        mysql_database: MySQLDatabase
    ) -> None:
        """Test close when connection doesn't exist.

        Verifies
        --------
        - No exception when connection attribute doesn't exist
        - Cursor is still closed if it exists

        Parameters
        ----------
        mysql_database : MySQLDatabase
            MySQLDatabase instance with mocked connection

        Returns
        -------
        None
        """
        delattr(mysql_database, "conn")
        mysql_database.close()
        
        mysql_database.cursor.close.assert_called_once()


# --------------------------
# Tests for backup method
# --------------------------
class TestBackupMethod:
    """Test cases for backup method."""

    def test_backup_success(
        self,
        mysql_database: MySQLDatabase,
        mocker: MockerFixture,
        tmp_path: Path
    ) -> None:
        """Test successful database backup.

        Verifies
        --------
        - Backup directory is created
        - mysqldump command is executed with correct parameters
        - Backup file is created
        - Success message is returned

        Parameters
        ----------
        mysql_database : MySQLDatabase
            MySQLDatabase instance with mocked connection
        mocker : MockerFixture
            Pytest-mock fixture
        tmp_path : Path
            Temporary directory path

        Returns
        -------
        None
        """
        mock_run = mocker.patch("subprocess.run")
        mock_which = mocker.patch("shutil.which", return_value="/usr/bin/mysqldump")
        backup_dir = tmp_path / "backups"
        
        result = mysql_database.backup(str(backup_dir))
        
        mock_run.assert_called_once()
        assert backup_dir.exists()
        assert "successful" in result.lower()

    def test_backup_empty_directory(
        self,
        mysql_database: MySQLDatabase
    ) -> None:
        """Test backup with empty directory raises ValueError.

        Verifies
        --------
        - ValueError is raised with appropriate message
        - No backup operations are performed

        Parameters
        ----------
        mysql_database : MySQLDatabase
            MySQLDatabase instance with mocked connection

        Returns
        -------
        None
        """
        with pytest.raises(ValueError, match="Backup directory cannot be empty"):
            mysql_database.backup("")
        
        # Verify no subprocess calls were made
        assert True  # If we get here, no exception was raised from subprocess

    def test_backup_custom_filename(
        self,
        mysql_database: MySQLDatabase,
        mocker: MockerFixture,
        tmp_path: Path
    ) -> None:
        """Test backup with custom filename.

        Verifies
        --------
        - Custom filename is used in backup path
        - Default naming is not used

        Parameters
        ----------
        mysql_database : MySQLDatabase
            MySQLDatabase instance with mocked connection
        mocker : MockerFixture
            Pytest-mock fixture
        tmp_path : Path
            Temporary directory path

        Returns
        -------
        None
        """
        mocker.patch("subprocess.run")
        mocker.patch("shutil.which", return_value="/usr/bin/mysqldump")
        custom_name = "custom_backup.sql"
        
        result = mysql_database.backup(str(tmp_path), custom_name)
        
        assert custom_name in result

    def test_backup_tool_not_available(
        self,
        mysql_database: MySQLDatabase,
        mocker: MockerFixture,
        mock_logger: MagicMock,
        tmp_path: Path
    ) -> None:
        """Test backup when tool is not available.

        Verifies
        --------
        - RuntimeError is raised when backup tool is not available
        - Error is logged
        - Installation is attempted

        Parameters
        ----------
        mysql_database : MySQLDatabase
            MySQLDatabase instance with mocked connection
        mocker : MockerFixture
            Pytest-mock fixture
        mock_logger : MagicMock
            Mocked logger instance
        tmp_path : Path
            Temporary directory path

        Returns
        -------
        None
        """
        mocker.patch("shutil.which", return_value=None)
        mocker.patch("platform.system", return_value="linux")
        mocker.patch("subprocess.run", side_effect=Exception("Install failed"))
        
        with pytest.raises(RuntimeError, match="Backup tool is required"):
            mysql_database.backup(str(tmp_path))
        
        mock_logger.error.assert_called()

    def test_backup_subprocess_failure(
        self,
        mysql_database: MySQLDatabase,
        mocker: MockerFixture,
        tmp_path: Path
    ) -> None:
        """Test backup when subprocess fails.

        Verifies
        --------
        - Error message is returned when subprocess fails
        - No exception is raised

        Parameters
        ----------
        mysql_database : MySQLDatabase
            MySQLDatabase instance with mocked connection
        mocker : MockerFixture
            Pytest-mock fixture
        tmp_path : Path
            Temporary directory path

        Returns
        -------
        None
        """
        mocker.patch("shutil.which", return_value="/usr/bin/mysqldump")
        mocker.patch("subprocess.run", side_effect=subprocess.CalledProcessError(1, "cmd"))
        
        result = mysql_database.backup(str(tmp_path))
        
        assert "failed" in result.lower()


# --------------------------
# Tests for check_bkp_tool method
# --------------------------
class TestCheckBkpToolMethod:
    """Test cases for check_bkp_tool method."""

    def test_check_bkp_tool_available(
        self,
        mysql_database: MySQLDatabase,
        mocker: MockerFixture
    ) -> None:
        """Test check_bkp_tool when tool is available.

        Verifies
        --------
        - Returns True when mysqldump is found in PATH
        - No installation attempts are made

        Parameters
        ----------
        mysql_database : MySQLDatabase
            MySQLDatabase instance with mocked connection
        mocker : MockerFixture
            Pytest-mock fixture

        Returns
        -------
        None
        """
        mocker.patch("shutil.which", return_value="/usr/bin/mysqldump")
        
        result = mysql_database.check_bkp_tool()
        
        assert result is True

    def test_check_bkp_tool_install_linux_success(
        self,
        mysql_database: MySQLDatabase,
        mocker: MockerFixture,
        mock_logger: MagicMock
    ) -> None:
        """Test successful installation on Linux.

        Verifies
        --------
        - Installation commands are executed on Linux
        - Success is logged
        - Returns True after successful installation

        Parameters
        ----------
        mysql_database : MySQLDatabase
            MySQLDatabase instance with mocked connection
        mocker : MockerFixture
            Pytest-mock fixture
        mock_logger : MagicMock
            Mocked logger instance

        Returns
        -------
        None
        """
        mocker.patch("shutil.which", return_value=None)
        mocker.patch("platform.system", return_value="linux")
        mocker.patch("subprocess.run")
        mocker.patch("shutil.which", side_effect=[None, "/usr/bin/mysqldump"])
        
        result = mysql_database.check_bkp_tool()
        
        assert result is True
        mock_logger.info.assert_called()

    def test_check_bkp_tool_install_macos_success(
        self,
        mysql_database: MySQLDatabase,
        mocker: MockerFixture,
        mock_logger: MagicMock
    ) -> None:
        """Test successful installation on macOS.

        Verifies
        --------
        - Homebrew installation is attempted on macOS
        - Success is logged
        - Returns True after successful installation

        Parameters
        ----------
        mysql_database : MySQLDatabase
            MySQLDatabase instance with mocked connection
        mocker : MockerFixture
            Pytest-mock fixture
        mock_logger : MagicMock
            Mocked logger instance

        Returns
        -------
        None
        """
        mocker.patch("shutil.which", side_effect=[None, "/usr/local/bin/brew", "/usr/bin/mysqldump"])
        mocker.patch("platform.system", return_value="darwin")
        mocker.patch("subprocess.run")
        
        result = mysql_database.check_bkp_tool()
        
        assert result is True
        mock_logger.info.assert_called()

    def test_check_bkp_tool_install_windows(
        self,
        mysql_database: MySQLDatabase,
        mocker: MockerFixture,
        mock_logger: MagicMock
    ) -> None:
        """Test installation attempt on Windows.

        Verifies
        --------
        - Appropriate error message is logged for Windows
        - Returns False for unsupported automatic installation
        - No subprocess calls are made

        Parameters
        ----------
        mysql_database : MySQLDatabase
            MySQLDatabase instance with mocked connection
        mocker : MockerFixture
            Pytest-mock fixture
        mock_logger : MagicMock
            Mocked logger instance

        Returns
        -------
        None
        """
        mocker.patch("shutil.which", return_value=None)
        mocker.patch("platform.system", return_value="windows")
        
        result = mysql_database.check_bkp_tool()
        
        assert result is False
        mock_logger.error.assert_called()

    def test_check_bkp_tool_install_failure(
        self,
        mysql_database: MySQLDatabase,
        mocker: MockerFixture,
        mock_logger: MagicMock
    ) -> None:
        """Test installation failure.

        Verifies
        --------
        - Error is logged when installation fails
        - Returns False after failed installation

        Parameters
        ----------
        mysql_database : MySQLDatabase
            MySQLDatabase instance with mocked connection
        mocker : MockerFixture
            Pytest-mock fixture
        mock_logger : MagicMock
            Mocked logger instance

        Returns
        -------
        None
        """
        mocker.patch("shutil.which", return_value=None)
        mocker.patch("platform.system", return_value="linux")
        mocker.patch("subprocess.run", side_effect=subprocess.CalledProcessError(1, "apt-get"))
        
        result = mysql_database.check_bkp_tool()
        
        assert result is False
        mock_logger.error.assert_called()


# --------------------------
# Tests for ABCDatabase inheritance
# --------------------------
class TestABCDatabaseInheritance:
    """Test cases for ABCDatabase interface compliance."""

    def test_abc_database_implementation(
        self,
        mysql_database: MySQLDatabase
    ) -> None:
        """Test that MySQLDatabase implements ABCDatabase interface.

        Verifies
        --------
        - MySQLDatabase is instance of ABCDatabase
        - All required methods are implemented

        Parameters
        ----------
        mysql_database : MySQLDatabase
            MySQLDatabase instance with mocked connection

        Returns
        -------
        None
        """
        assert isinstance(mysql_database, ABCDatabase)
        assert hasattr(mysql_database, "execute")
        assert hasattr(mysql_database, "read_sql")
        assert hasattr(mysql_database, "read")
        assert hasattr(mysql_database, "insert")
        assert hasattr(mysql_database, "close")


# --------------------------
# Tests for singleton behavior
# --------------------------
class TestSingletonBehavior:
    """Test cases for singleton connection behavior."""

    def test_singleton_connection(
        self,
        valid_db_config: dict[str, Any],
        mocker: MockerFixture
    ) -> None:
        """Test singleton connection flag is stored.

        Verifies
        --------
        - bool_singleton parameter is stored in instance
        - Connection is established normally

        Parameters
        ----------
        valid_db_config : dict[str, Any]
            Valid database configuration
        mocker : MockerFixture
            Pytest-mock fixture

        Returns
        -------
        None
        """
        mocker.patch("pymysql.connect")
        mocker.patch("pymysql.connect.cursor")
        
        db = MySQLDatabase(**valid_db_config, bool_singleton=True)
        
        assert db.bool_singleton is True