"""Unit tests for SqlServerDB class.

Tests the SQL Server database operations including:
- Connection initialization and validation
- Query execution and data reading
- Data insertion and error handling
- Edge cases and error conditions
"""

from logging import Logger
from typing import Any, Optional
from unittest.mock import MagicMock, Mock, patch

import numpy as np
from numpy.typing import NDArray
import pandas as pd
from pyodbc import Connection, ProgrammingError, connect
import pytest

from stpstone.transformations.validation.metaclass_type_checker import SQLComposable
from stpstone.utils.cals.handling_dates import DatesBR
from stpstone.utils.connections.databases.sql.database_abc import ABCDatabase
from stpstone.utils.connections.databases.sql.sqlserver import SqlServerDB
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
        Dictionary with valid SQL Server configuration parameters
    """
    return {
        "driver_sql": "{ODBC Driver 17 for SQL Server}",
        "server": "test-server",
        "port": 1433,
        "database": "test_db",
        "user_id": "test_user",
        "password": "test_password",
        "timeout": 7200,
        "logger": None,
        "bool_singleton": False,
    }


@pytest.fixture
def mock_connection() -> Mock:
    """Fixture providing mocked database connection.

    Returns
    -------
    Mock
        Mocked pyodbc Connection object
    """
    mock_conn = Mock(spec=Connection)
    mock_cursor = Mock()
    mock_conn.cursor.return_value = mock_cursor
    mock_cursor.execute.return_value = None
    return mock_conn


@pytest.fixture
def mock_dates_br() -> Mock:
    """Fixture providing mocked DatesBR class.

    Returns
    -------
    Mock
        Mocked DatesBR instance
    """
    mock_dates = Mock(spec=DatesBR)
    mock_dates.str_date_to_datetime.return_value = "2023-01-01"
    return mock_dates


@pytest.fixture
def mock_json_files() -> Mock:
    """Fixture providing mocked JsonFiles class.

    Returns
    -------
    Mock
        Mocked JsonFiles instance
    """
    mock_json = Mock(spec=JsonFiles)
    mock_json.normalize_json_keys.return_value = [{"col1": "val1", "col2": "val2"}]
    return mock_json


@pytest.fixture
def sample_query_result() -> pd.DataFrame:
    """Fixture providing sample query result.

    Returns
    -------
    pd.DataFrame
        DataFrame with sample data for testing
    """
    return pd.DataFrame({
        "id": [1, 2, 3],
        "name": ["Alice", "Bob", "Charlie"],
        "date_col": ["2023-01-01", "2023-01-02", "2023-01-03"]
    })


# --------------------------
# Tests for __init__
# --------------------------
class TestSqlServerDBInit:
    """Test cases for SqlServerDB initialization."""

    def test_init_with_valid_config(self, valid_db_config: dict[str, Any]) -> None:
        """Test initialization with valid configuration.

        Verifies
        --------
        - Instance is created successfully with valid config
        - All attributes are set correctly
        - Connection and cursor are created

        Parameters
        ----------
        valid_db_config : dict[str, Any]
            Valid database configuration from fixture

        Returns
        -------
        None
        """
        with patch("stpstone.utils.connections.databases.sql.sql_server.connect") as mock_connect:
            mock_conn = Mock()
            mock_cursor = Mock()
            mock_connect.return_value = mock_conn
            mock_conn.cursor.return_value = mock_cursor
            mock_cursor.execute.return_value = None

            db = SqlServerDB(**valid_db_config)

            assert db.driver_sql == valid_db_config["driver_sql"]
            assert db.server == valid_db_config["server"]
            assert db.port == valid_db_config["port"]
            assert db.database == valid_db_config["database"]
            assert db.user_id == valid_db_config["user_id"]
            assert db.password == valid_db_config["password"]
            assert db.timeout == valid_db_config["timeout"]
            assert db.logger is None
            assert db.bool_singleton is False
            mock_connect.assert_called_once()
            mock_cursor.execute.assert_called_once_with("SELECT 1")

    def test_init_with_invalid_driver(self, valid_db_config: dict[str, Any]) -> None:
        """Test initialization with invalid driver raises ValueError.

        Verifies
        --------
        - ValueError is raised when driver is not supported
        - Error message contains expected text

        Parameters
        ----------
        valid_db_config : dict[str, Any]
            Valid database configuration from fixture

        Returns
        -------
        None
        """
        valid_db_config["driver_sql"] = "invalid_driver"
        with pytest.raises(ValueError) as excinfo:
            SqlServerDB(**valid_db_config)
        assert "Driver must be one of" in str(excinfo.value)

    def test_init_with_empty_server(self, valid_db_config: dict[str, Any]) -> None:
        """Test initialization with empty server raises ValueError.

        Verifies
        --------
        - ValueError is raised when server is empty
        - Error message contains expected text

        Parameters
        ----------
        valid_db_config : dict[str, Any]
            Valid database configuration from fixture

        Returns
        -------
        None
        """
        valid_db_config["server"] = ""
        with pytest.raises(ValueError) as excinfo:
            SqlServerDB(**valid_db_config)
        assert "Server cannot be empty" in str(excinfo.value)

    def test_init_with_invalid_port(self, valid_db_config: dict[str, Any]) -> None:
        """Test initialization with invalid port raises ValueError.

        Verifies
        --------
        - ValueError is raised when port is not positive
        - Error message contains expected text

        Parameters
        ----------
        valid_db_config : dict[str, Any]
            Valid database configuration from fixture

        Returns
        -------
        None
        """
        valid_db_config["port"] = -1
        with pytest.raises(ValueError) as excinfo:
            SqlServerDB(**valid_db_config)
        assert "Port must be a positive integer" in str(excinfo.value)

    def test_init_with_empty_database(self, valid_db_config: dict[str, Any]) -> None:
        """Test initialization with empty database raises ValueError.

        Verifies
        --------
        - ValueError is raised when database is empty
        - Error message contains expected text

        Parameters
        ----------
        valid_db_config : dict[str, Any]
            Valid database configuration from fixture

        Returns
        -------
        None
        """
        valid_db_config["database"] = ""
        with pytest.raises(ValueError) as excinfo:
            SqlServerDB(**valid_db_config)
        assert "Database cannot be empty" in str(excinfo.value)

    def test_init_with_empty_user_id(self, valid_db_config: dict[str, Any]) -> None:
        """Test initialization with empty user_id raises ValueError.

        Verifies
        --------
        - ValueError is raised when user_id is empty
        - Error message contains expected text

        Parameters
        ----------
        valid_db_config : dict[str, Any]
            Valid database configuration from fixture

        Returns
        -------
        None
        """
        valid_db_config["user_id"] = ""
        with pytest.raises(ValueError) as excinfo:
            SqlServerDB(**valid_db_config)
        assert "User ID cannot be empty" in str(excinfo.value)

    def test_init_with_empty_password(self, valid_db_config: dict[str, Any]) -> None:
        """Test initialization with empty password raises ValueError.

        Verifies
        --------
        - ValueError is raised when password is empty
        - Error message contains expected text

        Parameters
        ----------
        valid_db_config : dict[str, Any]
            Valid database configuration from fixture

        Returns
        -------
        None
        """
        valid_db_config["password"] = ""
        with pytest.raises(ValueError) as excinfo:
            SqlServerDB(**valid_db_config)
        assert "Password cannot be empty" in str(excinfo.value)

    def test_init_with_invalid_timeout(self, valid_db_config: dict[str, Any]) -> None:
        """Test initialization with invalid timeout raises ValueError.

        Verifies
        --------
        - ValueError is raised when timeout is not positive
        - Error message contains expected text

        Parameters
        ----------
        valid_db_config : dict[str, Any]
            Valid database configuration from fixture

        Returns
        -------
        None
        """
        valid_db_config["timeout"] = 0
        with pytest.raises(ValueError) as excinfo:
            SqlServerDB(**valid_db_config)
        assert "Timeout must be a positive integer" in str(excinfo.value)

    def test_init_connection_failure(self, valid_db_config: dict[str, Any]) -> None:
        """Test initialization when connection fails raises ConnectionError.

        Verifies
        --------
        - ConnectionError is raised when connection fails
        - Error message contains expected text

        Parameters
        ----------
        valid_db_config : dict[str, Any]
            Valid database configuration from fixture

        Returns
        -------
        None
        """
        with patch("stpstone.utils.connections.databases.sql.sql_server.connect") as mock_connect:
            mock_connect.side_effect = Exception("Connection failed")
            with pytest.raises(ConnectionError) as excinfo:
                SqlServerDB(**valid_db_config)
            assert "Error connecting to database" in str(excinfo.value)


# --------------------------
# Tests for _create_connection
# --------------------------
class TestCreateConnection:
    """Test cases for _create_connection method."""

    def test_create_connection_sql_server_driver(self, valid_db_config: dict[str, Any]) -> None:
        """Test connection creation with SQL Server driver.

        Verifies
        --------
        - Correct connection string is built for SQL Server driver
        - connect is called with expected parameters

        Parameters
        ----------
        valid_db_config : dict[str, Any]
            Valid database configuration from fixture

        Returns
        -------
        None
        """
        valid_db_config["driver_sql"] = "{SQL Server}"
        db = SqlServerDB(**valid_db_config)
        
        with patch("stpstone.utils.connections.databases.sql.sql_server.connect") as mock_connect:
            mock_connect.return_value = Mock()
            db._create_connection()
            
            expected_conn_str = (
                f"Driver={{SQL Server}};"
                f"Server={valid_db_config['server']};"
                f"Database={valid_db_config['database']};"
                f"Uid={valid_db_config['user_id']};"
                f"Pwd={valid_db_config['password']}"
            )
            mock_connect.assert_called_once_with(expected_conn_str, autocommit=True, timeout=7200)

    def test_create_connection_odbc_driver(self, valid_db_config: dict[str, Any]) -> None:
        """Test connection creation with ODBC driver.

        Verifies
        --------
        - Correct connection string is built for ODBC driver
        - connect is called with expected parameters

        Parameters
        ----------
        valid_db_config : dict[str, Any]
            Valid database configuration from fixture

        Returns
        -------
        None
        """
        db = SqlServerDB(**valid_db_config)
        
        with patch("stpstone.utils.connections.databases.sql.sql_server.connect") as mock_connect:
            mock_connect.return_value = Mock()
            db._create_connection()
            
            expected_conn_str = (
                f"Driver={{ODBC Driver 17 for SQL Server}};"
                f"Server={valid_db_config['server']};"
                f"PORT={valid_db_config['port']};"
                f"Database={valid_db_config['database']};"
                f"Trusted_Connection=no;"
                f"Encrypt=yes;"
                f"TrustServerCertificate=no;"
                f"UID={valid_db_config['user_id']};"
                f"PWD={valid_db_config['password']}"
            )
            mock_connect.assert_called_once_with(expected_conn_str, autocommit=True, timeout=7200)

    def test_create_connection_unknown_driver(self, valid_db_config: dict[str, Any]) -> None:
        """Test connection creation with unknown driver raises ValueError.

        Verifies
        --------
        - ValueError is raised for unknown driver
        - Error message contains expected text

        Parameters
        ----------
        valid_db_config : dict[str, Any]
            Valid database configuration from fixture

        Returns
        -------
        None
        """
        db = SqlServerDB(**valid_db_config)
        db.driver_sql = "unknown_driver"
        
        with pytest.raises(ValueError) as excinfo:
            db._create_connection()
        assert "Driver SQL not identified" in str(excinfo.value)


# --------------------------
# Tests for execute
# --------------------------
class TestExecute:
    """Test cases for execute method."""

    def test_execute_valid_query(self, valid_db_config: dict[str, Any], mock_connection: Mock) -> None:
        """Test execute with valid query.

        Verifies
        --------
        - Query is executed successfully
        - Cursor execute is called with correct query

        Parameters
        ----------
        valid_db_config : dict[str, Any]
            Valid database configuration from fixture
        mock_connection : Mock
            Mocked database connection

        Returns
        -------
        None
        """
        with patch("stpstone.utils.connections.databases.sql.sql_server.connect", return_value=mock_connection):
            db = SqlServerDB(**valid_db_config)
            test_query = "SELECT * FROM test_table"
            
            db.execute(test_query)
            db.cursor.execute.assert_called_once_with(test_query)

    def test_execute_empty_query(self, valid_db_config: dict[str, Any], mock_connection: Mock) -> None:
        """Test execute with empty query raises ValueError.

        Verifies
        --------
        - ValueError is raised when query is empty
        - Error message contains expected text

        Parameters
        ----------
        valid_db_config : dict[str, Any]
            Valid database configuration from fixture
        mock_connection : Mock
            Mocked database connection

        Returns
        -------
        None
        """
        with patch("stpstone.utils.connections.databases.sql.sql_server.connect", return_value=mock_connection):
            db = SqlServerDB(**valid_db_config)
            
            with pytest.raises(ValueError) as excinfo:
                db.execute("")
            assert "Query cannot be empty" in str(excinfo.value)

    def test_execute_sql_composable(self, valid_db_config: dict[str, Any], mock_connection: Mock) -> None:
        """Test execute with SQLComposable object.

        Verifies
        --------
        - SQLComposable object is accepted as query
        - Cursor execute is called with the object

        Parameters
        ----------
        valid_db_config : dict[str, Any]
            Valid database configuration from fixture
        mock_connection : Mock
            Mocked database connection

        Returns
        -------
        None
        """
        with patch("stpstone.utils.connections.databases.sql.sql_server.connect", return_value=mock_connection):
            db = SqlServerDB(**valid_db_config)
            mock_sql_composable = Mock(spec=SQLComposable)
            
            db.execute(mock_sql_composable)
            db.cursor.execute.assert_called_once_with(mock_sql_composable)


# --------------------------
# Tests for read
# --------------------------
class TestRead:
    """Test cases for read method."""

    def test_read_valid_query(self, valid_db_config: dict[str, Any], mock_connection: Mock, sample_query_result: pd.DataFrame) -> None:
        """Test read with valid query.

        Verifies
        --------
        - Query is executed and DataFrame is returned
        - pd.read_sql is called with correct parameters

        Parameters
        ----------
        valid_db_config : dict[str, Any]
            Valid database configuration from fixture
        mock_connection : Mock
            Mocked database connection
        sample_query_result : pd.DataFrame
            Sample query result from fixture

        Returns
        -------
        None
        """
        with patch("stpstone.utils.connections.databases.sql.sql_server.connect", return_value=mock_connection):
            with patch("pandas.read_sql", return_value=sample_query_result):
                db = SqlServerDB(**valid_db_config)
                test_query = "SELECT * FROM test_table"
                
                result = db.read(test_query)
                pd.read_sql.assert_called_once_with(test_query, con=mock_connection)
                assert isinstance(result, pd.DataFrame)
                assert len(result) == 3

    def test_read_with_type_conversion(self, valid_db_config: dict[str, Any], mock_connection: Mock, sample_query_result: pd.DataFrame) -> None:
        """Test read with type conversion.

        Verifies
        --------
        - DataFrame astype is called when dict_type_cols provided
        - Column types are converted correctly

        Parameters
        ----------
        valid_db_config : dict[str, Any]
            Valid database configuration from fixture
        mock_connection : Mock
            Mocked database connection
        sample_query_result : pd.DataFrame
            Sample query result from fixture

        Returns
        -------
        None
        """
        with patch("stpstone.utils.connections.databases.sql.sql_server.connect", return_value=mock_connection):
            with patch("pandas.read_sql", return_value=sample_query_result):
                db = SqlServerDB(**valid_db_config)
                type_dict = {"id": "str", "name": "category"}
                
                result = db.read("SELECT * FROM test_table", dict_type_cols=type_dict)
                pd.DataFrame.astype.assert_called_once_with(type_dict)

    def test_read_with_date_conversion(self, valid_db_config: dict[str, Any], mock_connection: Mock, sample_query_result: pd.DataFrame, mock_dates_br: Mock) -> None:
        """Test read with date conversion.

        Verifies
        --------
        - Date columns are converted using DatesBR
        - Both list_cols_dt and str_fmt_dt are required

        Parameters
        ----------
        valid_db_config : dict[str, Any]
            Valid database configuration from fixture
        mock_connection : Mock
            Mocked database connection
        sample_query_result : pd.DataFrame
            Sample query result from fixture
        mock_dates_br : Mock
            Mocked DatesBR instance

        Returns
        -------
        None
        """
        with patch("stpstone.utils.connections.databases.sql.sql_server.connect", return_value=mock_connection):
            with patch("pandas.read_sql", return_value=sample_query_result):
                with patch("stpstone.utils.connections.databases.sql.sql_server.DatesBR", return_value=mock_dates_br):
                    db = SqlServerDB(**valid_db_config)
                    date_cols = ["date_col"]
                    date_format = "%Y-%m-%d"
                    
                    result = db.read("SELECT * FROM test_table", list_cols_dt=date_cols, str_fmt_dt=date_format)
                    assert mock_dates_br.str_date_to_datetime.call_count == 3

    def test_read_empty_query(self, valid_db_config: dict[str, Any], mock_connection: Mock) -> None:
        """Test read with empty query raises ValueError.

        Verifies
        --------
        - ValueError is raised when query is empty
        - Error message contains expected text

        Parameters
        ----------
        valid_db_config : dict[str, Any]
            Valid database configuration from fixture
        mock_connection : Mock
            Mocked database connection

        Returns
        -------
        None
        """
        with patch("stpstone.utils.connections.databases.sql.sql_server.connect", return_value=mock_connection):
            db = SqlServerDB(**valid_db_config)
            
            with pytest.raises(ValueError) as excinfo:
                db.read("")
            assert "Query cannot be empty" in str(excinfo.value)

    def test_read_incomplete_date_params(self, valid_db_config: dict[str, Any], mock_connection: Mock) -> None:
        """Test read with incomplete date parameters raises ValueError.

        Verifies
        --------
        - ValueError is raised when only one of list_cols_dt/str_fmt_dt provided
        - Error message contains expected text

        Parameters
        ----------
        valid_db_config : dict[str, Any]
            Valid database configuration from fixture
        mock_connection : Mock
            Mocked database connection

        Returns
        -------
        None
        """
        with patch("stpstone.utils.connections.databases.sql.sql_server.connect", return_value=mock_connection):
            db = SqlServerDB(**valid_db_config)
            
            # Test with only list_cols_dt
            with pytest.raises(ValueError) as excinfo:
                db.read("SELECT * FROM test_table", list_cols_dt=["date_col"])
            assert "Both list_cols_dt and str_fmt_dt must be provided or None" in str(excinfo.value)
            
            # Test with only str_fmt_dt
            with pytest.raises(ValueError) as excinfo:
                db.read("SELECT * FROM test_table", str_fmt_dt="%Y-%m-%d")
            assert "Both list_cols_dt and str_fmt_dt must be provided or None" in str(excinfo.value)


# --------------------------
# Tests for insert
# --------------------------
class TestInsert:
    """Test cases for insert method."""

    def test_insert_valid_data(self, valid_db_config: dict[str, Any], mock_connection: Mock, mock_json_files: Mock) -> None:
        """Test insert with valid data.

        Verifies
        --------
        - Data is inserted successfully
        - to_sql is called with correct parameters
        - Connection is committed

        Parameters
        ----------
        valid_db_config : dict[str, Any]
            Valid database configuration from fixture
        mock_connection : Mock
            Mocked database connection
        mock_json_files : Mock
            Mocked JsonFiles instance

        Returns
        -------
        None
        """
        with patch("stpstone.utils.connections.databases.sql.sql_server.connect", return_value=mock_connection):
            with patch("stpstone.utils.connections.databases.sql.sql_server.JsonFiles", return_value=mock_json_files):
                with patch("pandas.DataFrame.to_sql") as mock_to_sql:
                    db = SqlServerDB(**valid_db_config)
                    test_data = [{"col1": "val1", "col2": "val2"}]
                    table_name = "test_table"
                    
                    db.insert(test_data, table_name)
                    mock_json_files.normalize_json_keys.assert_called_once_with(test_data)
                    mock_to_sql.assert_called_once_with(table_name, mock_connection, if_exists="append", index=False)
                    mock_connection.commit.assert_called_once()

    def test_insert_empty_data(self, valid_db_config: dict[str, Any], mock_connection: Mock) -> None:
        """Test insert with empty data does nothing.

        Verifies
        --------
        - Method returns early when json_data is empty
        - No database operations are performed

        Parameters
        ----------
        valid_db_config : dict[str, Any]
            Valid database configuration from fixture
        mock_connection : Mock
            Mocked database connection

        Returns
        -------
        None
        """
        with patch("stpstone.utils.connections.databases.sql.sql_server.connect", return_value=mock_connection):
            db = SqlServerDB(**valid_db_config)
            
            # Should not raise any exception
            db.insert([], "test_table")
            mock_connection.cursor().execute.assert_not_called()

    def test_insert_empty_table_name(self, valid_db_config: dict[str, Any], mock_connection: Mock) -> None:
        """Test insert with empty table name raises ValueError.

        Verifies
        --------
        - ValueError is raised when table name is empty
        - Error message contains expected text

        Parameters
        ----------
        valid_db_config : dict[str, Any]
            Valid database configuration from fixture
        mock_connection : Mock
            Mocked database connection

        Returns
        -------
        None
        """
        with patch("stpstone.utils.connections.databases.sql.sql_server.connect", return_value=mock_connection):
            db = SqlServerDB(**valid_db_config)
            test_data = [{"col1": "val1"}]
            
            with pytest.raises(ValueError) as excinfo:
                db.insert(test_data, "")
            assert "Table name cannot be empty" in str(excinfo.value)

    def test_insert_with_insert_or_ignore(self, valid_db_config: dict[str, Any], mock_connection: Mock, mock_json_files: Mock) -> None:
        """Test insert with insert_or_ignore flag.

        Verifies
        --------
        - Existing data check is performed when bool_insert_or_ignore is True
        - Only non-existing data is inserted

        Parameters
        ----------
        valid_db_config : dict[str, Any]
            Valid database configuration from fixture
        mock_connection : Mock
            Mocked database connection
        mock_json_files : Mock
            Mocked JsonFiles instance

        Returns
        -------
        None
        """
        with patch("stpstone.utils.connections.databases.sql.sql_server.connect", return_value=mock_connection):
            with patch("stpstone.utils.connections.databases.sql.sql_server.JsonFiles", return_value=mock_json_files):
                with patch("pandas.read_sql") as mock_read_sql:
                    with patch("pandas.DataFrame.to_sql") as mock_to_sql:
                        # Mock existing data
                        existing_df = pd.DataFrame({"col1": ["existing_val"]})
                        mock_read_sql.return_value = existing_df
                        
                        db = SqlServerDB(**valid_db_config)
                        test_data = [{"col1": "new_val"}, {"col1": "existing_val"}]
                        
                        db.insert(test_data, "test_table", bool_insert_or_ignore=True)
                        
                        # Should only insert the non-existing row
                        inserted_df = mock_to_sql.call_args[0][0]
                        assert len(inserted_df) == 1
                        assert inserted_df.iloc[0]["col1"] == "new_val"

    def test_insert_failure_rollback(self, valid_db_config: dict[str, Any], mock_connection: Mock, mock_json_files: Mock) -> None:
        """Test insert failure triggers rollback.

        Verifies
        --------
        - Rollback is called on insertion failure
        - Connection is closed
        - Exception is re-raised

        Parameters
        ----------
        valid_db_config : dict[str, Any]
            Valid database configuration from fixture
        mock_connection : Mock
            Mocked database connection
        mock_json_files : Mock
            Mocked JsonFiles instance

        Returns
        -------
        None
        """
        with patch("stpstone.utils.connections.databases.sql.sql_server.connect", return_value=mock_connection):
            with patch("stpstone.utils.connections.databases.sql.sql_server.JsonFiles", return_value=mock_json_files):
                with patch("pandas.DataFrame.to_sql", side_effect=Exception("Insert failed")):
                    db = SqlServerDB(**valid_db_config)
                    test_data = [{"col1": "val1"}]
                    
                    with pytest.raises(Exception) as excinfo:
                        db.insert(test_data, "test_table")
                    assert "Insert failed" in str(excinfo.value)
                    mock_connection.rollback.assert_called_once()
                    mock_connection.close.assert_called_once()


# --------------------------
# Tests for close
# --------------------------
class TestClose:
    """Test cases for close method."""

    def test_close_connection(self, valid_db_config: dict[str, Any], mock_connection: Mock) -> None:
        """Test close method.

        Verifies
        --------
        - Connection close method is called
        - No exceptions are raised

        Parameters
        ----------
        valid_db_config : dict[str, Any]
            Valid database configuration from fixture
        mock_connection : Mock
            Mocked database connection

        Returns
        -------
        None
        """
        with patch("stpstone.utils.connections.databases.sql.sql_server.connect", return_value=mock_connection):
            db = SqlServerDB(**valid_db_config)
            db.close()
            mock_connection.close.assert_called_once()


# --------------------------
# Tests for backup and check_bkp_tool
# --------------------------
class TestBackupMethods:
    """Test cases for backup-related methods."""

    def test_backup_not_implemented(self, valid_db_config: dict[str, Any], mock_connection: Mock) -> None:
        """Test backup method raises NotImplementedError.

        Verifies
        --------
        - NotImplementedError is raised when backup is called
        - Error message contains expected text

        Parameters
        ----------
        valid_db_config : dict[str, Any]
            Valid database configuration from fixture
        mock_connection : Mock
            Mocked database connection

        Returns
        -------
        None
        """
        with patch("stpstone.utils.connections.databases.sql.sql_server.connect", return_value=mock_connection):
            db = SqlServerDB(**valid_db_config)
            
            with pytest.raises(NotImplementedError) as excinfo:
                db.backup("/backup/path")
            assert "Backup functionality not implemented for SQL Server" in str(excinfo.value)

    def test_check_bkp_tool_returns_false(self, valid_db_config: dict[str, Any], mock_connection: Mock) -> None:
        """Test check_bkp_tool returns False.

        Verifies
        --------
        - check_bkp_tool always returns False for SQL Server
        - No exceptions are raised

        Parameters
        ----------
        valid_db_config : dict[str, Any]
            Valid database configuration from fixture
        mock_connection : Mock
            Mocked database connection

        Returns
        -------
        None
        """
        with patch("stpstone.utils.connections.databases.sql.sql_server.connect", return_value=mock_connection):
            db = SqlServerDB(**valid_db_config)
            result = db.check_bkp_tool()
            assert result is False


# --------------------------
# Tests for ABCDatabase compliance
# --------------------------
class TestABCDatabaseCompliance:
    """Test cases for ABCDatabase interface compliance."""

    def test_implements_abc_methods(self, valid_db_config: dict[str, Any], mock_connection: Mock) -> None:
        """Test class implements all ABCDatabase methods.

        Verifies
        --------
        - All abstract methods from ABCDatabase are implemented
        - No AttributeError is raised when accessing methods

        Parameters
        ----------
        valid_db_config : dict[str, Any]
            Valid database configuration from fixture
        mock_connection : Mock
            Mocked database connection

        Returns
        -------
        None
        """
        with patch("stpstone.utils.connections.databases.sql.sql_server.connect", return_value=mock_connection):
            db = SqlServerDB(**valid_db_config)
            
            # Check that all abstract methods are implemented
            assert hasattr(db, "execute")
            assert hasattr(db, "read")
            assert hasattr(db, "insert")
            assert hasattr(db, "close")
            assert hasattr(db, "backup")
            assert hasattr(db, "check_bkp_tool")
            
            # Check they are callable
            assert callable(db.execute)
            assert callable(db.read)
            assert callable(db.insert)
            assert callable(db.close)
            assert callable(db.backup)
            assert callable(db.check_bkp_tool)


# --------------------------
# Tests for error logging
# --------------------------
class TestErrorLogging:
    """Test cases for error logging functionality."""

    def test_error_logging_on_connection_failure(self, valid_db_config: dict[str, Any]) -> None:
        """Test error logging on connection failure.

        Verifies
        --------
        - CreateLog.log_message is called with error level
        - Error message contains connection details

        Parameters
        ----------
        valid_db_config : dict[str, Any]
            Valid database configuration from fixture

        Returns
        -------
        None
        """
        with patch("stpstone.utils.connections.databases.sql.sql_server.connect") as mock_connect:
            with patch("stpstone.utils.connections.databases.sql.sql_server.CreateLog.log_message") as mock_log:
                mock_connect.side_effect = Exception("Connection failed")
                
                with pytest.raises(ConnectionError):
                    SqlServerDB(**valid_db_config)
                
                mock_log.assert_called_once()
                call_args = mock_log.call_args[0]
                assert call_args[1] == "error"
                assert "Error connecting to database" in call_args[0]

    def test_error_logging_on_insert_failure(self, valid_db_config: dict[str, Any], mock_connection: Mock, mock_json_files: Mock) -> None:
        """Test error logging on insert failure.

        Verifies
        --------
        - CreateLog.log_message is called with error level on insert failure
        - Error message contains database and table details

        Parameters
        ----------
        valid_db_config : dict[str, Any]
            Valid database configuration from fixture
        mock_connection : Mock
            Mocked database connection
        mock_json_files : Mock
            Mocked JsonFiles instance

        Returns
        -------
        None
        """
        with patch("stpstone.utils.connections.databases.sql.sql_server.connect", return_value=mock_connection):
            with patch("stpstone.utils.connections.databases.sql.sql_server.JsonFiles", return_value=mock_json_files):
                with patch("stpstone.utils.connections.databases.sql.sql_server.CreateLog.log_message") as mock_log:
                    with patch("pandas.DataFrame.to_sql", side_effect=Exception("Insert failed")):
                        db = SqlServerDB(**valid_db_config)
                        test_data = [{"col1": "val1"}]
                        
                        with pytest.raises(Exception):
                            db.insert(test_data, "test_table")
                        
                        mock_log.assert_called_once()
                        call_args = mock_log.call_args[0]
                        assert call_args[1] == "error"
                        assert valid_db_config["database"] in call_args[0]
                        assert "test_table" in call_args[0]
                        assert "Insert failed" in call_args[0]