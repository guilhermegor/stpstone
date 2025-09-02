"""Unit tests for PostgreSQLDB class.

Tests the PostgreSQL database connection and operations including:
- Database connection initialization and validation
- Query execution and data retrieval
- Data insertion with error handling
- Database backup functionality
- Connection management and cleanup
"""

import subprocess
from typing import Any
from unittest.mock import MagicMock, Mock, patch

import pandas as pd
from psycopg import Connection, Cursor
from psycopg.errors import OperationalError
from psycopg.sql import Composable
import pytest

from stpstone.utils.connections.databases.sql.postgresql_db import PostgreSQLDB


# --------------------------
# Fixtures
# --------------------------
@pytest.fixture
def mock_connection() -> MagicMock:
    """Fixture providing a mock database connection.

    Returns
    -------
    MagicMock
        Mock Connection object with cursor and close methods
    """
    mock_conn = MagicMock(spec=Connection)
    mock_cursor = MagicMock(spec=Cursor)
    mock_conn.cursor.return_value = mock_cursor
    mock_conn.commit.return_value = None
    mock_conn.rollback.return_value = None
    mock_conn.close.return_value = None
    mock_cursor.execute.return_value = None
    mock_cursor.executemany.return_value = None
    mock_cursor.fetchall.return_value = []
    return mock_conn


@pytest.fixture
def mock_cursor(mock_connection: MagicMock) -> MagicMock:
    """Fixture providing a mock database cursor.

    Parameters
    ----------
    mock_connection : MagicMock
        Mock connection fixture

    Returns
    -------
    MagicMock
        Mock Cursor object with execute and fetch methods
    """
    return mock_connection.cursor.return_value


@pytest.fixture
def postgresql_db_config() -> dict[str, Any]:
    """Fixture providing valid PostgreSQL database configuration.

    Returns
    -------
    dict[str, Any]
        Dictionary containing database connection parameters
    """
    return {
        "dbname": "testdb",
        "user": "testuser",
        "password": "testpass",
        "host": "localhost",
        "port": 5432,
        "str_schema": "public"
    }


@pytest.fixture
def sample_json_data() -> list[dict[str, Any]]:
    """Fixture providing sample JSON data for insertion tests.

    Returns
    -------
    list[dict[str, Any]]
        List of dictionaries representing database records
    """
    return [
        {"id": 1, "name": "Alice", "age": 30},
        {"id": 2, "name": "Bob", "age": 25}
    ]


@pytest.fixture
def sample_query_result() -> list[dict[str, Any]]:
    """Fixture providing sample query results.

    Returns
    -------
    list[dict[str, Any]]
        List of dictionaries representing query results
    """
    return [
        {"id": 1, "name": "Alice", "created_at": "2023-01-01"},
        {"id": 2, "name": "Bob", "created_at": "2023-01-02"}
    ]


# --------------------------
# Connection Tests
# --------------------------
class TestPostgreSQLDBConnection:
    """Test cases for PostgreSQLDB connection initialization and validation."""

    @patch("stpstone.utils.connections.databases.sql.postgresql_db.connect")
    def test_init_with_valid_config(
        self,
        mock_connect: Mock,
        postgresql_db_config: dict[str, Any],
        mock_connection: MagicMock
    ) -> None:
        """Test initialization with valid configuration parameters.

        Verifies
        --------
        - Database connection is established successfully
        - Cursor is created and basic query executes
        - Search path is set to specified schema

        Parameters
        ----------
        mock_connect : Mock
            Mock psycopg.connect function
        postgresql_db_config : dict[str, Any]
            Database configuration fixture
        mock_connection : MagicMock
            Mock connection fixture
        """
        mock_connect.return_value = mock_connection
        db = PostgreSQLDB(**postgresql_db_config)
        assert db.conn == mock_connection
        assert db.cursor == mock_connection.cursor.return_value
        mock_connection.cursor.return_value.execute.assert_any_call("SELECT 1")

    def test_init_with_invalid_dbname(self) -> None:
        """Test initialization with empty database name.

        Verifies
        --------
        - ValueError is raised when dbname is empty
        - Error message indicates database name cannot be empty
        """
        with pytest.raises(ValueError, match="Database name cannot be empty"):
            PostgreSQLDB(
                dbname="", 
                user="test", 
                password="test", # noqa S106: possible hardcoded password
                host="localhost", 
                port=5432
            )

    def test_init_with_invalid_user(self) -> None:
        """Test initialization with empty user.

        Verifies
        --------
        - ValueError is raised when user is empty
        - Error message indicates user cannot be empty
        """
        with pytest.raises(ValueError, match="Database user cannot be empty"):
            PostgreSQLDB(
                dbname="test", 
                user="", 
                password="test", # noqa S106: possible hardcoded password
                host="localhost", 
                port=5432
            )

    def test_init_with_invalid_password(self) -> None:
        """Test initialization with empty password.

        Verifies
        --------
        - ValueError is raised when password is empty
        - Error message indicates password cannot be empty
        """
        with pytest.raises(ValueError, match="Database password cannot be empty"):
            PostgreSQLDB(
                dbname="test", 
                user="test", 
                password="", # noqa S106: possible hardcoded password
                host="localhost", 
                port=5432
            )

    def test_init_with_invalid_host(self) -> None:
        """Test initialization with empty host.

        Verifies
        --------
        - ValueError is raised when host is empty
        - Error message indicates host cannot be empty
        """
        with pytest.raises(ValueError, match="Database host cannot be empty"):
            PostgreSQLDB(
                dbname="test", 
                user="test", 
                password="test", # noqa S106: possible hardcoded password
                host="", 
                port=5432
            )

    def test_init_with_invalid_port(self) -> None:
        """Test initialization with invalid port.

        Verifies
        --------
        - ValueError is raised when port is non-positive
        - Error message indicates port must be positive integer
        """
        with pytest.raises(ValueError, match="Database port must be a positive integer"):
            PostgreSQLDB(
                dbname="test", 
                user="test", 
                password="test", # noqa S106: possible hardcoded password
                host="localhost", 
                port=0
            )

    def test_init_with_invalid_schema(self) -> None:
        """Test initialization with empty schema.

        Verifies
        --------
        - ValueError is raised when schema is empty
        - Error message indicates schema cannot be empty
        """
        with pytest.raises(ValueError, match="Database schema cannot be empty"):
            PostgreSQLDB(
                dbname="test", 
                user="test", 
                password="test", # noqa S106: possible hardcoded password
                host="localhost", 
                port=5432, 
                str_schema=""
            )

    @patch("stpstone.utils.connections.databases.sql.postgresql_db.connect")
    def test_init_connection_failure(
        self, 
        mock_connect: Mock, 
        postgresql_db_config: dict[str, Any]
    ) -> None:
        """Test initialization when database connection fails.

        Verifies
        --------
        - ConnectionError is raised when connection fails
        - Error message contains original exception details

        Parameters
        ----------
        mock_connect : Mock
            Mock psycopg.connect function
        postgresql_db_config : dict[str, Any]
            Database configuration fixture
        
        Returns
        -------
        None
        """
        mock_connect.side_effect = OperationalError(
            "connection failed: connection to server at '127.0.0.1', port 5432 failed"
        )
        with pytest.raises(
            ConnectionError, 
            match="Error connecting to database: connection failed: connection to server"
        ):
            PostgreSQLDB(**postgresql_db_config)


# --------------------------
# Execute Method Tests
# --------------------------
class TestExecuteMethod:
    """Test cases for execute method."""

    @patch("stpstone.utils.connections.databases.sql.postgresql_db.connect")
    def test_execute_with_string_query(
        self,
        mock_connect: Mock,
        postgresql_db_config: dict[str, Any],
        mock_connection: MagicMock,
        mock_cursor: MagicMock
    ) -> None:
        """Test execute method with string query.

        Verifies
        --------
        - String queries are executed successfully
        - Cursor execute method is called with correct query

        Parameters
        ----------
        mock_connect : Mock
            Mock psycopg.connect function
        postgresql_db_config : dict[str, Any]
            Database configuration fixture
        mock_connection : MagicMock
            Mock connection fixture
        mock_cursor : MagicMock
            Mock cursor fixture
        """
        mock_connect.return_value = mock_connection
        db = PostgreSQLDB(**postgresql_db_config)
        query = "SELECT * FROM users"
        
        db.execute(query)
        
        mock_cursor.execute.assert_called_with(query)

    @patch("stpstone.utils.connections.databases.sql.postgresql_db.connect")
    def test_execute_with_composable_query(
        self,
        mock_connect: Mock,
        postgresql_db_config: dict[str, Any],
        mock_connection: MagicMock,
        mock_cursor: MagicMock
    ) -> None:
        """Test execute method with Composable query.

        Verifies
        --------
        - Composable queries are executed successfully
        - Cursor execute method is called with correct query object

        Parameters
        ----------
        mock_connect : Mock
            Mock psycopg.connect function
        postgresql_db_config : dict[str, Any]
            Database configuration fixture
        mock_connection : MagicMock
            Mock connection fixture
        mock_cursor : MagicMock
            Mock cursor fixture
        """
        mock_connect.return_value = mock_connection
        db = PostgreSQLDB(**postgresql_db_config)
        composable_query = Mock(spec=Composable)
        
        db.execute(composable_query)
        
        mock_cursor.execute.assert_called_with(composable_query)


# --------------------------
# Read Method Tests
# --------------------------
class TestReadMethod:
    """Test cases for read method."""

    @patch("stpstone.utils.connections.databases.sql.postgresql_db.connect")
    def test_read_with_empty_query(
        self,
        mock_connect: Mock,
        postgresql_db_config: dict[str, Any],
        mock_connection: MagicMock
    ) -> None:
        """Test read method with empty query string.

        Verifies
        --------
        - ValueError is raised when query is empty
        - Error message indicates query cannot be empty

        Parameters
        ----------
        mock_connect : Mock
            Mock psycopg.connect function
        postgresql_db_config : dict[str, Any]
            Database configuration fixture
        mock_connection : MagicMock
            Mock connection fixture
        """
        mock_connect.return_value = mock_connection
        db = PostgreSQLDB(**postgresql_db_config)
        
        with pytest.raises(ValueError, match="Query cannot be empty"):
            db.read("")

    @patch("stpstone.utils.connections.databases.sql.postgresql_db.connect")
    def test_read_with_partial_date_params(
        self,
        mock_connect: Mock,
        postgresql_db_config: dict[str, Any],
        mock_connection: MagicMock,
        mock_cursor: MagicMock,
        sample_query_result: list[dict[str, Any]]
    ) -> None:
        """Test read method with partial date conversion parameters.

        Verifies
        --------
        - ValueError is raised when date parameters are provided partially
        - Error message indicates both parameters must be provided or None

        Parameters
        ----------
        mock_connect : Mock
            Mock psycopg.connect function
        postgresql_db_config : dict[str, Any]
            Database configuration fixture
        mock_connection : MagicMock
            Mock connection fixture
        mock_cursor : MagicMock
            Mock cursor fixture
        sample_query_result : list[dict[str, Any]]
            Sample query results fixture
        """
        mock_connect.return_value = mock_connection
        mock_cursor.fetchall.return_value = sample_query_result
        db = PostgreSQLDB(**postgresql_db_config)
        
        with pytest.raises(
            ValueError, match="Both list_cols_dt and str_fmt_dt must be provided or None"):
            db.read("SELECT * FROM users", list_cols_dt=["created_at"])

    @patch("stpstone.utils.connections.databases.sql.postgresql_db.connect")
    def test_read_basic_query(
        self,
        mock_connect: Mock,
        postgresql_db_config: dict[str, Any],
        mock_connection: MagicMock,
        mock_cursor: MagicMock,
        sample_query_result: list[dict[str, Any]]
    ) -> None:
        """Test basic read operation without type conversion.

        Verifies
        --------
        - Query is executed successfully
        - Results are converted to DataFrame
        - DataFrame contains expected data

        Parameters
        ----------
        mock_connect : Mock
            Mock psycopg.connect function
        postgresql_db_config : dict[str, Any]
            Database configuration fixture
        mock_connection : MagicMock
            Mock connection fixture
        mock_cursor : MagicMock
            Mock cursor fixture
        sample_query_result : list[dict[str, Any]]
            Sample query results fixture
        """
        mock_connect.return_value = mock_connection
        mock_cursor.fetchall.return_value = sample_query_result
        db = PostgreSQLDB(**postgresql_db_config)
        
        result = db.read("SELECT * FROM users")
        
        mock_cursor.execute.assert_called_with("SELECT * FROM users")
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2
        assert list(result.columns) == ["id", "name", "created_at"]

    @patch("stpstone.utils.connections.databases.sql.postgresql_db.connect")
    def test_read_with_type_conversion(
        self,
        mock_connect: Mock,
        postgresql_db_config: dict[str, Any],
        mock_connection: MagicMock,
        mock_cursor: MagicMock,
        sample_query_result: list[dict[str, Any]]
    ) -> None:
        """Test read operation with type conversion.

        Verifies
        --------
        - Column types are converted according to dict_type_cols
        - DataFrame dtypes match expected types after conversion

        Parameters
        ----------
        mock_connect : Mock
            Mock psycopg.connect function
        postgresql_db_config : dict[str, Any]
            Database configuration fixture
        mock_connection : MagicMock
            Mock connection fixture
        mock_cursor : MagicMock
            Mock cursor fixture
        sample_query_result : list[dict[str, Any]]
            Sample query results fixture
        """
        mock_connect.return_value = mock_connection
        mock_cursor.fetchall.return_value = sample_query_result
        db = PostgreSQLDB(**postgresql_db_config)
        
        result = db.read(
            "SELECT * FROM users",
            dict_type_cols={"id": "int64", "name": "string"}
        )
        
        assert result["id"].dtype == "int64"
        assert result["name"].dtype == "string"

    @patch("stpstone.utils.calendars.calendar_br.ABCCalendarOperations")
    @patch("stpstone.utils.connections.databases.sql.postgresql_db.connect")
    def test_read_with_date_conversion(
        self,
        mock_connect: Mock,
        mock_dates_br: Mock,
        postgresql_db_config: dict[str, Any],
        mock_connection: MagicMock,
        mock_cursor: MagicMock,
        sample_query_result: list[dict[str, Any]]
    ) -> None:
        """Test read operation with date conversion.

        Verifies
        --------
        - Date columns are converted using ABCCalendarOperations utility
        - Date format is passed correctly to conversion function

        Parameters
        ----------
        mock_connect : Mock
            Mock psycopg.connect function
        mock_dates_br : Mock
            Mock DatesBRAnbima class
        postgresql_db_config : dict[str, Any]
            Database configuration fixture
        mock_connection : MagicMock
            Mock connection fixture
        mock_cursor : MagicMock
            Mock cursor fixture
        sample_query_result : list[dict[str, Any]]
            Sample query results fixture
        """
        mock_connect.return_value = mock_connection
        mock_cursor.fetchall.return_value = sample_query_result
        mock_dates_instance = Mock()
        mock_dates_br.return_value = mock_dates_instance
        db = PostgreSQLDB(**postgresql_db_config)
        
        _ = db.read(
            "SELECT * FROM users",
            list_cols_dt=["created_at"],
            str_fmt_dt="YYYY-MM-DD"
        )


# --------------------------
# Insert Method Tests
# --------------------------
class TestInsertMethod:
    """Test cases for insert method."""

    @patch("stpstone.utils.connections.databases.sql.postgresql_db.connect")
    def test_insert_with_empty_table_name(
        self,
        mock_connect: Mock,
        postgresql_db_config: dict[str, Any],
        mock_connection: MagicMock,
        sample_json_data: list[dict[str, Any]]
    ) -> None:
        """Test insert method with empty table name.

        Verifies
        --------
        - ValueError is raised when table name is empty
        - Error message indicates table name cannot be empty

        Parameters
        ----------
        mock_connect : Mock
            Mock psycopg.connect function
        postgresql_db_config : dict[str, Any]
            Database configuration fixture
        mock_connection : MagicMock
            Mock connection fixture
        sample_json_data : list[dict[str, Any]]
            Sample JSON data fixture

        Returns
        -------
        None
        """
        mock_connect.return_value = mock_connection
        db = PostgreSQLDB(**postgresql_db_config)
        
        with pytest.raises(ValueError, match="Table name cannot be empty"):
            db.insert(sample_json_data, "")

    @patch("stpstone.utils.connections.databases.sql.postgresql_db.connect")
    def test_insert_with_empty_data(
        self,
        mock_connect: Mock,
        postgresql_db_config: dict[str, Any],
        mock_connection: MagicMock
    ) -> None:
        """Test insert method with empty data list.

        Verifies
        --------
        - Method returns early without error when data is empty
        - No database operations are performed

        Parameters
        ----------
        mock_connect : Mock
            Mock psycopg.connect function
        postgresql_db_config : dict[str, Any]
            Database configuration fixture
        mock_connection : MagicMock
            Mock connection fixture

        Returns
        -------
        None
        """
        mock_connect.return_value = mock_connection
        mock_cursor = mock_connection.cursor.return_value
        db = PostgreSQLDB(**postgresql_db_config)
        
        db.insert([], "users")
        
        mock_cursor.executemany.assert_not_called()
        mock_connection.commit.assert_not_called()

    @patch("stpstone.utils.connections.databases.sql.postgresql_db.JsonFiles")
    @patch("stpstone.utils.connections.databases.sql.postgresql_db.connect")
    def test_insert_success(
        self,
        mock_connect: Mock,
        mock_json_files: Mock,
        postgresql_db_config: dict[str, Any],
        mock_connection: MagicMock,
        mock_cursor: MagicMock,
        sample_json_data: list[dict[str, Any]]
    ) -> None:
        """Test successful insert operation.

        Verifies
        --------
        - JSON data is normalized before insertion
        - Records are inserted using executemany
        - Transaction is committed successfully

        Parameters
        ----------
        mock_connect : Mock
            Mock psycopg.connect function
        mock_json_files : Mock
            Mock JsonFiles class
        postgresql_db_config : dict[str, Any]
            Database configuration fixture
        mock_connection : MagicMock
            Mock connection fixture
        mock_cursor : MagicMock
            Mock cursor fixture
        sample_json_data : list[dict[str, Any]]
            Sample JSON data fixture
        """
        mock_connect.return_value = mock_connection
        mock_json_instance = Mock()
        mock_json_files.return_value = mock_json_instance
        mock_json_instance.normalize_json_keys.return_value = sample_json_data
        
        db = PostgreSQLDB(**postgresql_db_config)
        db.insert(sample_json_data, "users")
        
        mock_json_instance.normalize_json_keys.assert_called_with(sample_json_data)
        mock_cursor.executemany.assert_called_once()
        mock_connection.commit.assert_called_once()

    @patch("stpstone.utils.connections.databases.sql.postgresql_db.JsonFiles")
    @patch("stpstone.utils.connections.databases.sql.postgresql_db.connect")
    def test_insert_with_insert_or_ignore(
        self,
        mock_connect: Mock,
        mock_json_files: Mock,
        postgresql_db_config: dict[str, Any],
        mock_connection: MagicMock,
        mock_cursor: MagicMock,
        sample_json_data: list[dict[str, Any]]
    ) -> None:
        """Test insert operation with insert_or_ignore flag.

        Verifies
        --------
        - INSERT OR IGNORE query is used when flag is True
        - Records are inserted using executemany

        Parameters
        ----------
        mock_connect : Mock
            Mock psycopg.connect function
        mock_json_files : Mock
            Mock JsonFiles class
        postgresql_db_config : dict[str, Any]
            Database configuration fixture
        mock_connection : MagicMock
            Mock connection fixture
        mock_cursor : MagicMock
            Mock cursor fixture
        sample_json_data : list[dict[str, Any]]
            Sample JSON data fixture
        """
        mock_connect.return_value = mock_connection
        mock_json_instance = Mock()
        mock_json_files.return_value = mock_json_instance
        mock_json_instance.normalize_json_keys.return_value = sample_json_data
        
        db = PostgreSQLDB(**postgresql_db_config)
        db.insert(sample_json_data, "users", bool_insert_or_ignore=True)
        
        mock_cursor.executemany.assert_called_once()
        mock_connection.commit.assert_called_once()

    @patch("stpstone.utils.connections.databases.sql.postgresql_db.JsonFiles")
    @patch("stpstone.utils.connections.databases.sql.postgresql_db.connect")
    def test_insert_failure_rollback(
        self,
        mock_connect: Mock,
        mock_json_files: Mock,
        postgresql_db_config: dict[str, Any],
        mock_connection: MagicMock,
        mock_cursor: MagicMock,
        sample_json_data: list[dict[str, Any]]
    ) -> None:
        """Test insert operation failure with rollback.

        Verifies
        --------
        - Transaction is rolled back on insertion failure
        - Connection is closed after rollback
        - Exception is re-raised with appropriate message

        Parameters
        ----------
        mock_connect : Mock
            Mock psycopg.connect function
        mock_json_files : Mock
            Mock JsonFiles class
        postgresql_db_config : dict[str, Any]
            Database configuration fixture
        mock_connection : MagicMock
            Mock connection fixture
        mock_cursor : MagicMock
            Mock cursor fixture
        sample_json_data : list[dict[str, Any]]
            Sample JSON data fixture
        """
        mock_connect.return_value = mock_connection
        mock_json_instance = Mock()
        mock_json_files.return_value = mock_json_instance
        mock_json_instance.normalize_json_keys.return_value = sample_json_data
        mock_cursor.executemany.side_effect = Exception("Insert failed")
        
        db = PostgreSQLDB(**postgresql_db_config)
        
        with pytest.raises(Exception, match="Insert failed"):
            db.insert(sample_json_data, "users")
        
        mock_connection.rollback.assert_called_once()
        mock_connection.close.assert_called_once()


# --------------------------
# Backup Method Tests
# --------------------------
class TestBackupMethod:
    """Test cases for backup method."""

    @patch("stpstone.utils.connections.databases.sql.postgresql_db.connect")
    def test_backup_with_empty_directory(
        self,
        mock_connect: Mock,
        postgresql_db_config: dict[str, Any],
        mock_connection: MagicMock
    ) -> None:
        """Test backup method with empty directory.

        Verifies
        --------
        - ValueError is raised when backup directory is empty
        - Error message indicates directory cannot be empty

        Parameters
        ----------
        mock_connect : Mock
            Mock psycopg.connect function
        postgresql_db_config : dict[str, Any]
            Database configuration fixture
        mock_connection : MagicMock
            Mock connection fixture
        """
        mock_connect.return_value = mock_connection
        db = PostgreSQLDB(**postgresql_db_config)
        
        with pytest.raises(ValueError, match="Backup directory cannot be empty"):
            db.backup("", "backup.bak")

    @patch("os.makedirs")
    @patch("subprocess.run")
    @patch("stpstone.utils.connections.databases.sql.postgresql_db.connect")
    def test_backup_success(
        self,
        mock_connect: Mock,
        mock_run: Mock,
        mock_makedirs: Mock,
        postgresql_db_config: dict[str, Any],
        mock_connection: MagicMock
    ) -> None:
        """Test successful backup operation.

        Verifies
        --------
        - Backup directory is created
        - pg_dump command is executed with correct parameters
        - Success message is returned

        Parameters
        ----------
        mock_connect : Mock
            Mock psycopg.connect function
        mock_run : Mock
            Mock subprocess.run
        mock_makedirs : Mock
            Mock os.makedirs
        postgresql_db_config : dict[str, Any]
            Database configuration fixture
        mock_connection : MagicMock
            Mock connection fixture
        """
        mock_connect.return_value = mock_connection
        db = PostgreSQLDB(**postgresql_db_config)
        
        result = db.backup("/tmp/backup", "test_backup.bak") # noqa S108: probable insecure usage of temporary file or directory
        
        mock_makedirs.assert_called_with("/tmp/backup", exist_ok=True) # noqa S108: probable insecure usage of temporary file or directory
        mock_run.assert_called_once()
        assert "Backup successful" in result

    @patch("os.makedirs")
    @patch("subprocess.run", side_effect=subprocess.CalledProcessError(1, "pg_dump"))
    @patch("stpstone.utils.connections.databases.sql.postgresql_db.connect")
    def test_backup_process_error(
        self,
        mock_connect: Mock,
        mock_run: Mock,
        mock_makedirs: Mock,
        postgresql_db_config: dict[str, Any],
        mock_connection: MagicMock
    ) -> None:
        """Test backup operation with subprocess error.

        Verifies
        --------
        - Error message indicates backup failure
        - CalledProcessError is handled gracefully

        Parameters
        ----------
        mock_connect : Mock
            Mock psycopg.connect function
        mock_run : Mock
            Mock subprocess.run
        mock_makedirs : Mock
            Mock os.makedirs
        postgresql_db_config : dict[str, Any]
            Database configuration fixture
        mock_connection : MagicMock
            Mock connection fixture
        """
        mock_connect.return_value = mock_connection
        db = PostgreSQLDB(**postgresql_db_config)
        
        result = db.backup("/tmp/backup", "test_backup.bak") # noqa S108: probable insecure usage of temporary file or directory
        
        assert "Backup failed" in result

    @patch("os.makedirs")
    @patch("subprocess.run", side_effect=Exception("Unexpected error"))
    @patch("stpstone.utils.connections.databases.sql.postgresql_db.connect")
    def test_backup_general_error(
        self,
        mock_connect: Mock,
        mock_run: Mock,
        mock_makedirs: Mock,
        postgresql_db_config: dict[str, Any],
        mock_connection: MagicMock
    ) -> None:
        """Test backup operation with general error.

        Verifies
        --------
        - Error message indicates unexpected error occurred
        - General exceptions are handled gracefully

        Parameters
        ----------
        mock_connect : Mock
            Mock psycopg.connect function
        mock_run : Mock
            Mock subprocess.run
        mock_makedirs : Mock
            Mock os.makedirs
        postgresql_db_config : dict[str, Any]
            Database configuration fixture
        mock_connection : MagicMock
            Mock connection fixture
        """
        mock_connect.return_value = mock_connection
        db = PostgreSQLDB(**postgresql_db_config)
        
        result = db.backup("/tmp/backup", "test_backup.bak") # noqa S108: probable insecure usage of temporary file or directory
        
        assert "An error occurred" in result


# --------------------------
# Close Method Tests
# --------------------------
class TestCloseMethod:
    """Test cases for close method."""

    @patch("stpstone.utils.connections.databases.sql.postgresql_db.connect")
    def test_close_connection(
        self,
        mock_connect: Mock,
        postgresql_db_config: dict[str, Any],
        mock_connection: MagicMock
    ) -> None:
        """Test connection close operation.

        Verifies
        --------
        - Connection close method is called
        - No exceptions are raised

        Parameters
        ----------
        mock_connect : Mock
            Mock psycopg.connect function
        postgresql_db_config : dict[str, Any]
            Database configuration fixture
        mock_connection : MagicMock
            Mock connection fixture
        """
        mock_connect.return_value = mock_connection
        db = PostgreSQLDB(**postgresql_db_config)
        
        db.close()
        
        mock_connection.close.assert_called_once()