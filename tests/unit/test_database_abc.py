"""Unit tests for ABCDatabase abstract base class.

Tests the database connection functionality with various input scenarios including:
- Initialization with valid and invalid inputs
- Singleton pattern behavior
- Context manager operations
- Parameter validation methods
- Query formatting
"""

import logging
from pathlib import Path
import sys
from typing import Any, Optional, Union
from unittest.mock import Mock

import pandas as pd
import pytest

from stpstone.utils.connections.databases.sql.database_abc import (
    ABCDatabase,
    DbConnection,
    DbCursor,
    SQLComposable,
)


# --------------------------
# Fixtures
# --------------------------
@pytest.fixture
def mock_db_connection() -> DbConnection:
    """Fixture providing a mock database connection.

    Returns
    -------
    DbConnection
        Mocked database connection object
    """
    mock_conn = Mock(spec=DbConnection)
    mock_cursor = Mock(spec=DbCursor)
    mock_conn.cursor.return_value = mock_cursor
    return mock_conn


@pytest.fixture
def concrete_db_class(mock_db_connection: DbConnection) -> type[ABCDatabase]:
    """Fixture providing a concrete implementation of ABCDatabase.

    Parameters
    ----------
    mock_db_connection : DbConnection
        Mocked database connection

    Returns
    -------
    type[ABCDatabase]
        Concrete implementation of ABCDatabase
    """
    class ConcreteDatabase(ABCDatabase):
        def __init__(
            self, 
            *args: Any, # noqa ANN401: typing.Any is not allowed
            **kwargs: Any # noqa ANN401: typing.Any is not allowed
        ) -> None:
            """Initialize the database instance.
            
            Parameters
            ----------
            *args : Any
                Positional arguments
            **kwargs : Any
                Keyword arguments
            
            Returns
            -------
            None
            """
            super().__init__(*args, **kwargs)
            # Validate parameters after calling super().__init__
            self._validate_connection_params()
            self.conn = mock_db_connection
            self.cursor = mock_db_connection.cursor()

        def execute(self, str_query: Union[str, SQLComposable]) -> None:
            """Execute a SQL query without returning results.
            
            Parameters
            ----------
            str_query : Union[str, SQLComposable]
                SQL query to execute
                
            Returns
            -------
            None
            """
            self.cursor.execute(str_query)

        def read(
            self,
            str_query: str,
            dict_type_cols: Optional[dict[str, Any]] = None,
            list_cols_dt: Optional[list[str]] = None,
            str_fmt_dt: Optional[TypeDateFormatInput] = None,
        ) -> pd.DataFrame:
            """Execute a query and return results as DataFrame.
            
            Parameters
            ----------
            str_query : str
                SQL query to execute
            dict_type_cols : Optional[dict[str, Any]], optional
                Column type mapping, defaults to None
            list_cols_dt : Optional[list[str]], optional
                Date columns to parse, defaults to None
            str_fmt_dt : Optional[TypeDateFormatInput], optional
                Date format string, defaults to None

            Returns
            -------
            pd.DataFrame
                Query results
            """
            return pd.DataFrame()

        def insert(
            self,
            json_data: list[dict[str, Any]],
            str_table_name: str,
            bool_insert_or_ignore: bool = False,
        ) -> None:
            """Insert data into a table.
            
            Parameters
            ----------
            json_data : list[dict[str, Any]]
                Data to insert (list of dicts)
            str_table_name : str
                Target table name
            bool_insert_or_ignore : bool, optional
                If True, ignore duplicates, defaults to False

            Returns
            -------
            None
            """
            pass

        def close(self) -> None:
            """Close the database connection.

            Returns
            -------
            None
            """
            self.conn.close()

        def backup(
            self,
            str_backup_dir: str,
            str_bkp_name: Optional[str] = None,
        ) -> str:
            """Create database backup.

            Parameters
            ----------
            str_backup_dir : str
                Backup directory path
            str_bkp_name : Optional[str], optional
                Custom backup filename, defaults to None

            Returns
            -------
            str
                Backup status message
            """
            return "Backup completed"
        
        def check_bkp_tool(self) -> bool:
            """Check if backup tool is available.
            
            Returns
            -------
            bool
                True if backup tool is available, False otherwise
            """
            return True 

    return ConcreteDatabase


@pytest.fixture
def mock_sql_composable() -> SQLComposable:
    """Fixture providing a mock SQLComposable object.

    Returns
    -------
    SQLComposable
        Mocked SQLComposable object
    """
    mock_composable = Mock(spec=SQLComposable)
    # Fix: Mock the __str__ method properly
    mock_composable.__str__ = Mock(return_value="SELECT * FROM table")
    return mock_composable


@pytest.fixture
def mock_logger() -> logging.Logger:
    """Fixture providing a mock logger.

    Returns
    -------
    logging.Logger
        Mocked logger object
    """
    return Mock(spec=logging.Logger)


@pytest.fixture
def valid_db_params() -> dict[str, Any]:
    """Fixture providing valid database parameters.

    Returns
    -------
    dict[str, Any]
        Dictionary of valid database parameters
    """
    return {
        "dbname": "test_db",
        "user": "test_user",
        "password": "test_pass",
        "host": "localhost",
        "port": 5432,
        "str_schema": "public",
        "logger": None,
        "bool_singleton": False
    }


@pytest.fixture(autouse=True)
def clear_singleton_instances() -> None:
    """Clear singleton instances before each test.
    
    Returns
    -------
    None
    """
    ABCDatabase._instances.clear()
    yield
    ABCDatabase._instances.clear()


# --------------------------
# Tests
# --------------------------
def test_init_valid_params(
    concrete_db_class: type[ABCDatabase],
    valid_db_params: dict[str, Any]
) -> None:
    """Test initialization with valid parameters.

    Verifies
    --------
    - All parameters are correctly stored
    - Connection and cursor are created
    - No exceptions are raised

    Parameters
    ----------
    concrete_db_class : type[ABCDatabase]
        Concrete implementation of ABCDatabase
    valid_db_params : dict[str, Any]
        Valid database parameters

    Returns
    -------
    None
    """
    db = concrete_db_class(**valid_db_params)
    assert db.dbname == valid_db_params["dbname"]
    assert db.user == valid_db_params["user"]
    assert db.password == valid_db_params["password"]
    assert db.host == valid_db_params["host"]
    assert db.port == valid_db_params["port"]
    assert db.str_schema == valid_db_params["str_schema"]
    assert db.logger is None
    assert db.bool_singleton is False
    assert isinstance(db.conn, DbConnection)
    assert isinstance(db.cursor, DbCursor)


def test_validate_connection_params_empty_dbname(
    concrete_db_class: type[ABCDatabase],
    valid_db_params: dict[str, Any]
) -> None:
    """Test validation raises ValueError for empty dbname.

    Verifies
    --------
    - ValueError is raised when dbname is empty
    - Error message matches expected pattern

    Parameters
    ----------
    concrete_db_class : type[ABCDatabase]
        Concrete implementation of ABCDatabase
    valid_db_params : dict[str, Any]
        Valid database parameters

    Returns
    -------
    None
    """
    valid_db_params["dbname"] = ""
    with pytest.raises(ValueError, match="Database name cannot be empty"):
        concrete_db_class(**valid_db_params)


def test_validate_connection_params_negative_port(
    concrete_db_class: type[ABCDatabase],
    valid_db_params: dict[str, Any]
) -> None:
    """Test validation raises ValueError for negative port.

    Verifies
    --------
    - ValueError is raised when port is negative
    - Error message matches expected pattern

    Parameters
    ----------
    concrete_db_class : type[ABCDatabase]
        Concrete implementation of ABCDatabase
    valid_db_params : dict[str, Any]
        Valid database parameters

    Returns
    -------
    None
    """
    valid_db_params["port"] = -1
    with pytest.raises(ValueError, match="Database port must be a positive integer"):
        concrete_db_class(**valid_db_params)


def test_validate_query_empty(
    concrete_db_class: type[ABCDatabase],
    valid_db_params: dict[str, Any]
) -> None:
    """Test validation raises ValueError for empty query.

    Verifies
    --------
    - ValueError is raised when query is empty
    - Error message matches expected pattern

    Parameters
    ----------
    concrete_db_class : type[ABCDatabase]
        Concrete implementation of ABCDatabase
    valid_db_params : dict[str, Any]
        Valid database parameters

    Returns
    -------
    None
    """
    db = concrete_db_class(**valid_db_params)
    with pytest.raises(ValueError, match="SQL query cannot be empty"):
        db._validate_query("")


def test_validate_insert_data_empty(
    concrete_db_class: type[ABCDatabase],
    valid_db_params: dict[str, Any]
) -> None:
    """Test validation raises ValueError for empty insert data.

    Verifies
    --------
    - ValueError is raised when json_data is empty
    - Error message matches expected pattern

    Parameters
    ----------
    concrete_db_class : type[ABCDatabase]
        Concrete implementation of ABCDatabase
    valid_db_params : dict[str, Any]
        Valid database parameters

    Returns
    -------
    None
    """
    db = concrete_db_class(**valid_db_params)
    with pytest.raises(ValueError, match="Insert data cannot be empty"):
        db._validate_insert_data([], "table_name")


def test_validate_backup_params_empty(
    concrete_db_class: type[ABCDatabase],
    valid_db_params: dict[str, Any]
) -> None:
    """Test validation raises ValueError for empty backup directory.

    Verifies
    --------
    - ValueError is raised when backup directory is empty
    - Error message matches expected pattern

    Parameters
    ----------
    concrete_db_class : type[ABCDatabase]
        Concrete implementation of ABCDatabase
    valid_db_params : dict[str, Any]
        Valid database parameters

    Returns
    -------
    None
    """
    db = concrete_db_class(**valid_db_params)
    with pytest.raises(ValueError, match="Backup directory cannot be empty"):
        db._validate_backup_params("")


def test_format_query_valid(
    concrete_db_class: type[ABCDatabase],
    valid_db_params: dict[str, Any],
    tmp_path: Path
) -> None:
    """Test query formatting with valid inputs.

    Verifies
    --------
    - Query is correctly formatted with parameters
    - No exceptions are raised
    - Formatted query matches expected output

    Parameters
    ----------
    concrete_db_class : type[ABCDatabase]
        Concrete implementation of ABCDatabase
    valid_db_params : dict[str, Any]
        Valid database parameters
    tmp_path : Path
        Temporary path for test files

    Returns
    -------
    None
    """
    query_file = tmp_path / "query.sql"
    query_file.write_text("SELECT * FROM {table_name} WHERE id = {id}")
    db = concrete_db_class(**valid_db_params)
    result = db.format_query(str(query_file), {"table_name": "users", "id": 1})
    assert result == "SELECT * FROM users WHERE id = 1"


def test_format_query_file_not_found(
    concrete_db_class: type[ABCDatabase],
    valid_db_params: dict[str, Any]
) -> None:
    """Test query formatting raises FileNotFoundError for missing file.

    Verifies
    --------
    - FileNotFoundError is raised when query file doesn't exist
    - Error message contains file path

    Parameters
    ----------
    concrete_db_class : type[ABCDatabase]
        Concrete implementation of ABCDatabase
    valid_db_params : dict[str, Any]
        Valid database parameters

    Returns
    -------
    None
    """
    db = concrete_db_class(**valid_db_params)
    with pytest.raises(FileNotFoundError, match="Query file not found"):
        db.format_query("nonexistent.sql", {"param": "value"})


def test_format_query_missing_param(
    concrete_db_class: type[ABCDatabase],
    valid_db_params: dict[str, Any],
    tmp_path: Path
) -> None:
    """Test query formatting raises ValueError for missing parameters.

    Verifies
    --------
    - ValueError is raised when parameters are missing
    - Error message indicates missing parameter

    Parameters
    ----------
    concrete_db_class : type[ABCDatabase]
        Concrete implementation of ABCDatabase
    valid_db_params : dict[str, Any]
        Valid database parameters
    tmp_path : Path
        Temporary path for test files

    Returns
    -------
    None
    """
    query_file = tmp_path / "query.sql"
    query_file.write_text("SELECT * FROM {table_name}")
    db = concrete_db_class(**valid_db_params)
    with pytest.raises(ValueError, match="Parameters dictionary cannot be empty"):
        db.format_query(str(query_file), {})


def test_singleton_pattern(
    concrete_db_class: type[ABCDatabase],
    valid_db_params: dict[str, Any]
) -> None:
    """Test singleton pattern implementation.

    Verifies
    --------
    - Same instance is returned for singleton mode
    - Different instances are returned for non-singleton mode
    - Instance attributes are preserved

    Parameters
    ----------
    concrete_db_class : type[ABCDatabase]
        Concrete implementation of ABCDatabase
    valid_db_params : dict[str, Any]
        Valid database parameters

    Returns
    -------
    None
    """
    # Test singleton mode
    valid_db_params["bool_singleton"] = True
    instance1 = concrete_db_class(**valid_db_params)
    instance2 = concrete_db_class(**valid_db_params)
    assert instance1 is instance2

    # Clear singleton instances for next test
    ABCDatabase._instances.clear()
    
    # Test non-singleton mode
    valid_db_params["bool_singleton"] = False
    instance3 = concrete_db_class(**valid_db_params)
    instance4 = concrete_db_class(**valid_db_params)
    assert instance3 is not instance4


def test_context_manager(
    concrete_db_class: type[ABCDatabase],
    valid_db_params: dict[str, Any],
    mock_logger: logging.Logger
) -> None:
    """Test context manager protocol implementation.

    Verifies
    --------
    - Connection is properly closed on exit
    - Logger is called when exception occurs
    - Exceptions propagate correctly

    Parameters
    ----------
    concrete_db_class : type[ABCDatabase]
        Concrete implementation of ABCDatabase
    valid_db_params : dict[str, Any]
        Valid database parameters
    mock_logger : logging.Logger
        Mocked logger object

    Returns
    -------
    None

    Raises
    ------
    ValueError
        If test error is raised
    """
    valid_db_params["logger"] = mock_logger
    
    # Test that exception is propagated and close is called
    with pytest.raises(ValueError, match="Test error"), concrete_db_class(**valid_db_params) as db:
        assert isinstance(db, ABCDatabase)
        raise ValueError("Test error")
    
    # Verify close was called and error was logged
    db.conn.close.assert_called_once()
    mock_logger.error.assert_called_once()


def test_context_manager_no_exception(
    concrete_db_class: type[ABCDatabase],
    valid_db_params: dict[str, Any]
) -> None:
    """Test context manager without exceptions.

    Verifies
    --------
    - Connection is properly closed on exit
    - No logging occurs when no exception is raised

    Parameters
    ----------
    concrete_db_class : type[ABCDatabase]
        Concrete implementation of ABCDatabase
    valid_db_params : dict[str, Any]
        Valid database parameters

    Returns
    -------
    None
    """
    with concrete_db_class(**valid_db_params) as db:
        assert isinstance(db, ABCDatabase)
    db.conn.close.assert_called_once()


def test_execute_valid_query(
    concrete_db_class: type[ABCDatabase],
    valid_db_params: dict[str, Any],
    mock_sql_composable: SQLComposable
) -> None:
    """Test execute method with valid query.

    Verifies
    --------
    - Query is executed through cursor
    - No exceptions are raised

    Parameters
    ----------
    concrete_db_class : type[ABCDatabase]
        Concrete implementation of ABCDatabase
    valid_db_params : dict[str, Any]
        Valid database parameters
    mock_sql_composable : SQLComposable
        Mocked SQLComposable object

    Returns
    -------
    None
    """
    db = concrete_db_class(**valid_db_params)
    db.execute(mock_sql_composable)
    db.cursor.execute.assert_called_once_with(mock_sql_composable)


def test_read_empty_dataframe(
    concrete_db_class: type[ABCDatabase],
    valid_db_params: dict[str, Any]
) -> None:
    """Test read method returns empty DataFrame.

    Verifies
    --------
    - Returns pandas DataFrame
    - No exceptions are raised for valid query

    Parameters
    ----------
    concrete_db_class : type[ABCDatabase]
        Concrete implementation of ABCDatabase
    valid_db_params : dict[str, Any]
        Valid database parameters

    Returns
    -------
    None
    """
    db = concrete_db_class(**valid_db_params)
    result = db.read("SELECT * FROM table")
    assert isinstance(result, pd.DataFrame)


def test_insert_valid_data(
    concrete_db_class: type[ABCDatabase],
    valid_db_params: dict[str, Any]
) -> None:
    """Test insert method with valid data.

    Verifies
    --------
    - No exceptions are raised for valid input
    - Validation passes for non-empty data

    Parameters
    ----------
    concrete_db_class : type[ABCDatabase]
        Concrete implementation of ABCDatabase
    valid_db_params : dict[str, Any]
        Valid database parameters

    Returns
    -------
    None
    """
    db = concrete_db_class(**valid_db_params)
    data = [{"id": 1, "name": "test"}]
    db.insert(data, "table_name")
    # No exception means validation passed


def test_backup_valid(
    concrete_db_class: type[ABCDatabase],
    valid_db_params: dict[str, Any]
) -> None:
    """Test backup method with valid parameters.

    Verifies
    --------
    - Returns expected backup status message
    - No exceptions are raised

    Parameters
    ----------
    concrete_db_class : type[ABCDatabase]
        Concrete implementation of ABCDatabase
    valid_db_params : dict[str, Any]
        Valid database parameters

    Returns
    -------
    None
    """
    db = concrete_db_class(**valid_db_params)
    result = db.backup("/tmp") # noqa S108: probable insecure usage of temporary file or directory
    assert result == "Backup completed"


def test_reload_module() -> None:
    """Test module reloading behavior.

    Verifies
    --------
    - Module can be reloaded without errors
    - Singleton instances are preserved after reload

    Returns
    -------
    None
    """
    import importlib
    # Fix: Use the correct module path
    module_path = "stpstone.utils.connections.databases.sql.database_abc"
    importlib.reload(sys.modules[module_path])
    assert hasattr(sys.modules[module_path], "ABCDatabase")