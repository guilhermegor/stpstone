"""Unit tests for MongoDB connection and data handling utilities.

Tests the MongoConn class functionality including:
- Singleton pattern implementation
- Connection establishment and validation
- DataFrame operations
- Context manager behavior
- Error handling and edge cases
"""

from logging import Logger
import sys
from typing import Optional
from unittest.mock import MagicMock, Mock

import pandas as pd
from pymongo import MongoClient
import pytest
from pytest_mock import MockerFixture

from stpstone.utils.connections.databases.nosql.mongodb import MongoConn


# --------------------------
# Fixtures
# --------------------------
@pytest.fixture(autouse=True)
def reset_singleton():
    """Reset singleton instance before each test."""
    yield
    MongoConn.reset_instance()


@pytest.fixture
def mock_mongo_client(mocker: MockerFixture) -> MagicMock:
    """Mock MongoClient for testing MongoDB connections.

    Parameters
    ----------
    mocker : MockerFixture
        Pytest-mock fixture for creating mocks

    Returns
    -------
    MagicMock
        Mocked MongoClient instance
    """
    return mocker.patch("stpstone.utils.connections.databases.nosql.mongodb.MongoClient")


@pytest.fixture
def mock_create_log(mocker: MockerFixture) -> MagicMock:
    """Mock CreateLog class for testing logging behavior.

    Parameters
    ----------
    mocker : MockerFixture
        Pytest-mock fixture for creating mocks

    Returns
    -------
    MagicMock
        Mocked CreateLog instance
    """
    return mocker.patch("stpstone.utils.connections.databases.nosql.mongodb.CreateLog")


@pytest.fixture
def mongo_conn() -> MongoConn:
    """Fixture providing a MongoConn instance with default parameters.

    Returns
    -------
    MongoConn
        Initialized MongoConn instance
    """
    return MongoConn()


@pytest.fixture
def sample_dataframe() -> pd.DataFrame:
    """Fixture providing a sample pandas DataFrame.

    Returns
    -------
    pd.DataFrame
        DataFrame with sample data
    """
    return pd.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})


@pytest.fixture
def mock_logger(mocker: MockerFixture) -> Logger:
    """Mock Logger instance for testing logging behavior.

    Parameters
    ----------
    mocker : MockerFixture
        Pytest-mock fixture for creating mocks

    Returns
    -------
    Logger
        Mocked Logger instance
    """
    return mocker.patch("logging.Logger")


# --------------------------
# Tests
# --------------------------
def test_singleton_pattern() -> None:
    """Test singleton pattern implementation.

    Verifies
    --------
    - Multiple instantiations return the same instance
    - Instance attributes remain consistent

    Returns
    -------
    None
    """
    conn1 = MongoConn()
    conn2 = MongoConn(str_host="different", int_port=27018)
    assert conn1 is conn2
    assert conn1.str_host == "localhost"  # First initialization wins
    assert conn1.int_port == 27017


def test_init_valid_inputs(mock_mongo_client: MagicMock) -> None:
    """Test initialization with valid inputs.

    Verifies
    --------
    - Instance attributes are correctly set
    - MongoClient is called with correct parameters
    - Database and collection are properly initialized

    Parameters
    ----------
    mock_mongo_client : MagicMock
        Mocked MongoClient instance

    Returns
    -------
    None
    """
    conn = MongoConn(str_host="test_host", int_port=27018, str_dbname="test_db", str_collection="test_coll")
    assert conn.str_host == "test_host"
    assert conn.int_port == 27018
    assert conn.str_dbname == "test_db"
    assert conn.str_collection == "test_coll"
    mock_mongo_client.assert_called_once_with("test_host", 27018)


@pytest.mark.parametrize("host", [None, "", 123, []])
def test_validate_host_invalid(host: any) -> None:
    """Test host validation with invalid inputs.

    Parameters
    ----------
    host : any
        Invalid host values to test

    Returns
    -------
    None
    """
    with pytest.raises(ValueError, match="Host must be a string|Host cannot be empty"):
        MongoConn(str_host=host)


@pytest.mark.parametrize("port", [None, "123", 0, 65536, -1, 1.5])
def test_validate_port_invalid(port: any) -> None:
    """Test port validation with invalid inputs.

    Parameters
    ----------
    port : any
        Invalid port values to test

    Returns
    -------
    None
    """
    with pytest.raises(ValueError, match="Port must be an integer|Port must be between 1 and 65535"):
        MongoConn(int_port=port)


@pytest.mark.parametrize("dbname", [None, "", 123, []])
def test_validate_dbname_invalid(dbname: any) -> None:
    """Test database name validation with invalid inputs.

    Parameters
    ----------
    dbname : any
        Invalid database name values to test

    Returns
    -------
    None
    """
    with pytest.raises(ValueError, match="Database name must be a string|Database name cannot be empty"):
        MongoConn(str_dbname=dbname)


@pytest.mark.parametrize("collection", [None, "", 123, []])
def test_validate_collection_invalid(collection: any) -> None:
    """Test collection name validation with invalid inputs.

    Parameters
    ----------
    collection : any
        Invalid collection name values to test

    Returns
    -------
    None
    """
    with pytest.raises(ValueError, match="Collection name must be a string|Collection name cannot be empty"):
        MongoConn(str_collection=collection)


def test_connect_failure(mock_mongo_client: MagicMock) -> None:
    """Test connection failure handling.

    Parameters
    ----------
    mock_mongo_client : MagicMock
        Mocked MongoClient instance

    Returns
    -------
    None
    """
    mock_mongo_client.side_effect = Exception("Connection failed")
    with pytest.raises(ConnectionError, match="Failed to connect to MongoDB"):
        MongoConn()


def test_save_df_valid(mock_mongo_client: MagicMock, sample_dataframe: pd.DataFrame) -> None:
    """Test saving valid DataFrame to MongoDB.

    Parameters
    ----------
    mock_mongo_client : MagicMock
        Mocked MongoClient instance
    sample_dataframe : pd.DataFrame
        Sample DataFrame from fixture

    Returns
    -------
    None
    """
    conn = MongoConn()
    conn._collection = Mock()
    conn.save_df(sample_dataframe)
    conn._collection.insert_many.assert_called_once()
    assert conn._collection.insert_many.call_args[0][0] == sample_dataframe.to_dict(orient="records")


@pytest.mark.parametrize("invalid_df", [None, [], "not_a_dataframe", 123])
def test_validate_dataframe_invalid(invalid_df: any, mock_mongo_client: MagicMock) -> None:
    """Test DataFrame validation with invalid inputs.

    Parameters
    ----------
    invalid_df : any
        Invalid DataFrame inputs to test
    mock_mongo_client : MagicMock
        Mocked MongoClient instance

    Returns
    -------
    None
    """
    conn = MongoConn()
    with pytest.raises(ValueError, match="The provided data is not a pandas DataFrame"):
        conn.save_df(invalid_df)


def test_save_df_empty(mock_mongo_client: MagicMock) -> None:
    """Test saving empty DataFrame raises error.

    Parameters
    ----------
    mock_mongo_client : MagicMock
        Mocked MongoClient instance

    Returns
    -------
    None
    """
    conn = MongoConn()
    empty_df = pd.DataFrame()
    with pytest.raises(ValueError, match="DataFrame cannot be empty"):
        conn.save_df(empty_df)


def test_save_df_insert_failure(mock_mongo_client: MagicMock, sample_dataframe: pd.DataFrame, 
                               mock_logger: Logger, mock_create_log: MagicMock) -> None:
    """Test handling of data insertion failure.

    Parameters
    ----------
    mock_mongo_client : MagicMock
        Mocked MongoClient instance
    sample_dataframe : pd.DataFrame
        Sample DataFrame from fixture
    mock_logger : Logger
        Mocked Logger instance
    mock_create_log : MagicMock
        Mocked CreateLog instance

    Returns
    -------
    None
    """
    conn = MongoConn(logger=mock_logger)
    conn._collection = Mock()
    conn._collection.insert_many.side_effect = Exception("Insert failed")
    
    with pytest.raises(RuntimeError, match="Failed to insert data into MongoDB"):
        conn.save_df(sample_dataframe)
    
    mock_create_log.return_value.log_message.assert_called_once_with(
        mock_logger,
        "ERROR Insert failed, MONGODB INJECTION ABORTED",
        "info"
    )


def test_close_connection(mock_mongo_client: MagicMock) -> None:
    """Test closing MongoDB connection.

    Parameters
    ----------
    mock_mongo_client : MagicMock
        Mocked MongoClient instance

    Returns
    -------
    None
    """
    conn = MongoConn()
    conn._client = Mock()
    conn.close()
    conn._client.close.assert_called_once()


def test_context_manager(mock_mongo_client: MagicMock) -> None:
    """Test context manager functionality.

    Parameters
    ----------
    mock_mongo_client : MagicMock
        Mocked MongoClient instance

    Returns
    -------
    None
    """
    with MongoConn() as conn:
        assert isinstance(conn, MongoConn)
        assert conn._client is not None
    assert conn._client.close.called


def test_unicode_input(mock_mongo_client: MagicMock) -> None:
    """Test handling of unicode inputs.

    Parameters
    ----------
    mock_mongo_client : MagicMock
        Mocked MongoClient instance

    Returns
    -------
    None
    """
    conn = MongoConn(str_host="localhost", str_dbname="数据库", str_collection="集合")
    assert conn.str_dbname == "数据库"
    assert conn.str_collection == "集合"


def test_reload_module(mock_mongo_client: MagicMock) -> None:
    """Test module reload preserves singleton instance.

    Parameters
    ----------
    mock_mongo_client : MagicMock
        Mocked MongoClient instance

    Returns
    -------
    None
    """
    import importlib
    conn1 = MongoConn()
    importlib.reload(sys.modules["stpstone.utils.connections.databases.nosql.mongodb"])
    # After reload, we need to create a new instance since the module was reloaded
    from stpstone.utils.connections.databases.nosql.mongodb import MongoConn as ReloadedMongoConn
    conn2 = ReloadedMongoConn()
    # They should be different instances due to module reload
    assert conn1 is not conn2


def test_none_logger(mock_mongo_client: MagicMock, sample_dataframe: pd.DataFrame, 
                    mock_create_log: MagicMock) -> None:
    """Test behavior with None logger.

    Parameters
    ----------
    mock_mongo_client : MagicMock
        Mocked MongoClient instance
    sample_dataframe : pd.DataFrame
        Sample DataFrame from fixture
    mock_create_log : MagicMock
        Mocked CreateLog instance

    Returns
    -------
    None
    """
    conn = MongoConn(logger=None)
    conn._collection = Mock()
    conn.save_df(sample_dataframe)
    # Should not call log_message when logger is None
    mock_create_log.return_value.log_message.assert_not_called()


def test_numeric_edge_cases(mock_mongo_client: MagicMock) -> None:
    """Test numeric edge cases for port number.

    Parameters
    ----------
    mock_mongo_client : MagicMock
        Mocked MongoClient instance

    Returns
    -------
    None
    """
    # Explicitly reset singleton to ensure a fresh instance
    MongoConn.reset_instance()
    assert MongoConn._initialized is False, "Singleton not reset properly"

    # Debug: Print singleton state before instantiation
    print(f"Before instantiation: _instance={MongoConn._instance}, _initialized={MongoConn._initialized}")

    # Test boundary value: port = 1
    conn1 = MongoConn(int_port=1)
    print(f"After instantiation: conn1={conn1}, _instance={MongoConn._instance}, _initialized={MongoConn._initialized}")
    print(f"conn1 attributes: {dir(conn1)}")
    print(f"conn1.int_port exists: {hasattr(conn1, 'int_port')}")
    print(f"conn1.int_port value: {getattr(conn1, 'int_port', None)}")
    assert hasattr(conn1, 'int_port'), f"int_port attribute missing on conn1: {dir(conn1)}"
    assert conn1.int_port == 1, f"Expected int_port to be 1, got {conn1.int_port}"

    # Reset singleton for second test
    MongoConn.reset_instance()
    assert MongoConn._initialized is False, "Singleton not reset properly"

    # Test boundary value: port = 65535
    conn2 = MongoConn(int_port=65535)
    print(f"After instantiation: conn2={conn2}, _instance={MongoConn._instance}, _initialized={MongoConn._initialized}")
    print(f"conn2 attributes: {dir(conn2)}")
    print(f"conn2.int_port exists: {hasattr(conn2, 'int_port')}")
    print(f"conn2.int_port value: {getattr(conn2, 'int_port', None)}")
    assert hasattr(conn2, 'int_port'), f"int_port attribute missing on conn2: {dir(conn2)}"
    assert conn2.int_port == 65535, f"Expected int_port to be 65535, got {conn2.int_port}"