"""Unit tests for MongoDB connection and data handling utilities.

Tests the MongoConn singleton class for MongoDB connection management and data operations,
covering initialization, data saving, connection handling, and edge cases.
"""

from typing import Any, Union

import pandas as pd
from pymongo import MongoClient
import pytest
from pytest_mock import MockerFixture

from stpstone.utils.connections.databases.nosql.mongodb import MongoConn
from stpstone.utils.loggs.create_logs import CreateLog


# --------------------------
# Fixtures
# --------------------------
@pytest.fixture
def default_params() -> dict[str, Union[int, str, None]]:
    """Provide default parameters for MongoConn initialization.

    Returns
    -------
    dict[str, Union[int, str, None]]
        Dictionary with default parameters: host, port, dbname, collection, logger
    """
    return {
        "str_host": "localhost",
        "int_port": 27017,
        "str_dbname": "test",
        "str_collection": "data",
        "logger": None
    }

@pytest.fixture
def sample_dataframe() -> pd.DataFrame:
    """Provide a sample DataFrame for testing.

    Returns
    -------
    pd.DataFrame
        Sample DataFrame with test data
    """
    return pd.DataFrame({
        "name": ["Alice", "Bob", "Charlie"],
        "age": [25, 30, 35]
    })

@pytest.fixture
def mock_mongo_client(mocker: MockerFixture) -> MongoClient:
    """Mock MongoClient for testing.

    Parameters
    ----------
    mocker : MockerFixture
        Pytest-mock fixture for creating mocks

    Returns
    -------
    MongoClient
        Mocked MongoClient instance
    """
    return mocker.patch("pymongo.MongoClient")

@pytest.fixture
def mongo_conn(default_params: dict[str, Union[int, str, None]]) -> MongoConn:
    """Provide MongoConn instance with default parameters.

    Parameters
    ----------
    default_params : dict[str, Union[int, str, None]]
        Default parameters for MongoConn initialization

    Returns
    -------
    MongoConn
        Initialized MongoConn instance
    """
    MongoConn.reset_instance()
    return MongoConn(**default_params)


# --------------------------
# Tests
# --------------------------
def test_singleton_pattern(default_params: dict[str, Union[int, str, None]]) -> None:
    """Test singleton pattern implementation.

    Verifies
    --------
    - Multiple instantiations return the same instance
    - Instance attributes are set from first initialization
    - Subsequent initializations don't change attributes

    Parameters
    ----------
    default_params : dict[str, Union[int, str, None]]
        Default parameters for MongoConn initialization

    Returns
    -------
    None
    """
    instance1 = MongoConn(**default_params)
    instance2 = MongoConn(str_host="different", int_port=9999)
    assert instance1 is instance2
    assert instance2.str_host == default_params["str_host"]
    assert instance2.int_port == default_params["int_port"]

def test_init_valid_inputs(default_params: dict[str, Union[int, str, None]]) -> None:
    """Test initialization with valid inputs.

    Verifies
    --------
    - Instance attributes are correctly set
    - Types of attributes are correct
    - Connection components are initialized

    Parameters
    ----------
    default_params : dict[str, Union[int, str, None]]
        Default parameters for MongoConn initialization

    Returns
    -------
    None
    """
    MongoConn.reset_instance()
    conn = MongoConn(**default_params)
    assert conn.str_host == default_params["str_host"]
    assert conn.int_port == default_params["int_port"]
    assert conn.str_dbname == default_params["str_dbname"]
    assert conn.str_collection == default_params["str_collection"]
    assert isinstance(conn.str_host, str)
    assert isinstance(conn.int_port, int)
    assert isinstance(conn.str_dbname, str)
    assert isinstance(conn.str_collection, str)

@pytest.mark.parametrize("host", [None, "", 123, [], {}])
def test_validate_host_invalid(
    host: Any, # noqa ANN401: typing.Any is not allowed
    default_params: dict[str, Union[int, str, None]]
) -> None:
    """Test host validation with invalid inputs.

    Parameters
    ----------
    host : Any
        Invalid host values to test
    default_params : dict[str, Union[int, str, None]]
        Default parameters for MongoConn initialization

    Returns
    -------
    None
    """
    default_params["str_host"] = host
    with pytest.raises(TypeError, match="must be of type"):
        MongoConn(**default_params)

@pytest.mark.parametrize("port", [None, "123", 0, 65536, -1, 1.5])
def test_validate_port_invalid(
    port: Any, # noqa ANN401: typing.Any is not allowed
    default_params: dict[str, Union[int, str, None]]
) -> None:
    """Test port validation with invalid inputs.

    Parameters
    ----------
    port : Any
        Invalid port values to test
    default_params : dict[str, Union[int, str, None]]
        Default parameters for MongoConn initialization

    Returns
    -------
    None
    """
    default_params["int_port"] = port
    with pytest.raises(TypeError, match="must be of type"):
        MongoConn(**default_params)

@pytest.mark.parametrize("dbname", [None, "", 123, [], {}])
def test_validate_dbname_invalid(
    dbname: Any, # noqa ANN401: typing.Any is not allowed
    default_params: dict[str, Union[int, str, None]]
) -> None:
    """Test database name validation with invalid inputs.

    Parameters
    ----------
    dbname : Any
        Invalid database name values to test
    default_params : dict[str, Union[int, str, None]]
        Default parameters for MongoConn initialization

    Returns
    -------
    None
    """
    default_params["str_dbname"] = dbname
    with pytest.raises(TypeError, match="must be of type"):
        MongoConn(**default_params)

@pytest.mark.parametrize("collection", [None, "", 123, [], {}])
def test_validate_collection_invalid(
    collection: Any, # noqa ANN401: typing.Any is not allowed
    default_params: dict[str, Union[int, str, None]]
) -> None:
    """Test collection name validation with invalid inputs.

    Parameters
    ----------
    collection : Any
        Invalid collection name values to test
    default_params : dict[str, Union[int, str, None]]
        Default parameters for MongoConn initialization

    Returns
    -------
    None
    """
    default_params["str_collection"] = collection
    with pytest.raises(TypeError, match="must be of type"):
        MongoConn(**default_params)

def test_save_df_valid(
    mongo_conn: MongoConn, 
    sample_dataframe: pd.DataFrame, mocker: MockerFixture
) -> None:
    """Test saving valid DataFrame to MongoDB.

    Parameters
    ----------
    mongo_conn : MongoConn
        Initialized MongoConn instance
    sample_dataframe : pd.DataFrame
        Sample DataFrame for testing
    mocker : MockerFixture
        Pytest-mock fixture for creating mocks

    Returns
    -------
    None
    """
    mock_collection = mocker.MagicMock()
    mongo_conn._collection = mock_collection
    mongo_conn.save_df(sample_dataframe)
    mock_collection.insert_many.assert_called_once()
    assert mock_collection.insert_many.call_args[0][0] \
        == sample_dataframe.to_dict(orient="records")

@pytest.mark.parametrize("invalid_df", [None, [], {}, "string", 123])
def test_save_df_invalid_type(
    mongo_conn: MongoConn, 
    invalid_df: Any # noqa ANN401: typing.Any is not allowed
) -> None:
    """Test saving invalid DataFrame types.

    Parameters
    ----------
    mongo_conn : MongoConn
        Initialized MongoConn instance
    invalid_df : Any
        Invalid DataFrame inputs to test

    Returns
    -------
    None
    """
    with pytest.raises(TypeError, match="must be of type"):
        mongo_conn.save_df(invalid_df)

def test_save_df_empty(mongo_conn: MongoConn) -> None:
    """Test saving empty DataFrame.

    Parameters
    ----------
    mongo_conn : MongoConn
        Initialized MongoConn instance

    Returns
    -------
    None
    """
    empty_df = pd.DataFrame()
    with pytest.raises(ValueError, match="DataFrame cannot be empty"):
        mongo_conn.save_df(empty_df)

def test_save_df_insertion_failure(
    mongo_conn: MongoConn, 
    sample_dataframe: pd.DataFrame, 
    mocker: MockerFixture
) -> None:
    """Test DataFrame insertion failure handling.

    Parameters
    ----------
    mongo_conn : MongoConn
        Initialized MongoConn instance
    sample_dataframe : pd.DataFrame
        Sample DataFrame for testing
    mocker : MockerFixture
        Pytest-mock fixture for creating mocks

    Returns
    -------
    None
    """
    mock_collection = mocker.MagicMock()
    mock_collection.insert_many.side_effect = Exception("Insertion failed")
    mongo_conn._collection = mock_collection
    mock_logger = mocker.patch.object(CreateLog, "log_message")
    with pytest.raises(RuntimeError, match="Failed to insert data into MongoDB"):
        mongo_conn.save_df(sample_dataframe)
    if mongo_conn.logger:
        mock_logger.assert_called_with(
            mongo_conn.logger,
            "ERROR Insertion failed, MONGODB INJECTION ABORTED",
            "info"
        )

def test_close_connection(mongo_conn: MongoConn, mocker: MockerFixture) -> None:
    """Test closing MongoDB connection.

    Parameters
    ----------
    mongo_conn : MongoConn
        Initialized MongoConn instance
    mocker : MockerFixture
        Pytest-mock fixture for creating mocks

    Returns
    -------
    None
    """
    mock_client = mocker.MagicMock()
    mongo_conn._client = mock_client
    mongo_conn.close()
    mock_client.close.assert_called_once()

def test_context_manager(mongo_conn: MongoConn, mocker: MockerFixture) -> None:
    """Test context manager functionality.

    Parameters
    ----------
    mongo_conn : MongoConn
        Initialized MongoConn instance
    mocker : MockerFixture
        Pytest-mock fixture for creating mocks

    Returns
    -------
    None
    """
    mock_client = mocker.MagicMock()
    mongo_conn._client = mock_client
    with mongo_conn as conn:
        assert conn is mongo_conn
    mock_client.close.assert_called_once()

def test_context_manager_exception(mongo_conn: MongoConn, mocker: MockerFixture) -> None:
    """Test context manager with exception.

    Parameters
    ----------
    mongo_conn : MongoConn
        Initialized MongoConn instance
    mocker : MockerFixture
        Pytest-mock fixture for creating mocks

    Returns
    -------
    None

    Raises
    ------
    ValueError
        Expected exception
    """
    mock_client = mocker.MagicMock()
    mongo_conn._client = mock_client
    with pytest.raises(ValueError), mongo_conn:
        raise ValueError("Test error")
    mock_client.close.assert_called_once()

def test_reset_instance() -> None:
    """Test resetting singleton instance.

    Verifies
    --------
    - Instance is reset to None
    - Initialized flag is reset to False

    Returns
    -------
    None
    """
    MongoConn.reset_instance()
    assert MongoConn._instance is None
    assert MongoConn._initialized is False

def test_save_df_special_characters(mongo_conn: MongoConn, mocker: MockerFixture) -> None:
    """Test saving DataFrame with special characters.

    Parameters
    ----------
    mongo_conn : MongoConn
        Initialized MongoConn instance
    mocker : MockerFixture
        Pytest-mock fixture for creating mocks

    Returns
    -------
    None
    """
    df_ = pd.DataFrame({"name": ["😊Alice", "Bob🚀", "Charlie🌟"]})
    mock_collection = mocker.MagicMock()
    mongo_conn._collection = mock_collection
    mongo_conn.save_df(df_)
    mock_collection.insert_many.assert_called_once()
    assert mock_collection.insert_many.call_args[0][0] == df_.to_dict(orient="records")