"""Unit tests for Databricks SQL connection and query execution utilities.

Tests the Databricks class functionality including initialization, connection handling,
query execution, timeout management, and error recovery scenarios.
"""

from collections.abc import Callable
import sys
from threading import Timer
from typing import Optional, Union
from unittest.mock import ANY, Mock, patch

import pandas as pd
import pyodbc as pyo
import pytest

from stpstone.utils.connections.databases.spark.databricks_dsn import Databricks


# --------------------------
# Fixtures
# --------------------------
@pytest.fixture
def valid_query() -> str:
    """Provide a valid SQL query string.

    Returns
    -------
    str
        A simple SQL query string
    """
    return "SELECT * FROM test_table"


@pytest.fixture
def valid_dsns() -> list[str]:
    """Provide a list of valid DSN strings.

    Returns
    -------
    list[str]
        List containing valid DSN names
    """
    return ["DSN1", "DSN2"]


@pytest.fixture
def mock_logger() -> Mock:
    """Provide a mock logger object.

    Returns
    -------
    Mock
        Mocked logger object for testing
    """
    return Mock()


@pytest.fixture
def databricks_instance(valid_query: str, valid_dsns: list[str], mock_logger: Mock) -> Databricks:
    """Provide a Databricks instance with valid parameters.

    Parameters
    ----------
    valid_query : str
        Valid SQL query from fixture
    valid_dsns : list[str]
        List of DSN names from fixture
    mock_logger : Mock
        Mocked logger object

    Returns
    -------
    Databricks
        Initialized Databricks instance
    """
    return Databricks(str_query=valid_query, list_dsns=valid_dsns, logger=mock_logger, 
                      max_error_attempts=3)


# --------------------------
# Tests for __init__ and _validate_init_params
# --------------------------
def test_init_valid_params(valid_query: str, valid_dsns: list[str], mock_logger: Mock) -> None:
    """Test initialization with valid parameters.

    Verifies
    --------
    - Instance is created with valid query, DSNs, logger, and max attempts
    - Attributes are correctly set
    - No exceptions are raised

    Parameters
    ----------
    valid_query : str
        Valid SQL query from fixture
    valid_dsns : list[str]
        List of DSN names from fixture
    mock_logger : Mock
        Mocked logger object

    Returns
    -------
    None
    """
    databricks = Databricks(valid_query, valid_dsns, mock_logger, 3)
    assert databricks.str_query == valid_query
    assert databricks.list_dsns == valid_dsns
    assert databricks.logger == mock_logger
    assert databricks.max_error_attempts == 3


@pytest.mark.parametrize("invalid_query", [None, "", 123, []])
def test_init_invalid_query(
    invalid_query: Optional[Union[str, int, list]], 
    valid_dsns: list[str]
) -> None:
    """Test initialization with invalid query parameter.

    Verifies
    --------
    - Raises TypeError or ValueError for None, empty string, non-string, or empty query
    - Error message contains appropriate validation message

    Parameters
    ----------
    invalid_query : Optional[Union[str, int, list]]
        Invalid query values to test
    valid_dsns : list[str]
        List of DSN names from fixture

    Returns
    -------
    None
    """
    if invalid_query == "":
        with pytest.raises(ValueError, match="str_query must be a non-empty string"):
            Databricks(str_query=invalid_query, list_dsns=valid_dsns)
    else:
        with pytest.raises(TypeError, match="str_query must be of type str"):
            Databricks(str_query=invalid_query, list_dsns=valid_dsns)


@pytest.mark.parametrize("invalid_dsns", [None, [], 123, "not_a_list"])
def test_init_invalid_dsns(
    invalid_dsns: Optional[Union[list, int, str]], 
    valid_query: str
) -> None:
    """Test initialization with invalid DSN list.

    Verifies
    --------
    - Raises TypeError or ValueError for None, empty list, or non-list DSNs
    - Error message contains appropriate validation message

    Parameters
    ----------
    invalid_dsns : Optional[Union[list, int, str]]
        Invalid DSN list values to test
    valid_query : str
        Valid SQL query from fixture

    Returns
    -------
    None
    """
    if invalid_dsns == []:
        with pytest.raises(ValueError, match="list_dsns must be a non-empty list"):
            Databricks(str_query=valid_query, list_dsns=invalid_dsns)
    else:
        with pytest.raises(TypeError, match="list_dsns must be of type list"):
            Databricks(str_query=valid_query, list_dsns=invalid_dsns)


@pytest.mark.parametrize("invalid_attempts", [0, -1, "not_an_int"])
def test_init_invalid_max_attempts(
    invalid_attempts: Union[int, str], 
    valid_query: str, 
    valid_dsns: list[str]
) -> None:
    """Test initialization with invalid max_error_attempts.

    Verifies
    --------
    - Raises TypeError or ValueError for non-positive or non-integer max attempts
    - Error message contains appropriate validation message

    Parameters
    ----------
    invalid_attempts : Union[int, str]
        Invalid max attempts values to test
    valid_query : str
        Valid SQL query from fixture
    valid_dsns : list[str]
        List of DSN names from fixture

    Returns
    -------
    None
    """
    if isinstance(invalid_attempts, int):
        with pytest.raises(ValueError, match="max_error_attempts must be a positive integer"):
            Databricks(str_query=valid_query, list_dsns=valid_dsns, 
                       max_error_attempts=invalid_attempts)
    else:
        with pytest.raises(TypeError, match="max_error_attempts must be of type int"):
            Databricks(str_query=valid_query, list_dsns=valid_dsns, 
                       max_error_attempts=invalid_attempts)


# --------------------------
# Tests for _validate_dsn_conn
# --------------------------
def test_validate_dsn_conn_valid(databricks_instance: Databricks) -> None:
    """Test DSN connection string validation with valid input.

    Verifies
    --------
    - No exception is raised for valid DSN string
    - Method executes without errors

    Parameters
    ----------
    databricks_instance : Databricks
        Databricks instance from fixture

    Returns
    -------
    None
    """
    databricks_instance._validate_dsn_conn("Valid_DSN")


@pytest.mark.parametrize("invalid_dsn", [None, "", 123, []])
def test_validate_dsn_conn_invalid(
    databricks_instance: Databricks, 
    invalid_dsn: Optional[Union[str, int, list]]
) -> None:
    """Test DSN connection string validation with invalid inputs.

    Verifies
    --------
    - Raises TypeError or ValueError for None, empty string, or non-string DSN
    - Error message contains appropriate validation message

    Parameters
    ----------
    databricks_instance : Databricks
        Databricks instance from fixture
    invalid_dsn : Optional[Union[str, int, list]]
        Invalid DSN values to test

    Returns
    -------
    None
    """
    if invalid_dsn == "":
        with pytest.raises(ValueError, match="dsn_conn must be a non-empty string"):
            databricks_instance._validate_dsn_conn(invalid_dsn)
    else:
        with pytest.raises(TypeError, match="dsn_conn must be of type str"):
            databricks_instance._validate_dsn_conn(invalid_dsn)


# --------------------------
# Tests for _validate_int_timeout
# --------------------------
def test_validate_int_timeout_valid(databricks_instance: Databricks) -> None:
    """Test timeout validation with valid input.

    Verifies
    --------
    - No exception is raised for positive integer timeout
    - Method executes without errors

    Parameters
    ----------
    databricks_instance : Databricks
        Databricks instance from fixture

    Returns
    -------
    None
    """
    databricks_instance._validate_int_timeout(100)


@pytest.mark.parametrize("invalid_timeout", [0, -1, "not_an_int", None])
def test_validate_int_timeout_invalid(
    databricks_instance: Databricks, 
    invalid_timeout: Optional[Union[int, str]]
) -> None:
    """Test timeout validation with invalid inputs.

    Verifies
    --------
    - Raises TypeError or ValueError for non-positive or non-integer timeout
    - Error message contains appropriate validation message

    Parameters
    ----------
    databricks_instance : Databricks
        Databricks instance from fixture
    invalid_timeout : Optional[Union[int, str]]
        Invalid timeout values to test

    Returns
    -------
    None
    """
    if isinstance(invalid_timeout, int):
        with pytest.raises(ValueError, match="int_timeout must be a positive integer"):
            databricks_instance._validate_int_timeout(invalid_timeout)
    else:
        with pytest.raises(TypeError, match="int_timeout must be of type int"):
            databricks_instance._validate_int_timeout(invalid_timeout)


# --------------------------
# Tests for conn_dsn_databricks
# --------------------------
@patch("pyodbc.connect")
def test_conn_dsn_databricks_success(mock_connect: Mock, databricks_instance: Databricks) -> None:
    """Test successful Databricks connection.

    Verifies
    --------
    - Connection is established with correct parameters
    - Returns pyodbc Connection object
    - No exceptions are raised

    Parameters
    ----------
    mock_connect : Mock
        Mocked pyodbc.connect method
    databricks_instance : Databricks
        Databricks instance from fixture

    Returns
    -------
    None
    """
    mock_connection = Mock(spec=pyo.Connection)
    mock_connect.return_value = mock_connection
    result = databricks_instance.conn_dsn_databricks("DSN1", int_timeout=100, bool_autocommit=True)
    assert result == mock_connection
    mock_connect.assert_called_once_with("DSN=DSN1", autocommit=True, timeout=100)


@patch("pyodbc.connect")
def test_conn_dsn_databricks_failure(mock_connect: Mock, databricks_instance: Databricks) -> None:
    """Test failed Databricks connection.

    Verifies
    --------
    - Raises pyodbc.Error when connection fails
    - Error message includes DSN name

    Parameters
    ----------
    mock_connect : Mock
        Mocked pyodbc.connect method
    databricks_instance : Databricks
        Databricks instance from fixture

    Returns
    -------
    None
    """
    mock_connect.side_effect = pyo.Error("Connection failed")
    with pytest.raises(pyo.Error, match="Failed to connect to DSN DSN1"):
        databricks_instance.conn_dsn_databricks("DSN1")


# --------------------------
# Tests for fetch_data_from_databricks
# --------------------------
@patch("pandas.read_sql")
def test_fetch_data_from_databricks_success(
    mock_read_sql: Mock, 
    databricks_instance: Databricks
) -> None:
    """Test successful query execution and data fetching.

    Verifies
    --------
    - Returns pandas DataFrame for valid connection
    - read_sql is called with correct query and connection
    - No exceptions are raised

    Parameters
    ----------
    mock_read_sql : Mock
        Mocked pandas.read_sql function
    databricks_instance : Databricks
        Databricks instance from fixture

    Returns
    -------
    None
    """
    mock_connection = Mock(spec=pyo.Connection)
    mock_df = pd.DataFrame({"col1": [1, 2, 3]})
    mock_read_sql.return_value = mock_df
    result = databricks_instance.fetch_data_from_databricks(mock_connection)
    assert result.equals(mock_df)
    mock_read_sql.assert_called_once_with(databricks_instance.str_query, mock_connection)


def test_fetch_data_from_databricks_none_connection(databricks_instance: Databricks) -> None:
    """Test fetch data with None connection.

    Verifies
    --------
    - Raises ValueError when connection is None
    - Error message contains "Connection object cannot be None"

    Parameters
    ----------
    databricks_instance : Databricks
        Databricks instance from fixture

    Returns
    -------
    None
    """
    with pytest.raises(TypeError, match="must be of type"):
        databricks_instance.fetch_data_from_databricks(None)


@patch("pandas.read_sql")
def test_fetch_data_from_databricks_sql_error(
    mock_read_sql: Mock, 
    databricks_instance: Databricks
) -> None:
    """Test query execution with SQL error.

    Verifies
    --------
    - Raises pyodbc.Error when SQL execution fails
    - Error message includes "Failed to execute query"

    Parameters
    ----------
    mock_read_sql : Mock
        Mocked pandas.read_sql function
    databricks_instance : Databricks
        Databricks instance from fixture

    Returns
    -------
    None
    """
    mock_connection = Mock(spec=pyo.Connection)
    mock_read_sql.side_effect = pyo.Error("SQL error")
    with pytest.raises(pyo.Error, match="Failed to execute query"):
        databricks_instance.fetch_data_from_databricks(mock_connection)


@patch("pandas.read_sql")
def test_fetch_data_from_databricks_unexpected_error(
    mock_read_sql: Mock, 
    databricks_instance: Databricks
) -> None:
    """Test query execution with unexpected error.

    Verifies
    --------
    - Raises ValueError for unexpected exceptions
    - Error message includes "Unexpected error during query execution"

    Parameters
    ----------
    mock_read_sql : Mock
        Mocked pandas.read_sql function
    databricks_instance : Databricks
        Databricks instance from fixture

    Returns
    -------
    None
    """
    mock_connection = Mock(spec=pyo.Connection)
    mock_read_sql.side_effect = Exception("Unexpected error")
    with pytest.raises(ValueError, match="Unexpected error during query execution"):
        databricks_instance.fetch_data_from_databricks(mock_connection)


# --------------------------
# Tests for conn_databricks
# --------------------------
@patch("stpstone.utils.loggs.create_logs.CreateLog.log_message")
@patch.object(Databricks, "conn_dsn_databricks")
@patch.object(Databricks, "fetch_data_from_databricks")
def test_conn_databricks_success(
    mock_fetch_data: Mock,
    mock_conn_dsn: Mock,
    mock_log_message: Mock,
    databricks_instance: Databricks
) -> None:
    """Test successful connection and data retrieval.

    Verifies
    --------
    - Returns DataFrame when connection and query succeed
    - Correct DSN is used
    - Timer is started and cancelled
    - Logger is not called for successful case

    Parameters
    ----------
    mock_fetch_data : Mock
        Mocked fetch_data_from_databricks method
    mock_conn_dsn : Mock
        Mocked conn_dsn_databricks method
    mock_log_message : Mock
        Mocked CreateLog.log_message method
    databricks_instance : Databricks
        Databricks instance from fixture

    Returns
    -------
    None
    """
    mock_connection = Mock(spec=pyo.Connection)
    mock_df = pd.DataFrame({"col1": [1, 2, 3]})
    mock_conn_dsn.return_value = mock_connection
    mock_fetch_data.return_value = mock_df
    with patch("threading.Timer") as mock_timer:
        result = databricks_instance.conn_databricks(int_max_wait=10)
        assert result.equals(mock_df)
        mock_conn_dsn.assert_called_once_with(databricks_instance.list_dsns[0])
        mock_fetch_data.assert_called_once_with(mock_connection)
        mock_timer.assert_called_once_with(10, ANY)
        mock_timer.return_value.start.assert_called_once()
        mock_timer.return_value.cancel.assert_called_once()
        mock_log_message.assert_not_called()


@patch("stpstone.utils.loggs.create_logs.CreateLog.log_message")
@patch.object(Databricks, "conn_dsn_databricks")
@patch.object(Databricks, "fetch_data_from_databricks")
def test_conn_databricks_timeout(
    mock_fetch_data: Mock,
    mock_conn_dsn: Mock,
    mock_log_message: Mock,
    databricks_instance: Databricks
) -> None:
    """Test connection timeout scenario.

    Verifies
    --------
    - Logs timeout warning when query times out
    - Tries next DSN after timeout
    - Returns error string if all DSNs fail
    - Logger is called with correct message

    Parameters
    ----------
    mock_fetch_data : Mock
        Mocked fetch_data_from_databricks method
    mock_conn_dsn : Mock
        Mocked conn_dsn_databricks method
    mock_log_message : Mock
        Mocked CreateLog.log_message method
    databricks_instance : Databricks
        Databricks instance from fixture

    Returns
    -------
    None
    """
    mock_connection = Mock(spec=pyo.Connection)
    mock_conn_dsn.return_value = mock_connection
    
    # Create a custom TimeoutError to match what the code expects
    class TimeoutError(Exception):
        pass
    
    # Mock the timer to immediately call the timeout handler
    def mock_timer_side_effect(timeout: int, handler: Callable) -> Timer:
        """Mock the Timer class to immediately call the timeout handler.
        
        Parameters
        ----------
        timeout : int
            Timeout duration in seconds
        handler : Callable
            Timeout handler function
        
        Returns
        -------
        Timer
            Mocked Timer object
        """
        timer_mock = Mock()
        timer_mock.start.side_effect = handler  # Call handler immediately when start() is called
        timer_mock.cancel = Mock()
        return timer_mock
    
    with patch("threading.Timer", side_effect=mock_timer_side_effect):
        result = databricks_instance.conn_databricks(int_max_wait=10)
        assert result == "CONNECTION TIMEOUT EXPIRED"
        assert mock_conn_dsn.call_count == len(databricks_instance.list_dsns)
        
        # Check that timeout-specific message was logged for each DSN
        for dsn in databricks_instance.list_dsns:
            mock_log_message.assert_any_call(
                databricks_instance.logger,
                f"Connection could not be established in the DSN {dsn} due to timeout",
                "warning"
            )
        
        # Also check the final error message
        mock_log_message.assert_any_call(
            databricks_instance.logger,
            f"Connection to Databricks could not be established in any of the DSNs, "
            "please validate the stability of the service. "
            f"list of DSNs configured: {databricks_instance.list_dsns}",
            "warning"
        )


@patch("stpstone.utils.loggs.create_logs.CreateLog.log_message")
@patch.object(Databricks, "conn_dsn_databricks")
def test_conn_databricks_connection_error(
    mock_conn_dsn: Mock,
    mock_log_message: Mock,
    databricks_instance: Databricks
) -> None:
    """Test connection failure scenario.

    Verifies
    --------
    - Logs error when connection fails
    - Tries next DSN after failure
    - Returns error string if all DSNs fail
    - Logger is called with correct message

    Parameters
    ----------
    mock_conn_dsn : Mock
        Mocked conn_dsn_databricks method
    mock_log_message : Mock
        Mocked CreateLog.log_message method
    databricks_instance : Databricks
        Databricks instance from fixture

    Returns
    -------
    None
    """
    mock_conn_dsn.side_effect = pyo.Error("Connection failed")
    result = databricks_instance.conn_databricks(int_max_wait=10)
    assert result == "CONNECTION TIMEOUT EXPIRED"
    assert mock_conn_dsn.call_count == len(databricks_instance.list_dsns)
    mock_log_message.assert_any_call(
        databricks_instance.logger,
        f"Connection could not be established in the DSN {databricks_instance.list_dsns[0]}. "
        "Error: Connection failed",
        "warning"
    )


@patch("stpstone.utils.loggs.create_logs.CreateLog.log_message")
@patch.object(Databricks, "conn_dsn_databricks")
def test_conn_databricks_kill_process(
    mock_conn_dsn: Mock,
    mock_log_message: Mock,
    databricks_instance: Databricks
) -> None:
    """Test connection failure with kill process flag.

    Verifies
    --------
    - Raises ValueError when all DSNs fail and kill_process is True
    - Error message includes DSN list
    - Logger is called with correct message

    Parameters
    ----------
    mock_conn_dsn : Mock
        Mocked conn_dsn_databricks method
    mock_log_message : Mock
        Mocked CreateLog.log_message method
    databricks_instance : Databricks
        Databricks instance from fixture

    Returns
    -------
    None
    """
    mock_conn_dsn.side_effect = pyo.Error("Connection failed")
    with pytest.raises(ValueError, match="Connection to Databricks could not be established"):
        databricks_instance.conn_databricks(int_max_wait=10, 
                                            bool_kill_process_when_databricks_down=True)
    mock_log_message.assert_any_call(
        databricks_instance.logger,
        f"Connection to Databricks could not be established in any of the DSNs, "
        "please validate the stability of the service. list of DSNs configured: "
        f"{databricks_instance.list_dsns}",
        "warning"
    )


# --------------------------
# Tests for retrieve_query_data
# --------------------------
@patch("stpstone.utils.loggs.create_logs.CreateLog.log_message")
@patch.object(Databricks, "conn_databricks")
def test_retrieve_query_data_success(
    mock_conn_databricks: Mock,
    mock_log_message: Mock,
    databricks_instance: Databricks
) -> None:
    """Test successful query data retrieval.

    Verifies
    --------
    - Returns DataFrame when connection succeeds on first attempt
    - Logger is called with attempt message
    - conn_databricks is called once

    Parameters
    ----------
    mock_conn_databricks : Mock
        Mocked conn_databricks method
    mock_log_message : Mock
        Mocked CreateLog.log_message method
    databricks_instance : Databricks
        Databricks instance from fixture

    Returns
    -------
    None
    """
    mock_df = pd.DataFrame({"col1": [1, 2, 3]})
    mock_conn_databricks.return_value = mock_df
    result = databricks_instance.retrieve_query_data(int_max_wait=10)
    assert result.equals(mock_df)
    mock_conn_databricks.assert_called_once_with(int_max_wait=10)
    mock_log_message.assert_called_once_with(
        databricks_instance.logger,
        "#0 Attempting connection with DSNs to Databricks",
        "info"
    )


@patch("stpstone.utils.loggs.create_logs.CreateLog.log_message")
@patch.object(Databricks, "conn_databricks")
def test_retrieve_query_data_retries(
    mock_conn_databricks: Mock,
    mock_log_message: Mock,
    databricks_instance: Databricks
) -> None:
    """Test query data retrieval with retries.

    Verifies
    --------
    - Retries up to max_error_attempts
    - Returns DataFrame when successful after retries
    - Logger is called for each attempt
    - conn_databricks is called correct number of times

    Parameters
    ----------
    mock_conn_databricks : Mock
        Mocked conn_databricks method
    mock_log_message : Mock
        Mocked CreateLog.log_message method
    databricks_instance : Databricks
        Databricks instance from fixture

    Returns
    -------
    None
    """
    mock_df = pd.DataFrame({"col1": [1, 2, 3]})
    mock_conn_databricks.side_effect = \
        ["CONNECTION TIMEOUT EXPIRED", "CONNECTION TIMEOUT EXPIRED", mock_df]
    result = databricks_instance.retrieve_query_data(int_max_wait=10)
    assert result.equals(mock_df)
    assert mock_conn_databricks.call_count == 3
    assert mock_log_message.call_count == 3
    mock_log_message.assert_any_call(
        databricks_instance.logger,
        "#2 Attempting connection with DSNs to Databricks",
        "info"
    )


@patch("stpstone.utils.loggs.create_logs.CreateLog.log_message")
@patch.object(Databricks, "conn_databricks")
def test_retrieve_query_data_all_fail(
    mock_conn_databricks: Mock,
    mock_log_message: Mock,
    databricks_instance: Databricks
) -> None:
    """Test query data retrieval when all retries fail.

    Verifies
    --------
    - Returns error string after max_error_attempts
    - Logger is called for each attempt
    - conn_databricks is called max_error_attempts + 1 times

    Parameters
    ----------
    mock_conn_databricks : Mock
        Mocked conn_databricks method
    mock_log_message : Mock
        Mocked CreateLog.log_message method
    databricks_instance : Databricks
        Databricks instance from fixture

    Returns
    -------
    None
    """
    mock_conn_databricks.return_value = "CONNECTION TIMEOUT EXPIRED"
    result = databricks_instance.retrieve_query_data(int_max_wait=10)
    assert result == "CONNECTION TIMEOUT EXPIRED"
    assert mock_conn_databricks.call_count == databricks_instance.max_error_attempts + 1
    assert mock_log_message.call_count == databricks_instance.max_error_attempts + 1


# --------------------------
# Tests for reload logic
# --------------------------
def test_module_reload(
    databricks_instance: Databricks, 
    valid_query: str, 
    valid_dsns: list[str]
) -> None:
    """Test module reloading behavior.

    Verifies
    --------
    - Module can be reloaded without affecting instance state
    - Instance attributes remain unchanged after reload
    - No exceptions are raised during reload

    Parameters
    ----------
    databricks_instance : Databricks
        Databricks instance from fixture
    valid_query : str
        Valid SQL query from fixture
    valid_dsns : list[str]
        List of DSN names from fixture

    Returns
    -------
    None
    """
    import importlib
    original_query = databricks_instance.str_query
    original_dsns = databricks_instance.list_dsns
    importlib.reload(sys.modules["stpstone.utils.connections.databases.spark.databricks_dsn"])
    assert databricks_instance.str_query == original_query
    assert databricks_instance.list_dsns == original_dsns
    new_instance = Databricks(valid_query, valid_dsns)
    assert new_instance.str_query == valid_query
    assert new_instance.list_dsns == valid_dsns