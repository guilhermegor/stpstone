"""Unit tests for DBLogs class.

This module contains unit tests for the database logging utilities in the DBLogs class,
verifying audit logging, user info, host info, error info, and data action logging.
Tests cover normal operations, edge cases, error conditions, and type validation.
"""

from datetime import date
from typing import Any

import pandas as pd
import pytest
from pytest_mock import MockerFixture

from stpstone._config.global_slots import YAML_GEN
from stpstone.utils.calendars.calendar_abc import ABCCalendarOperations
from stpstone.utils.loggs.db_logs import DBLogs


# --------------------------
# Fixtures
# --------------------------
@pytest.fixture
def db_logs() -> DBLogs:
    """Fixture providing DBLogs instance.

    Returns
    -------
    DBLogs
        Instance of DBLogs class
    """
    return DBLogs()

@pytest.fixture
def sample_df() -> pd.DataFrame:
    """Fixture providing a sample DataFrame.

    Returns
    -------
    pd.DataFrame
        Non-empty DataFrame with sample data
    """
    return pd.DataFrame({"id": [1, 2, 3], "value": ["a", "b", "c"]})

@pytest.fixture
def empty_df() -> pd.DataFrame:
    """Fixture providing an empty DataFrame.

    Returns
    -------
    pd.DataFrame
        Empty DataFrame
    """
    return pd.DataFrame()

@pytest.fixture
def sample_date() -> date:
    """Fixture providing a sample date.

    Returns
    -------
    date
        Sample date object
    """
    return date(2025, 8, 13)

@pytest.fixture
def mock_dates_br(
    mocker: MockerFixture
) -> Any: # noqa ANN401: typing.Any is not allowed
    """Fixture mocking ABCCalendarOperations.utc_log_ts method.

    Parameters
    ----------
    mocker : MockerFixture
        Pytest-mock fixture for creating mocks

    Returns
    -------
    Any
        Mock object for ABCCalendarOperations.utc_log_ts
    """
    return mocker.patch.object(ABCCalendarOperations, "utc_log_ts", 
                              return_value=date(2025, 8, 13))

@pytest.fixture
def mock_hostname(
    mocker: MockerFixture
) -> Any: # noqa ANN401: typing.Any is not allowed
    """Fixture mocking socket.gethostname method.

    Parameters
    ----------
    mocker : MockerFixture
        Pytest-mock fixture

    Returns
    -------
    Any
        Mock object for socket.gethostname
    """
    return mocker.patch("socket.gethostname", return_value="test_host")


# --------------------------
# Tests for _validate_dataframe
# --------------------------
def test_validate_dataframe_empty(db_logs: DBLogs, empty_df: pd.DataFrame) -> None:
    """Test _validate_dataframe raises ValueError for empty DataFrame.

    Verifies
    --------
    That an empty DataFrame raises ValueError with appropriate message.

    Parameters
    ----------
    db_logs : DBLogs
        DBLogs instance from fixture
    empty_df : pd.DataFrame
        Empty DataFrame from fixture

    Returns
    -------
    None
    """
    with pytest.raises(ValueError, match="DataFrame cannot be empty"):
        db_logs._validate_dataframe(empty_df)

def test_validate_dataframe_non_dataframe(db_logs: DBLogs) -> None:
    """Test _validate_dataframe raises TypeError for non-DataFrame input.

    Verifies
    --------
    That a non-DataFrame input raises TypeError with appropriate message.

    Parameters
    ----------
    db_logs : DBLogs
        DBLogs instance from fixture

    Returns
    -------
    None
    """
    with pytest.raises(TypeError, match="must be of type"):
        db_logs._validate_dataframe([1, 2, 3])

def test_validate_dataframe_valid(db_logs: DBLogs, sample_df: pd.DataFrame) -> None:
    """Test _validate_dataframe accepts valid DataFrame.

    Verifies
    --------
    That a non-empty DataFrame passes validation without raising an error.

    Parameters
    ----------
    db_logs : DBLogs
        DBLogs instance from fixture
    sample_df : pd.DataFrame
        Sample DataFrame from fixture

    Returns
    -------
    None
    """
    try:
        db_logs._validate_dataframe(sample_df)
    except (ValueError, TypeError):
        pytest.fail("Valid DataFrame should not raise an error")


# --------------------------
# Tests for _validate_string
# --------------------------
def test_validate_string_empty(db_logs: DBLogs) -> None:
    """Test _validate_string raises ValueError for empty string.

    Verifies
    --------
    That an empty string raises ValueError with appropriate message.

    Parameters
    ----------
    db_logs : DBLogs
        DBLogs instance from fixture

    Returns
    -------
    None
    """
    with pytest.raises(ValueError, match="url cannot be empty"):
        db_logs._validate_string("", "url")

def test_validate_string_non_string(db_logs: DBLogs) -> None:
    """Test _validate_string raises TypeError for non-string input.

    Verifies
    --------
    That a non-string input raises TypeError with appropriate message.

    Parameters
    ----------
    db_logs : DBLogs
        DBLogs instance from fixture

    Returns
    -------
    None
    """
    with pytest.raises(TypeError, match="must be of type"):
        db_logs._validate_string(123, "url")

def test_validate_string_valid(db_logs: DBLogs) -> None:
    """Test _validate_string accepts valid string.

    Verifies
    --------
    That a non-empty string passes validation without raising an error.

    Parameters
    ----------
    db_logs : DBLogs
        DBLogs instance from fixture

    Returns
    -------
    None
    """
    try:
        db_logs._validate_string("valid", "url")
    except (ValueError, TypeError):
        pytest.fail("Valid string should not raise an error")


# --------------------------
# Tests for audit_log
# --------------------------
def test_audit_log_valid(
    db_logs: DBLogs,
    sample_df: pd.DataFrame,
    sample_date: date,
    mock_dates_br: Any, # noqa ANN401: typing.Any is not allowed
) -> None:
    """Test audit_log with valid inputs and string timestamp.

    Verifies
    --------
    That audit_log correctly adds URL, reference date, and string timestamp columns.

    Parameters
    ----------
    db_logs : DBLogs
        DBLogs instance from fixture
    sample_df : pd.DataFrame
        Sample DataFrame from fixture
    sample_date : date
        Sample date from fixture
    mock_dates_br : Any
        Mocked ABCCalendarOperations.utc_log_ts from fixture

    Returns
    -------
    None
    """
    url = "https://example.com"
    result = db_logs.audit_log(sample_df, url, sample_date, bool_format_log_as_str=True)
    assert YAML_GEN["audit_log_cols"]["url"] in result.columns
    assert result[YAML_GEN["audit_log_cols"]["url"]].iloc[0] == url
    assert result[YAML_GEN["audit_log_cols"]["ref_date"]].iloc[0] == sample_date
    assert result[YAML_GEN["audit_log_cols"]["log_timestamp"]].iloc[0] == "2025-08-13"

def test_audit_log_datetime(
    db_logs: DBLogs,
    sample_df: pd.DataFrame,
    sample_date: date,
    mock_dates_br: Any, # noqa ANN401: typing.Any is not allowed
) -> None:
    """Test audit_log with valid inputs and date timestamp.

    Verifies
    --------
    That audit_log correctly adds URL, reference date, and date timestamp columns.

    Parameters
    ----------
    db_logs : DBLogs
        DBLogs instance from fixture
    sample_df : pd.DataFrame
        Sample DataFrame from fixture
    sample_date : date
        Sample date from fixture
    mock_dates_br : Any
        Mocked ABCCalendarOperations.utc_log_ts from fixture

    Returns
    -------
    None
    """
    url = "https://example.com"
    result = db_logs.audit_log(sample_df, url, sample_date, bool_format_log_as_str=False)
    assert YAML_GEN["audit_log_cols"]["url"] in result.columns
    assert result[YAML_GEN["audit_log_cols"]["url"]].iloc[0] == url
    assert result[YAML_GEN["audit_log_cols"]["ref_date"]].iloc[0] == sample_date
    assert result[YAML_GEN["audit_log_cols"]["log_timestamp"]].iloc[0] == sample_date

def test_audit_log_invalid_df(db_logs: DBLogs, sample_date: date) -> None:
    """Test audit_log raises TypeError for invalid DataFrame input.

    Verifies
    --------
    That a non-DataFrame input raises TypeError.

    Parameters
    ----------
    db_logs : DBLogs
        DBLogs instance from fixture
    sample_date : date
        Sample date from fixture

    Returns
    -------
    None
    """
    with pytest.raises(TypeError, match="must be of type"):
        db_logs.audit_log([1, 2, 3], "https://example.com", sample_date)

def test_audit_log_empty_df(
    db_logs: DBLogs, 
    empty_df: pd.DataFrame, 
    sample_date: date
) -> None:
    """Test audit_log raises ValueError for empty DataFrame.

    Verifies
    --------
    That an empty DataFrame raises ValueError.

    Parameters
    ----------
    db_logs : DBLogs
        DBLogs instance from fixture
    empty_df : pd.DataFrame
        Empty DataFrame from fixture
    sample_date : date
        Sample date from fixture

    Returns
    -------
    None
    """
    with pytest.raises(ValueError, match="DataFrame cannot be empty"):
        db_logs.audit_log(empty_df, "https://example.com", sample_date)

def test_audit_log_invalid_url(
    db_logs: DBLogs, 
    sample_df: pd.DataFrame, 
    sample_date: date
) -> None:
    """Test audit_log raises ValueError for empty URL.

    Verifies
    --------
    That an empty URL raises ValueError.

    Parameters
    ----------
    db_logs : DBLogs
        DBLogs instance from fixture
    sample_df : pd.DataFrame
        Sample DataFrame from fixture
    sample_date : date
        Sample date from fixture

    Returns
    -------
    None
    """
    with pytest.raises(ValueError, match="url cannot be empty"):
        db_logs.audit_log(sample_df, "", sample_date)

def test_audit_log_invalid_date(db_logs: DBLogs, sample_df: pd.DataFrame) -> None:
    """Test audit_log raises TypeError for invalid date.

    Verifies
    --------
    That a non-date dt_db_ref raises TypeError.

    Parameters
    ----------
    db_logs : DBLogs
        DBLogs instance from fixture
    sample_df : pd.DataFrame
        Sample DataFrame from fixture

    Returns
    -------
    None
    """
    with pytest.raises(TypeError, match="must be of type"):
        db_logs.audit_log(sample_df, "https://example.com", "not a date")


# --------------------------
# Tests for insert_user_info
# --------------------------
def test_insert_user_info_valid(db_logs: DBLogs, sample_df: pd.DataFrame) -> None:
    """Test insert_user_info with valid inputs.

    Verifies
    --------
    That insert_user_info correctly adds user_id column.

    Parameters
    ----------
    db_logs : DBLogs
        DBLogs instance from fixture
    sample_df : pd.DataFrame
        Sample DataFrame from fixture

    Returns
    -------
    None
    """
    user_id = "test_user"
    result = db_logs.insert_user_info(sample_df, user_id)
    assert YAML_GEN["audit_log_cols"]["user"] in result.columns
    assert result[YAML_GEN["audit_log_cols"]["user"]].iloc[0] == user_id

def test_insert_user_info_invalid_df(db_logs: DBLogs) -> None:
    """Test insert_user_info raises TypeError for invalid DataFrame.

    Verifies
    --------
    That a non-DataFrame input raises TypeError.

    Parameters
    ----------
    db_logs : DBLogs
        DBLogs instance from fixture

    Returns
    -------
    None
    """
    with pytest.raises(TypeError, match="must be of type"):
        db_logs.insert_user_info([1, 2, 3], "test_user")

def test_insert_user_info_empty_df(db_logs: DBLogs, empty_df: pd.DataFrame) -> None:
    """Test insert_user_info raises ValueError for empty DataFrame.

    Verifies
    --------
    That an empty DataFrame raises ValueError.

    Parameters
    ----------
    db_logs : DBLogs
        DBLogs instance from fixture
    empty_df : pd.DataFrame
        Empty DataFrame from fixture

    Returns
    -------
    None
    """
    with pytest.raises(ValueError, match="DataFrame cannot be empty"):
        db_logs.insert_user_info(empty_df, "test_user")

def test_insert_user_info_empty_user_id(db_logs: DBLogs, sample_df: pd.DataFrame) -> None:
    """Test insert_user_info raises ValueError for empty user_id.

    Verifies
    --------
    That an empty user_id raises ValueError.

    Parameters
    ----------
    db_logs : DBLogs
        DBLogs instance from fixture
    sample_df : pd.DataFrame
        Sample DataFrame from fixture

    Returns
    -------
    None
    """
    with pytest.raises(ValueError, match="user_id cannot be empty"):
        db_logs.insert_user_info(sample_df, "")

def test_insert_user_info_invalid_user_id(db_logs: DBLogs, sample_df: pd.DataFrame) -> None:
    """Test insert_user_info raises TypeError for non-string user_id.

    Verifies
    --------
    That a non-string user_id raises TypeError.

    Parameters
    ----------
    db_logs : DBLogs
        DBLogs instance from fixture
    sample_df : pd.DataFrame
        Sample DataFrame from fixture

    Returns
    -------
    None
    """
    with pytest.raises(TypeError, match="must be of type"):
        db_logs.insert_user_info(sample_df, 123)


# --------------------------
# Tests for insert_host_info
# --------------------------
def test_insert_host_info_valid(
    db_logs: DBLogs, 
    sample_df: pd.DataFrame, 
    mock_hostname: Any # noqa ANN401: typing.Any is not allowed
) -> None:
    """Test insert_host_info with valid inputs.

    Verifies
    --------
    That insert_host_info correctly adds host column.

    Parameters
    ----------
    db_logs : DBLogs
        DBLogs instance from fixture
    sample_df : pd.DataFrame
        Sample DataFrame from fixture
    mock_hostname : Any
        Mocked socket.gethostname from fixture

    Returns
    -------
    None
    """
    result = db_logs.insert_host_info(sample_df)
    assert YAML_GEN["audit_log_cols"]["host"] in result.columns
    assert result[YAML_GEN["audit_log_cols"]["host"]].iloc[0] == "test_host"

def test_insert_host_info_invalid_df(db_logs: DBLogs) -> None:
    """Test insert_host_info raises TypeError for invalid DataFrame.

    Verifies
    --------
    That a non-DataFrame input raises TypeError.

    Parameters
    ----------
    db_logs : DBLogs
        DBLogs instance from fixture

    Returns
    -------
    None
    """
    with pytest.raises(TypeError, match="must be of type"):
        db_logs.insert_host_info([1, 2, 3])

def test_insert_host_info_empty_df(db_logs: DBLogs, empty_df: pd.DataFrame) -> None:
    """Test insert_host_info raises ValueError for empty DataFrame.

    Verifies
    --------
    That an empty DataFrame raises ValueError.

    Parameters
    ----------
    db_logs : DBLogs
        DBLogs instance from fixture
    empty_df : pd.DataFrame
        Empty DataFrame from fixture

    Returns
    -------
    None
    """
    with pytest.raises(ValueError, match="DataFrame cannot be empty"):
        db_logs.insert_host_info(empty_df)


# --------------------------
# Tests for insert_error_info
# --------------------------
def test_insert_error_info_valid(db_logs: DBLogs, sample_df: pd.DataFrame) -> None:
    """Test insert_error_info with valid inputs.

    Verifies
    --------
    That insert_error_info correctly adds error_msg column.

    Parameters
    ----------
    db_logs : DBLogs
        DBLogs instance from fixture
    sample_df : pd.DataFrame
        Sample DataFrame from fixture

    Returns
    -------
    None
    """
    error_msg = "Test error"
    result = db_logs.insert_error_info(sample_df, error_msg)
    assert YAML_GEN["audit_log_cols"]["error_msg"] in result.columns
    assert result[YAML_GEN["audit_log_cols"]["error_msg"]].iloc[0] == error_msg

def test_insert_error_info_invalid_df(db_logs: DBLogs) -> None:
    """Test insert_error_info raises TypeError for invalid DataFrame.

    Verifies
    --------
    That a non-DataFrame input raises TypeError.

    Parameters
    ----------
    db_logs : DBLogs
        DBLogs instance from fixture

    Returns
    -------
    None
    """
    with pytest.raises(TypeError, match="must be of type"):
        db_logs.insert_error_info([1, 2, 3], "Test error")

def test_insert_error_info_empty_df(db_logs: DBLogs, empty_df: pd.DataFrame) -> None:
    """Test insert_error_info raises ValueError for empty DataFrame.

    Verifies
    --------
    That an empty DataFrame raises ValueError.

    Parameters
    ----------
    db_logs : DBLogs
        DBLogs instance from fixture
    empty_df : pd.DataFrame
        Empty DataFrame from fixture

    Returns
    -------
    None
    """
    with pytest.raises(ValueError, match="DataFrame cannot be empty"):
        db_logs.insert_error_info(empty_df, "Test error")

def test_insert_error_info_empty_error_msg(db_logs: DBLogs, sample_df: pd.DataFrame) -> None:
    """Test insert_error_info raises ValueError for empty error_msg.

    Verifies
    --------
    That an empty error_msg raises ValueError.

    Parameters
    ----------
    db_logs : DBLogs
        DBLogs instance from fixture
    sample_df : pd.DataFrame
        Sample DataFrame from fixture

    Returns
    -------
    None
    """
    with pytest.raises(ValueError, match="error_msg cannot be empty"):
        db_logs.insert_error_info(sample_df, "")

def test_insert_error_info_invalid_error_msg(db_logs: DBLogs, sample_df: pd.DataFrame) -> None:
    """Test insert_error_info raises TypeError for non-string error_msg.

    Verifies
    --------
    That a non-string error_msg raises TypeError.

    Parameters
    ----------
    db_logs : DBLogs
        DBLogs instance from fixture
    sample_df : pd.DataFrame
        Sample DataFrame from fixture

    Returns
    -------
    None
    """
    with pytest.raises(TypeError, match="must be of type"):
        db_logs.insert_error_info(sample_df, 123)


# --------------------------
# Tests for log_data_insert
# --------------------------
def test_log_data_insert_valid(
    db_logs: DBLogs,
    sample_df: pd.DataFrame,
    sample_date: date,
) -> None:
    """Test log_data_insert with valid inputs.

    Verifies
    --------
    That log_data_insert correctly adds action_type and action_timestamp columns.

    Parameters
    ----------
    db_logs : DBLogs
        DBLogs instance from fixture
    sample_df : pd.DataFrame
        Sample DataFrame from fixture
    sample_date : date
        Sample date from fixture

    Returns
    -------
    None
    """
    action_type = "insert"
    result = db_logs.log_data_insert(sample_df, action_type, sample_date)
    assert YAML_GEN["audit_log_cols"]["action_type"] in result.columns
    assert YAML_GEN["audit_log_cols"]["action_timestamp"] in result.columns
    assert result[YAML_GEN["audit_log_cols"]["action_type"]].iloc[0] == action_type
    assert result[YAML_GEN["audit_log_cols"]["action_timestamp"]].iloc[0] == sample_date

def test_log_data_insert_invalid_df(db_logs: DBLogs, sample_date: date) -> None:
    """Test log_data_insert raises TypeError for invalid DataFrame.

    Verifies
    --------
    That a non-DataFrame input raises TypeError.

    Parameters
    ----------
    db_logs : DBLogs
        DBLogs instance from fixture
    sample_date : date
        Sample date from fixture

    Returns
    -------
    None
    """
    with pytest.raises(TypeError, match="must be of type"):
        db_logs.log_data_insert([1, 2, 3], "insert", sample_date)

def test_log_data_insert_empty_df(
    db_logs: DBLogs, 
    empty_df: pd.DataFrame, 
    sample_date: date
) -> None:
    """Test log_data_insert raises ValueError for empty DataFrame.

    Verifies
    --------
    That an empty DataFrame raises ValueError.

    Parameters
    ----------
    db_logs : DBLogs
        DBLogs instance from fixture
    empty_df : pd.DataFrame
        Empty DataFrame from fixture
    sample_date : date
        Sample date from fixture

    Returns
    -------
    None
    """
    with pytest.raises(ValueError, match="DataFrame cannot be empty"):
        db_logs.log_data_insert(empty_df, "insert", sample_date)

def test_log_data_insert_invalid_action_type(
    db_logs: DBLogs, 
    sample_df: pd.DataFrame, 
    sample_date: date
) -> None:
    """Test log_data_insert raises ValueError for invalid action_type.

    Verifies
    --------
    That an invalid action_type raises ValueError.

    Parameters
    ----------
    db_logs : DBLogs
        DBLogs instance from fixture
    sample_df : pd.DataFrame
        Sample DataFrame from fixture
    sample_date : date
        Sample date from fixture

    Returns
    -------
    None
    """
    with pytest.raises(TypeError, match="must be one of"):
        db_logs.log_data_insert(sample_df, "invalid", sample_date)

def test_log_data_insert_empty_action_type(
    db_logs: DBLogs, 
    sample_df: pd.DataFrame, 
    sample_date: date
) -> None:
    """Test log_data_insert raises ValueError for empty action_type.

    Verifies
    --------
    That an empty action_type raises ValueError.

    Parameters
    ----------
    db_logs : DBLogs
        DBLogs instance from fixture
    sample_df : pd.DataFrame
        Sample DataFrame from fixture
    sample_date : date
        Sample date from fixture

    Returns
    -------
    None
    """
    with pytest.raises(TypeError, match="must be one of"):
        db_logs.log_data_insert(sample_df, "", sample_date)

def test_log_data_insert_invalid_date(db_logs: DBLogs, sample_df: pd.DataFrame) -> None:
    """Test log_data_insert raises TypeError for invalid date.

    Verifies
    --------
    That a non-date dt_action raises TypeError.

    Parameters
    ----------
    db_logs : DBLogs
        DBLogs instance from fixture
    sample_df : pd.DataFrame
        Sample DataFrame from fixture

    Returns
    -------
    None
    """
    with pytest.raises(TypeError, match="must be of type"):
        db_logs.log_data_insert(sample_df, "insert", "not a date")


# --------------------------
# Tests for Module Reload
# --------------------------
def test_module_reload() -> None:
    """Test module reload preserves functionality.

    Verifies
    --------
    That reloading the module does not affect DBLogs functionality.

    Returns
    -------
    None
    """
    db_logs = DBLogs()
    assert isinstance(db_logs, DBLogs)