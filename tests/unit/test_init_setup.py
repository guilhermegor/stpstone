"""Unit tests for logging utilities module.

This module contains tests for the logging utilities, verifying:
- Log path validation
- Logging initialization with various path scenarios
- Error handling for invalid inputs
- Proper logging of routine start and operator information
"""

from logging import Logger
from unittest.mock import Mock

import pytest
from pytest_mock import MockerFixture

from stpstone.utils.loggs.init_setup import _validate_path_log, initiate_logging


# --------------------------
# Fixtures
# --------------------------
@pytest.fixture
def mock_logger() -> Logger:
    """Fixture providing a mock Logger instance.

    Returns
    -------
    Logger
        Mocked Logger instance for testing
    """
    return Mock(spec=Logger)


@pytest.fixture
def mock_create_log(mocker: MockerFixture) -> object:
    """Fixture mocking CreateLog class methods.

    Parameters
    ----------
    mocker : MockerFixture
        Pytest-mock fixture for creating mocks

    Returns
    -------
    object
        Mocked CreateLog instance
    """
    mock = mocker.patch("stpstone.utils.loggs.init_setup.CreateLog", autospec=True)
    instance = mock.return_value
    instance.log_message.return_value = None
    instance.creating_parent_folder.return_value = True
    mocker.patch("stpstone.utils.loggs.create_logs.os.makedirs")
    return mock


@pytest.fixture
def mock_dates_br(mocker: MockerFixture) -> object:
    """Fixture mocking DatesBR class methods.

    Parameters
    ----------
    mocker : MockerFixture
        Pytest-mock fixture for creating mocks

    Returns
    -------
    object
        Mocked DatesBR instance
    """
    mock = mocker.patch("stpstone.utils.loggs.init_setup.DatesBR")
    mock.return_value.curr_datetime.return_value = "2025-08-13 11:43:11"
    return mock


@pytest.fixture
def mock_getuser(mocker: MockerFixture) -> object:
    """Fixture mocking getuser function.

    Parameters
    ----------
    mocker : MockerFixture
        Pytest-mock fixture for creating mocks

    Returns
    -------
    object
        Mocked getuser function returning 'test_user'
    """
    return mocker.patch(
        "stpstone.utils.loggs.init_setup.getuser", 
        return_value="test_user"
    )


# --------------------------
# Tests
# --------------------------
def test_validate_path_log_empty_string() -> None:
    """Test _validate_path_log raises ValueError for empty string.

    Verifies
    --------
    That providing an empty string for path_log raises ValueError with appropriate message.

    Returns
    -------
    None
    """
    with pytest.raises(ValueError, match="Log path cannot be an empty string"):
        _validate_path_log("")


def test_validate_path_log_none() -> None:
    """Test _validate_path_log accepts None.

    Verifies
    --------
    That providing None for path_log does not raise an exception.

    Returns
    -------
    None
    """
    _validate_path_log(None)
    assert True  # no exception raised


def test_validate_path_log_type_validation(mocker: MockerFixture) -> None:
    """Test _validate_path_log type checking via type_checker decorator.

    Verifies
    --------
    That providing a non-string/non-None path_log raises TypeError with appropriate message.

    Parameters
    ----------
    mocker : MockerFixture
        Pytest-mock fixture for creating mocks

    Returns
    -------
    None
    """
    with pytest.raises(TypeError, match="path_log must be one of types: str, NoneType"):
        _validate_path_log(123)


def test_initiate_logging_no_path(
    mock_logger: Logger,
    mock_create_log: object,
    mock_dates_br: object,
    mock_getuser: object,
    mocker: MockerFixture
) -> None:
    """Test initiate_logging with no path_log provided.

    Verifies
    --------
    - Logger receives correct messages for routine start and operator
    - CreateLog.log_message called with correct arguments
    - No directory creation attempted

    Parameters
    ----------
    mock_logger : Logger
        Mocked Logger instance
    mock_create_log : object
        Mocked CreateLog instance
    mock_dates_br : object
        Mocked DatesBR instance
    mock_getuser : object
        Mocked getuser function
    mocker : MockerFixture
        Pytest-mock fixture for creating mocks

    Returns
    -------
    None
    """
    initiate_logging(mock_logger)
    mock_create_log.return_value.log_message.assert_has_calls([
        mocker.call(mock_logger, "Routine started at 2025-08-13 11:43:11", "info"),
        mocker.call(mock_logger, "Routine operator test_user", "info")
    ])


def test_initiate_logging_valid_path_success(
    mock_logger: Logger,
    mock_create_log: object,
    mock_dates_br: object,
    mock_getuser: object,
    mocker: MockerFixture
) -> None:
    """Test initiate_logging with valid path and successful directory creation.

    Verifies
    --------
    - Directory creation attempted
    - Logger receives correct messages
    - CreateLog.log_message called with correct arguments

    Parameters
    ----------
    mock_logger : Logger
        Mocked Logger instance
    mock_create_log : object
        Mocked CreateLog instance
    mock_dates_br : object
        Mocked DatesBR instance
    mock_getuser : object
        Mocked getuser function
    mocker : MockerFixture
        Pytest-mock fixture for creating mocks

    Returns
    -------
    None
    """
    mock_create_log.return_value.creating_parent_folder.return_value = True
    initiate_logging(mock_logger, path_log="/valid/path")
    mock_create_log.return_value.creating_parent_folder.assert_called_once_with("/valid/path")
    mock_create_log.return_value.log_message.assert_has_calls([
        mocker.call(mock_logger, "Logs parent directory: /valid/path", "info"),
        mocker.call(mock_logger, "Logs parent directory created successfully.", "info"),
        mocker.call(mock_logger, "Routine started at 2025-08-13 11:43:11", "info"),
        mocker.call(mock_logger, "Routine operator test_user", "info")
    ])


def test_initiate_logging_valid_path_failure(
    mock_logger: Logger,
    mock_create_log: object,
    mock_dates_br: object,
    mock_getuser: object,
    mocker: MockerFixture
) -> None:
    """Test initiate_logging with valid path but failed directory creation.

    Verifies
    --------
    - Directory creation attempted
    - Logger receives failure message
    - CreateLog.log_message called with correct arguments

    Parameters
    ----------
    mock_logger : Logger
        Mocked Logger instance
    mock_create_log : object
        Mocked CreateLog instance
    mock_dates_br : object
        Mocked DatesBR instance
    mock_getuser : object
        Mocked getuser function
    mocker : MockerFixture
        Pytest-mock fixture for creating mocks

    Returns
    -------
    None
    """
    mock_create_log.return_value.creating_parent_folder.return_value = False
    initiate_logging(mock_logger, path_log="/valid/path")
    mock_create_log.return_value.creating_parent_folder.assert_called_once_with("/valid/path")
    mock_create_log.return_value.log_message.assert_has_calls([
        mocker.call(mock_logger, "Logs parent directory: /valid/path", "info"),
        mocker.call(mock_logger, "Logs parent directory could not be created.", "info"),
        mocker.call(mock_logger, "Routine started at 2025-08-13 11:43:11", "info"),
        mocker.call(mock_logger, "Routine operator test_user", "info")
    ])


def test_initiate_logging_invalid_dispatch(
    mock_logger: Logger,
    mock_create_log: object,
    mocker: MockerFixture
) -> None:
    """Test initiate_logging with unexpected dispatch value.

    Verifies
    --------
    - Raises RuntimeError for unexpected dispatch value
    - Correct error message is included

    Parameters
    ----------
    mock_logger : Logger
        Mocked Logger instance
    mock_create_log : object
        Mocked CreateLog instance
    mocker : MockerFixture
        Pytest-mock fixture for creating mocks

    Returns
    -------
    None
    """
    mock_create_log.return_value.creating_parent_folder.return_value = "invalid"
    with pytest.raises(RuntimeError, match="Unexpected dispatch value: invalid"):
        initiate_logging(mock_logger, path_log="/valid/path")


def test_initiate_logging_type_validation_logger(
    mock_create_log: object,
    mocker: MockerFixture
) -> None:
    """Test initiate_logging type checking for logger via type_checker decorator.

    Verifies
    --------
    - TypeChecker raises TypeError for invalid logger type
    - Correct error message is included

    Parameters
    ----------
    mock_create_log : object
        Mocked CreateLog instance
    mocker : MockerFixture
        Pytest-mock fixture for creating mocks

    Returns
    -------
    None
    """
    with pytest.raises(TypeError, match="logger must be of type Logger"):
        initiate_logging("not_a_logger", path_log="/valid/path")


def test_initiate_logging_type_validation_path_log(
    mock_logger: Logger,
    mocker: MockerFixture
) -> None:
    """Test initiate_logging type checking for path_log via type_checker decorator.

    Verifies
    --------
    - TypeChecker raises TypeError for invalid path_log type
    - Correct error message is included

    Parameters
    ----------
    mock_logger : Logger
        Mocked Logger instance
    mocker : MockerFixture
        Pytest-mock fixture for creating mocks

    Returns
    -------
    None
    """
    with pytest.raises(TypeError, match="path_log must be one of types: str, NoneType"):
        initiate_logging(mock_logger, path_log=123)


def test_initiate_logging_special_characters_path(
    mock_logger: Logger,
    mock_create_log: object,
    mock_dates_br: object,
    mock_getuser: object,
    mocker: MockerFixture
) -> None:
    """Test initiate_logging with path containing special characters.

    Verifies
    --------
    - Directory creation attempted with special characters in path
    - Logger receives correct messages
    - CreateLog.log_message called with correct arguments

    Parameters
    ----------
    mock_logger : Logger
        Mocked Logger instance
    mock_create_log : object
        Mocked CreateLog instance
    mock_dates_br : object
        Mocked DatesBR instance
    mock_getuser : object
        Mocked getuser function
    mocker : MockerFixture
        Pytest-mock fixture for creating mocks

    Returns
    -------
    None
    """
    mock_create_log.return_value.creating_parent_folder.return_value = True
    special_path = "/valid/path/with_特殊字符"
    initiate_logging(mock_logger, path_log=special_path)
    mock_create_log.return_value.creating_parent_folder.assert_called_once_with(special_path)
    mock_create_log.return_value.log_message.assert_has_calls([
        mocker.call(mock_logger, f"Logs parent directory: {special_path}", "info"),
        mocker.call(mock_logger, "Logs parent directory created successfully.", "info"),
        mocker.call(mock_logger, "Routine started at 2025-08-13 11:43:11", "info"),
        mocker.call(mock_logger, "Routine operator test_user", "info")
    ])