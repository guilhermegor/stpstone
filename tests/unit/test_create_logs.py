"""Unit tests for CreateLog class and associated utilities.

Tests logging functionality including:
- Folder creation and validation
- Logger configuration
- Message logging with caller context
- Timing decorators
"""

import logging
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from stpstone.utils.loggs.create_logs import CreateLog, conditional_timeit, timeit


# --------------------------
# Fixtures
# --------------------------
@pytest.fixture
def create_log_instance() -> Any: # noqa ANN401: typing.Any is not allowed
    """Fixture providing a CreateLog instance for testing.

    Returns
    -------
    Any
        Instance of CreateLog class
    """
    return CreateLog()


@pytest.fixture
def sample_logger(
    tmp_path: Any # noqa ANN401: typing.Any is not allowed
) -> logging.Logger:
    """Fixture providing a configured logger for testing.

    Parameters
    ----------
    tmp_path : Any
        Pytest temporary path fixture

    Returns
    -------
    logging.Logger
        Configured logger instance
    """
    log_file = tmp_path / "test.log"
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    handler = logging.FileHandler(log_file)
    logger.addHandler(handler)
    return logger


@pytest.fixture
def mock_time() -> MagicMock:
    """Fixture mocking time.time() for consistent timing tests.

    Returns
    -------
    MagicMock
        Mocked time.time() function
    """
    with patch("time.time") as mock:
        mock.side_effect = [100.0, 100.5]  # Start and end times
        yield mock


# --------------------------
# Test Classes
# --------------------------
class TestCreateLog:
    """Test cases for CreateLog class functionality."""

    def test_validate_path_empty(
        self, 
        create_log_instance: Any # noqa ANN401: typing.Any is not allowed
    ) -> None:
        """Test path validation with empty string.

        Verifies
        --------
        That empty path raises ValueError with appropriate message.

        Parameters
        ----------
        create_log_instance : Any
            Instance of CreateLog class
        """
        with pytest.raises(ValueError, match="Path cannot be empty"):
            create_log_instance._validate_path("")

    def test_validate_path_non_string(
        self, 
        create_log_instance: Any # noqa ANN401: typing.Any is not allowed
    ) -> None:
        """Test path validation with non-string input.

        Verifies
        --------
        That non-string path raises TypeError with appropriate message.

        Parameters
        ----------
        create_log_instance : Any
            Instance of CreateLog class
        """
        with pytest.raises(TypeError, match="must be of type"):
            create_log_instance._validate_path(123)

    def test_creating_parent_folder_new(
        self, 
        create_log_instance: Any, # noqa ANN401: typing.Any is not allowed
        tmp_path: Any # noqa ANN401: typing.Any is not allowed
    ) -> None:
        """Test folder creation for new directory.

        Verifies
        --------
        That new directory is created and returns True.

        Parameters
        ----------
        create_log_instance : Any
            Instance of CreateLog class
        tmp_path : Any
            Pytest temporary path fixture

        Returns
        -------
        None
        """
        new_dir = tmp_path / "new_dir"
        assert create_log_instance.creating_parent_folder(str(new_dir))
        assert new_dir.exists()

    def test_creating_parent_folder_exists(
        self, 
        create_log_instance: Any, # noqa ANN401: typing.Any is not allowed
        tmp_path: Any # noqa ANN401: typing.Any is not allowed
    ) -> None:
        """Test folder creation for existing directory.

        Verifies
        --------
        That existing directory returns False.

        Parameters
        ----------
        create_log_instance : Any
            Instance of CreateLog class
        tmp_path : Any
            Pytest temporary path fixture

        Returns
        -------
        None
        """
        existing_dir = tmp_path / "existing_dir"
        existing_dir.mkdir()
        assert not create_log_instance.creating_parent_folder(str(existing_dir))

    def test_basic_conf_info_level(
        self, 
        create_log_instance: Any, # noqa ANN401: typing.Any is not allowed
        tmp_path: Any # noqa ANN401: typing.Any is not allowed
    ) -> None:
        """Test logger configuration with info level.

        Verifies
        --------
        That logger is configured with INFO level.

        Parameters
        ----------
        create_log_instance : Any
            Instance of CreateLog class
        tmp_path : Any
            Pytest temporary path fixture

        Returns
        -------
        None
        """
        log_file = tmp_path / "test.log"
        logger = create_log_instance.basic_conf(str(log_file), "info")
        assert logger.level == logging.INFO
        assert log_file.exists()

    def test_basic_conf_debug_level(
        self, 
        create_log_instance: Any, # noqa ANN401: typing.Any is not allowed
        tmp_path: Any # noqa ANN401: typing.Any is not allowed
    ) -> None:
        """Test logger configuration with debug level.

        Verifies
        --------
        That logger is configured with DEBUG level.

        Parameters
        ----------
        create_log_instance : Any
            Instance of CreateLog class
        tmp_path : Any
            Pytest temporary path fixture

        Returns
        -------
        None
        """
        log_file = tmp_path / "test.log"
        logger = create_log_instance.basic_conf(str(log_file), "debug")
        assert logger.level == logging.DEBUG
        assert log_file.exists()

    def test_basic_conf_invalid_level(
        self, 
        create_log_instance: Any, # noqa ANN401: typing.Any is not allowed
        tmp_path: Any # noqa ANN401: typing.Any is not allowed
    ) -> None:
        """Test logger configuration with invalid level.

        Verifies
        --------
        That invalid level raises ValueError.

        Parameters
        ----------
        create_log_instance : Any
            Instance of CreateLog class
        tmp_path : Any
            Pytest temporary path fixture

        Returns
        -------
        None
        """
        log_file = tmp_path / "test.log"
        with pytest.raises(TypeError, match="must be one of"):
            create_log_instance.basic_conf(str(log_file), "invalid")

    def test_log_message_with_logger(
        self, 
        create_log_instance: Any, # noqa ANN401: typing.Any is not allowed
        sample_logger: logging.Logger
    ) -> None:
        """Test logging message with logger instance.

        Verifies
        --------
        That message is logged with correct format and level.

        Parameters
        ----------
        create_log_instance : Any
            Instance of CreateLog class
        sample_logger : logging.Logger
            Configured logger instance

        Returns
        -------
        None
        """
        with patch.object(sample_logger, "info") as mock_info:
            create_log_instance.log_message(sample_logger, "test message", "info")
            mock_info.assert_called_once()
            assert "[TestCreateLog.test_log_message_with_logger] test message" \
                in mock_info.call_args[0][0]

    def test_log_message_without_logger(
        self, 
        create_log_instance: Any, # noqa ANN401: typing.Any is not allowed
        capsys: Any # noqa ANN401: typing.Any is not allowed
    ) -> None:
        """Test logging message without logger (console output).

        Verifies
        --------
        That message is printed to console with correct format.

        Parameters
        ----------
        create_log_instance : Any
            Instance of CreateLog class
        capsys : Any
            Pytest capture fixture

        Returns
        -------
        None
        """
        create_log_instance.log_message(None, "test message", "info")
        captured = capsys.readouterr()
        assert "INFO {TestCreateLog} [test_log_message_without_logger] test message" \
            in captured.out

    def test_log_message_invalid_level(
        self, 
        create_log_instance: Any # noqa ANN401: typing.Any is not allowed
    ) -> None:
        """Test logging with invalid level.

        Verifies
        --------
        That invalid log level raises TypeError.

        Parameters
        ----------
        create_log_instance : Any
            Instance of CreateLog class

        Returns
        -------
        None
        """
        with pytest.raises(TypeError, match="must be one of"):
            create_log_instance.log_message(None, "test message", "invalid")

    def test_log_message_empty_level(
        self, 
        create_log_instance: Any # noqa ANN401: typing.Any is not allowed
    ) -> None:
        """Test logging with empty level.

        Verifies
        --------
        That empty log level raises TypeError.

        Parameters
        ----------
        create_log_instance : Any
            Instance of CreateLog class

        Returns
        -------
        None
        """
        with pytest.raises(TypeError, match="must be one of"):
            create_log_instance.log_message(None, "test message", "")


class TestTimeitDecorators:
    """Test cases for timeit and conditional_timeit decorators."""

    def test_timeit_decorator(self, mock_time: MagicMock) -> None:
        """Test timeit decorator measures execution time.

        Verifies
        --------
        That decorated function returns timing information.

        Parameters
        ----------
        mock_time : MagicMock
            Mocked time.time() function

        Returns
        -------
        None
        """
        @timeit
        def sample_func() -> int:
            """Sample function.
            
            Returns
            -------
            int
                42
            """
            return 42

        result = sample_func()
        assert result["result"] == 42
        assert result["execution_time_ms"] == 500.0

    def test_timeit_with_log_time(self, mock_time: MagicMock) -> None:
        """Test timeit decorator with log_time parameter.

        Verifies
        --------
        That execution time is stored in log_time dict.

        Parameters
        ----------
        mock_time : MagicMock
            Mocked time.time() function

        Returns
        -------
        None
        """
        log_time = {}

        @timeit
        def sample_func(log_time: dict = None) -> int:  # type: ignore
            """Sample function.
            
            Parameters
            ----------
            log_time : dict
                Dictionary to store execution time

            Returns
            -------
            int
                42
            """
            return 42

        sample_func(log_time=log_time)
        assert log_time["SAMPLE_FUNC"] == 500

    def test_conditional_timeit_true(self, mock_time: MagicMock) -> None:
        """Test conditional_timeit with True condition.

        Verifies
        --------
        That decorator applies timeit when condition is True.

        Parameters
        ----------
        mock_time : MagicMock
            Mocked time.time() function

        Returns
        -------
        None
        """
        @conditional_timeit(True)
        def sample_func() -> int:
            """Sample function.
            
            Returns
            -------
            int
                42
            """
            return 42

        result = sample_func()
        assert "execution_time_ms" in result

    def test_conditional_timeit_false(self) -> None:
        """Test conditional_timeit with False condition.

        Verifies
        --------
        That decorator doesn't apply timeit when condition is False.

        Parameters
        ----------
        mock_time : MagicMock
            Mocked time.time() function

        Returns
        -------
        None
        """
        @conditional_timeit(False)
        def sample_func() -> int:
            """Sample function.
            
            Returns
            -------
            int
                42
            """
            return 42

        result = sample_func()
        assert result == 42