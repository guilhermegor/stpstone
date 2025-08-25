"""Unit tests for ConditionalCacheReset class.

This module tests the ConditionalCacheReset decorator functionality, covering:
- Initialization with valid and invalid inputs
- Cache clearing based on file modification
- Edge cases and error conditions
"""

from datetime import datetime
from pathlib import Path
from typing import Any
from unittest.mock import Mock, patch

import pytest

from stpstone.utils.cache.cache_conditional import ConditionalCacheReset


# --------------------------
# Fixtures
# --------------------------
@pytest.fixture
def valid_cache_reset() -> ConditionalCacheReset:
    """Fixture providing a valid ConditionalCacheReset instance.

    Returns
    -------
    ConditionalCacheReset
        Instance initialized with valid cache_clear_method
    """
    return ConditionalCacheReset(cache_clear_method="clear_cache")


@pytest.fixture
def mock_file_path() -> str:
    """Fixture providing a mock file path.

    Returns
    -------
    str
        A temporary file path string
    """
    return "/tmp/test_file.txt"  # noqa S108: probable insecure usage of temporary file or directory


@pytest.fixture
def mock_instance() -> Any: # noqa ANN401: typing.Any is not allowed
    """Fixture providing a mock class instance with clear_cache method.

    Returns
    -------
    Any
        An instance of a class with a clear_cache method
    """

    class MockClass:
        """Mock class with clear_cache method."""

        def clear_cache(self) -> None:
            """Mock clear_cache method.

            Returns
            -------
            None
            """
            pass

    return MockClass()


# --------------------------
# Tests
# --------------------------
def test_init_valid_inputs() -> None:
    """Test initialization with valid inputs.

    Verifies
    --------
    - Instance is created with valid cache_clear_method
    - Attributes are correctly set
    - last_modified is initialized as None

    Returns
    -------
    None
    """
    decorator = ConditionalCacheReset("clear_cache", None)
    assert decorator.cache_clear_method == "clear_cache"
    assert decorator.condition_file is None
    assert decorator.last_modified is None


@pytest.mark.parametrize("cache_clear_method", ["", "  "])
def test_init_empty_cache_clear_method(cache_clear_method: str) -> None:
    """Test initialization with empty cache_clear_method.

    Verifies
    --------
    - Raises ValueError for empty or whitespace-only cache_clear_method

    Parameters
    ----------
    cache_clear_method : str
        Invalid cache clear method name to test

    Returns
    -------
    None
    """
    with pytest.raises(ValueError, match="cache_clear_method cannot be empty"):
        ConditionalCacheReset(cache_clear_method, None)


@pytest.mark.parametrize("cache_clear_method", [123, None])
def test_init_non_string_cache_clear_method(
    cache_clear_method: Any # noqa ANN401: typing.Any is not allowed
) -> None:
    """Test initialization with non-string cache_clear_method.

    Verifies
    --------
    - Raises TypeError for non-string cache_clear_method
    - Validates type enforcement

    Parameters
    ----------
    cache_clear_method : Any
        Invalid non-string cache clear method to test

    Returns
    -------
    None
    """
    with pytest.raises(TypeError, match="cache_clear_method must be of type str"):
        ConditionalCacheReset(cache_clear_method, None)


@pytest.mark.parametrize("condition_file", [123, ["not a string"]])
def test_init_non_string_condition_file(
    condition_file: Any # noqa ANN401: typing.Any is not allowed
) -> None:
    """Test initialization with non-string condition_file.

    Verifies
    --------
    - Raises TypeError for non-string condition_file
    - Validates type enforcement

    Parameters
    ----------
    condition_file : Any
        Invalid non-string condition file to test

    Returns
    -------
    None
    """
    with pytest.raises(TypeError, match="condition_file must be one of types: str, NoneType"):
        ConditionalCacheReset("clear_cache", condition_file)


def test_validate_file_path_empty(mock_file_path: str) -> None:
    """Test _validate_file_path with empty condition_file.

    Verifies
    --------
    - Raises ValueError when condition_file is empty

    Parameters
    ----------
    mock_file_path : str
        Mock file path from fixture

    Returns
    -------
    None
    """
    decorator = ConditionalCacheReset("clear_cache", "")
    with pytest.raises(ValueError, match="condition_file cannot be empty"):
        decorator._validate_file_path()


def test_validate_file_path_non_string(mock_file_path: str) -> None:
    """Test _validate_file_path with non-string condition_file.

    Verifies
    --------
    - Raises ValueError when condition_file is not a string

    Parameters
    ----------
    mock_file_path : str
        Mock file path from fixture

    Returns
    -------
    None
    """
    decorator = ConditionalCacheReset("clear_cache", None)
    decorator.condition_file = 123  # type: ignore
    with pytest.raises(ValueError, match="condition_file must be a string"):
        decorator._validate_file_path()


@patch.object(Path, "exists", return_value=False)
def test_call_nonexistent_file(
    mock_exists: Mock, 
    mock_instance: Any, # noqa ANN401: typing.Any is not allowed
    mock_file_path: str
) -> None:
    """Test decorator with nonexistent file path.

    Verifies
    --------
    - Method executes without cache clearing when file does not exist
    - No attempt to access file stats

    Parameters
    ----------
    mock_exists : Mock
        Mock for Path.exists method
    mock_instance : Any
        Mock class instance with clear_cache method
    mock_file_path : str
        Mock file path from fixture

    Returns
    -------
    None
    """
    decorator = ConditionalCacheReset("clear_cache", mock_file_path)
    mock_method = Mock(return_value="result")
    wrapped_method = decorator(mock_method)
    result = wrapped_method(mock_instance)
    assert result == "result"
    mock_exists.assert_called_once()
    mock_method.assert_called_once_with(mock_instance)


@patch.object(Path, "exists", return_value=True)
@patch.object(Path, "stat", return_value=Mock(st_mtime=1000))
def test_call_file_modified(
    mock_stat: Mock, 
    mock_exists: Mock, 
    mock_instance: Any, # noqa ANN401: typing.Any is not allowed
    mock_file_path: str
) -> None:
    """Test decorator when file is modified.

    Verifies
    --------
    - Cache is cleared when file is modified
    - last_modified is updated
    - Original method is called

    Parameters
    ----------
    mock_stat : Mock
        Mock for Path.stat method
    mock_exists : Mock
        Mock for Path.exists method
    mock_instance : Any
        Mock class instance with clear_cache method
    mock_file_path : str
        Mock file path from fixture

    Returns
    -------
    None
    """
    decorator = ConditionalCacheReset("clear_cache", mock_file_path)
    mock_method = Mock(return_value="result")
    mock_instance.clear_cache = Mock()
    wrapped_method = decorator(mock_method)
    result = wrapped_method(mock_instance)
    assert result == "result"
    mock_instance.clear_cache.assert_called_once()
    assert decorator.last_modified == datetime.fromtimestamp(1000)
    mock_method.assert_called_once_with(mock_instance)


@patch.object(Path, "exists", return_value=True)
@patch.object(Path, "stat", return_value=Mock(st_mtime=1000))
def test_call_file_not_modified(
    mock_stat: Mock, 
    mock_exists: Mock, 
    mock_instance: Any, # noqa ANN401: typing.Any is not allowed
    mock_file_path: str
) -> None:
    """Test decorator when file is not modified.

    Verifies
    --------
    - Cache is not cleared when file is not modified
    - last_modified is not updated
    - Original method is called

    Parameters
    ----------
    mock_stat : Mock
        Mock for Path.stat method
    mock_exists : Mock
        Mock for Path.exists method
    mock_instance : Any
        Mock class instance with clear_cache method
    mock_file_path : str
        Mock file path from fixture

    Returns
    -------
    None
    """
    decorator = ConditionalCacheReset("clear_cache", mock_file_path)
    decorator.last_modified = datetime.fromtimestamp(2000)
    mock_method = Mock(return_value="result")
    mock_instance.clear_cache = Mock()
    wrapped_method = decorator(mock_method)
    result = wrapped_method(mock_instance)
    assert result == "result"
    mock_instance.clear_cache.assert_not_called()
    assert decorator.last_modified == datetime.fromtimestamp(2000)
    mock_method.assert_called_once_with(mock_instance)


def test_call_no_condition_file(
    mock_instance: Any # noqa ANN401: typing.Any is not allowed
) -> None:
    """Test decorator with no condition file.

    Verifies
    --------
    - Method executes without validation or cache clearing when condition_file is None

    Parameters
    ----------
    mock_instance : Any
        Mock class instance with clear_cache method

    Returns
    -------
    None
    """
    decorator = ConditionalCacheReset("clear_cache", None)
    mock_method = Mock(return_value="result")
    wrapped_method = decorator(mock_method)
    result = wrapped_method(mock_instance)
    assert result == "result"
    mock_method.assert_called_once_with(mock_instance)


@patch.object(Path, "exists", return_value=True)
@patch.object(Path, "stat", return_value=Mock(st_mtime=1000))
def test_call_non_callable_clear_method(
    mock_stat: Mock, 
    mock_exists: Mock, 
    mock_instance: Any, # noqa ANN401: typing.Any is not allowed
    mock_file_path: str
) -> None:
    """Test decorator with non-callable clear_cache method.

    Verifies
    --------
    - Method executes without error when clear_cache is not callable
    - last_modified is still updated

    Parameters
    ----------
    mock_stat : Mock
        Mock for Path.stat method
    mock_exists : Mock
        Mock for Path.exists method
    mock_instance : Any
        Mock class instance with clear_cache method
    mock_file_path : str
        Mock file path from fixture

    Returns
    -------
    None
    """
    decorator = ConditionalCacheReset("clear_cache", mock_file_path)
    mock_method = Mock(return_value="result")
    mock_instance.clear_cache = "not_callable"
    wrapped_method = decorator(mock_method)
    result = wrapped_method(mock_instance)
    assert result == "result"
    assert decorator.last_modified == datetime.fromtimestamp(1000)
    mock_method.assert_called_once_with(mock_instance)


@patch.object(Path, "exists", side_effect=OSError)
def test_call_file_access_error(
    mock_exists: Mock, 
    mock_instance: Any, # noqa ANN401: typing.Any is not allowed
    mock_file_path: str
) -> None:
    """Test decorator with file access error.

    Verifies
    --------
    - Method executes despite file access error
    - No cache clearing occurs
    - Original method is called

    Parameters
    ----------
    mock_exists : Mock
        Mock for Path.exists method raising OSError
    mock_instance : Any
        Mock class instance with clear_cache method
    mock_file_path : str
        Mock file path from fixture

    Returns
    -------
    None
    """
    decorator = ConditionalCacheReset("clear_cache", mock_file_path)
    mock_method = Mock(return_value="result")
    mock_instance.clear_cache = Mock()
    wrapped_method = decorator(mock_method)
    result = wrapped_method(mock_instance)
    assert result == "result"
    mock_instance.clear_cache.assert_not_called()
    mock_method.assert_called_once_with(mock_instance)


def test_reload_module() -> None:
    """Test module reloading behavior.

    Verifies
    --------
    - Module can be reloaded without errors
    - ConditionalCacheReset class is still available after reload

    Returns
    -------
    None
    """
    import importlib

    import stpstone.utils.cache.cache_conditional

    importlib.reload(stpstone.utils.cache.cache_conditional)
    assert hasattr(stpstone.utils.cache.cache_conditional, "ConditionalCacheReset")