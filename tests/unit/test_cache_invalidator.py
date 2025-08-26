"""Unit tests for CacheInvalidator class.

Tests the cache invalidation functionality with various input scenarios including:
- Initialization with valid and invalid inputs
- Cache invalidation with and without conditions
- Edge cases and error conditions
- Type validation for method parameters
"""

import importlib
import sys
from typing import Any, Callable

import pytest
from pytest_mock import MockerFixture

from stpstone.utils.cache.cache_invalidator import CacheInvalidator


# --------------------------
# Fixtures
# --------------------------
@pytest.fixture
def valid_cache_clear_method() -> str:
    """Fixture providing a valid cache clearing method name.

    Returns
    -------
    str
        Valid method name for cache clearing
    """
    return "clear_cache"

@pytest.fixture
def cache_invalidator(valid_cache_clear_method: str) -> CacheInvalidator:
    """Fixture providing a CacheInvalidator instance.

    Parameters
    ----------
    valid_cache_clear_method : str
        Valid cache clearing method name from fixture

    Returns
    -------
    CacheInvalidator
        Instance initialized with valid cache clear method
    """
    return CacheInvalidator(valid_cache_clear_method)

@pytest.fixture
def mock_instance(mocker: MockerFixture) -> object:
    """Fixture providing a mock instance with a callable clear_cache method.

    Parameters
    ----------
    mocker : MockerFixture
        Pytest-mock fixture for creating mocks

    Returns
    -------
    object
        Mock object with callable clear_cache method
    """
    instance = mocker.Mock()
    instance.clear_cache = mocker.Mock()
    return instance

@pytest.fixture
def true_condition() -> Callable[[], bool]:
    """Fixture providing a condition function that returns True.

    Returns
    -------
    Callable[[], bool]
        Function that always returns True
    """
    def condition() -> bool:
        """Condition function that always returns True.
        
        Returns
        -------
        bool
            True
        """
        return True
    return condition

@pytest.fixture
def false_condition() -> Callable[[], bool]:
    """Fixture providing a condition function that returns False.

    Returns
    -------
    Callable[[], bool]
        Function that always returns False
    """
    def condition() -> bool:
        """Condition function that always returns False.
        
        Returns
        -------
        bool
            False
        """
        return False
    return condition


# --------------------------
# Tests for __init__
# --------------------------
def test_init_valid_cache_clear_method(valid_cache_clear_method: str) -> None:
    """Test initialization with valid cache clearing method.

    Verifies
    --------
    - CacheInvalidator initializes with valid string input
    - cache_clear_method attribute is correctly set
    - Attribute maintains original value and type

    Parameters
    ----------
    valid_cache_clear_method : str
        Valid cache clearing method name from fixture

    Returns
    -------
    None
    """
    invalidator = CacheInvalidator(valid_cache_clear_method)
    assert invalidator.cache_clear_method == valid_cache_clear_method
    assert isinstance(invalidator.cache_clear_method, str)

def test_init_empty_cache_clear_method() -> None:
    """Test initialization with empty cache clearing method raises ValueError.

    Verifies
    --------
    - Empty string raises ValueError
    - Error message matches expected pattern

    Returns
    -------
    None
    """
    with pytest.raises(ValueError, match="Cache clearing method cannot be empty"):
        CacheInvalidator("")

def test_init_whitespace_cache_clear_method() -> None:
    """Test initialization with whitespace cache clearing method.

    Verifies
    --------
    - Whitespace string is accepted (current implementation behavior)
    - Attribute is correctly set

    Returns
    -------
    None
    """
    invalidator = CacheInvalidator("  ")
    assert invalidator.cache_clear_method == "  "

def test_init_non_string_cache_clear_method() -> None:
    """Test initialization with non-string cache clearing method raises TypeError.

    Verifies
    --------
    - Non-string input (e.g., None, int) raises TypeError
    - Error message contains "must be of type"

    Returns
    -------
    None
    """
    with pytest.raises(TypeError, match="must be of type"):
        CacheInvalidator(None)

# --------------------------
# Tests for _validate_cache_clear_method
# --------------------------
def test_validate_cache_clear_method_empty() -> None:
    """Test _validate_cache_clear_method with empty string raises ValueError.

    Verifies
    --------
    - Empty cache_clear_method raises ValueError
    - Error message matches expected pattern

    Returns
    -------
    None
    """
    invalidator = CacheInvalidator("clear_cache")
    with pytest.raises(ValueError, match="Cache clearing method cannot be empty"):
        invalidator._validate_cache_clear_method("")

# --------------------------
# Tests for _validate_condition
# --------------------------
def test_validate_condition_none() -> None:
    """Test _validate_condition with None input.

    Verifies
    --------
    - None condition is accepted without raising an error

    Returns
    -------
    None
    """
    invalidator = CacheInvalidator("clear_cache")
    invalidator._validate_condition(None)  # Should not raise

@pytest.mark.parametrize("invalid_condition", [123, "string", []])
def test_validate_condition_invalid(
    invalid_condition: Any # noqa ANN401: typing.Any is not allowed
) -> None:
    """Test _validate_condition with non-callable inputs raises TypeError.

    Verifies
    --------
    - Non-callable condition inputs raise TypeError (due to TypeChecker)
    - Error message matches expected pattern

    Parameters
    ----------
    invalid_condition : Any
        Invalid non-callable input to test

    Returns
    -------
    None
    """
    invalidator = CacheInvalidator("clear_cache")
    with pytest.raises(TypeError, match="condition must be one of types:"):
        invalidator._validate_condition(invalid_condition)

def test_validate_condition_callable(true_condition: Callable[[], bool]) -> None:
    """Test _validate_condition with valid callable.

    Verifies
    --------
    - Valid callable condition is accepted without raising an error

    Parameters
    ----------
    true_condition : Callable[[], bool]
        Valid condition function that returns True

    Returns
    -------
    None
    """
    invalidator = CacheInvalidator("clear_cache")
    invalidator._validate_condition(true_condition)  # Should not raise

# --------------------------
# Tests for invalidate
# --------------------------
def test_invalidate_no_condition(
    cache_invalidator: CacheInvalidator,
    mock_instance: object
) -> None:
    """Test invalidate method without condition.

    Verifies
    --------
    - Cache clearing method is called when no condition is provided
    - No errors are raised with valid instance

    Parameters
    ----------
    cache_invalidator : CacheInvalidator
        CacheInvalidator instance from fixture
    mock_instance : object
        Mock instance with callable clear_cache method

    Returns
    -------
    None
    """
    cache_invalidator.invalidate(mock_instance)
    mock_instance.clear_cache.assert_called_once()

def test_invalidate_true_condition(
    cache_invalidator: CacheInvalidator,
    mock_instance: object,
    true_condition: Callable[[], bool]
) -> None:
    """Test invalidate method with condition returning True.

    Verifies
    --------
    - Cache clearing method is called when condition returns True
    - No errors are raised with valid instance and condition

    Parameters
    ----------
    cache_invalidator : CacheInvalidator
        CacheInvalidator instance from fixture
    mock_instance : object
        Mock instance with callable clear_cache method
    true_condition : Callable[[], bool]
        Condition function that returns True

    Returns
    -------
    None
    """
    cache_invalidator.invalidate(mock_instance, true_condition)
    mock_instance.clear_cache.assert_called_once()

def test_invalidate_false_condition(
    cache_invalidator: CacheInvalidator,
    mock_instance: object,
    false_condition: Callable[[], bool]
) -> None:
    """Test invalidate method with condition returning False.

    Verifies
    --------
    - Cache clearing method is not called when condition returns False
    - No errors are raised

    Parameters
    ----------
    cache_invalidator : CacheInvalidator
        CacheInvalidator instance from fixture
    mock_instance : object
        Mock instance with callable clear_cache method
    false_condition : Callable[[], bool]
        Condition function that returns False

    Returns
    -------
    None
    """
    cache_invalidator.invalidate(mock_instance, false_condition)
    mock_instance.clear_cache.assert_not_called()

def test_invalidate_none_instance(cache_invalidator: CacheInvalidator) -> None:
    """Test invalidate with None instance raises ValueError.

    Verifies
    --------
    - Passing None as instance raises ValueError
    - Error message matches expected pattern

    Parameters
    ----------
    cache_invalidator : CacheInvalidator
        CacheInvalidator instance from fixture

    Returns
    -------
    None
    """
    with pytest.raises(ValueError, match="Instance cannot be None"):
        cache_invalidator.invalidate(None)

def test_invalidate_missing_method(
    cache_invalidator: CacheInvalidator, 
    mocker: MockerFixture
) -> None:
    """Test invalidate with instance missing cache clearing method raises AttributeError.

    Verifies
    --------
    - AttributeError is raised when cache_clear_method is not found
    - Error message matches expected pattern

    Parameters
    ----------
    cache_invalidator : CacheInvalidator
        CacheInvalidator instance from fixture
    mocker : MockerFixture
        Pytest-mock fixture for creating mocks

    Returns
    -------
    None
    """
    instance = mocker.Mock()
    instance.clear_cache = None
    with pytest.raises(
        AttributeError, 
        match="Cache-clearing method 'clear_cache' not found or not callable"
    ):
        cache_invalidator.invalidate(instance)

def test_invalidate_non_callable_method(
    cache_invalidator: CacheInvalidator, 
    mocker: MockerFixture
) -> None:
    """Test invalidate with non-callable cache clearing method raises AttributeError.

    Verifies
    --------
    - AttributeError is raised when cache_clear_method is not callable
    - Error message matches expected pattern

    Parameters
    ----------
    cache_invalidator : CacheInvalidator
        CacheInvalidator instance from fixture
    mocker : MockerFixture
        Pytest-mock fixture for creating mocks

    Returns
    -------
    None
    """
    instance = mocker.Mock()
    instance.clear_cache = "not_callable"
    with pytest.raises(
        AttributeError, 
        match="Cache-clearing method 'clear_cache' not found or not callable"
    ):
        cache_invalidator.invalidate(instance)

# --------------------------
# Tests for TypeChecker Metaclass
# --------------------------
def test_type_checker_cache_clear_method_none() -> None:
    """Test TypeChecker raises TypeError for None cache_clear_method.

    Verifies
    --------
    - TypeChecker metaclass enforces type checking on cache_clear_method
    - TypeError is raised for None input
    - Error message contains "must be of type"

    Returns
    -------
    None
    """
    with pytest.raises(TypeError, match="must be of type"):
        CacheInvalidator(None)

# --------------------------
# Edge Cases and Additional Coverage
# --------------------------
def test_invalidate_unicode_method_name() -> None:
    """Test initialization with unicode method name.

    Verifies
    --------
    - Unicode method names are accepted
    - Attribute is correctly set

    Returns
    -------
    None
    """
    unicode_method = "clear_cache_测试"
    invalidator = CacheInvalidator(unicode_method)
    assert invalidator.cache_clear_method == unicode_method

def test_invalidate_long_method_name() -> None:
    """Test initialization with very long method name.

    Verifies
    --------
    - Long method names (within Python limits) are accepted
    - Attribute is correctly set

    Returns
    -------
    None
    """
    long_method = "clear_cache_" + "x" * 80
    invalidator = CacheInvalidator(long_method)
    assert invalidator.cache_clear_method == long_method

def test_reload_module(mocker: MockerFixture) -> None:
    """Test module reloading preserves functionality.

    Verifies
    --------
    - Module can be reloaded without breaking functionality
    - CacheInvalidator still works after reload
    - Mocked instance method is called correctly

    Parameters
    ----------
    mocker : MockerFixture
        Pytest-mock fixture for creating mocks

    Returns
    -------
    None
    """
    instance = mocker.Mock()
    instance.clear_cache = mocker.Mock()
    invalidator = CacheInvalidator("clear_cache")
    importlib.reload(sys.modules["stpstone.utils.cache.cache_invalidator"])
    invalidator.invalidate(instance)
    instance.clear_cache.assert_called_once()


# --------------------------
# Tests for condition function returning non-boolean
# --------------------------
def test_invalidate_non_boolean_condition(
    cache_invalidator: CacheInvalidator, 
    mock_instance: object, 
    mocker: MockerFixture
) -> None:
    """Test invalidate with condition returning non-boolean raises ValueError.

    Verifies
    --------
    - Condition function returning non-boolean value raises ValueError
    - Error message matches expected pattern

    Parameters
    ----------
    cache_invalidator : CacheInvalidator
        CacheInvalidator instance from fixture
    mock_instance : object
        Mock instance with callable clear_cache method
    mocker : MockerFixture
        Pytest-mock fixture for creating mocks

    Returns
    -------
    None
    """
    def non_boolean_condition() -> Any: # noqa ANN401: typing.Any is not allowed
        """Non-boolean condition function.
        
        Returns
        -------
        Any
            Non-boolean value
        """
        return "not_a_boolean"
    with pytest.raises(ValueError, match="Condition function must return a boolean"):
        cache_invalidator.invalidate(mock_instance, non_boolean_condition)