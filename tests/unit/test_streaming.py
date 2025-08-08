"""Unit tests for streaming pipeline functionality.

Tests the stream processing pipeline with various input scenarios including:
- Normal operation with valid inputs
- Edge cases and error conditions
- Type validation and error handling
"""

from collections.abc import Iterable
from typing import Callable
from unittest.mock import Mock

import pytest

from stpstone.utils.pipelines.streaming import streaming_pipeline


# --------------------------
# Fixtures
# --------------------------
@pytest.fixture
def uppercase_func() -> Callable[[str], str]:
    """Fixture providing uppercase transformation function.
    
    Returns
    -------
    Callable[[str], str]
        Function that converts input string to uppercase
    """
    return lambda x: x.upper()


@pytest.fixture
def exclamation_func() -> Callable[[str], str]:
    """Fixture providing exclamation mark transformation function.
    
    Returns
    -------
    Callable[[str], str]
        Function that adds exclamation mark to input string
    """
    return lambda x: x + "!"


@pytest.fixture
def sample_stream() -> Iterable[str]:
    """Fixture providing sample string stream.
    
    Returns
    -------
    Iterable[str]
        Sample stream of strings ['hello', 'world']
    """
    return iter(["hello", "world"])


@pytest.fixture
def empty_stream() -> Iterable[str]:
    """Fixture providing empty stream.
    
    Returns
    -------
    Iterable[str]
        Empty stream
    """
    return iter([])


@pytest.fixture
def mock_func() -> Mock:
    """Fixture providing mock transformation function.
    
    Returns
    -------
    Mock
        Mock function that records calls
    """
    return Mock(return_value="processed")


# --------------------------
# Tests
# --------------------------
def test_normal_operation(
    sample_stream: Iterable[str],
    uppercase_func: Callable[[str], str],
    exclamation_func: Callable[[str], str],
) -> None:
    """Test pipeline with valid inputs and transformations.
    
    Verifies
    --------
    - Pipeline correctly processes each element through all functions
    - Output matches expected transformed values
    - Original generator is consumed properly
    
    Parameters
    ----------
    sample_stream : Iterable[str]
        Input stream fixture
    uppercase_func : Callable[[str], str]
        Uppercase transformation fixture
    exclamation_func : Callable[[str], str]
        Exclamation transformation fixture

    Returns
    -------
    None
    """
    processed = streaming_pipeline(
        sample_stream,
        [uppercase_func, exclamation_func]
    )
    
    results = list(processed)
    assert results == ["HELLO!", "WORLD!"]


def test_empty_stream(
    empty_stream: Iterable[str],
    uppercase_func: Callable[[str], str],
) -> None:
    """Test pipeline with empty input stream.
    
    Verifies
    --------
    - Pipeline handles empty stream without errors
    - Result is empty iterator
    
    Parameters
    ----------
    empty_stream : Iterable[str]
        Empty stream fixture
    uppercase_func : Callable[[str], str]
        Uppercase transformation fixture

    Returns
    -------
    None
    """
    processed = streaming_pipeline(empty_stream, [uppercase_func])
    assert list(processed) == []


def test_no_transformations(sample_stream: Iterable[str]) -> None:
    """Test pipeline with empty transformations list.
    
    Verifies
    --------
    - Pipeline returns original elements when no transformations provided
    - Original stream passes through unchanged
    
    Parameters
    ----------
    sample_stream : Iterable[str]
        Input stream fixture

    Returns
    -------
    None
    """
    processed = streaming_pipeline(sample_stream, [])
    assert list(processed) == ["hello", "world"]


def test_function_calls(
    sample_stream: Iterable[str],
    mock_func: Mock,
) -> None:
    """Test pipeline calls each function correctly.
    
    Verifies
    --------
    - Each transformation function is called exactly once per element
    - Functions are called in correct order
    - Arguments are passed correctly between functions
    
    Parameters
    ----------
    sample_stream : Iterable[str]
        Input stream fixture
    mock_func : Mock
        Mock transformation function fixture

    Returns
    -------
    None
    """
    processed = streaming_pipeline(sample_stream, [mock_func])
    list(processed)
    
    assert mock_func.call_count == 2
    assert mock_func.call_args_list[0][0][0] == "hello"
    assert mock_func.call_args_list[1][0][0] == "world"


def test_invalid_generator_type() -> None:
    """Test pipeline with invalid generator type.
    
    Verifies
    --------
    - TypeError is raised when generator is not Iterable[str]
    - Error message indicates expected type

    Returns
    -------
    None
    """    
    with pytest.raises(TypeError, match="must be of type"):
        streaming_pipeline(123, [])


def test_invalid_function_type() -> None:
    """Test pipeline with invalid function type.
    
    Verifies
    --------
    - TypeError is raised when functions contains non-callable
    - Error message indicates expected type

    Returns
    -------
    None
    """
    with pytest.raises(TypeError, match="must be of type"):
        streaming_pipeline(iter(["test"]), ["not a function"])


def test_function_return_type_validation(
    sample_stream: Iterable[str],
) -> None:
    """Test pipeline validates function return types.
    
    Verifies
    --------
    - TypeError is raised when function returns non-string
    - Error message indicates expected return type
    
    Parameters
    ----------
    sample_stream : Iterable[str]
        Input stream fixture

    Returns
    -------
    None
    """
    def bad_func(x: str) -> int:
        """Return a function with non-string.
        
        Parameters
        ----------
        x : str
            Input string
        
        Returns
        -------
        int
            Non-string value
        """
        return 123
    
    with pytest.raises(TypeError, match="must return a str, but got int"):
        processed = streaming_pipeline(sample_stream, [bad_func])
        list(processed)


def test_docstring_example() -> None:
    """Test the example provided in the function docstring.
    
    Verifies
    --------
    - The documented example works as shown
    - Output matches expected values

    Returns
    -------
    None
    """
    
    def to_uppercase(text: str) -> str:
        """Convert a string to uppercase.
        
        Parameters
        ----------
        text : str
            Input string
        
        Returns
        -------
        str
            Uppercase version of the input string
        """
        return text.upper()
    
    def add_exclamation(text: str) -> str:
        """Add an exclamation mark to a string.
        
        Parameters
        ----------
        text : str
            Input string
        
        Returns
        -------
        str
            String with an exclamation mark appended
        """
        return text + "!"
    
    stream = iter(['hello', 'world'])
    processed_stream = streaming_pipeline(stream, [to_uppercase, add_exclamation])
    
    results = list(processed_stream)
    assert results == ["HELLO!", "WORLD!"]