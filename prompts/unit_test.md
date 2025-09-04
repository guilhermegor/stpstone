# Unit Test Generation Prompt

## Instructions

Generate comprehensive unit tests for the provided Python module using pytest. Create tests that cover normal operations, edge cases, error conditions, and type validation while adhering to the specified coding standards.

## Requirements

### Code Quality Standards
- **Line length**: Maximum 99 characters
- **Indentation**: Use tabs (4 spaces equivalent)
- **Target Python version**: 3.9+
- **Quote style**: Double quotes
- **Type hints**:
1. Avoid `Any` type hint whenever possible; use specific types
2. Avoid `typing import Dict, Tuple, List` and affiliated, please resort to primitive ones, like dict, tuple, list, which would avoid ruff linting raising warnings
3. Use from numpy.typing import NDArray, NDArray[...] (e.g. NDArray[np.float64]) instead of np.ndarray for type hints
4. Use class Return<method_name>(TypedDict) for dictionaries typing (import from typing import TypedDict)
5. Add type hints to every method, function and whenever is possible
- **Docstring format**:
1. Numpy style with 79 character line limits, Parameters/Returns/Raises/Notes/References sections
2. Include brief description of what is being tested
3. Include module description
4. Example of docstring formatting:
```python
"""Unit tests for BinaryComparator class.

Tests the binary comparison functionality with various input scenarios including:
- Initialization with valid inputs
- Comparison operations
- Edge cases and error conditions
"""

import pytest

from stpstone.analytics.arithmetic.binary_comparator import BinaryComparator


# --------------------------
# Fixtures
# --------------------------
@pytest.fixture
def comparator_a_less_than_b() -> BinaryComparator:
    """Fixture providing BinaryComparator instance where a < b.

    Returns
    -------
    BinaryComparator
        Instance initialized with a=5 and b=10
    """
    return BinaryComparator(a=5, b=10)


# --------------------------
# Tests
# --------------------------
def test_init_with_valid_inputs() -> None:
    """Test initialization with valid integer inputs.

    Verifies
    --------
    - The BinaryComparator can be initialized with integer values
    - The values are correctly stored in the instance attributes
    - The values maintain their original types and values

    Returns
    -------
    None
    """
    comparator = BinaryComparator(a=5, b=10)
    assert comparator.a == 5
    assert comparator.b == 10
```
- **Comments**: Use lowercase (not docstrings)
- **Import organization**: Follow isort with single-line imports when logical

### Test Quality Standards
- **100% code coverage**: All functions, classes and methods
- **Zero ruff violations**: Code must pass all ruff checks without warnings
- **Fallback testing**: Include tests for fallback mechanisms and error recovery
- **Reload logic**: Test module reloading scenarios when applicable
- **Non Optional variables**: check if, for variables that haven not the Optional[...] type, a TypeError is raised, due to TypeChecker metaclass usage / type_checker decorator, with a text that matches with "must be of type"
```python
"""Example of unit test for empty variable, inappropriately declared."""

"""Example of unit tests for variable validation functions."""

from typing import Optional
import pytest


def _validate_non_empty_string(data: Optional[str], param_name: str) -> None:
    """Validate that the provided data is a non-empty string.

    Parameters
    ----------
    data : Optional[str]
        The input data to validate. Can be None or string.
    param_name : str
        The name of the parameter being validated.

    Raises
    ------
    TypeError
        If `data` is not a string, is None, or is empty/whitespace-only.
    """
    if type(data) is not str or data is None or len(data.strip()) == 0:
        raise TypeError(f"{param_name} must be of type str")


def _validate_non_zero_float(data: Optional[float], param_name: str) -> None:
    """Validate that the provided data is a non-zero float.

    Parameters
    ----------
    data : Optional[float]
        The input data to validate. Can be None or float.
    param_name : str
        The name of the parameter being validated.

    Raises
    ------
    TypeError
        If `data` is not a float, is None, or equals zero.
    """
    if type(data) is not float or data is None or data == 0.0:
        raise TypeError(f"{param_name} must be of type float")


@pytest.mark.parametrize("data", [None, "", "  "])
def test_validate_non_empty_string_invalid_data(data: Optional[str]) -> None:
    """Test that invalid string inputs raise an exception.

    Parameters
    ----------
    data : Optional[str]
        Invalid values such as None, empty, or whitespace-only strings.

    Returns
    -------
    None
    """
    with pytest.raises(ValueError, match="must be of type"):
        _validate_non_empty_string(data, "input_string")


@pytest.mark.parametrize("param_name", ["input_string", "test_string", "data_string"])
def test_validate_non_empty_string_invalid_param_name(param_name: str) -> None:
    """Test that invalid param_name handling raises an exception.

    Parameters
    ----------
    param_name : str
        Various parameter names tested against invalid input.

    Returns
    -------
    None
    """
    with pytest.raises(ValueError, match="must be of type"):
        _validate_non_empty_string(None, param_name)


@pytest.mark.parametrize("data", [None, 0.0])
def test_validate_non_zero_float_invalid_data(data: Optional[float]) -> None:
    """Test that invalid float inputs raise an exception.

    Parameters
    ----------
    data : Optional[float]
        Invalid values such as None or zero.

    Returns
    -------
    None
    """
    with pytest.raises(ValueError, match="must be of type"):
        _validate_non_zero_float(data, "input_float")


@pytest.mark.parametrize("param_name", ["input_float", "test_float", "data_float"])
def test_validate_non_zero_float_invalid_param_name(param_name: str) -> None:
    """Test that invalid float param_name handling raises an exception.

    Parameters
    ----------
    param_name : str
        Various parameter names tested against invalid float input.

    Returns
    -------
    None
    """
    with pytest.raises(ValueError, match="must be of type"):
        _validate_non_zero_float(None, param_name)

```
- **Variables sanity checks**: Tests for all variables validation checks within methods/functions, like values between 0 and 1, positive, negative, shapes of arrays and so on
```python
"""Example of unit tests for sanity checks.
Please create tests for every validations.
"""
import numpy as np
from scipy.optimize import curve_fit

class NonLinearEquations:

    def optimize_curve_fit(
        self, func: callable, array_x: np.ndarray, array_y: np.ndarray
    ) -> tuple[np.ndarray, np.ndarray]:
        """
        Optimize curve fitting.

        Parameters
        ----------
        func : callable
            Function to fit
        array_x : np.ndarray
            Input feature array
        array_y : np.ndarray
            Target values

        Returns
        -------
        tuple[np.ndarray, np.ndarray]
            Tuple containing optimal parameters and covariance matrix
        """
        if not callable(func):
            raise TypeError("func must be callable")
        if not isinstance(array_x, np.ndarray) or not isinstance(array_y, np.ndarray):
            raise TypeError("Input arrays must be numpy arrays")
        if array_x.size == 0 or array_y.size == 0:
            raise ValueError("Input arrays cannot be empty")

        return curve_fit(func, xdata=array_x, ydata=array_y)

"""Test cases for optimize_curve_fit method.

This module contains unit tests for the curve fitting optimization functionality,
verifying proper handling of various input scenarios and error conditions.
"""

import pytest
import numpy as np
from numpy.typing import NDArray
from unittest.mock import patch
from typing import Any, Callable


class TestOptimizeCurveFit:
    """Test cases for optimize_curve_fit method.

    This test class verifies the behavior of the curve fitting optimization
    function with different input types and edge cases.
    """

    @pytest.fixture
    def sample_data(self) -> tuple[NDArray[np.float64], NDArray[np.float64]]:
        """Fixture providing sample data for curve fitting.

        Returns
        -------
        tuple[NDArray[np.float64], NDArray[np.float64]]
            A tuple containing two numpy arrays:
            - x: Input feature array [1, 2, 3, 4, 5]
            - y: Target values array [2.1, 3.9, 6.2, 8.1, 9.8]
        """
        x = np.array([1, 2, 3, 4, 5], dtype=np.float64)
        y = np.array([2.1, 3.9, 6.2, 8.1, 9.8], dtype=np.float64)
        return x, y

    @pytest.fixture
    def linear_func(self) -> Callable[[NDArray[np.float64], float, float], NDArray[np.float64]]:
        """Fixture providing a simple linear function for testing.

        Returns
        -------
        Callable[[NDArray[np.float64], float, float], NDArray[np.float64]]
            A linear function of form f(x) = a*x + b where:
            - x: Input array
            - a: Slope parameter
            - b: Intercept parameter
        """
        def func(x: NDArray[np.float64], a: float, b: float) -> NDArray[np.float64]:
            """Linear test function for curve fitting.

            Parameters
            ----------
            x : NDArray[np.float64]
                Input values
            a : float
                Slope parameter
            b : float
                Intercept parameter

            Returns
            -------
            NDArray[np.float64]
                Computed linear values
            """
            return a * x + b
        return func

    def test_non_callable_func(
        self, nonlinear_equations: Any, sample_data: tuple[NDArray[np.float64], NDArray[np.float64]]
    ) -> None:
        """Test raises TypeError when func is not callable.

        Verifies
        --------
        That providing a non-callable function argument raises TypeError
        with appropriate error message.

        Parameters
        ----------
        nonlinear_equations : Any
            Instance of the class containing optimize_curve_fit method
        sample_data : tuple[NDArray[np.float64], NDArray[np.float64]]
            Tuple of (x, y) test data from fixture

        Returns
        -------
        None
        """
        x, y = sample_data
        with pytest.raises(TypeError) as excinfo:
            nonlinear_equations.optimize_curve_fit("not a function", x, y)
        assert "func must be callable" in str(excinfo.value)

    def test_non_array_input_x(
        self, nonlinear_equations: Any, 
        sample_data: tuple[NDArray[np.float64], NDArray[np.float64]], 
        linear_func: Callable
    ) -> None:
        """Test raises TypeError when array_x is not numpy array.

        Verifies
        --------
        That providing a non-array input for x raises TypeError
        with appropriate error message.

        Parameters
        ----------
        nonlinear_equations : Any
            Instance of the class containing optimize_curve_fit method
        sample_data : tuple[NDArray[np.float64], NDArray[np.float64]]
            Tuple of (x, y) test data from fixture
        linear_func : Callable
            Linear test function from fixture

        Returns
        -------
        None
        """
        _, y = sample_data
        with pytest.raises(TypeError) as excinfo:
            nonlinear_equations.optimize_curve_fit(linear_func, [1, 2, 3], y)
        assert "Input arrays must be numpy arrays" in str(excinfo.value)

    def test_non_array_input_y(
        self, nonlinear_equations: Any, 
        sample_data: tuple[NDArray[np.float64], NDArray[np.float64]], 
        linear_func: Callable
    ) -> None:
        """Test raises TypeError when array_y is not numpy array.

        Verifies
        --------
        That providing a non-array input for y raises TypeError
        with appropriate error message.

        Parameters
        ----------
        nonlinear_equations : Any
            Instance of the class containing optimize_curve_fit method
        sample_data : tuple[NDArray[np.float64], NDArray[np.float64]]
            Tuple of (x, y) test data from fixture
        linear_func : Callable
            Linear test function from fixture

        Returns
        -------
        None
        """
        x, _ = sample_data
        with pytest.raises(TypeError) as excinfo:
            nonlinear_equations.optimize_curve_fit(linear_func, x, [1, 2, 3])
        assert "Input arrays must be numpy arrays" in str(excinfo.value)

    def test_empty_array_x(
        self, nonlinear_equations: Any, 
        sample_data: tuple[NDArray[np.float64], NDArray[np.float64]], 
        linear_func: Callable
    ) -> None:
        """Test raises ValueError when array_x is empty.

        Verifies
        --------
        That providing an empty x array raises ValueError
        with appropriate error message.

        Parameters
        ----------
        nonlinear_equations : Any
            Instance of the class containing optimize_curve_fit method
        sample_data : tuple[NDArray[np.float64], NDArray[np.float64]]
            Tuple of (x, y) test data from fixture
        linear_func : Callable
            Linear test function from fixture

        Returns
        -------
        None
        """
        _, y = sample_data
        with pytest.raises(ValueError, match="Input arrays cannot be empty"):
            nonlinear_equations.optimize_curve_fit(linear_func, np.array([], dtype=np.float64), y)

    def test_empty_array_y(
        self, nonlinear_equations: Any, 
        sample_data: tuple[NDArray[np.float64], NDArray[np.float64]], 
        linear_func: Callable
    ) -> None:
        """Test raises ValueError when array_y is empty.

        Verifies
        --------
        That providing an empty y array raises ValueError
        with appropriate error message.

        Parameters
        ----------
        nonlinear_equations : Any
            Instance of the class containing optimize_curve_fit method
        sample_data : tuple[NDArray[np.float64], NDArray[np.float64]]
            Tuple of (x, y) test data from fixture
        linear_func : Callable
            Linear test function from fixture

        Returns
        -------
        None
        """
        x, _ = sample_data
        with pytest.raises(ValueError, match="Input arrays cannot be empty"):
            nonlinear_equations.optimize_curve_fit(linear_func, x, np.array([], dtype=np.float64))
```

### Test Structure Template

```python

"""Unit tests for stpstone package initialization.

Tests the package's version management and path extension functionality.
"""

import importlib
from importlib.metadata import PackageNotFoundError
from pathlib import Path
import sys
from typing import Any

import pytest
from pytest_mock import MockerFixture

import stpstone


try:
    import tomllib  # Python 3.11+
except ImportError:
    import toml as tomllib  # fallback for Python ≤3.10


# --------------------------
# Module Utilities
# --------------------------
def get_package_version() -> str:
    """Get the package version from pyproject.toml.

    Returns
    -------
    str
        The version string from pyproject.toml

    Raises
    ------
    FileNotFoundError
        If pyproject.toml cannot be found
    KeyError
        If the version field is missing
    """
    pyproject_path = Path(__file__).parent.parent.parent / "pyproject.toml"

    if sys.version_info >= (3, 11):
        with open(pyproject_path, "rb") as f:
            pyproject = tomllib.load(f)
    else:
        with open(pyproject_path, encoding="utf-8") as f:
            pyproject = tomllib.load(f)

    return pyproject["tool"]["poetry"]["version"]


# --------------------------
# Fixtures
# --------------------------
@pytest.fixture
def expected_version() -> str:
    """Fixture providing the expected package version.

    Returns
    -------
    str
        The expected version string from pyproject.toml
    """
    return get_package_version()


@pytest.fixture
def mock_version_not_found(mocker: MockerFixture) -> object:
    """Mock importlib.metadata.version to raise PackageNotFoundError.

    Parameters
    ----------
    mocker : MockerFixture
        Pytest-mock fixture for creating mocks

    Returns
    -------
    object
        Mock object for importlib.metadata.version
    """
    return mocker.patch(
        "importlib.metadata.version",
        side_effect=PackageNotFoundError
    )


@pytest.fixture
def mock_metadata_success(mocker: MockerFixture, expected_version: str) -> object:
    """Mock importlib.metadata.metadata to return valid version.

    Parameters
    ----------
    mocker : MockerFixture
        Pytest-mock fixture for creating mocks
    expected_version : str
        The version string to return in the mock

    Returns
    -------
    object
        Mock object for importlib.metadata.metadata
    """
    return mocker.patch(
        "importlib.metadata.metadata",
        return_value={"version": expected_version}
    )


@pytest.fixture
def mock_metadata_not_found(mocker: MockerFixture) -> object:
    """Mock importlib.metadata.metadata to raise PackageNotFoundError.

    Parameters
    ----------
    mocker : MockerFixture
        Pytest-mock fixture for creating mocks

    Returns
    -------
    object
        Mock object for importlib.metadata.metadata
    """
    return mocker.patch(
        "importlib.metadata.metadata",
        side_effect=PackageNotFoundError
    )


@pytest.fixture
def mock_metadata_import_error(mocker: MockerFixture) -> object:
    """Mock importlib.metadata.metadata to raise ImportError.

    Parameters
    ----------
    mocker : MockerFixture
        Pytest-mock fixture for creating mocks

    Returns
    -------
    object
        Mock object for importlib.metadata.metadata
    """
    return mocker.patch(
        "importlib.metadata.metadata",
        side_effect=ImportError
    )


# --------------------------
# Tests
# --------------------------
def test_version_exists() -> None:
    """Test that __version__ exists and is a string.

    Verifies
    --------
    1. The package has a __version__ attribute
    2. The attribute is a string
    3. The string is not empty

    Returns
    -------
    None
    """
    assert hasattr(stpstone, "__version__")
    assert isinstance(stpstone.__version__, str)
    assert len(stpstone.__version__) > 0


def test_path_extension() -> None:
    """Test that __path__ is properly extended.

    Verifies
    --------
    1. The package has a __path__ attribute
    2. The attribute is a list
    3. The list is not empty

    Returns
    -------
    None
    """
    assert hasattr(stpstone, "__path__")
    assert isinstance(stpstone.__path__, list)
    assert len(stpstone.__path__) > 0


def test_version_fallback_metadata(
    mock_version_not_found: object,
    mock_metadata_success: object,
    expected_version: str,
) -> None:
    """Test version fallback to metadata when package not found.

    Verifies
    --------
    1. The package falls back to metadata when version is not found
    2. The correct version is retrieved
    3. The metadata mock was called exactly once

    Parameters
    ----------
    mock_version_not_found : object
        Mock for importlib.metadata.version that raises PackageNotFoundError
    mock_metadata_success : object
        Mock for importlib.metadata.metadata that returns valid version
    expected_version : str
        The expected version string to compare against

    Returns
    -------
    None
    """
    importlib.reload(sys.modules["stpstone"])
    assert stpstone.__version__ == expected_version
    mock_metadata_success.assert_called_once_with("stpstone")


def test_version_fallback_hardcoded(
    mock_version_not_found: object,
    mock_metadata_not_found: object,
    expected_version: str,
) -> None:
    """Test fallback to pyproject.toml when metadata is unavailable.

    Verifies
    --------
    1. The package falls back to pyproject.toml when metadata is unavailable
    2. The correct version is retrieved
    3. The metadata mock was called exactly once

    Parameters
    ----------
    mock_version_not_found : object
        Mock for importlib.metadata.version that raises PackageNotFoundError
    mock_metadata_not_found : object
        Mock for importlib.metadata.metadata that raises PackageNotFoundError
    expected_version : str
        The expected version string to compare against

    Returns
    -------
    None
    """
    importlib.reload(sys.modules["stpstone"])
    assert stpstone.__version__ == expected_version
    mock_metadata_not_found.assert_called_once_with("stpstone")


def test_version_fallback_import_error(
    mock_version_not_found: object,
    mock_metadata_import_error: object,
    expected_version: str,
) -> None:
    """Test fallback when importlib.metadata is not available.

    Verifies
    --------
    1. The package falls back to pyproject.toml when importlib.metadata raises ImportError
    2. The correct version is retrieved
    3. The metadata mock was called exactly once

    Parameters
    ----------
    mock_version_not_found : object
        Mock for importlib.metadata.version that raises PackageNotFoundError
    mock_metadata_import_error : object
        Mock for importlib.metadata.metadata that raises ImportError
    expected_version : str
        The expected version string to compare against

    Returns
    -------
    None
    """
    importlib.reload(sys.modules["stpstone"])
    assert stpstone.__version__ == expected_version
    mock_metadata_import_error.assert_called_once_with("stpstone")
```

## Test Coverage Areas

### 1. Normal Operations
- **Happy path scenarios**: Test standard use cases with valid inputs
- **Expected inputs**: Test with typical, well-formed inputs
- **Return values**: Verify correct return values and types
- **Side effects**: Check expected side effects (prints, file writes, state changes)
- **State changes**: Verify object state changes and persistence
- **Module initialization**: Test proper module setup and imports

### 2. Edge Cases
- **Boundary conditions**: Test min/max values, empty collections, single elements
- **Special values**: Test with None, 0, empty strings, whitespace, infinity
- **Large inputs**: Test with large data sets, memory limits
- **Unicode/special characters**: Test international characters, emojis, control chars
- **Case sensitivity**: Test upper/lower case variations
- **Numeric edge cases**: Test floating point precision, overflow, underflow
- **Collection edge cases**: Test empty, single-element, and large collections

### 3. Error Conditions
- **Invalid types**: Test with completely wrong data types
- **Invalid values**: Test with out-of-range, malformed, or inappropriate values
- **Missing resources**: Test when files, network, databases are unavailable
- **System errors**: Test I/O errors, memory errors, permission errors
- **Exception propagation**: Verify exceptions are properly raised and handled
- **Timeout scenarios**: Test operations that might hang or timeout
- **Resource exhaustion**: Test behavior when resources are depleted

### 4. Type Validation
- **Function signatures**: Verify parameter and return type annotations
- **Input validation**: Test runtime type checking on inputs
- **Return type verification**: Ensure returns match declared types
- **Generic type handling**: Test with List[T], Dict[K,V], Optional[T]
- **Union type support**: Test Union[str, int], Optional parameters
- **Protocol compliance**: Test structural subtyping if applicable

### 5. Fallback Logic
- **Primary method failures**: Test when main implementation fails
- **Import fallbacks**: Test behavior when optional imports fail
- **Configuration fallbacks**: Test default config when files missing
- **Version detection**: Test fallback version detection mechanisms
- **Dependency fallbacks**: Test graceful degradation without optional deps
- **Data source fallbacks**: Test switching between data sources

### 6. Reload Logic
- **Module reloading**: Test importlib.reload() behavior
- **State preservation**: Test what state survives reloads
- **Cache invalidation**: Test cache clearing during reloads
- **Patch reapplication**: Test that patches are reapplied correctly
- **Circular imports**: Test handling of circular import scenarios
- **Dependency updates**: Test behavior when dependencies change

### 7. Examples in Docstrings
- **Examples section**: Add code examples from docstrings in unit tests

### 8. Coverage Validation
- **Line coverage**: Ensure every line of code is executed
- **Branch coverage**: Test all conditional branches (if/else, try/except)
- **Function coverage**: Test all functions and methods
- **Exception coverage**: Test all exception handling paths
- **Import coverage**: Test all import statements and fallbacks

## Fast Test Execution Guidelines

### Critical Performance Requirements

#### Network Request Mocking
- **NEVER make real HTTP requests in tests** - Always mock requests.get, requests.post, etc.
- **Mock at module level** when requests are imported directly: `patch("module.requests.get")`
- **Mock external API calls** to prevent IP bans and rate limiting
- **Use fixtures for common responses** to avoid repetitive mocking

```python
# Example of proper request mocking
@pytest.fixture
def mock_requests_get(mocker: MockerFixture) -> object:
    """Mock requests.get to prevent real HTTP calls."""
    return mocker.patch("requests.get")

@pytest.fixture
def mock_response() -> Response:
    """Mock Response object with sample content."""
    response = MagicMock(spec=Response)
    response.content = b"Sample content"
    response.url = "https://example.com/test"
    response.status_code = 200
    response.raise_for_status = MagicMock()
    return response
```

#### Retry/Backoff Mechanisms
- **Always bypass retry decorators in tests** - They cause significant delays
- **Mock backoff decorators**: `patch("backoff.on_exception", lambda *args, **kwargs: lambda func: func)`
- **Mock time.sleep calls**: `patch("module.sleep")` or `patch("time.sleep")`
- **Mock rate limiting**: Disable any built-in rate limiting for tests

```python
# Example of disabling backoff in tests
def test_http_error_handling(instance, mock_requests_get):
    """Test error handling without retry delays."""
    mock_requests_get.side_effect = HTTPError("Request failed")
    
    # Mock backoff to bypass retry mechanism
    with patch("backoff.on_exception", lambda *args, **kwargs: lambda func: func):
        with pytest.raises(HTTPError, match="Request failed"):
            instance.method_with_backoff()
```

#### Time-Based Operations
- **Mock all time operations**: `time.sleep`, `datetime.now()`, `time.time()`
- **Use deterministic time values**: Fixed timestamps for consistent testing
- **Mock async operations**: Use `pytest-asyncio` with mocked async functions

```python
# Example of time mocking
@pytest.fixture
def mock_sleep(mocker: MockerFixture) -> object:
    """Mock sleep to eliminate delays."""
    return mocker.patch("time.sleep")

def test_with_time_operations(instance, mock_sleep):
    """Test operations that normally include delays."""
    result = instance.method_with_sleep()
    mock_sleep.assert_called_with(10)  # Verify sleep was called
    assert result is not None  # Test actual functionality
```

### Mocking Guidelines

#### When to Mock
- **External dependencies**: APIs, databases, file systems, network services
- **System calls**: Time, random, environment variables, system info
- **Resource-intensive operations**: Network calls, heavy computations, large file operations
- **Side effects**: To isolate the unit being tested from external changes
- **Error simulation**: To test error conditions that are hard to reproduce naturally
- **Deterministic testing**: To ensure consistent, repeatable test results

#### Fast Mocking Patterns
```python
# Mock entire modules for performance
@pytest.fixture(autouse=True)
def mock_expensive_imports(mocker: MockerFixture) -> None:
    """Auto-mock expensive operations for all tests."""
    mocker.patch("requests.get")
    mocker.patch("time.sleep")
    mocker.patch("backoff.on_exception", lambda *args, **kwargs: lambda func: func)

# Mock with realistic but fast responses
@pytest.fixture
def fast_mock_response() -> dict:
    """Provide minimal but valid response data."""
    return {"status": "success", "data": []}

# Use parametrized tests for multiple scenarios
@pytest.mark.parametrize("status_code,expected", [
    (200, "success"),
    (404, "not_found"),
    (500, "server_error"),
])
def test_multiple_status_codes(instance, mock_requests, status_code, expected):
    """Test multiple HTTP status codes efficiently."""
    mock_requests.return_value.status_code = status_code
    result = instance.handle_response()
    assert result == expected
```

## Assertion Guidelines

### Specific Assertions
- Use specific assertions over generic ones
- `assert actual == expected` over `assert actual`
- `assert len(result) == 3` over `assert result`
- Use `pytest.raises()` for exception testing
- Use `pytest.warns()` for warning testing
- Use `pytest.approx(result, abs=...) == pytest.approx(expected, abs=...)` in order to avoid raising errors due to different floating points, irrelevant significance-wise

### Common Assertion Patterns
```python
# exception testing with message matching
with pytest.raises(ValueError, match=r"specific.*pattern"):
    function_that_should_raise()

# multiple exception types
with pytest.raises((ValueError, TypeError)):
    function_that_might_raise_either()

# warning testing
with pytest.warns(UserWarning, match="deprecation"):
    deprecated_function()

# approximate comparisons
assert result == pytest.approx(expected, rel=1e-3, abs=1e-6)

# collection assertions
assert set(result) == set(expected)  # order doesn't matter
assert result == expected  # order matters
assert all(isinstance(item, int) for item in result)

# type assertions
assert isinstance(result, ExpectedType)
assert type(result) is ExactType  # exact type check
assert hasattr(result, "required_method")

# string assertions
assert "substring" in result
assert result.startswith("prefix")
assert result.endswith("suffix")
assert re.match(r"pattern", result)

# numeric assertions
assert 0 <= result <= 100
assert result > 0
assert math.isfinite(result)
assert not math.isnan(result)

# file and path assertions
assert path.exists()
assert path.is_file()
assert path.stat().st_size > 0

# mock assertions
mock_function.assert_called_once_with(expected_arg)
mock_function.assert_called_with(expected_arg)
mock_function.assert_not_called()
assert mock_function.call_count == 2
```

### Coverage-Specific Assertions
```python
# ensure all branches are tested
def test_all_branches():
    # test condition True
    result_true = function_with_condition(True)
    assert result_true == expected_true

    # test condition False
    result_false = function_with_condition(False)
    assert result_false == expected_false

# ensure all exception paths are tested
def test_all_exception_paths():
    # test normal path
    result = function_that_might_fail("valid")
    assert result == expected

    # test each exception path
    with pytest.raises(ValueError):
        function_that_might_fail("invalid_value")

    with pytest.raises(TypeError):
        function_that_might_fail(123)

# ensure loop edge cases are tested
def test_loop_edge_cases():
    # test empty loop
    result = function_with_loop([])
    assert result == empty_result

    # test single iteration
    result = function_with_loop([1])
    assert result == single_result

    # test multiple iterations
    result = function_with_loop([1, 2, 3])
    assert result == multi_result
```

## Performance Considerations

### Test Efficiency
- Use fixtures for expensive setup operations
- Mock external dependencies to speed up tests
- Use parametrized tests for similar test cases
- Avoid unnecessary file I/O in tests
- 100% code coverage achieved

### Resource Management
- Clean up resources in teardown/fixtures
- Use context managers for file operations
- Mock time-dependent operations
- Limit test data size for performance

## Advanced Mocking Techniques

### Patching Strategies
```python
# Patch at class level for multiple tests
@pytest.fixture(autouse=True)
def setup_mocks(mocker: MockerFixture) -> None:
    """Setup common mocks for all tests in this class."""
    mocker.patch("requests.get")
    mocker.patch("time.sleep")
    mocker.patch("module.expensive_function", return_value="fast_result")

# Context-specific patching
@pytest.mark.parametrize("side_effect", [
    None,  # successful case
    ConnectionError("Network error"),
    TimeoutError("Request timeout"),
])
def test_network_resilience(instance, mocker, side_effect):
    """Test network error handling with various failures."""
    mock_get = mocker.patch("requests.get")
    if side_effect:
        mock_get.side_effect = side_effect
        with pytest.raises(type(side_effect)):
            instance.fetch_data()
    else:
        mock_get.return_value.json.return_value = {"data": "success"}
        result = instance.fetch_data()
        assert result == {"data": "success"}

# Mock complex objects with spec
@pytest.fixture
def mock_database_session(mocker: MockerFixture) -> MagicMock:
    """Create a properly specified database session mock."""
    mock_session = MagicMock(spec=Session)
    mock_session.query.return_value.filter.return_value.first.return_value = None
    return mocker.patch("module.Session", return_value=mock_session)
```

### Fast Data Generation
```python
# Use minimal test data
@pytest.fixture
def minimal_test_data() -> dict:
    """Provide the smallest valid data set for testing."""
    return {
        "required_field": "value",
        "optional_list": [],
        "count": 0
    }

# Generate data efficiently
def generate_test_items(count: int = 3) -> list[dict]:
    """Generate minimal test items for performance."""
    return [{"id": i, "name": f"item_{i}"} for i in range(count)]

# Use factory functions instead of complex fixtures
def create_test_instance(**overrides) -> TestClass:
    """Factory function for test instances with overrides."""
    defaults = {"param1": "default", "param2": 0}
    defaults.update(overrides)
    return TestClass(**defaults)
```

### Error Simulation Without Delays
```python
# Fast error testing
@pytest.mark.parametrize("error_class,error_message", [
    (HTTPError, "404 Not Found"),
    (ConnectionError, "Connection refused"),
    (TimeoutError, "Request timeout"),
])
def test_error_handling_fast(instance, mocker, error_class, error_message):
    """Test various error conditions without network delays."""
    mock_request = mocker.patch("requests.get")
    mock_request.side_effect = error_class(error_message)
    
    # Mock backoff to prevent retry delays
    mocker.patch("backoff.on_exception", lambda *args, **kwargs: lambda func: func)
    
    with pytest.raises(error_class, match=error_message):
        instance.method_with_requests()

# Fast timeout simulation
def test_timeout_handling(instance, mocker):
    """Test timeout handling without actual waiting."""
    # Mock the timeout to occur immediately
    mock_request = mocker.patch("requests.get")
    mock_request.side_effect = TimeoutError("Simulated timeout")
    
    with pytest.raises(TimeoutError):
        instance.method_with_timeout()
    
    # Verify timeout was handled correctly
    mock_request.assert_called_once()
```

### Concurrent Operations Testing
```python
# Test async operations without actual delays
@pytest.mark.asyncio
async def test_async_operations(mocker):
    """Test async functionality with mocked delays."""
    # Mock asyncio.sleep to eliminate delays
    mocker.patch("asyncio.sleep")
    
    # Mock async HTTP calls
    mock_session = AsyncMock()
    mock_session.get.return_value.__aenter__.return_value.json = AsyncMock(
        return_value={"data": "test"}
    )
    
    result = await async_function_under_test()
    assert result["data"] == "test"

# Test thread-safe operations
def test_thread_safety(instance, mocker):
    """Test thread safety without actual threading delays."""
    import threading
    from unittest.mock import call
    
    mock_method = mocker.patch.object(instance, "thread_safe_method")
    threads = []
    
    # Create threads but don't add delays
    for i in range(5):
        thread = threading.Thread(target=instance.concurrent_operation, args=(i,))
        threads.append(thread)
        thread.start()
    
    # Wait for completion (should be fast with mocked operations)
    for thread in threads:
        thread.join(timeout=1)  # Fail fast if something hangs
    
    # Verify all calls were made
    assert mock_method.call_count == 5
```

### Database and File System Mocking
```python
# Fast database testing
@pytest.fixture
def mock_database_operations(mocker: MockerFixture) -> dict:
    """Mock all database operations for speed."""
    mocks = {
        "connect": mocker.patch("sqlalchemy.create_engine"),
        "session": mocker.patch("sqlalchemy.orm.sessionmaker"),
        "query": mocker.MagicMock(),
    }
    
    # Setup realistic but fast responses
    mocks["query"].filter.return_value.first.return_value = None
    mocks["query"].filter.return_value.all.return_value = []
    
    return mocks

# Fast file system testing
@pytest.fixture
def mock_filesystem(mocker: MockerFixture) -> dict:
    """Mock file system operations."""
    return {
        "open": mocker.mock_open(read_data="test content"),
        "exists": mocker.patch("pathlib.Path.exists", return_value=True),
        "mkdir": mocker.patch("pathlib.Path.mkdir"),
        "unlink": mocker.patch("pathlib.Path.unlink"),
    }

def test_file_operations(instance, mock_filesystem):
    """Test file operations without actual I/O."""
    with patch("builtins.open", mock_filesystem["open"]):
        result = instance.read_config_file()
        assert "test content" in result
    
    mock_filesystem["exists"].assert_called()
```

## Test Organization for Speed

### Group Related Tests
```python
class TestFastNetworkOperations:
    """Group network-related tests with shared mocking."""
    
    @pytest.fixture(autouse=True)
    def setup_network_mocks(self, mocker: MockerFixture) -> None:
        """Setup network mocks for all tests in this class."""
        self.mock_get = mocker.patch("requests.get")
        self.mock_post = mocker.patch("requests.post")
        self.mock_sleep = mocker.patch("time.sleep")
        
        # Default successful response
        self.mock_response = MagicMock()
        self.mock_response.status_code = 200
        self.mock_response.json.return_value = {"success": True}
        self.mock_get.return_value = self.mock_response
    
    def test_get_request(self, instance):
        """Test GET request handling."""
        result = instance.fetch_data("test_url")
        assert result["success"] is True
        self.mock_get.assert_called_once_with("test_url")
    
    def test_post_request(self, instance):
        """Test POST request handling."""
        self.mock_post.return_value = self.mock_response
        result = instance.send_data("test_url", {"key": "value"})
        assert result["success"] is True
        self.mock_post.assert_called_once()
```

### Optimize Fixture Scope
```python
# Use session-scoped fixtures for expensive setup
@pytest.fixture(scope="session")
def expensive_resource():
    """Create expensive resource once per test session."""
    # This runs once for all tests
    return create_expensive_resource()

# Use function-scoped mocks for isolation
@pytest.fixture(scope="function")
def isolated_mock(mocker: MockerFixture):
    """Create fresh mock for each test function."""
    return mocker.patch("module.function")

# Use class-scoped fixtures for related test groups
@pytest.fixture(scope="class")
def shared_test_data():
    """Share data across tests in the same class."""
    return generate_large_test_dataset()
```

### Feature to be Tested

```python
<FILL_WITH_FEATURE_IMPLEMENTED>
```