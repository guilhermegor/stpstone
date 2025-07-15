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
3. Add type hints to every method, function and whenever is possible
- **Docstring format**:
1. NumPy style
2. Include brief description of what is being tested
3. Include module description
- **Comments**: Use lowercase (not docstrings)
- **Import organization**: Follow isort with single-line imports when logical

### Test Quality Standards
- **100% code coverage**: All lines, branches, and edge cases must be tested
- **Zero ruff violations**: Code must pass all ruff checks without warnings
- **Fallback testing**: Include tests for fallback mechanisms and error recovery
- **Reload logic**: Test module reloading scenarios when applicable

### Feature to be Tested

```python

<FILL_WITH_FEATURE_IMPLEMENTED>
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
    """Fixture providing the expected package version."""
    return get_package_version()


@pytest.fixture
def mock_version_not_found(mocker: MockerFixture) -> type[Any]:
    """Mock importlib.metadata.version to raise PackageNotFoundError."""
    return mocker.patch(
        "importlib.metadata.version",
        side_effect=PackageNotFoundError
    )


@pytest.fixture
def mock_metadata_success(mocker: MockerFixture, expected_version: str) -> type[Any]:
    """Mock importlib.metadata.metadata to return valid version."""
    return mocker.patch(
        "importlib.metadata.metadata",
        return_value={"version": expected_version}
    )


@pytest.fixture
def mock_metadata_not_found(mocker: MockerFixture) -> type[Any]:
    """Mock importlib.metadata.metadata to raise PackageNotFoundError."""
    return mocker.patch(
        "importlib.metadata.metadata",
        side_effect=PackageNotFoundError
    )


@pytest.fixture
def mock_metadata_import_error(mocker: MockerFixture) -> type[Any]:
    """Mock importlib.metadata.metadata to raise ImportError."""
    return mocker.patch(
        "importlib.metadata.metadata",
        side_effect=ImportError
    )


# --------------------------
# Tests
# --------------------------
def test_version_exists() -> None:
    """Test that __version__ exists and is a string."""
    assert hasattr(stpstone, "__version__")
    assert isinstance(stpstone.__version__, str)
    assert len(stpstone.__version__) > 0


def test_path_extension() -> None:
    """Test that __path__ is properly extended."""
    assert hasattr(stpstone, "__path__")
    assert isinstance(stpstone.__path__, list)
    assert len(stpstone.__path__) > 0


def test_version_fallback_metadata(
    mock_version_not_found: type[Any],
    mock_metadata_success: type[Any],
    expected_version: str,
) -> None:
    """Test version fallback to metadata when package not found."""
    importlib.reload(sys.modules["stpstone"])
    assert stpstone.__version__ == expected_version
    mock_metadata_success.assert_called_once_with("stpstone")


def test_version_fallback_hardcoded(
    mock_version_not_found: type[Any],
    mock_metadata_not_found: type[Any],
    expected_version: str,
) -> None:
    """Test fallback to pyproject.toml when metadata is unavailable."""
    importlib.reload(sys.modules["stpstone"])
    assert stpstone.__version__ == expected_version
    mock_metadata_not_found.assert_called_once_with("stpstone")


def test_version_fallback_import_error(
    mock_version_not_found: type[Any],
    mock_metadata_import_error: type[Any],
    expected_version: str,
) -> None:
    """Test fallback when importlib.metadata is not available."""
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

### 8. Coverage Validation
- **Line coverage**: Ensure every line of code is executed
- **Branch coverage**: Test all conditional branches (if/else, try/except)
- **Function coverage**: Test all functions and methods
- **Exception coverage**: Test all exception handling paths
- **Import coverage**: Test all import statements and fallbacks

## Mocking Guidelines

### When to Mock
- **External dependencies**: APIs, databases, file systems, network services
- **System calls**: Time, random, environment variables, system info
- **Resource-intensive operations**: Network calls, heavy computations, large file operations
- **Side effects**: To isolate the unit being tested from external changes
- **Error simulation**: To test error conditions that are hard to reproduce naturally
- **Deterministic testing**: To ensure consistent, repeatable test results


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
