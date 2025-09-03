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
    """Mock importlib.metadata.version to raise PackageNotFoundError.

    Parameters
    ----------
    mocker : MockerFixture
        Pytest-mock fixture for creating mocks
    
    Returns
    -------
    type[Any]
        Mocked version function
    """
    return mocker.patch(
        "importlib.metadata.version", 
        side_effect=PackageNotFoundError
    )


@pytest.fixture
def mock_metadata_success(mocker: MockerFixture, expected_version: str) -> type[Any]:
    """Mock importlib.metadata.metadata to return valid version.
    
    Parameters
    ----------
    mocker : MockerFixture
        Pytest-mock fixture for creating mocks
    expected_version : str
        The expected version string to return
    
    Returns
    -------
    type[Any]
        Mocked metadata function
    """
    return mocker.patch(
        "importlib.metadata.metadata",
        return_value={"version": expected_version}
    )


@pytest.fixture
def mock_metadata_not_found(mocker: MockerFixture) -> type[Any]:
    """Mock importlib.metadata.metadata to raise PackageNotFoundError.
    
    Parameters
    ----------
    mocker : MockerFixture
        Pytest-mock fixture for creating mocks
    
    Returns
    -------
    type[Any]
        Mocked metadata function
    """
    return mocker.patch(
        "importlib.metadata.metadata",
        side_effect=PackageNotFoundError
    )


@pytest.fixture
def mock_metadata_import_error(mocker: MockerFixture) -> type[Any]:
    """Mock importlib.metadata.metadata to raise ImportError.
    
    Parameters
    ----------
    mocker : MockerFixture
        Pytest-mock fixture for creating mocks
    
    Returns
    -------
    type[Any]
        Mocked metadata function
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
    
    Returns
    -------
    None
    """
    assert hasattr(stpstone, "__version__")
    assert isinstance(stpstone.__version__, str)
    assert len(stpstone.__version__) > 0


def test_path_extension() -> None:
    """Test that __path__ is properly extended.
    
    Returns
    -------
    None
    """
    assert hasattr(stpstone, "__path__")
    assert isinstance(stpstone.__path__, list)
    assert len(stpstone.__path__) > 0


def test_version_fallback_metadata(
    mock_version_not_found: type[Any],
    mock_metadata_success: type[Any],
    expected_version: str,
) -> None:
    """Test version fallback to metadata when package not found.
    
    Parameters
    ----------
    mock_version_not_found : type[Any]
        Mocked version function
    mock_metadata_success : type[Any]
        Mocked metadata function
    expected_version : str
        The expected version string to return
    
    Returns
    -------
    None
    """
    importlib.reload(sys.modules["stpstone"])
    assert stpstone.__version__ == expected_version
    mock_metadata_success.assert_called_once_with("stpstone")


def test_version_fallback_hardcoded(
    mock_version_not_found: type[Any],
    mock_metadata_not_found: type[Any],
    expected_version: str,
) -> None:
    """Test fallback to pyproject.toml when metadata is unavailable.
    
    Parameters
    ----------
    mock_version_not_found : type[Any]
        Mocked version function
    mock_metadata_not_found : type[Any]
        Mocked metadata function
    expected_version : str
        The expected version string to return
    
    Returns
    -------
    None
    """
    importlib.reload(sys.modules["stpstone"])
    assert stpstone.__version__ == expected_version
    mock_metadata_not_found.assert_called_once_with("stpstone")


def test_version_fallback_import_error(
    mock_version_not_found: type[Any],
    mock_metadata_import_error: type[Any],
    expected_version: str,
) -> None:
    """Test fallback when importlib.metadata is not available.
    
    Parameters
    ----------
    mock_version_not_found : type[Any]
        Mocked version function
    mock_metadata_import_error : type[Any]
        Mocked metadata function
    expected_version : str
        The expected version string to return
    
    Returns
    -------
    None
    """
    importlib.reload(sys.modules["stpstone"])
    assert stpstone.__version__ == expected_version
    mock_metadata_import_error.assert_called_once_with("stpstone")