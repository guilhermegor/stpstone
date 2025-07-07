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