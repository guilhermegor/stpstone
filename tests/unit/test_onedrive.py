"""Unit tests for OneDrive synchronization status monitoring.

Tests the functionality to check OneDrive sync status by examining log files,
including validation checks and file processing behavior.
"""

import platform

import pytest


if platform.system() != "Windows":
    pytest.skip("CMD tests require Windows", allow_module_level=True)

from datetime import datetime
from getpass import getuser
from unittest.mock import MagicMock, patch

from stpstone.utils.microsoft_apps.onedrive import OneDrive


# --------------------------
# Fixtures
# --------------------------
@pytest.fixture
def onedrive() -> OneDrive:
    """Fixture providing a OneDrive instance for testing.

    Returns
    -------
    OneDrive
        Instance of the OneDrive class
    """
    return OneDrive()


@pytest.fixture
def mock_dir_files_management() -> MagicMock:
    """Fixture providing a mocked DirFilesManagement instance.

    Returns
    -------
    MagicMock
        Mocked DirFilesManagement class
    """
    with patch(
        "stpstone.transformations.onedrive.DirFilesManagement",
        autospec=True
    ) as mock:
        yield mock


# --------------------------
# Validation Tests
# --------------------------
def test_validate_dir_path_empty(onedrive: OneDrive) -> None:
    """Test _validate_dir_path raises ValueError for empty path.

    Verifies
    --------
    - Empty directory path raises ValueError
    - Error message matches expected

    Parameters
    ----------
    onedrive : OneDrive
        OneDrive instance from fixture

    Returns
    -------
    None
    """
    with pytest.raises(ValueError) as excinfo:
        onedrive._validate_dir_path("")
    assert "Directory path cannot be empty" in str(excinfo.value)


def test_validate_dir_path_missing_format(onedrive: OneDrive) -> None:
    """Test _validate_dir_path raises ValueError for missing format placeholder.

    Verifies
    --------
    - Path without '{}' raises ValueError
    - Error message matches expected

    Parameters
    ----------
    onedrive : OneDrive
        OneDrive instance from fixture

    Returns
    -------
    None
    """
    with pytest.raises(ValueError) as excinfo:
        onedrive._validate_dir_path("C:\\path\\without\\format")
    assert "Directory path must contain '{}'" in str(excinfo.value)


def test_validate_dir_path_valid(onedrive: OneDrive) -> None:
    """Test _validate_dir_path passes with valid path.

    Verifies
    --------
    - Valid path with '{}' placeholder passes validation
    - No exceptions raised

    Parameters
    ----------
    onedrive : OneDrive
        OneDrive instance from fixture

    Returns
    -------
    None
    """
    try:
        onedrive._validate_dir_path("C:\\path\\with\\{}\\format")
    except ValueError:
        pytest.fail("Validation failed unexpectedly for valid path")


# --------------------------
# Main Function Tests
# --------------------------
def test_check_sync_status_default_args(
    onedrive: OneDrive,
    mock_dir_files_management: MagicMock
) -> None:
    """Test check_sync_status with default arguments.

    Verifies
    --------
    - Default arguments are processed correctly
    - Path formatting includes current user
    - Proper methods are called on DirFilesManagement

    Parameters
    ----------
    onedrive : OneDrive
        OneDrive instance from fixture
    mock_dir_files_management : MagicMock
        Mocked DirFilesManagement class

    Returns
    -------
    None
    """
    mock_instance = mock_dir_files_management.return_value
    mock_instance.choose_last_saved_file_w_rule.return_value = "dummy_path"
    mock_instance.time_last_edition.return_value = datetime.now()

    result = onedrive.check_sync_status()

    expected_path = \
        f"C:\\Users\\{getuser()}\\AppData\\Local\\Microsoft\\OneDrive\\logs\\Business1\\"
    mock_instance.choose_last_saved_file_w_rule.assert_called_once_with(
        expected_path, "SyncEngine-*"
    )
    mock_instance.time_last_edition.assert_called_once_with(
        "dummy_path", bool_to_datetime=True
    )
    assert isinstance(result, datetime)


def test_check_sync_status_custom_args(
    onedrive: OneDrive,
    mock_dir_files_management: MagicMock
) -> None:
    """Test check_sync_status with custom arguments.

    Verifies
    --------
    - Custom arguments are processed correctly
    - Path formatting includes current user
    - Proper methods are called with custom parameters

    Parameters
    ----------
    onedrive : OneDrive
        OneDrive instance from fixture
    mock_dir_files_management : MagicMock
        Mocked DirFilesManagement class

    Returns
    -------
    None
    """
    mock_instance = mock_dir_files_management.return_value
    mock_instance.choose_last_saved_file_w_rule.return_value = "dummy_path"
    mock_instance.time_last_edition.return_value = True

    custom_path = "C:\\custom\\path\\{}\\logs\\"
    custom_pattern = "CustomLog-*"

    result = onedrive.check_sync_status(
        dir_path_business=custom_path,
        name_like=custom_pattern,
        bool_to_datetime=False
    )

    expected_path = f"C:\\custom\\path\\{getuser()}\\logs\\"
    mock_instance.choose_last_saved_file_w_rule.assert_called_once_with(
        expected_path, custom_pattern
    )
    mock_instance.time_last_edition.assert_called_once_with(
        "dummy_path", bool_to_datetime=False
    )
    assert result is True


def test_check_sync_status_no_files_found(
    onedrive: OneDrive,
    mock_dir_files_management: MagicMock
) -> None:
    """Test check_sync_status when no matching files are found.

    Verifies
    --------
    - ValueError is raised when no files match pattern
    - Error message is appropriate

    Parameters
    ----------
    onedrive : OneDrive
        OneDrive instance from fixture
    mock_dir_files_management : MagicMock
        Mocked DirFilesManagement class

    Returns
    -------
    None
    """
    mock_instance = mock_dir_files_management.return_value
    mock_instance.choose_last_saved_file_w_rule.return_value = None

    with pytest.raises(ValueError) as excinfo:
        onedrive.check_sync_status()
    assert "No matching log files found" in str(excinfo.value)


def test_check_sync_status_invalid_path(
    onedrive: OneDrive,
    mock_dir_files_management: MagicMock
) -> None:
    """Test check_sync_status with invalid directory path.

    Verifies
    --------
    - ValueError is raised for invalid path
    - DirFilesManagement methods are not called

    Parameters
    ----------
    onedrive : OneDrive
        OneDrive instance from fixture
    mock_dir_files_management : MagicMock
        Mocked DirFilesManagement class

    Returns
    -------
    None
    """
    with pytest.raises(ValueError):
        onedrive.check_sync_status(dir_path_business="invalid_path")

    mock_instance = mock_dir_files_management.return_value
    mock_instance.choose_last_saved_file_w_rule.assert_not_called()
    mock_instance.time_last_edition.assert_not_called()


# --------------------------
# Type Validation Tests
# --------------------------
def test_check_sync_status_return_types(
    onedrive: OneDrive,
    mock_dir_files_management: MagicMock
) -> None:
    """Test check_sync_status return type variations.

    Verifies
    --------
    - Method returns datetime when bool_to_datetime=True
    - Method returns bool when bool_to_datetime=False

    Parameters
    ----------
    onedrive : OneDrive
        OneDrive instance from fixture
    mock_dir_files_management : MagicMock
        Mocked DirFilesManagement class

    Returns
    -------
    None
    """
    mock_instance = mock_dir_files_management.return_value
    mock_instance.choose_last_saved_file_w_rule.return_value = "dummy_path"
    
    # Test datetime return
    mock_instance.time_last_edition.return_value = datetime.now()
    result_datetime = onedrive.check_sync_status(bool_to_datetime=True)
    assert isinstance(result_datetime, datetime)

    # Test bool return
    mock_instance.time_last_edition.return_value = True
    result_bool = onedrive.check_sync_status(bool_to_datetime=False)
    assert isinstance(result_bool, bool)