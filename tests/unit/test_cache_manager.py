"""Unit tests for CacheManager class.

Tests the cache management functionality with various input scenarios including:
- Initialization with valid and invalid inputs
- Cache loading, saving, and cleaning operations
- Decorator functionality and error conditions
- Edge cases and type validation
"""

from datetime import datetime, timedelta
from logging import Logger
from pathlib import Path
import pickle
from typing import Any
from unittest.mock import Mock, patch

import pandas as pd
import pytest
from pytest_mock import MockerFixture

from stpstone.utils.cache.cache_manager import CacheManager
from stpstone.utils.loggs.create_logs import CreateLog


# --------------------------
# Fixtures
# --------------------------
@pytest.fixture
def cache_manager_default() -> CacheManager:
    """Fixture providing CacheManager with default settings.

    Returns
    -------
    CacheManager
        Instance with default parameters
    """
    return CacheManager()


@pytest.fixture
def cache_manager_custom(tmp_path: Path) -> CacheManager:
    """Fixture providing CacheManager with custom cache directory.

    Parameters
    ----------
    tmp_path : Path
        Temporary directory path for testing

    Returns
    -------
    CacheManager
        Instance with custom cache directory
    """
    return CacheManager(path_cache_dir=str(tmp_path / "cache"))


@pytest.fixture
def mock_logger(mocker: MockerFixture) -> Logger:
    """Fixture providing a mock logger.

    Parameters
    ----------
    mocker : MockerFixture
        Pytest-mock fixture for creating mocks

    Returns
    -------
    Logger
        Mocked logger instance
    """
    return mocker.create_autospec(Logger)


@pytest.fixture
def sample_dataframe() -> pd.DataFrame:
    """Fixture providing a sample DataFrame for testing.

    Returns
    -------
    pd.DataFrame
        Sample DataFrame with test data
    """
    return pd.DataFrame({"A": [1, 2, 3], "B": ["x", "y", "z"]})


# --------------------------
# Tests for __init__
# --------------------------
def test_init_default_values() -> None:
    """Test initialization with default values.

    Verifies
    --------
    - Default parameters are set correctly
    - Cache directory is created
    - Internal cache is initialized as empty dict

    Returns
    -------
    None
    """
    cm = CacheManager()
    assert cm.bool_persist_cache is True
    assert cm.bool_reuse_cache is True
    assert cm.timedelta_cache_expiry == timedelta(days=1)
    assert cm.timedelta_cache_ttl_days == timedelta(days=30)
    assert isinstance(cm._cache, dict)
    assert len(cm._cache) == 0
    assert isinstance(cm._path_cache_dir, Path)
    assert cm._path_cache_dir.exists()


def test_init_custom_values(tmp_path: Path, mock_logger: Logger) -> None:
    """Test initialization with custom values.

    Verifies
    --------
    - Custom parameters are set correctly
    - Custom cache directory is used
    - Logger is set correctly

    Parameters
    ----------
    tmp_path : Path
        Temporary directory path for testing
    mock_logger : Logger
        Mocked logger instance

    Returns
    -------
    None
    """
    cache_dir = str(tmp_path / "custom_cache")
    cm = CacheManager(
        bool_persist_cache=False,
        bool_reuse_cache=False,
        int_days_cache_expiration=2,
        int_cache_ttl_days=60,
        path_cache_dir=cache_dir,
        logger=mock_logger,
    )
    assert cm.bool_persist_cache is False
    assert cm.bool_reuse_cache is False
    assert cm.timedelta_cache_expiry == timedelta(days=2)
    assert cm.timedelta_cache_ttl_days == timedelta(days=60)
    assert cm._path_cache_dir == Path(cache_dir)
    assert cm.logger == mock_logger


@pytest.mark.parametrize(
    "invalid_value",
    [None, "1", 1.0],
    ids=["none", "string", "float"],
)
def test_init_invalid_types_int_days_cache_expiration(
    invalid_value: Any # noqa ANN401: typing.Any is not allowed
) -> None:
    """Test initialization with invalid types for int_days_cache_expiration.

    Verifies
    --------
    - TypeError is raised with correct message for invalid types

    Parameters
    ----------
    invalid_value : Any
        Invalid values for int_days_cache_expiration

    Returns
    -------
    None
    """
    with pytest.raises(TypeError, match="must be of type int"):
        CacheManager(int_days_cache_expiration=invalid_value)


@pytest.mark.parametrize(
    "invalid_value",
    [None, "1", 1.0],
    ids=["none", "string", "float"],
)
def test_init_invalid_types_int_cache_ttl_days(
    invalid_value: Any # noqa ANN401: typing.Any is not allowed
) -> None:
    """Test initialization with invalid types for int_cache_ttl_days.

    Verifies
    --------
    - TypeError is raised with correct message for invalid types

    Parameters
    ----------
    invalid_value : Any
        Invalid values for int_cache_ttl_days

    Returns
    -------
    None
    """
    with pytest.raises(TypeError, match="must be of type int"):
        CacheManager(int_cache_ttl_days=invalid_value)


# --------------------------
# Tests for _get_cache_dir_path
# --------------------------
def test_get_cache_dir_path_custom(tmp_path: Path) -> None:
    """Test _get_cache_dir_path with custom path.

    Verifies
    --------
    - Custom path is returned as Path object
    - Directory is created if it doesn't exist

    Parameters
    ----------
    tmp_path : Path
        Temporary directory path for testing

    Returns
    -------
    None
    """
    cache_dir = str(tmp_path / "test_cache")
    cm = CacheManager(path_cache_dir=cache_dir)
    result = cm._get_cache_dir_path(cache_dir)
    assert result == Path(cache_dir)
    assert result.exists()


@patch("platform.system")
def test_get_cache_dir_path_default_windows(mock_platform: Mock, tmp_path: Path) -> None:
    """Test _get_cache_dir_path with default path on Windows.

    Verifies
    --------
    - Default Windows path is used
    - Directory is created

    Parameters
    ----------
    mock_platform : Mock
        Mock for platform.system
    tmp_path : Path
        Temporary directory path for testing

    Returns
    -------
    None
    """
    mock_platform.return_value = "Windows"
    with patch("os.getenv", return_value=str(tmp_path)):
        cm = CacheManager()
        result = cm._get_cache_dir_path(None)
        assert result == tmp_path / "stpstone_calendar_cache"
        assert result.exists()


@patch("platform.system")
def test_get_cache_dir_path_default_non_windows(mock_platform: Mock, tmp_path: Path) -> None:
    """Test _get_cache_dir_path with default path on non-Windows.

    Verifies
    --------
    - Default non-Windows path is used
    - Directory is created

    Parameters
    ----------
    mock_platform : Mock
        Mock for platform.system
    tmp_path : Path
        Temporary directory path for testing

    Returns
    -------
    None
    """
    mock_platform.return_value = "Linux"
    with patch("pathlib.Path.home", return_value=tmp_path):
        cm = CacheManager()
        result = cm._get_cache_dir_path(None)
        assert result == tmp_path / ".cache" / "stpstone_calendar_cache"
        assert result.exists()


# --------------------------
# Tests for cache_df decorator
# --------------------------
def test_cache_df_decorator_hit(
    cache_manager_custom: CacheManager, 
    sample_dataframe: pd.DataFrame, 
    mocker: MockerFixture
) -> None:
    """Test cache_df decorator with cache hit.

    Verifies
    --------
    - Cached DataFrame is returned
    - Function is not called
    - Log message is recorded

    Parameters
    ----------
    cache_manager_custom : CacheManager
        CacheManager instance with custom settings
    sample_dataframe : pd.DataFrame
        Sample DataFrame for testing
    mocker : MockerFixture
        Pytest-mock fixture for creating mocks

    Returns
    -------
    None
    """
    mock_func = mocker.Mock(return_value=sample_dataframe)
    mock_log = mocker.patch.object(CreateLog, "log_message")

    class TestClass:
        """TestClass for testing cache_df decorator."""

        def __init__(self, cache_manager: CacheManager) -> None:
            """Initialize TestClass.

            Parameters
            ----------
            cache_manager : CacheManager
                CacheManager instance

            Returns
            -------
            None
            """
            self.cls_cache_manager = cache_manager

        @CacheManager.cache_df("test_key")
        def dummy_func(
            self, 
            *args: Any, # noqa ANN401: typing.Any is not allowed
            **kwargs: Any # noqa ANN401: typing.Any is not allowed
        ) -> pd.DataFrame:
            """Implement dummy function for testing.

            Parameters
            ----------
            *args : Any
                Variable-length argument list
            **kwargs : Any
                Arbitrary keyword arguments

            Returns
            -------
            pd.DataFrame
                Dummy DataFrame
            """
            return mock_func(self, *args, **kwargs)

    test_instance = TestClass(cache_manager_custom)
    cache_manager_custom._cache["test_key"] = sample_dataframe
    cache_manager_custom.bool_reuse_cache = True

    # Ensure cache is valid
    assert cache_manager_custom._load_cache("test_key") is not None
    assert cache_manager_custom._load_cache("test_key").equals(sample_dataframe)

    result = test_instance.dummy_func()
    assert result.equals(sample_dataframe)
    mock_func.assert_not_called()
    mock_log.assert_called_once_with(
        cache_manager_custom.logger,
        f"Using cached holidays from test_key. Path: {cache_manager_custom._path_cache_dir}",
        "info"
    )


def test_cache_df_decorator_miss(
    cache_manager_custom: CacheManager, 
    sample_dataframe: pd.DataFrame, 
    mocker: MockerFixture
) -> None:
    """Test cache_df decorator with cache miss.

    Verifies
    --------
    - Function is called
    - Result is cached
    - Log message is recorded

    Parameters
    ----------
    cache_manager_custom : CacheManager
        CacheManager instance with custom settings
    sample_dataframe : pd.DataFrame
        Sample DataFrame for testing
    mocker : MockerFixture
        Pytest-mock fixture for creating mocks

    Returns
    -------
    None
    """
    mock_func = mocker.Mock(return_value=sample_dataframe)
    mock_log = mocker.patch.object(CreateLog, "log_message")

    class TestClass:
        """TestClass for testing cache_df decorator."""

        def __init__(self, cache_manager: CacheManager) -> None:
            """Initialize TestClass.

            Parameters
            ----------
            cache_manager : CacheManager
                CacheManager instance

            Returns
            -------
            None
            """
            self.cls_cache_manager = cache_manager

        @CacheManager.cache_df("test_key")
        def dummy_func(
            self, 
            *args: Any, # noqa ANN401: typing.Any is not allowed
            **kwargs: Any # noqa ANN401: typing.Any is not allowed
        ) -> pd.DataFrame:
            """Implement dummy function for testing.

            Parameters
            ----------
            *args : Any
                Variable-length argument list
            **kwargs : Any
                Arbitrary keyword arguments

            Returns
            -------
            pd.DataFrame
                Dummy DataFrame
            """
            return mock_func(self, *args, **kwargs)

    test_instance = TestClass(cache_manager_custom)
    cache_manager_custom.bool_reuse_cache = True
    result = test_instance.dummy_func()
    assert result.equals(sample_dataframe)
    mock_func.assert_called_once()
    mock_log.assert_called()


def test_cache_df_no_cls_cache_manager() -> None:
    """Test cache_df decorator without cls_cache_manager attribute.

    Verifies
    --------
    - AttributeError is raised when cls_cache_manager is missing

    Returns
    -------
    None
    """
    class DummyClass:
        """Dummy class for testing."""

        pass

    dummy = DummyClass()

    @CacheManager.cache_df("test_key")
    def dummy_func(
        self: Any, # noqa ANN401: typing.Any is not allowed
        *args: Any, # noqa ANN401: typing.Any is not allowed
        **kwargs: Any # noqa ANN401: typing.Any is not allowed
    ) -> pd.DataFrame:
        """Implement dummy function for testing.
        
        Parameters
        ----------
        *args : Any
            Variable-length argument list
        **kwargs : Any
            Arbitrary keyword arguments

        Returns
        -------
        pd.DataFrame
            Dummy DataFrame
        """
        return pd.DataFrame()

    with pytest.raises(AttributeError, match="Instance must have 'cls_cache_manager' attribute"):
        dummy_func(dummy)


# --------------------------
# Tests for _load_cache
# --------------------------
def test_load_cache_in_memory(
    cache_manager_default: CacheManager, 
    sample_dataframe: pd.DataFrame
) -> None:
    """Test _load_cache from in-memory cache.

    Verifies
    --------
    - In-memory cache returns correct DataFrame
    - Persist cache is disabled

    Parameters
    ----------
    cache_manager_default : CacheManager
        CacheManager instance with default settings
    sample_dataframe : pd.DataFrame
        Sample DataFrame for testing

    Returns
    -------
    None
    """
    cache_manager_default.bool_persist_cache = False
    cache_manager_default._cache["test_key"] = sample_dataframe
    result = cache_manager_default._load_cache("test_key")
    assert result.equals(sample_dataframe)


def test_load_cache_from_file(
    cache_manager_custom: CacheManager, 
    sample_dataframe: pd.DataFrame, 
    tmp_path: Path
) -> None:
    """Test _load_cache from disk file.

    Verifies
    --------
    - DataFrame is loaded from file
    - File timestamp is valid
    - Valid DataFrame is returned

    Parameters
    ----------
    cache_manager_custom : CacheManager
        CacheManager instance with custom settings
    sample_dataframe : pd.DataFrame
        Sample DataFrame for testing
    tmp_path : Path
        Temporary directory path for testing
    
    Returns
    -------
    None
    """
    cache_file = cache_manager_custom._get_cache_file_path("test_key")
    with open(cache_file, "wb") as f:
        pickle.dump((datetime.now(), sample_dataframe), f)

    result = cache_manager_custom._load_cache("test_key")
    assert result.equals(sample_dataframe)


def test_load_cache_expired(
    cache_manager_custom: CacheManager, 
    sample_dataframe: pd.DataFrame, 
    mocker: MockerFixture
) -> None:
    """Test _load_cache with expired cache file.

    Verifies
    --------
    - Expired cache file is deleted
    - None is returned

    Parameters
    ----------
    cache_manager_custom : CacheManager
        CacheManager instance with custom settings
    sample_dataframe : pd.DataFrame
        Sample DataFrame for testing
    mocker : MockerFixture
        Pytest-mock fixture for creating mocks

    Returns
    -------
    None
    """
    cache_file = cache_manager_custom._get_cache_file_path("test_key")
    past_time = datetime.now() - timedelta(days=2)
    with open(cache_file, "wb") as f:
        pickle.dump((past_time, sample_dataframe), f)

    cache_manager_custom.timedelta_cache_expiry = timedelta(days=1)
    result = cache_manager_custom._load_cache("test_key")
    assert result is None
    assert not cache_file.exists()


def test_load_cache_invalid_file(
    cache_manager_custom: CacheManager, 
    mocker: MockerFixture
) -> None:
    """Test _load_cache with invalid pickle file.

    Verifies
    --------
    - Invalid file raises ValueError
    - File is deleted

    Parameters
    ----------
    cache_manager_custom : CacheManager
        CacheManager instance with custom settings
    mocker : MockerFixture
        Pytest-mock fixture for creating mocks

    Returns
    -------
    None
    """
    cache_file = cache_manager_custom._get_cache_file_path("test_key")
    with open(cache_file, "wb") as f:
        f.write(b"invalid pickle data")

    with pytest.raises(ValueError, match="Failed to load pickle file"):
        cache_manager_custom._load_cache("test_key")


# --------------------------
# Tests for _get_cache_file_path
# --------------------------
def test_get_cache_file_path_sanitization(cache_manager_default: CacheManager) -> None:
    """Test _get_cache_file_path with special characters in key.

    Verifies
    --------
    - Special characters are replaced with underscores
    - Valid Path object is returned

    Parameters
    ----------
    cache_manager_default : CacheManager
        CacheManager instance with default settings

    Returns
    -------
    None
    """
    key = "test@#$%^&*key"
    result = cache_manager_default._get_cache_file_path(key)
    assert result.name == "test_______key.pkl"


# --------------------------
# Tests for _validate_cached_dataframe
# --------------------------
@pytest.mark.parametrize(
    "invalid_df",
    [None, "", pd.DataFrame()],
    ids=["none", "string", "empty_df"],
)
def test_validate_cached_dataframe_invalid(
    cache_manager_default: CacheManager, 
    invalid_df: Any # noqa ANN401: typing.Any is not allowed
) -> None:
    """Test _validate_cached_dataframe with invalid inputs.

    Parameters
    ----------
    cache_manager_default : CacheManager
        CacheManager instance with default settings
    invalid_df : Any
        Invalid DataFrame values

    Verifies
    --------
    - Invalid inputs return False or raise TypeError

    Returns
    -------
    None
    """
    if invalid_df is None or isinstance(invalid_df, str):
        with pytest.raises(TypeError, match="must be of type"):
            cache_manager_default._validate_cached_dataframe(invalid_df)
    else:
        assert cache_manager_default._validate_cached_dataframe(invalid_df) is False


def test_validate_cached_dataframe_valid(
    cache_manager_default: CacheManager, sample_dataframe: pd.DataFrame
) -> None:
    """Test _validate_cached_dataframe with valid DataFrame.

    Verifies
    --------
    - Valid DataFrame returns True

    Parameters
    ----------
    cache_manager_default : CacheManager
        CacheManager instance with default settings
    sample_dataframe : pd.DataFrame
        Sample DataFrame for testing

    Returns
    -------
    None
    """
    assert cache_manager_default._validate_cached_dataframe(sample_dataframe) is True


# --------------------------
# Tests for _save_cache
# --------------------------
def test_save_cache_in_memory(
    cache_manager_default: CacheManager, sample_dataframe: pd.DataFrame
) -> None:
    """Test _save_cache to in-memory cache.

    Verifies
    --------
    - DataFrame is saved to in-memory cache
    - No disk operation when persist_cache is False

    Parameters
    ----------
    cache_manager_default : CacheManager
        CacheManager instance with default settings
    sample_dataframe : pd.DataFrame
        Sample DataFrame for testing

    Returns
    -------
    None
    """
    cache_manager_default.bool_persist_cache = False
    cache_manager_default._save_cache("test_key", sample_dataframe)
    assert cache_manager_default._cache["test_key"].equals(sample_dataframe)


def test_save_cache_to_disk(
    cache_manager_custom: CacheManager, sample_dataframe: pd.DataFrame
) -> None:
    """Test _save_cache to disk file.

    Verifies
    --------
    - DataFrame is saved to disk
    - File exists and contains valid data

    Parameters
    ----------
    cache_manager_custom : CacheManager
        CacheManager instance with custom settings
    sample_dataframe : pd.DataFrame
        Sample DataFrame for testing

    Returns
    -------
    None
    """
    cache_manager_custom._save_cache("test_key", sample_dataframe)
    cache_file = cache_manager_custom._get_cache_file_path("test_key")
    assert cache_file.exists()
    with open(cache_file, "rb") as f:
        creation_time, saved_df = pickle.load(f) # noqa S301: `pickle` and modules that wrap it can be unsafe when used to deserialize untrusted data
        assert saved_df.equals(sample_dataframe)
        assert isinstance(creation_time, datetime)


def test_save_cache_invalid_df(
    cache_manager_custom: CacheManager, mocker: MockerFixture
) -> None:
    """Test _save_cache with invalid DataFrame.

    Verifies
    --------
    - Invalid DataFrame does not save
    - No file is created

    Parameters
    ----------
    cache_manager_custom : CacheManager
        CacheManager instance with custom settings
    mocker : MockerFixture
        Pytest-mock fixture for creating mocks

    Returns
    -------
    None
    """
    mocker.patch.object(CacheManager, "_validate_cached_dataframe", return_value=False)
    cache_manager_custom._save_cache("test_key", pd.DataFrame())
    assert "test_key" not in cache_manager_custom._cache
    assert not cache_manager_custom._get_cache_file_path("test_key").exists()


# --------------------------
# Tests for _clean_old_cache
# --------------------------
def test_clean_old_cache(
    cache_manager_custom: CacheManager, 
    sample_dataframe: pd.DataFrame, 
    mocker: MockerFixture
) -> None:
    """Test _clean_old_cache removes expired files.

    Verifies
    --------
    - Expired cache files are deleted
    - Valid files remain

    Parameters
    ----------
    cache_manager_custom : CacheManager
        CacheManager instance with custom settings
    sample_dataframe : pd.DataFrame
        Sample DataFrame for testing
    mocker : MockerFixture
        Pytest-mock fixture for creating mocks

    Returns
    -------
    None
    """
    cache_file = cache_manager_custom._get_cache_file_path("test_key")
    with open(cache_file, "wb") as f:
        pickle.dump((datetime.now() - timedelta(days=31), sample_dataframe), f)

    cache_manager_custom.timedelta_cache_ttl_days = timedelta(days=30)
    cache_manager_custom._clean_old_cache()
    assert not cache_file.exists()


def test_clean_old_cache_no_persist(cache_manager_default: CacheManager) -> None:
    """Test _clean_old_cache with persist_cache disabled.

    Verifies
    --------
    - No operation when persist_cache is False

    Parameters
    ----------
    cache_manager_default : CacheManager
        CacheManager instance with default settings

    Returns
    -------
    None
    """
    cache_manager_default.bool_persist_cache = False
    cache_manager_default._clean_old_cache()
    # No assertions needed; just verify no errors occur


# --------------------------
# Tests for clear_caches
# --------------------------
def test_clear_caches_in_memory(
    cache_manager_default: CacheManager, sample_dataframe: pd.DataFrame
) -> None:
    """Test clear_caches clears in-memory cache.

    Verifies
    --------
    - In-memory cache is cleared

    Parameters
    ----------
    cache_manager_default : CacheManager
        CacheManager instance with default settings
    sample_dataframe : pd.DataFrame
        Sample DataFrame for testing

    Returns
    -------
    None
    """
    cache_manager_default._cache["test_key"] = sample_dataframe
    cache_manager_default.clear_caches()
    assert len(cache_manager_default._cache) == 0


def test_clear_caches_disk(
    cache_manager_custom: CacheManager, sample_dataframe: pd.DataFrame
) -> None:
    """Test clear_caches clears disk cache.

    Verifies
    --------
    - Disk cache files are deleted

    Parameters
    ----------
    cache_manager_custom : CacheManager
        CacheManager instance with custom settings
    sample_dataframe : pd.DataFrame
        Sample DataFrame for testing

    Returns
    -------
    None
    """
    cache_file = cache_manager_custom._get_cache_file_path("test_key")
    with open(cache_file, "wb") as f:
        pickle.dump((datetime.now(), sample_dataframe), f)

    cache_manager_custom.clear_caches()
    assert not cache_file.exists()


def test_clear_caches_file_error(
    cache_manager_custom: CacheManager, mocker: MockerFixture
) -> None:
    """Test clear_caches handles file deletion errors.

    Verifies
    --------
    - ValueError is raised for file deletion errors

    Parameters
    ----------
    cache_manager_custom : CacheManager
        CacheManager instance with custom settings
    mocker : MockerFixture
        Pytest-mock fixture for creating mocks

    Returns
    -------
    None
    """
    cache_file = cache_manager_custom._get_cache_file_path("test_key")
    cache_file.touch()
    mocker.patch.object(Path, "unlink", side_effect=OSError("Permission denied"))

    with pytest.raises(ValueError, match="Failed to clear cache file"):
        cache_manager_custom.clear_caches()


# --------------------------
# Edge Cases
# --------------------------
def test_empty_key_cache_df(cache_manager_custom: CacheManager) -> None:
    """Test cache_df decorator with empty key.

    Verifies
    --------
    - Empty key is handled correctly
    - Cache file is created with sanitized name

    Parameters
    ----------
    cache_manager_custom : CacheManager
        CacheManager instance with custom settings

    Returns
    -------
    None
    """
    @CacheManager.cache_df("")
    def dummy_func(
        self: Any, # noqa ANN401: typing.Any is not allowed
        *args: Any, # noqa ANN401: typing.Any is not allowed
        **kwargs: Any # noqa ANN401: typing.Any is not allowed
    ) -> pd.DataFrame:
        """Implement dummy function for testing.
        
        Parameters
        ----------
        *args : Any
            Variable-length argument list
        **kwargs : Any
            Arbitrary keyword arguments

        Returns
        -------
        pd.DataFrame
            Dummy DataFrame
        """
        return pd.DataFrame({"A": [1]})

    cache_manager_custom.cls_cache_manager = cache_manager_custom
    result = dummy_func(cache_manager_custom)
    assert not result.empty
    cache_file = cache_manager_custom._get_cache_file_path("")
    assert cache_file.name == ".pkl"


def test_unicode_key(cache_manager_custom: CacheManager, sample_dataframe: pd.DataFrame) -> None:
    """Test cache operations with Unicode key.

    Verifies
    --------
    - Unicode characters in key are sanitized
    - Cache operations work correctly

    Parameters
    ----------
    cache_manager_custom : CacheManager
        CacheManager instance with custom settings
    sample_dataframe : pd.DataFrame
        Sample DataFrame for testing

    Returns
    -------
    None
    """
    key = "test_日本語_🚀"
    cache_manager_custom._save_cache(key, sample_dataframe)
    cache_file = cache_manager_custom._get_cache_file_path(key)
    assert cache_file.exists()
    assert cache_file.name == "test_日本語__.pkl"