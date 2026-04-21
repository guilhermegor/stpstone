"""Unit tests for PersistentCacheDecorator class.

Tests the caching functionality with various scenarios including:
- Initialization with valid and invalid inputs
- Cache loading and saving operations
- Thread-safe cache access
- Cache clearing functionality
- Method decoration and result caching
"""

from logging import Logger
from pathlib import Path
import pickle  # noqa: S301 (test intentionally uses pickle)
import re
import sys
from typing import Any
from unittest.mock import Mock

import pytest
from pytest_mock import MockerFixture

from stpstone.transformations.validation.metaclass_type_checker import TypeChecker
from stpstone.utils.cache.cache_persistent import PersistentCacheDecorator
from stpstone.utils.loggs.create_logs import CreateLog
from stpstone.utils.parsers.pickle import PickleFiles


# --------------------------
# Fixtures
# --------------------------
@pytest.fixture
def temp_cache_dir(tmp_path: Path) -> Path:
	"""Fixture providing a temporary directory for cache files.

	Parameters
	----------
	tmp_path : Path
		Temporary directory path provided by pytest.

	Returns
	-------
	Path
		Path to the temporary cache directory
	"""
	return tmp_path / "cache"


@pytest.fixture
def cache_decorator(temp_cache_dir: Path) -> PersistentCacheDecorator:
	"""Fixture providing a PersistentCacheDecorator instance.

	Parameters
	----------
	temp_cache_dir : Path
		Path to the temporary cache directory

	Returns
	-------
	PersistentCacheDecorator
		PersistentCacheDecorator instance
	"""
	return PersistentCacheDecorator(
		path_cache=str(temp_cache_dir / "cache"), cache_key="test_key", bool_persist_cache=True
	)


@pytest.fixture
def mock_logger(mocker: MockerFixture) -> Logger:
	"""Fixture providing a mocked Logger instance.

	Parameters
	----------
	mocker : MockerFixture
		Pytest-mock fixture for creating mocks

	Returns
	-------
	Logger
		Mocked Logger instance
	"""
	return mocker.create_autospec(Logger)


@pytest.fixture
def bypass_type_checker(mocker: MockerFixture) -> None:
	"""Bypass TypeChecker metaclass for performance and ensure proper initialization.

	Parameters
	----------
	mocker : MockerFixture
		Pytest-mock fixture for creating mocks

	Returns
	-------
	None
	"""

	def create_instance(
		cls: type,
		*args: Any,  # noqa ANN401: typing.Any not allowed
		**kwargs: Any,  # noqa ANN401: typing.Any not allowed
	) -> TypeChecker:
		"""Create a mock instance of the TypeChecker metaclass.

		Parameters
		----------
		cls : type
			Class of the TypeChecker metaclass
		*args : Any
			Variable-length argument list
		**kwargs : Any
			Arbitrary keyword arguments

		Returns
		-------
		TypeChecker
			Mocked instance of the TypeChecker metaclass
		"""
		instance = cls.__new__(cls)
		cls.__init__(instance, *args, **kwargs)
		return instance

	mocker.patch(
		"stpstone.transformations.validation.metaclass_type_checker.TypeChecker.__call__",
		create_instance,
	)


# --------------------------
# Tests for Initialization
# --------------------------
@pytest.mark.parametrize("invalid_path", ["", "  ", None])
def test_init_invalid_path_cache(
	temp_cache_dir: Path,
	invalid_path: Any,  # noqa ANN401: typing.Any not allowed
) -> None:
	"""Test initialization raises TypeError for empty, whitespace, or None path_cache.

	Parameters
	----------
	temp_cache_dir : Path
		Path to the temporary cache directory
	invalid_path : Any
		Invalid path_cache value

	Returns
	-------
	None
	"""
	if invalid_path is None:
		expected_message = "path_cache must be one of types: Path, str, got NoneType"
	else:
		expected_message = "path_cache must be a non-empty string"
	with pytest.raises(TypeError, match=expected_message):
		PersistentCacheDecorator(path_cache=invalid_path, cache_key="test_key")


@pytest.mark.parametrize("invalid_key", ["", "  "])
def test_init_invalid_cache_key(
	temp_cache_dir: Path,
	invalid_key: Any,  # noqa ANN401: typing.Any not allowed
) -> None:
	"""Test initialization raises TypeError for empty cache_key.

	Parameters
	----------
	temp_cache_dir : Path
		Path to the temporary cache directory
	invalid_key : Any
		Invalid cache_key value

	Returns
	-------
	None
	"""
	with pytest.raises(TypeError, match="cache_key must be a non-empty string"):
		PersistentCacheDecorator(path_cache=str(temp_cache_dir / "cache"), cache_key=invalid_key)


def test_init_invalid_cache_key_none(temp_cache_dir: Path) -> None:
	"""Test initialization raises TypeError for None cache_key.

	Parameters
	----------
	temp_cache_dir : Path
		Path to the temporary cache directory

	Returns
	-------
	None
	"""
	with pytest.raises(TypeError, match="cache_key must be of type str, got NoneType"):
		PersistentCacheDecorator(path_cache=str(temp_cache_dir / "cache"), cache_key=None)


def test_init_valid_inputs(
	temp_cache_dir: Path, mock_logger: Logger, mocker: MockerFixture
) -> None:
	"""Test initialization with valid inputs.

	Parameters
	----------
	temp_cache_dir : Path
		Path to the temporary cache directory
	mock_logger : Logger
		Mocked Logger instance
	mocker : MockerFixture
		Pytest-mock fixture for creating mocks

	Returns
	-------
	None
	"""
	mocker.patch("pathlib.Path.exists", return_value=False)
	decorator = PersistentCacheDecorator(
		path_cache=str(temp_cache_dir / "test_cache"),
		cache_key="test_key",
		bool_persist_cache=False,
		logger=mock_logger,
	)
	assert decorator.path_cache == temp_cache_dir / "test_cache.pkl"
	assert decorator.cache_key == "test_key"
	assert decorator.bool_persist_cache is False
	assert isinstance(decorator.cache, dict)
	assert decorator.logger is mock_logger


def test_decorator_no_thread_safety(temp_cache_dir: Path, mocker: MockerFixture) -> None:
	"""Test decorator with thread safety disabled.

	Parameters
	----------
	temp_cache_dir : Path
		Path to the temporary cache directory
	mocker : MockerFixture
		Pytest-mock fixture for creating mocks

	Returns
	-------
	None
	"""
	decorator = PersistentCacheDecorator(
		path_cache=str(temp_cache_dir / "cache"),
		cache_key="test_key",
		bool_persist_cache=False,
		bool_thread_safe=False,
	)
	mock_method = mocker.Mock(return_value="result")
	mock_lock = mocker.patch("threading.Lock", autospec=True)
	decorator._lock = mock_lock.return_value
	decorated = decorator(mock_method)

	mock_instance = Mock(bool_persist_cache=False)
	result = decorated(mock_instance, 1)
	assert result == "result"
	mock_method.assert_called_once_with(mock_instance, 1)
	assert decorator.cache["test_key:(1,):{}"] == "result"
	mock_lock.return_value.__enter__.assert_not_called()
	mock_lock.return_value.__exit__.assert_not_called()


# --------------------------
# Tests for _validate_cache_key
# --------------------------
@pytest.mark.parametrize("invalid_key", ["", "  "])
def test_validate_cache_key_invalid(
	invalid_key: Any,  # noqa ANN401: typing.Any not allowed
) -> None:
	"""Test _validate_cache_key raises TypeError for invalid keys.

	Parameters
	----------
	invalid_key : Any
		Invalid cache_key value

	Returns
	-------
	None
	"""
	decorator = PersistentCacheDecorator.__new__(PersistentCacheDecorator)
	with pytest.raises(TypeError, match="cache_key must be a non-empty string"):
		decorator._validate_cache_key(invalid_key)


def test_validate_cache_key_none() -> None:
	"""Test _validate_cache_key raises TypeError for None.

	Returns
	-------
	None
	"""
	decorator = PersistentCacheDecorator.__new__(PersistentCacheDecorator)
	with pytest.raises(TypeError, match="cache_key must be of type str, got NoneType"):
		decorator._validate_cache_key(None)


def test_validate_cache_key_valid() -> None:
	"""Test _validate_cache_key with valid key.

	Returns
	-------
	None
	"""
	decorator = PersistentCacheDecorator.__new__(PersistentCacheDecorator)
	decorator._validate_cache_key("valid_key")


# --------------------------
# Tests for _load_cache
# --------------------------
def test_load_cache_existing_file(temp_cache_dir: Path, mocker: MockerFixture) -> None:
	"""Test loading cache from existing file.

	Parameters
	----------
	temp_cache_dir : Path
		Path to the temporary cache directory
	mocker : MockerFixture
		Pytest-mock fixture for creating mocks

	Returns
	-------
	None
	"""
	cache_path = temp_cache_dir / "cache.pkl"
	expected_cache = {"test_key": "test_value"}
	mock_load = mocker.patch.object(PickleFiles, "load_message", return_value=expected_cache)
	mocker.patch("pathlib.Path.exists", return_value=True)
	decorator = PersistentCacheDecorator(
		path_cache=str(temp_cache_dir / "cache"), cache_key="test_key"
	)
	assert decorator.cache == expected_cache
	mock_load.assert_called_once_with(cache_path)


def test_load_cache_non_existing_file(temp_cache_dir: Path, mocker: MockerFixture) -> None:
	"""Test loading cache when file does not exist.

	Parameters
	----------
	temp_cache_dir : Path
		Path to the temporary cache directory
	mocker : MockerFixture
		Pytest-mock fixture for creating mocks

	Returns
	-------
	None
	"""
	mocker.patch.object(PickleFiles, "load_message")
	mocker.patch("pathlib.Path.exists", return_value=False)
	decorator = PersistentCacheDecorator(
		path_cache=str(temp_cache_dir / "cache"), cache_key="test_key"
	)
	assert decorator.cache == {}
	PickleFiles.load_message.assert_not_called()


@pytest.mark.parametrize("exception", [pickle.PickleError, EOFError])
def test_load_cache_pickle_error(
	temp_cache_dir: Path, mock_logger: Logger, mocker: MockerFixture, exception: type
) -> None:
	"""Test handling of pickle errors during cache load.

	Parameters
	----------
	temp_cache_dir : Path
		Path to the temporary cache directory
	mock_logger : Logger
		Mock logger
	mocker : MockerFixture
		Pytest-mock fixture for creating mocks
	exception : type
		Exception type to raise

	Returns
	-------
	None
	"""
	cache_path = temp_cache_dir / "cache.pkl"
	mocker.patch.object(PickleFiles, "load_message", side_effect=exception("pickle error"))
	mocker.patch("pathlib.Path.exists", return_value=True)
	mocker.patch.object(CreateLog, "log_message")
	with pytest.raises(ValueError, match=re.escape(f"Failed to load cache from {cache_path}: pickle error")):
		PersistentCacheDecorator(
			path_cache=str(temp_cache_dir / "cache"), cache_key="test_key", logger=mock_logger
		)
	CreateLog.log_message.assert_called_once_with(
		mock_logger, f"Failed to load cache from {cache_path}: pickle error", "error"
	)


# --------------------------
# Tests for _save_cache
# --------------------------
def test_save_cache_success(temp_cache_dir: Path, mocker: MockerFixture) -> None:
	"""Test successful cache saving.

	Parameters
	----------
	temp_cache_dir : Path
		Path to the temporary cache directory
	mocker : MockerFixture
		Pytest-mock fixture for creating mocks

	Returns
	-------
	None
	"""
	cache_path = temp_cache_dir / "nonexistent/cache.pkl"
	mocker.patch("pathlib.Path.exists", return_value=False)
	mock_mkdir = mocker.patch("pathlib.Path.mkdir")
	mocker.patch.object(PickleFiles, "dump_message", return_value=True)
	decorator = PersistentCacheDecorator(
		path_cache=str(cache_path), cache_key="test_key", bool_persist_cache=True
	)
	cache = {"test_key": "test_value"}
	result = decorator._save_cache(cache)
	assert result is True
	mock_mkdir.assert_called_once_with(parents=True, exist_ok=True)
	PickleFiles.dump_message.assert_called_once_with(cache, cache_path)


def test_save_cache_no_persist(temp_cache_dir: Path, mocker: MockerFixture) -> None:
	"""Test cache not saved when bool_persist_cache is False.

	Parameters
	----------
	temp_cache_dir : Path
		Path to the temporary cache directory
	mocker : MockerFixture
		Pytest-mock fixture for creating mocks

	Returns
	-------
	None
	"""
	decorator = PersistentCacheDecorator(
		path_cache=str(temp_cache_dir / "cache"), cache_key="test_key", bool_persist_cache=False
	)
	mocker.patch.object(PickleFiles, "dump_message")
	mocker.patch("pathlib.Path.exists")
	mocker.patch("pathlib.Path.mkdir")
	result = decorator._save_cache({"test_key": "test_value"})
	assert result is False
	PickleFiles.dump_message.assert_not_called()
	mocker.patch("pathlib.Path.mkdir").assert_not_called()


def test_save_cache_to_disk(temp_cache_dir: Path, bypass_type_checker: None) -> None:
	"""Test that the cache is actually saved to disk and can be read back.

	Parameters
	----------
	temp_cache_dir : Path
		Path to the temporary cache directory
	bypass_type_checker : None
		Bypass type checker for decorator

	Returns
	-------
	None
	"""
	cache_path = temp_cache_dir / "cache.pkl"
	decorator = PersistentCacheDecorator(
		path_cache=str(cache_path), cache_key="test_key", bool_persist_cache=True
	)

	class TestClass:
		def test_method(self, arg: int) -> str:
			"""Test method for decorator testing.

			Parameters
			----------
			arg : int
				Argument to the test method

			Returns
			-------
			str
				Result of the test method
			"""
			return f"result_{arg}"

	decorated = decorator(TestClass.test_method)
	mock_instance = Mock()
	mock_instance.bool_persist_cache = True

	result = decorated(mock_instance, 1)
	assert result == "result_1"
	assert decorator.cache["test_key:(1,):{}"] == "result_1"

	assert cache_path.exists(), f"Cache file {cache_path} was not created"

	try:
		with cache_path.open("rb") as f:
			saved_cache = pickle.load(f)  # noqa S301
		assert saved_cache == {"test_key:(1,):{}": "result_1"}, "Cache file contents mismatch"
	except Exception as e:
		pytest.fail(f"Failed to read cache file {cache_path}: {e}")


def test_save_cache_pickle_error(
	temp_cache_dir: Path, mock_logger: Logger, mocker: MockerFixture
) -> None:
	"""Test handling of pickle errors during cache save.

	Parameters
	----------
	temp_cache_dir : Path
		Path to the temporary cache directory
	mock_logger : Logger
		Mock logger object
	mocker : MockerFixture
		Pytest-mock fixture for creating mocks

	Returns
	-------
	None
	"""
	cache_path = temp_cache_dir / "cache.pkl"
	mocker.patch("pathlib.Path.exists", return_value=False)
	mocker.patch("pathlib.Path.mkdir")
	mocker.patch.object(
		PickleFiles, "dump_message", side_effect=pickle.PickleError("pickle error")
	)
	mocker.patch.object(CreateLog, "log_message")
	decorator = PersistentCacheDecorator(
		path_cache=str(cache_path),
		cache_key="test_key",
		bool_persist_cache=True,
		logger=mock_logger,
	)
	with pytest.raises(ValueError, match=re.escape(f"Failed to save cache to {cache_path}: pickle error")):
		decorator._save_cache({"test_key": "test_value"})
	CreateLog.log_message.assert_called_once_with(
		mock_logger, f"Warning: Failed to save cache to {cache_path}: pickle error", "error"
	)


# --------------------------
# Tests for clear_cache
# --------------------------
def test_clear_cache_existing_file(temp_cache_dir: Path, mocker: MockerFixture) -> None:
	"""Test clearing cache when file exists.

	Parameters
	----------
	temp_cache_dir : Path
		Path to the temporary cache directory
	mocker : MockerFixture
		Pytest-mock fixture for creating mocks

	Returns
	-------
	None
	"""
	cache_path = temp_cache_dir / "cache.pkl"
	mocker.patch("pathlib.Path.exists", return_value=True)
	mock_unlink = mocker.patch("pathlib.Path.unlink")
	mocker.patch.object(PickleFiles, "load_message", return_value={})
	decorator = PersistentCacheDecorator(path_cache=str(cache_path), cache_key="test_key")
	decorator.cache = {"test_key": "test_value"}
	decorator.clear_cache()
	assert decorator.cache == {}
	mock_unlink.assert_called_once_with()


def test_clear_cache_non_existing_file(temp_cache_dir: Path, mocker: MockerFixture) -> None:
	"""Test clearing cache when file does not exist.

	Parameters
	----------
	temp_cache_dir : Path
		Path to the temporary cache directory
	mocker : MockerFixture
		Pytest-mock fixture for creating mocks

	Returns
	-------
	None
	"""
	cache_path = temp_cache_dir / "cache.pkl"
	mocker.patch("pathlib.Path.exists", return_value=False)
	mocker.patch("pathlib.Path.unlink")
	decorator = PersistentCacheDecorator(path_cache=str(cache_path), cache_key="test_key")
	decorator.cache = {"test_key": "test_value"}
	decorator.clear_cache()
	assert decorator.cache == {}
	mocker.patch("pathlib.Path.unlink").assert_not_called()


def test_clear_cache_file_deletion_error(
	temp_cache_dir: Path, mock_logger: Logger, mocker: MockerFixture
) -> None:
	"""Test handling of errors during cache file deletion.

	Parameters
	----------
	temp_cache_dir : Path
		Path to the temporary cache directory
	mock_logger : Logger
		Mock logger object
	mocker : MockerFixture
		Pytest-mock fixture for creating mocks

	Returns
	-------
	None
	"""
	cache_path = temp_cache_dir / "cache.pkl"
	mocker.patch("pathlib.Path.exists", return_value=True)
	mocker.patch("pathlib.Path.unlink", side_effect=OSError("deletion error"))
	mocker.patch.object(PickleFiles, "load_message", return_value={})
	mocker.patch.object(CreateLog, "log_message")
	decorator = PersistentCacheDecorator(
		path_cache=str(cache_path), cache_key="test_key", logger=mock_logger
	)
	decorator.cache = {"test_key": "test_value"}
	with pytest.raises(
		ValueError, match=re.escape(f"Failed to delete cache file {cache_path}: deletion error")
	):
		decorator.clear_cache()
	assert decorator.cache == {}
	CreateLog.log_message.assert_called_once_with(
		mock_logger, f"Warning: Failed to delete cache file {cache_path}: deletion error", "error"
	)


# --------------------------
# Tests for Decorator Functionality
# --------------------------
def test_decorator_cache_hit(temp_cache_dir: Path, mocker: MockerFixture) -> None:
	"""Test decorator returns cached result on cache hit.

	Parameters
	----------
	temp_cache_dir : Path
		Path to the temporary cache directory
	mocker : MockerFixture
		Pytest-mock fixture for creating mocks

	Returns
	-------
	None
	"""
	decorator = PersistentCacheDecorator(
		path_cache=str(temp_cache_dir / "cache"), cache_key="test_key", bool_persist_cache=False
	)
	mock_method = mocker.Mock(return_value="result")
	decorated = decorator(mock_method)
	decorator.cache["test_key:(1,):{}"] = "cached_result"

	mock_instance = Mock()
	result = decorated(mock_instance, 1)
	assert result == "cached_result"
	mock_method.assert_not_called()


def test_decorator_cache_miss_persist(temp_cache_dir: Path, mocker: MockerFixture) -> None:
	"""Test decorator saves and returns result on cache miss.

	Parameters
	----------
	temp_cache_dir : Path
		Path to the temporary cache directory
	mocker : MockerFixture
		Pytest-mock fixture for creating mocks

	Returns
	-------
	None
	"""
	decorator = PersistentCacheDecorator(
		path_cache=str(temp_cache_dir / "cache"), cache_key="test_key", bool_persist_cache=True
	)
	mock_method = mocker.Mock(return_value="result")
	mocker.patch.object(decorator, "_save_cache", return_value=True)
	decorated = decorator(mock_method)

	mock_instance = Mock(bool_persist_cache=True)
	result = decorated(mock_instance, 1)
	assert result == "result"
	mock_method.assert_called_once_with(mock_instance, 1)
	assert decorator.cache["test_key:(1,):{}"] == "result"
	decorator._save_cache.assert_called_once()


def test_decorator_cache_miss_no_persist(temp_cache_dir: Path, mocker: MockerFixture) -> None:
	"""Test decorator on cache miss with persistence disabled.

	Parameters
	----------
	temp_cache_dir : Path
		Path to the temporary cache directory
	mocker : MockerFixture
		Pytest-mock fixture for creating mocks

	Returns
	-------
	None
	"""
	decorator = PersistentCacheDecorator(
		path_cache=str(temp_cache_dir / "cache"), cache_key="test_key", bool_persist_cache=False
	)
	mock_method = mocker.Mock(return_value="result")
	mocker.patch.object(decorator, "_save_cache")
	decorated = decorator(mock_method)

	mock_instance = Mock(bool_persist_cache=False)
	result = decorated(mock_instance, 1)
	assert result == "result"
	mock_method.assert_called_once_with(mock_instance, 1)
	assert decorator.cache["test_key:(1,):{}"] == "result"
	decorator._save_cache.assert_not_called()


def test_decorator_thread_safety(temp_cache_dir: Path, mocker: MockerFixture) -> None:
	"""Test decorator maintains thread safety during cache access.

	Parameters
	----------
	temp_cache_dir : Path
		Path to the temporary cache directory
	mocker : MockerFixture
		Pytest-mock fixture for creating mocks

	Returns
	-------
	None
	"""
	decorator = PersistentCacheDecorator(
		path_cache=str(temp_cache_dir / "cache"),
		cache_key="test_key",
		bool_persist_cache=False,
		bool_thread_safe=True,
	)
	mock_method = mocker.Mock(return_value="result")
	mock_lock = mocker.patch("threading.Lock", autospec=True)
	decorator._lock = mock_lock.return_value
	decorated = decorator(mock_method)

	mock_instance = Mock()
	decorated(mock_instance, 1)
	mock_lock.return_value.__enter__.assert_called_once()
	mock_lock.return_value.__exit__.assert_called_once()


# --------------------------
# Tests for Edge Cases
# --------------------------
def test_decorator_empty_args_kwargs(temp_cache_dir: Path, mocker: MockerFixture) -> None:
	"""Test decorator with empty args and kwargs.

	Parameters
	----------
	temp_cache_dir : Path
		Path to the temporary cache directory
	mocker : MockerFixture
		Pytest-mock fixture for creating mocks

	Returns
	-------
	None
	"""
	decorator = PersistentCacheDecorator(
		path_cache=str(temp_cache_dir / "cache"), cache_key="test_key", bool_persist_cache=False
	)
	mock_method = mocker.Mock(return_value="result")
	decorated = decorator(mock_method)

	mock_instance = Mock()
	result = decorated(mock_instance)
	assert result == "result"
	assert "test_key:():{}" in decorator.cache
	mock_method.assert_called_once_with(mock_instance)


def test_decorator_unicode_args(temp_cache_dir: Path, mocker: MockerFixture) -> None:
	"""Test decorator with unicode arguments.

	Parameters
	----------
	temp_cache_dir : Path
		Path to the temporary cache directory
	mocker : MockerFixture
		Pytest-mock fixture for creating mocks

	Returns
	-------
	None
	"""
	decorator = PersistentCacheDecorator(
		path_cache=str(temp_cache_dir / "cache"), cache_key="test_key", bool_persist_cache=False
	)
	mock_method = mocker.Mock(return_value="result")
	decorated = decorator(mock_method)

	mock_instance = Mock()
	result = decorated(mock_instance, "测试")
	assert result == "result"
	assert "test_key:('测试',):{}" in decorator.cache
	mock_method.assert_called_once_with(mock_instance, "测试")


# --------------------------
# Tests for Reload Logic
# --------------------------
def test_reload_preservation(temp_cache_dir: Path, mocker: MockerFixture) -> None:
	"""Test cache preservation after module reload.

	Parameters
	----------
	temp_cache_dir : Path
		Path to the temporary cache directory
	mocker : MockerFixture
		Pytest-mock fixture for creating mocks

	Returns
	-------
	None
	"""
	_ = temp_cache_dir / "cache.pkl"
	decorator = PersistentCacheDecorator(
		path_cache=str(temp_cache_dir / "cache"), cache_key="test_key", bool_persist_cache=True
	)
	decorator.cache["test_key:(1,):{}"] = "cached_result"
	mocker.patch.object(PickleFiles, "dump_message", return_value=True)
	decorator._save_cache(decorator.cache)

	import importlib

	importlib.reload(sys.modules["stpstone.utils.cache.cache_persistent"])
	new_decorator = PersistentCacheDecorator(
		path_cache=str(temp_cache_dir / "cache"), cache_key="test_key", bool_persist_cache=True
	)
	mocker.patch("pathlib.Path.exists", return_value=True)
	mocker.patch.object(
		PickleFiles, "load_message", return_value={"test_key:(1,):{}": "cached_result"}
	)

	assert new_decorator._load_cache() == {"test_key:(1,):{}": "cached_result"}
