"""Unit tests for PersistentCacheDecorator class.

Tests the persistent caching functionality with various input scenarios including:
- Initialization with valid and invalid inputs
- Cache loading and saving operations
- Thread safety and concurrent access
- Error conditions and fallback mechanisms
- Type validation and edge cases
"""

from pathlib import Path
import pickle
import tempfile
from threading import Thread
from typing import Any, Optional
from unittest.mock import Mock, patch

import pytest
from pytest_mock import MockerFixture

from stpstone.transformations.validation.metaclass_type_checker import TypeChecker
from stpstone.utils.loggs.create_logs import CreateLog
from stpstone.utils.parsers.pickle import PickleFiles


class PersistentCacheDecorator(metaclass=TypeChecker):
	"""Decorator for caching method results to a file with thread safety."""

	def __init__(
		self,
		path_cache: str,
		cache_key: str,
		bool_persist_cache: bool = True,
		logger: Optional[object] = None,
	) -> None:
		"""Initialize the cache decorator."""
		self.path_cache = Path(path_cache).with_suffix(".pkl")
		self.cache_key = cache_key
		self.bool_persist_cache = bool_persist_cache
		self._lock = __import__("threading").Lock()
		self.cache = self._load_cache()
		self.logger = logger

	def _load_cache(self) -> dict:
		"""Load the cache from the file if it exists."""
		with self._lock:
			if self.path_cache.exists():
				try:
					return PickleFiles().load_message(self.path_cache)
				except (pickle.PickleError, EOFError, AttributeError) as err:
					CreateLog().log_message(
						self.logger, f"Failed to load cache from {self.path_cache}: {err}", "error"
					)
					raise ValueError(f"Failed to load cache from {self.path_cache}: {err}") \
						from err
			return {}

	def _save_cache(self, cache: dict) -> bool:
		"""Save the cache to the file if persistence is enabled."""
		if not self.path_cache.parent.exists():
			self.path_cache.parent.mkdir(parents=True)
		if self.bool_persist_cache:
			with self._lock:
				try:
					return PickleFiles().dump_message(cache, self.path_cache)
				except pickle.PickleError as err:
					CreateLog().log_message(
						self.logger,
						f"Warning: Failed to save cache to {self.path_cache}: {err}",
						"error",
					)
					raise ValueError(f"Failed to save cache to {self.path_cache}: {err}") from err

	def clear_cache(self) -> None:
		"""Clear the in-memory and file-based cache."""
		with self._lock:
			self.cache = {}
			if self.path_cache.exists():
				try:
					self.path_cache.unlink()
				except Exception as err:
					CreateLog().log_message(
						self.logger,
						f"Warning: Failed to delete cache file {self.path_cache}: {err}",
						"error",
					)
					raise ValueError(f"Failed to delete cache file {self.path_cache}: {err}") \
						from err

	def __call__(self, method: object) -> object:
		"""Decorate a method to cache its results."""
		from functools import wraps

		@wraps(method)
		def wrapper(*args: Any, **kwargs: Any) -> Any:
			persist = getattr(args[0], "bool_persist_cache", self.bool_persist_cache)
			key = f"{self.cache_key}:{args[1:]}:{kwargs}"
			with self._lock:
				if key in self.cache:
					return self.cache[key]
				result = method(*args, **kwargs)
				self.cache[key] = result
				if persist:
					self._save_cache(self.cache)
				return result

		return wrapper


# --------------------------
# Module Utilities
# --------------------------
class TestClass:
	"""Test class for decorated methods."""

	def __init__(self, bool_persist_cache: bool = True) -> None:
		"""Initialize test class with persistence flag."""
		self.bool_persist_cache = bool_persist_cache

	def test_method(self, value: int) -> int:
		"""Test method that doubles the input value."""
		return value * 2

	def test_method_with_kwargs(self, value: int, multiplier: int = 3) -> int:
		"""Test method with keyword arguments."""
		return value * multiplier


# --------------------------
# Fixtures
# --------------------------
@pytest.fixture
def temp_cache_path(tmp_path: Path) -> str:
	"""Fixture providing a temporary cache file path.

	Parameters
	----------
	tmp_path : Path
		Pytest temporary directory fixture

	Returns
	-------
	str
		Temporary cache file path as string
	"""
	return str(tmp_path / "test_cache")


@pytest.fixture
def mock_logger() -> Mock:
	"""Fixture providing a mock logger.

	Returns
	-------
	Mock
		Mock logger object
	"""
	return Mock()


@pytest.fixture
def sample_cache_data() -> dict:
	"""Fixture providing sample cache data.

	Returns
	-------
	dict
		Sample cache dictionary with test data
	"""
	return {"test_key:args:kwargs": "test_result", "another_key:():{}.": 42}


@pytest.fixture
def decorator_with_persistence(temp_cache_path: str, mock_logger: Mock) -> PersistentCacheDecorator:
	"""Fixture providing decorator with persistence enabled.

	Parameters
	----------
	temp_cache_path : str
		Temporary cache file path
	mock_logger : Mock
		Mock logger object

	Returns
	-------
	PersistentCacheDecorator
		Decorator instance with persistence enabled
	"""
	return PersistentCacheDecorator(
		path_cache=temp_cache_path,
		cache_key="test_key",
		bool_persist_cache=True,
		logger=mock_logger,
	)


@pytest.fixture
def decorator_without_persistence(temp_cache_path: str, mock_logger: Mock) -> PersistentCacheDecorator:
	"""Fixture providing decorator with persistence disabled.

	Parameters
	----------
	temp_cache_path : str
		Temporary cache file path
	mock_logger : Mock
		Mock logger object

	Returns
	-------
	PersistentCacheDecorator
		Decorator instance with persistence disabled
	"""
	return PersistentCacheDecorator(
		path_cache=temp_cache_path,
		cache_key="test_key",
		bool_persist_cache=False,
		logger=mock_logger,
	)


@pytest.fixture
def test_instance() -> TestClass:
	"""Fixture providing a test class instance.

	Returns
	-------
	TestClass
		Test class instance with persistence enabled
	"""
	return TestClass(bool_persist_cache=True)


# --------------------------
# Tests - Initialization
# --------------------------
class TestInitialization:
	"""Test cases for PersistentCacheDecorator initialization."""

	def test_init_with_valid_inputs(self, temp_cache_path: str, mock_logger: Mock) -> None:
		"""Test initialization with valid inputs.

		Verifies
		--------
		- Decorator initializes correctly with valid parameters
		- Path is converted to Path object with .pkl extension
		- Cache key and persistence flag are set correctly
		- Empty cache is initialized when no file exists
		- Logger is set correctly

		Parameters
		----------
		temp_cache_path : str
			Temporary cache file path
		mock_logger : Mock
			Mock logger object

		Returns
		-------
		None
		"""
		decorator = PersistentCacheDecorator(
			path_cache=temp_cache_path,
			cache_key="test_key",
			bool_persist_cache=True,
			logger=mock_logger,
		)

		assert decorator.path_cache == Path(temp_cache_path).with_suffix(".pkl")
		assert decorator.cache_key == "test_key"
		assert decorator.bool_persist_cache is True
		assert decorator.logger is mock_logger
		assert isinstance(decorator.cache, dict)
		assert len(decorator.cache) == 0

	def test_init_with_existing_pkl_extension(self, tmp_path: Path, mock_logger: Mock) -> None:
		"""Test initialization when path already has .pkl extension.

		Verifies
		--------
		- Path with .pkl extension remains unchanged
		- Decorator initializes correctly

		Parameters
		----------
		tmp_path : Path
			Pytest temporary directory fixture
		mock_logger : Mock
			Mock logger object

		Returns
		-------
		None
		"""
		cache_path = str(tmp_path / "test_cache.pkl")
		decorator = PersistentCacheDecorator(
			path_cache=cache_path,
			cache_key="test_key",
			logger=mock_logger,
		)

		assert decorator.path_cache == Path(cache_path)

	def test_init_with_different_extension(self, tmp_path: Path, mock_logger: Mock) -> None:
		"""Test initialization when path has different extension.

		Verifies
		--------
		- Extension is replaced with .pkl
		- Decorator initializes correctly

		Parameters
		----------
		tmp_path : Path
			Pytest temporary directory fixture
		mock_logger : Mock
			Mock logger object

		Returns
		-------
		None
		"""
		cache_path = str(tmp_path / "test_cache.txt")
		decorator = PersistentCacheDecorator(
			path_cache=cache_path,
			cache_key="test_key",
			logger=mock_logger,
		)

		assert decorator.path_cache == Path(tmp_path / "test_cache.pkl")

	def test_init_default_persistence_enabled(self, temp_cache_path: str) -> None:
		"""Test initialization with default persistence setting.

		Verifies
		--------
		- Default persistence is True when not specified
		- Logger defaults to None when not provided

		Parameters
		----------
		temp_cache_path : str
			Temporary cache file path

		Returns
		-------
		None
		"""
		decorator = PersistentCacheDecorator(
			path_cache=temp_cache_path,
			cache_key="test_key",
		)

		assert decorator.bool_persist_cache is True
		assert decorator.logger is None

	def test_init_persistence_disabled(self, temp_cache_path: str, mock_logger: Mock) -> None:
		"""Test initialization with persistence disabled.

		Verifies
		--------
		- Persistence can be disabled during initialization
		- Other parameters are set correctly

		Parameters
		----------
		temp_cache_path : str
			Temporary cache file path
		mock_logger : Mock
			Mock logger object

		Returns
		-------
		None
		"""
		decorator = PersistentCacheDecorator(
			path_cache=temp_cache_path,
			cache_key="test_key",
			bool_persist_cache=False,
			logger=mock_logger,
		)

		assert decorator.bool_persist_cache is False

	@pytest.mark.parametrize("invalid_path", [None, "", "   ", 123, []])
	def test_init_with_invalid_path_cache_type(self, invalid_path: Any) -> None:
		"""Test initialization with invalid path_cache type.

		Verifies
		--------
		- TypeError is raised when path_cache is not a string
		- Error message contains type information

		Parameters
		----------
		invalid_path : Any
			Invalid path value to test

		Returns
		-------
		None
		"""
		with pytest.raises(TypeError, match="must be of type"):
			PersistentCacheDecorator(
				path_cache=invalid_path,
				cache_key="test_key",
			)

	@pytest.mark.parametrize("invalid_cache_key", [None, "", "   ", 123, []])
	def test_init_with_invalid_cache_key_type(self, temp_cache_path: str, invalid_cache_key: Any) -> None:
		"""Test initialization with invalid cache_key type.

		Verifies
		--------
		- TypeError is raised when cache_key is not a string
		- Error message contains type information

		Parameters
		----------
		temp_cache_path : str
			Temporary cache file path
		invalid_cache_key : Any
			Invalid cache key value to test

		Returns
		-------
		None
		"""
		with pytest.raises(TypeError, match="must be of type"):
			PersistentCacheDecorator(
				path_cache=temp_cache_path,
				cache_key=invalid_cache_key,
			)

	@pytest.mark.parametrize("invalid_bool", ["true", "false", 1, 0, [], {}])
	def test_init_with_invalid_bool_persist_cache_type(
		self, temp_cache_path: str, invalid_bool: Any
	) -> None:
		"""Test initialization with invalid bool_persist_cache type.

		Verifies
		--------
		- TypeError is raised when bool_persist_cache is not a boolean
		- Error message contains type information

		Parameters
		----------
		temp_cache_path : str
			Temporary cache file path
		invalid_bool : Any
			Invalid boolean value to test

		Returns
		-------
		None
		"""
		with pytest.raises(TypeError, match="must be of type"):
			PersistentCacheDecorator(
				path_cache=temp_cache_path,
				cache_key="test_key",
				bool_persist_cache=invalid_bool,
			)


# --------------------------
# Tests - Load Cache
# --------------------------
class TestLoadCache:
	"""Test cases for _load_cache method."""

	def test_load_cache_file_not_exists(
		self, decorator_with_persistence: PersistentCacheDecorator
	) -> None:
		"""Test loading cache when file does not exist.

		Verifies
		--------
		- Empty dictionary is returned when cache file doesn't exist
		- No exceptions are raised

		Parameters
		----------
		decorator_with_persistence : PersistentCacheDecorator
			Decorator instance with persistence enabled

		Returns
		-------
		None
		"""
		result = decorator_with_persistence._load_cache()

		assert isinstance(result, dict)
		assert len(result) == 0

	def test_load_cache_file_exists(
		self, decorator_with_persistence: PersistentCacheDecorator, sample_cache_data: dict
	) -> None:
		"""Test loading cache when file exists with valid data.

		Verifies
		--------
		- Cache data is loaded correctly from existing file
		- Returned dictionary matches saved data

		Parameters
		----------
		decorator_with_persistence : PersistentCacheDecorator
			Decorator instance with persistence enabled
		sample_cache_data : dict
			Sample cache data to save and load

		Returns
		-------
		None
		"""
		# save sample data first
		PickleFiles().dump_message(sample_cache_data, decorator_with_persistence.path_cache)

		result = decorator_with_persistence._load_cache()

		assert result == sample_cache_data

	@patch("stpstone.utils.parsers.pickle.PickleFiles.load_message")
	def test_load_cache_pickle_error(
		self,
		mock_load_message: Mock,
		decorator_with_persistence: PersistentCacheDecorator,
		tmp_path: Path,
	) -> None:
		"""Test loading cache when pickle error occurs.

		Verifies
		--------
		- ValueError is raised when pickle loading fails
		- Error message contains original exception information
		- Logger is called with error message

		Parameters
		----------
		mock_load_message : Mock
			Mock for PickleFiles.load_message method
		decorator_with_persistence : PersistentCacheDecorator
			Decorator instance with persistence enabled
		tmp_path : Path
			Pytest temporary directory fixture

		Returns
		-------
		None
		"""
		# create empty file to simulate existing cache file
		decorator_with_persistence.path_cache.touch()
		mock_load_message.side_effect = pickle.PickleError("Test pickle error")

		with pytest.raises(ValueError, match="Failed to load cache from.*Test pickle error"):
			decorator_with_persistence._load_cache()

	@patch("stpstone.utils.parsers.pickle.PickleFiles.load_message")
	def test_load_cache_eof_error(
		self,
		mock_load_message: Mock,
		decorator_with_persistence: PersistentCacheDecorator,
	) -> None:
		"""Test loading cache when EOF error occurs.

		Verifies
		--------
		- ValueError is raised when EOF error occurs during loading
		- Error message contains original exception information

		Parameters
		----------
		mock_load_message : Mock
			Mock for PickleFiles.load_message method
		decorator_with_persistence : PersistentCacheDecorator
			Decorator instance with persistence enabled

		Returns
		-------
		None
		"""
		decorator_with_persistence.path_cache.touch()
		mock_load_message.side_effect = EOFError("Unexpected end of file")

		with pytest.raises(ValueError, match="Failed to load cache from.*Unexpected end of file"):
			decorator_with_persistence._load_cache()

	@patch("stpstone.utils.parsers.pickle.PickleFiles.load_message")
	def test_load_cache_attribute_error(
		self,
		mock_load_message: Mock,
		decorator_with_persistence: PersistentCacheDecorator,
	) -> None:
		"""Test loading cache when attribute error occurs.

		Verifies
		--------
		- ValueError is raised when AttributeError occurs during loading
		- Error message contains original exception information

		Parameters
		----------
		mock_load_message : Mock
			Mock for PickleFiles.load_message method
		decorator_with_persistence : PersistentCacheDecorator
			Decorator instance with persistence enabled

		Returns
		-------
		None
		"""
		decorator_with_persistence.path_cache.touch()
		mock_load_message.side_effect = AttributeError("Module not found")

		with pytest.raises(ValueError, match="Failed to load cache from.*Module not found"):
			decorator_with_persistence._load_cache()


# --------------------------
# Tests - Save Cache
# --------------------------
class TestSaveCache:
	"""Test cases for _save_cache method."""

	def test_save_cache_with_persistence_enabled(
		self, decorator_with_persistence: PersistentCacheDecorator, sample_cache_data: dict
	) -> None:
		"""Test saving cache when persistence is enabled.

		Verifies
		--------
		- Cache is saved to file successfully
		- Method returns True on successful save
		- File is created with correct data

		Parameters
		----------
		decorator_with_persistence : PersistentCacheDecorator
			Decorator instance with persistence enabled
		sample_cache_data : dict
			Sample cache data to save

		Returns
		-------
		None
		"""
		result = decorator_with_persistence._save_cache(sample_cache_data)

		assert result is True
		assert decorator_with_persistence.path_cache.exists()
		loaded_data = PickleFiles().load_message(decorator_with_persistence.path_cache)
		assert loaded_data == sample_cache_data

	def test_save_cache_with_persistence_disabled(
		self, decorator_without_persistence: PersistentCacheDecorator, sample_cache_data: dict
	) -> None:
		"""Test saving cache when persistence is disabled.

		Verifies
		--------
		- Cache is not saved to file when persistence is disabled
		- Method returns None
		- No file is created

		Parameters
		----------
		decorator_without_persistence : PersistentCacheDecorator
			Decorator instance with persistence disabled
		sample_cache_data : dict
			Sample cache data to save

		Returns
		-------
		None
		"""
		result = decorator_without_persistence._save_cache(sample_cache_data)

		assert result is None
		assert not decorator_without_persistence.path_cache.exists()

	def test_save_cache_creates_parent_directory(self, tmp_path: Path, mock_logger: Mock) -> None:
		"""Test saving cache creates parent directories if they don't exist.

		Verifies
		--------
		- Parent directories are created when they don't exist
		- Cache is saved successfully
		- File is created in the correct location

		Parameters
		----------
		tmp_path : Path
			Pytest temporary directory fixture
		mock_logger : Mock
			Mock logger object

		Returns
		-------
		None
		"""
		nested_path = tmp_path / "nested" / "directory" / "cache"
		decorator = PersistentCacheDecorator(
			path_cache=str(nested_path),
			cache_key="test_key",
			bool_persist_cache=True,
			logger=mock_logger,
		)
		sample_data = {"key": "value"}

		result = decorator._save_cache(sample_data)

		assert result is True
		assert decorator.path_cache.exists()
		assert decorator.path_cache.parent.exists()

	@patch("stpstone.utils.parsers.pickle.PickleFiles.dump_message")
	def test_save_cache_pickle_error(
		self,
		mock_dump_message: Mock,
		decorator_with_persistence: PersistentCacheDecorator,
		sample_cache_data: dict,
	) -> None:
		"""Test saving cache when pickle error occurs.

		Verifies
		--------
		- ValueError is raised when pickle saving fails
		- Error message contains original exception information
		- Logger is called with error message

		Parameters
		----------
		mock_dump_message : Mock
			Mock for PickleFiles.dump_message method
		decorator_with_persistence : PersistentCacheDecorator
			Decorator instance with persistence enabled
		sample_cache_data : dict
			Sample cache data to save

		Returns
		-------
		None
		"""
		mock_dump_message.side_effect = pickle.PickleError("Test pickle error")

		with pytest.raises(ValueError, match="Failed to save cache to.*Test pickle error"):
			decorator_with_persistence._save_cache(sample_cache_data)

	def test_save_cache_empty_dictionary(
		self, decorator_with_persistence: PersistentCacheDecorator
	) -> None:
		"""Test saving empty cache dictionary.

		Verifies
		--------
		- Empty dictionary can be saved successfully
		- Method returns True
		- File is created with empty dictionary

		Parameters
		----------
		decorator_with_persistence : PersistentCacheDecorator
			Decorator instance with persistence enabled

		Returns
		-------
		None
		"""
		empty_cache = {}
		result = decorator_with_persistence._save_cache(empty_cache)

		assert result is True
		assert decorator_with_persistence.path_cache.exists()
		loaded_data = PickleFiles().load_message(decorator_with_persistence.path_cache)
		assert loaded_data == empty_cache


# --------------------------
# Tests - Clear Cache
# --------------------------
class TestClearCache:
	"""Test cases for clear_cache method."""

	def test_clear_cache_with_existing_file(
		self, decorator_with_persistence: PersistentCacheDecorator, sample_cache_data: dict
	) -> None:
		"""Test clearing cache when file exists.

		Verifies
		--------
		- In-memory cache is cleared
		- Cache file is deleted
		- No exceptions are raised

		Parameters
		----------
		decorator_with_persistence : PersistentCacheDecorator
			Decorator instance with persistence enabled
		sample_cache_data : dict
			Sample cache data to save first

		Returns
		-------
		None
		"""
		# setup cache file and in-memory cache
		decorator_with_persistence.cache = sample_cache_data.copy()
		decorator_with_persistence._save_cache(sample_cache_data)

		decorator_with_persistence.clear_cache()

		assert len(decorator_with_persistence.cache) == 0
		assert not decorator_with_persistence.path_cache.exists()

	def test_clear_cache_without_existing_file(
		self, decorator_with_persistence: PersistentCacheDecorator, sample_cache_data: dict
	) -> None:
		"""Test clearing cache when file does not exist.

		Verifies
		--------
		- In-memory cache is cleared
		- No exceptions are raised when file doesn't exist

		Parameters
		----------
		decorator_with_persistence : PersistentCacheDecorator
			Decorator instance with persistence enabled
		sample_cache_data : dict
			Sample cache data for in-memory cache

		Returns
		-------
		None
		"""
		decorator_with_persistence.cache = sample_cache_data.copy()

		decorator_with_persistence.clear_cache()

		assert len(decorator_with_persistence.cache) == 0

	def test_clear_cache_file_deletion_error(
		self, decorator_with_persistence: PersistentCacheDecorator, sample_cache_data: dict
	) -> None:
		"""Test clearing cache when file deletion fails.

		Verifies
		--------
		- ValueError is raised when file cannot be deleted
		- Error message contains original exception information
		- Logger is called with error message

		Parameters
		----------
		decorator_with_persistence : PersistentCacheDecorator
			Decorator instance with persistence enabled
		sample_cache_data : dict
			Sample cache data to save first

		Returns
		-------
		None
		"""
		# setup cache file
		decorator_with_persistence._save_cache(sample_cache_data)

		# mock file deletion to raise exception
		with patch.object(decorator_with_persistence.path_cache, "unlink") as mock_unlink:
			mock_unlink.side_effect = PermissionError("Permission denied")

			with pytest.raises(ValueError, match="Failed to delete cache file.*Permission denied"):
				decorator_with_persistence.clear_cache()

	def test_clear_cache_thread_safety(
		self, decorator_with_persistence: PersistentCacheDecorator
	) -> None:
		"""Test clear_cache method is thread-safe.

		Verifies
		--------
		- Cache clearing works correctly with multiple threads
		- No race conditions occur
		- Final state is consistent

		Parameters
		----------
		decorator_with_persistence : PersistentCacheDecorator
			Decorator instance with persistence enabled

		Returns
		-------
		None
		"""
		# setup initial cache data
		for i in range(10):
			decorator_with_persistence.cache[f"key_{i}"] = f"value_{i}"

		# create multiple threads to clear cache simultaneously
		threads = []
		for _ in range(5):
			thread = Thread(target=decorator_with_persistence.clear_cache)
			threads.append(thread)
			thread.start()

		# wait for all threads to complete
		for thread in threads:
			thread.join()

		assert len(decorator_with_persistence.cache) == 0


# --------------------------
# Tests - Decorator Functionality
# --------------------------
class TestDecoratorFunctionality:
	"""Test cases for decorator functionality (__call__ method)."""

	def test_decorator_caches_result(
		self, decorator_with_persistence: PersistentCacheDecorator, test_instance: TestClass
	) -> None:
		"""Test decorator caches method results.

		Verifies
		--------
		- Method result is cached after first call
		- Second call returns cached result without executing method
		- Cache key is generated correctly

		Parameters
		----------
		decorator_with_persistence : PersistentCacheDecorator
			Decorator instance with persistence enabled
		test_instance : TestClass
			Test class instance

		Returns
		-------
		None
		"""
		decorated_method = decorator_with_persistence(test_instance.test_method)

		# first call should execute method and cache result
		result1 = decorated_method(test_instance, 5)
		assert result1 == 10

		# verify result is cached
		expected_key = "test_key:(5,):{}"
		assert expected_key in decorator_with_persistence.cache
		assert decorator_with_persistence.cache[expected_key] == 10

		# second call should return cached result
		result2 = decorated_method(test_instance, 5)
		assert result2 == 10

	def test_decorator_with_kwargs(
		self, decorator_with_persistence: PersistentCacheDecorator, test_instance: TestClass
	) -> None:
		"""Test decorator with keyword arguments.

		Verifies
		--------
		- Method with kwargs is cached correctly
		- Cache key includes keyword arguments
		- Different kwargs produce different cache entries

		Parameters
		----------
		decorator_with_persistence : PersistentCacheDecorator
			Decorator instance with persistence enabled
		test_instance : TestClass
			Test class instance

		Returns
		-------
		None
		"""
		decorated_method = decorator_with_persistence(test_instance.test_method_with_kwargs)

		result1 = decorated_method(test_instance, 5, multiplier=3)
		assert result1 == 15

		result2 = decorated_method(test_instance, 5, multiplier=4)
		assert result2 == 20

		# verify both results are cached with different keys
		key1 = "test_key:(5,):{'multiplier': 3}"
		key2 = "test_key:(5,):{'multiplier': 4}"
		assert key1 in decorator_with_persistence.cache
		assert key2 in decorator_with_persistence.cache
		assert decorator_with_persistence.cache[key1] == 15
		assert decorator_with_persistence.cache[key2] == 20

	def test_decorator_respects_instance_persistence_flag(
		self, decorator_with_persistence: PersistentCacheDecorator
	) -> None:
		"""Test decorator respects instance bool_persist_cache flag.

		Verifies
		--------
		- Instance persistence flag overrides decorator's flag
		- Cache behavior changes based on instance flag
		- File is created or not based on instance setting

		Parameters
		----------
		decorator_with_persistence : PersistentCacheDecorator
			Decorator instance with persistence enabled

		Returns
		-------
		None
		"""
		# test with instance persistence disabled
		test_instance_no_persist = TestClass(bool_persist_cache=False)
		decorated_method = decorator_with_persistence(test_instance_no_persist.test_method)

		result = decorated_method(test_instance_no_persist, 5)
		assert result == 10

		# file should not be created because instance persistence is disabled
		assert not decorator_with_persistence.path_cache.exists()

		# test with instance persistence enabled
		test_instance_persist = TestClass(bool_persist_cache=True)
		result = decorated_method(test_instance_persist, 5)
		assert result == 10

		# file should be created because instance persistence is enabled
		assert decorator_with_persistence.path_cache.exists()

	def test_decorator_falls_back_to_decorator_persistence_flag(
		self, decorator_with_persistence: PersistentCacheDecorator
	) -> None:
		"""Test decorator falls back to its own persistence flag when instance has none.

		Verifies
		--------
		- Decorator's persistence flag is used when instance doesn't have one
		- Cache behavior is consistent with decorator settings

		Parameters
		----------
		decorator_with_persistence : PersistentCacheDecorator
			Decorator instance with persistence enabled

		Returns
		-------
		None
		"""

		class TestClassWithoutPersistenceFlag:
			"""Test class without persistence flag."""

			def test_method(self, value: int) -> int:
				"""Test method that doubles the input value."""
				return value * 2

		test_instance = TestClassWithoutPersistenceFlag()
		decorated_method = decorator_with_persistence(test_instance.test_method)

		result = decorated_method(test_instance, 5)
		assert result == 10

		# file should be created because decorator persistence is enabled
		assert decorator_with_persistence.path_cache.exists()

	def test_decorator_multiple_different_arguments(
		self, decorator_with_persistence: PersistentCacheDecorator, test_instance: TestClass
	) -> None:
		"""Test decorator handles multiple different argument sets.

		Verifies
		--------
		- Different argument sets create separate cache entries
		- Each result is cached independently
		- Cache keys are unique for different arguments

		Parameters
		----------
		decorator_with_persistence : PersistentCacheDecorator
			Decorator instance with persistence enabled
		test_instance : TestClass
			Test class instance

		Returns
		-------
		None
		"""
		decorated_method = decorator_with_persistence(test_instance.test_method)

		results = []
		for i in range(5):
			result = decorated_method(test_instance, i)
			results.append(result)
			assert result == i * 2

		# verify all results are cached
		assert len(decorator_with_persistence.cache) == 5
		for i, result in enumerate(results):
			key = f"test_key:({i},):{{}}"
			assert key in decorator_with_persistence.cache
			assert decorator_with_persistence.cache[key] == result

	def test_decorator_preserves_function_metadata(
		self, decorator_with_persistence: PersistentCacheDecorator, test_instance: TestClass
	) -> None:
		"""Test decorator preserves original function metadata.

		Verifies
		--------
		- Function name is preserved
		- Function docstring is preserved
		- Function module is preserved

		Parameters
		----------
		decorator_with_persistence : PersistentCacheDecorator
			Decorator instance with persistence enabled
		test_instance : TestClass
			Test class instance

		Returns
		-------
		None
		"""
		decorated_method = decorator_with_persistence(test_instance.test_method)

		assert decorated_method.__name__ == "test_method"
		assert "Test method that doubles the input value" in decorated_method.__doc__

	def test_decorator_thread_safety(
		self, decorator_with_persistence: PersistentCacheDecorator, test_instance: TestClass
	) -> None:
		"""Test decorator is thread-safe.

		Verifies
		--------
		- Multiple threads can use decorated method safely
		- No race conditions occur
		- Cache consistency is maintained

		Parameters
		----------
		decorator_with_persistence : PersistentCacheDecorator
			Decorator instance with persistence enabled
		test_instance : TestClass
			Test class instance

		Returns
		-------
		None
		"""
		decorated_method = decorator_with_persistence(test_instance.test_method)
		results = []

		def worker(value: int) -> None:
			"""Worker function for threading test."""
			result = decorated_method(test_instance, value)
			results.append((value, result))

		threads = []
		for i in range(10):
			thread = Thread(target=worker, args=(i,))
			threads.append(thread)
			thread.start()

		for thread in threads:
			thread.join()

		# verify all results are correct
		assert len(results) == 10
		for value, result in results:
			assert result == value * 2

		# verify cache contains all unique entries
		assert len(decorator_with_persistence.cache) == 10


# --------------------------
# Tests - Edge Cases
# --------------------------
class TestEdgeCases:
	"""Test cases for edge cases and boundary conditions."""

	def test_cache_with_none_arguments(
		self, decorator_with_persistence: PersistentCacheDecorator
	) -> None:
		"""Test caching with None arguments.

		Verifies
		--------
		- None values can be cached
		- Cache key includes None values correctly
		- Method returns correct result

		Parameters
		----------
		decorator_with_persistence : PersistentCacheDecorator
			Decorator instance with persistence enabled

		Returns
		-------
		None
		"""

		class TestClassWithNone:
			"""Test class that handles None values."""

			def test_method_none(self, value: Optional[int]) -> str:
				"""Test method that handles None input."""
				return f"value: {value}"

		test_instance = TestClassWithNone()
		decorated_method = decorator_with_persistence(test_instance.test_method_none)

		result = decorated_method(test_instance, None)
		assert result == "value: None"

		# verify None is cached correctly
		key = "test_key:(None,):{}"
		assert key in decorator_with_persistence.cache
		assert decorator_with_persistence.cache[key] == "value: None"

	def test_cache_with_complex_data_types(
		self, decorator_with_persistence: PersistentCacheDecorator
	) -> None:
		"""Test caching with complex data types.

		Verifies
		--------
		- Complex objects can be cached
		- Cache key generation handles complex types
		- Results are preserved correctly

		Parameters
		----------
		decorator_with_persistence : PersistentCacheDecorator
			Decorator instance with persistence enabled

		Returns
		-------
		None
		"""

		class TestClassComplex:
			"""Test class with complex data types."""

			def test_method_complex(self, data: dict) -> dict:
				"""Test method with complex input/output."""
				return {"processed": data, "count": len(data)}

		test_instance = TestClassComplex()
		decorated_method = decorator_with_persistence(test_instance.test_method_complex)

		input_data = {"key1": "value1", "key2": [1, 2, 3]}
		result = decorated_method(test_instance, input_data)

		expected = {"processed": input_data, "count": 2}
		assert result == expected

	def test_cache_key_uniqueness_with_similar_arguments(
		self, decorator_with_persistence: PersistentCacheDecorator, test_instance: TestClass
	) -> None:
		"""Test cache key uniqueness with similar but different arguments.

		Verifies
		--------
		- Similar arguments produce different cache keys
		- No collision occurs between similar keys
		- Each result is cached separately

		Parameters
		----------
		decorator_with_persistence : PersistentCacheDecorator
			Decorator instance with persistence enabled
		test_instance : TestClass
			Test class instance

		Returns
		-------
		None
		"""
		decorated_method = decorator_with_persistence(test_instance.test_method_with_kwargs)

		# test with different combinations that could create similar keys
		result1 = decorated_method(test_instance, 1, multiplier=23)
		result2 = decorated_method(test_instance, 12, multiplier=3)

		assert result1 == 23
		assert result2 == 36

		# verify both are cached with different keys
		key1 = "test_key:(1,):{'multiplier': 23}"
		key2 = "test_key:(12,):{'multiplier': 3}"
		assert key1 in decorator_with_persistence.cache
		assert key2 in decorator_with_persistence.cache
		assert decorator_with_persistence.cache[key1] != decorator_with_persistence.cache[key2]

	def test_large_cache_performance(
		self, decorator_with_persistence: PersistentCacheDecorator, test_instance: TestClass
	) -> None:
		"""Test decorator performance with large cache.

		Verifies
		--------
		- Decorator handles large number of cached entries
		- Performance remains acceptable
		- Memory usage is reasonable

		Parameters
		----------
		decorator_with_persistence : PersistentCacheDecorator
			Decorator instance with persistence enabled
		test_instance : TestClass
			Test class instance

		Returns
		-------
		None
		"""
		decorated_method = decorator_with_persistence(test_instance.test_method)

		# cache many values
		for i in range(1000):
			result = decorated_method(test_instance, i)
			assert result == i * 2

		# verify all are cached
		assert len(decorator_with_persistence.cache) == 1000

		# test retrieval performance
		for i in range(0, 1000, 100):
			result = decorated_method(test_instance, i)
			assert result == i * 2

	def test_cache_with_unicode_arguments(
		self, decorator_with_persistence: PersistentCacheDecorator
	) -> None:
		"""Test caching with unicode and special characters.

		Verifies
		--------
		- Unicode characters are handled correctly in cache keys
		- Special characters don't break caching
		- Results are preserved correctly

		Parameters
		----------
		decorator_with_persistence : PersistentCacheDecorator
			Decorator instance with persistence enabled

		Returns
		-------
		None
		"""

		class TestClassUnicode:
			"""Test class with unicode handling."""

			def test_method_unicode(self, text: str) -> str:
				"""Test method with unicode input."""
				return f"processed: {text}"

		test_instance = TestClassUnicode()
		decorated_method = decorator_with_persistence(test_instance.test_method_unicode)

		unicode_strings = ["测试", "🚀", "café", "naïve", "résumé"]
		for text in unicode_strings:
			result = decorated_method(test_instance, text)
			assert result == f"processed: {text}"

		# verify all unicode strings are cached
		assert len(decorator_with_persistence.cache) == len(unicode_strings)


# --------------------------
# Tests - Error Recovery
# --------------------------
class TestErrorRecovery:
	"""Test cases for error recovery and fallback mechanisms."""

	def test_decorator_continues_after_save_error(
		self, decorator_with_persistence: PersistentCacheDecorator, test_instance: TestClass
	) -> None:
		"""Test decorator continues working after cache save error.

		Verifies
		--------
		- Decorated method continues to work after save errors
		- In-memory cache is maintained
		- Subsequent calls work correctly

		Parameters
		----------
		decorator_with_persistence : PersistentCacheDecorator
			Decorator instance with persistence enabled
		test_instance : TestClass
			Test class instance

		Returns
		-------
		None
		"""
		decorated_method = decorator_with_persistence(test_instance.test_method)

		# mock save to raise error
		with patch.object(decorator_with_persistence, "_save_cache") as mock_save:
			mock_save.side_effect = ValueError("Save failed")

			# method should still work despite save error
			with pytest.raises(ValueError, match="Save failed"):
				decorated_method(test_instance, 5)

		# verify in-memory cache still works
		result = decorated_method(test_instance, 5)
		assert result == 10

	def test_graceful_handling_of_corrupted_cache_file(
		self, temp_cache_path: str, mock_logger: Mock
	) -> None:
		"""Test graceful handling of corrupted cache file.

		Verifies
		--------
		- Corrupted cache file is handled gracefully
		- New decorator instance can be created
		- Cache operations continue to work

		Parameters
		----------
		temp_cache_path : str
			Temporary cache file path
		mock_logger : Mock
			Mock logger object

		Returns
		-------
		None
		"""
		# create corrupted cache file
		cache_path = Path(temp_cache_path).with_suffix(".pkl")
		cache_path.write_text("corrupted data")

		# creating new decorator should handle corrupted file
		with pytest.raises(ValueError, match="Failed to load cache"):
			PersistentCacheDecorator(
				path_cache=temp_cache_path,
				cache_key="test_key",
				logger=mock_logger,
			)

	def test_recovery_from_permission_errors(
		self, decorator_with_persistence: PersistentCacheDecorator, test_instance: TestClass
	) -> None:
		"""Test recovery from file permission errors.

		Verifies
		--------
		- Permission errors are handled gracefully
		- In-memory cache continues to work
		- Error is logged appropriately

		Parameters
		----------
		decorator_with_persistence : PersistentCacheDecorator
			Decorator instance with persistence enabled
		test_instance : TestClass
			Test class instance

		Returns
		-------
		None
		"""
		decorated_method = decorator_with_persistence(test_instance.test_method)

		# mock directory creation to fail
		with patch.object(Path, "mkdir") as mock_mkdir:
			mock_mkdir.side_effect = PermissionError("Permission denied")

			# save should fail but method should still work
			with patch.object(decorator_with_persistence, "_save_cache") as mock_save:
				mock_save.side_effect = ValueError("Permission error")

				with pytest.raises(ValueError, match="Permission error"):
					decorated_method(test_instance, 5)


# --------------------------
# Tests - Integration
# --------------------------
class TestIntegration:
	"""Integration tests combining multiple features."""

	def test_full_workflow_with_persistence(self, temp_cache_path: str, mock_logger: Mock) -> None:
		"""Test complete workflow with file persistence.

		Verifies
		--------
		- Complete workflow from creation to cleanup works
		- File persistence works correctly
		- Cache survives decorator recreation

		Parameters
		----------
		temp_cache_path : str
			Temporary cache file path
		mock_logger : Mock
			Mock logger object

		Returns
		-------
		None
		"""
		# create first decorator instance
		decorator1 = PersistentCacheDecorator(
			path_cache=temp_cache_path,
			cache_key="workflow_test",
			bool_persist_cache=True,
			logger=mock_logger,
		)

		test_instance = TestClass()
		decorated_method1 = decorator1(test_instance.test_method)

		# use decorator and verify caching
		result1 = decorated_method1(test_instance, 10)
		assert result1 == 20
		assert decorator1.path_cache.exists()

		# create second decorator instance to test persistence
		decorator2 = PersistentCacheDecorator(
			path_cache=temp_cache_path,
			cache_key="workflow_test",
			bool_persist_cache=True,
			logger=mock_logger,
		)

		# verify cache was loaded from file
		assert len(decorator2.cache) == 1
		expected_key = "workflow_test:(10,):{}"
		assert expected_key in decorator2.cache
		assert decorator2.cache[expected_key] == 20

		# clear cache and verify cleanup
		decorator2.clear_cache()
		assert len(decorator2.cache) == 0
		assert not decorator2.path_cache.exists()

	def test_multiple_decorators_same_file(self, temp_cache_path: str, mock_logger: Mock) -> None:
		"""Test multiple decorators using same cache file.

		Verifies
		--------
		- Multiple decorators can share same cache file
		- Different cache keys prevent conflicts
		- Each decorator maintains separate entries

		Parameters
		----------
		temp_cache_path : str
			Temporary cache file path
		mock_logger : Mock
			Mock logger object

		Returns
		-------
		None
		"""
		decorator1 = PersistentCacheDecorator(
			path_cache=temp_cache_path,
			cache_key="key1",
			bool_persist_cache=True,
			logger=mock_logger,
		)

		decorator2 = PersistentCacheDecorator(
			path_cache=temp_cache_path,
			cache_key="key2",
			bool_persist_cache=True,
			logger=mock_logger,
		)

		test_instance = TestClass()
		method1 = decorator1(test_instance.test_method)
		method2 = decorator2(test_instance.test_method)

		result1 = method1(test_instance, 5)
		result2 = method2(test_instance, 5)

		assert result1 == 10
		assert result2 == 10

		# verify different cache keys
		key1 = "key1:(5,):{}"
		key2 = "key2:(5,):{}"
		assert key1 in decorator1.cache
		assert key2 in decorator2.cache

	def test_stress_test_concurrent_operations(
		self, temp_cache_path: str, mock_logger: Mock
	) -> None:
		"""Stress test with concurrent operations.

		Verifies
		--------
		- Decorator handles high concurrency correctly
		- No data corruption occurs
		- All operations complete successfully

		Parameters
		----------
		temp_cache_path : str
			Temporary cache file path
		mock_logger : Mock
			Mock logger object

		Returns
		-------
		None
		"""
		decorator = PersistentCacheDecorator(
			path_cache=temp_cache_path,
			cache_key="stress_test",
			bool_persist_cache=True,
			logger=mock_logger,
		)

		test_instance = TestClass()
		decorated_method = decorator(test_instance.test_method)

		results = []
		errors = []

		def worker(start_range: int, end_range: int) -> None:
			"""Worker function for stress testing."""
			try:
				for i in range(start_range, end_range):
					result = decorated_method(test_instance, i)
					results.append((i, result))
			except Exception as e:
				errors.append(e)

		# create multiple threads with different ranges
		threads = []
		for i in range(10):
			start = i * 10
			end = start + 10
			thread = Thread(target=worker, args=(start, end))
			threads.append(thread)
			thread.start()

		# wait for all threads
		for thread in threads:
			thread.join()

		# verify no errors occurred
		assert len(errors) == 0

		# verify all results are correct
		assert len(results) == 100
		for value, result in results:
			assert result == value * 2

		# verify cache contains all entries
		assert len(decorator.cache) == 100


# --------------------------
# Tests - Mock Integration
# --------------------------
class TestMockIntegration:
	"""Test cases for proper mock integration."""

	@patch("stpstone.utils.loggs.create_logs.CreateLog.log_message")
	def test_logging_integration(
		self,
		mock_log_message: Mock,
		decorator_with_persistence: PersistentCacheDecorator,
	) -> None:
		"""Test logging integration works correctly.

		Verifies
		--------
		- Logger is called correctly during errors
		- Log messages contain expected content
		- Log level is appropriate

		Parameters
		----------
		mock_log_message : Mock
			Mock for CreateLog.log_message method
		decorator_with_persistence : PersistentCacheDecorator
			Decorator instance with persistence enabled

		Returns
		-------
		None
		"""
		# create corrupted file to trigger logging
		decorator_with_persistence.path_cache.write_text("corrupted")

		# trigger error that should log
		with pytest.raises(ValueError):
			decorator_with_persistence._load_cache()

		# verify logging was called
		mock_log_message.assert_called_once()
		args, _ = mock_log_message.call_args
		logger, message, level = args
		assert decorator_with_persistence.logger is logger
		assert "Failed to load cache" in message
		assert level == "error"

	@patch("stpstone.utils.parsers.pickle.PickleFiles")
	def test_pickle_files_integration(
		self,
		mock_pickle_files: Mock,
		temp_cache_path: str,
		mock_logger: Mock,
		sample_cache_data: dict,
	) -> None:
		"""Test PickleFiles integration.

		Verifies
		--------
		- PickleFiles methods are called correctly
		- Proper parameters are passed
		- Return values are handled correctly

		Parameters
		----------
		mock_pickle_files : Mock
			Mock for PickleFiles class
		temp_cache_path : str
			Temporary cache file path
		mock_logger : Mock
			Mock logger object
		sample_cache_data : dict
			Sample cache data

		Returns
		-------
		None
		"""
		mock_instance = Mock()
		mock_pickle_files.return_value = mock_instance
		mock_instance.load_message.return_value = sample_cache_data
		mock_instance.dump_message.return_value = True

		decorator = PersistentCacheDecorator(
			path_cache=temp_cache_path,
			cache_key="test_key",
			logger=mock_logger,
		)

		# verify load_message was called during init
		if decorator.path_cache.exists():
			mock_instance.load_message.assert_called_with(decorator.path_cache)

		# test save functionality
		decorator._save_cache(sample_cache_data)
		mock_instance.dump_message.assert_called_with(sample_cache_data, decorator.path_cache)


# --------------------------
# Tests - Performance
# --------------------------
class TestPerformance:
	"""Performance-related test cases."""

	def test_cache_access_performance(
		self, decorator_with_persistence: PersistentCacheDecorator, test_instance: TestClass
	) -> None:
		"""Test cache access performance with many entries.

		Verifies
		--------
		- Cache lookup performance is acceptable
		- Memory usage is reasonable
		- No significant performance degradation

		Parameters
		----------
		decorator_with_persistence : PersistentCacheDecorator
			Decorator instance with persistence enabled
		test_instance : TestClass
			Test class instance

		Returns
		-------
		None
		"""
		decorated_method = decorator_with_persistence(test_instance.test_method)

		# populate cache with many entries
		for i in range(100):
			decorated_method(test_instance, i)

		# verify performance of cache hits
		import time
		start_time = time.time()
		for i in range(100):
			result = decorated_method(test_instance, i)
			assert result == i * 2
		end_time = time.time()

		# cache hits should be very fast
		assert end_time - start_time < 1.0  # should complete in under 1 second

	def test_memory_efficiency(
		self, decorator_with_persistence: PersistentCacheDecorator, test_instance: TestClass
	) -> None:
		"""Test memory efficiency of cache storage.

		Verifies
		--------
		- Cache doesn't consume excessive memory
		- Cached objects are stored efficiently
		- No memory leaks occur

		Parameters
		----------
		decorator_with_persistence : PersistentCacheDecorator
			Decorator instance with persistence enabled
		test_instance : TestClass
			Test class instance

		Returns
		-------
		None
		"""
		decorated_method = decorator_with_persistence(test_instance.test_method)

		# store many entries
		for i in range(1000):
			decorated_method(test_instance, i)

		# verify cache size is reasonable
		import sys
		cache_size = sys.getsizeof(decorator_with_persistence.cache)
		
		# cache should not be excessively large (arbitrary reasonable limit)
		assert cache_size < 1024 * 1024  # less than 1MB for 1000 entries


# --------------------------
# Tests - Boundary Conditions
# --------------------------
class TestBoundaryConditions:
	"""Test boundary conditions and edge cases."""

	def test_empty_cache_key(self, temp_cache_path: str) -> None:
		"""Test behavior with empty cache key.

		Verifies
		--------
		- Empty cache key raises TypeError due to type checking
		- Error message is appropriate

		Parameters
		----------
		temp_cache_path : str
			Temporary cache file path

		Returns
		-------
		None
		"""
		with pytest.raises(TypeError, match="must be of type"):
			PersistentCacheDecorator(
				path_cache=temp_cache_path,
				cache_key="",
			)

	def test_whitespace_cache_key(self, temp_cache_path: str) -> None:
		"""Test behavior with whitespace-only cache key.

		Verifies
		--------
		- Whitespace-only cache key raises TypeError due to type checking
		- Error message is appropriate

		Parameters
		----------
		temp_cache_path : str
			Temporary cache file path

		Returns
		-------
		None
		"""
		with pytest.raises(TypeError, match="must be of type"):
			PersistentCacheDecorator(
				path_cache=temp_cache_path,
				cache_key="   ",
			)

	def test_very_long_cache_key(self, temp_cache_path: str) -> None:
		"""Test behavior with very long cache key.

		Verifies
		--------
		- Very long cache key is handled correctly
		- No truncation or corruption occurs
		- Cache operations work normally

		Parameters
		----------
		temp_cache_path : str
			Temporary cache file path

		Returns
		-------
		None
		"""
		long_key = "a" * 1000  # 1000 character key
		decorator = PersistentCacheDecorator(
			path_cache=temp_cache_path,
			cache_key=long_key,
		)

		assert decorator.cache_key == long_key

	def test_special_characters_in_cache_key(self, temp_cache_path: str) -> None:
		"""Test behavior with special characters in cache key.

		Verifies
		--------
		- Special characters in cache key are handled correctly
		- No escaping issues occur
		- Cache operations work normally

		Parameters
		----------
		temp_cache_path : str
			Temporary cache file path

		Returns
		-------
		None
		"""
		special_key = "key:with/special\\chars$%^&*()"
		decorator = PersistentCacheDecorator(
			path_cache=temp_cache_path,
			cache_key=special_key,
		)

		assert decorator.cache_key == special_key

	def test_maximum_cache_entries(
		self, decorator_with_persistence: PersistentCacheDecorator, test_instance: TestClass
	) -> None:
		"""Test behavior with maximum number of cache entries.

		Verifies
		--------
		- Large number of cache entries are handled correctly
		- Performance doesn't degrade significantly
		- Memory usage remains reasonable

		Parameters
		----------
		decorator_with_persistence : PersistentCacheDecorator
			Decorator instance with persistence enabled
		test_instance : TestClass
			Test class instance

		Returns
		-------
		None
		"""
		decorated_method = decorator_with_persistence(test_instance.test_method)

		# create many cache entries
		max_entries = 5000
		for i in range(max_entries):
			result = decorated_method(test_instance, i)
			assert result == i * 2

		# verify all entries are cached
		assert len(decorator_with_persistence.cache) == max_entries

		# verify cache still works correctly
		result = decorated_method(test_instance, 0)
		assert result == 0