"""Unit tests for TTLCacheDecorator class.

Tests the TTL caching functionality with various input scenarios including:
- Initialization with valid and invalid inputs
- Cache behavior with different TTL values
- Edge cases and error conditions
- Type validation and method wrapping
"""

from datetime import datetime, timedelta
from typing import Any, Callable
from unittest.mock import Mock, patch

import pytest

from stpstone.transformations.validation.metaclass_type_checker import type_checker
from stpstone.utils.cache.cache_ttl import TTLCacheDecorator


# --------------------------
# Fixtures
# --------------------------
@pytest.fixture
def sample_method() -> Callable:
	"""Fixture providing a sample method for testing.

	Returns
	-------
	Callable
		A simple method that returns a fixed value
	"""

	def method(
		self_instance: Any,  # noqa ANN401: typing.Any is not allowed
		*args: Any,  # noqa ANN401: typing.Any is not allowed
		**kwargs: Any,  # noqa ANN401: typing.Any is not allowed
	) -> str:
		"""Sample method for testing.

		Parameters
		----------
		self_instance : Any
			Instance of the class containing the decorated method
		*args : Any
			Positional arguments passed to the method
		**kwargs : Any
			Keyword arguments passed to the method

		Returns
		-------
		str
			Result of the method
		"""
		return "test_result"

	return method


@pytest.fixture
def counting_method() -> Callable:
	"""Fixture providing a counting method for testing cache hits.

	Returns
	-------
	Callable
		A method that counts how many times it's been called
	"""
	call_count = 0

	def method(
		self_instance: Any,  # noqa ANN401: typing.Any is not allowed
		*args: Any,  # noqa ANN401: typing.Any is not allowed
		**kwargs: Any,  # noqa ANN401: typing.Any is not allowed
	) -> int:
		"""Sample method for testing.

		Parameters
		----------
		self_instance : Any
			Instance of the class containing the decorated method
		*args : Any
			Positional arguments passed to the method
		**kwargs : Any
			Keyword arguments passed to the method

		Returns
		-------
		int
			Result of the method
		"""
		nonlocal call_count
		call_count += 1
		return call_count

	return method


@pytest.fixture
def ttl_cache_instance() -> TTLCacheDecorator:
	"""Fixture providing a TTLCacheDecorator instance with 1 second TTL.

	Returns
	-------
	TTLCacheDecorator
		Instance initialized with ttl_seconds=1 and cache_key="test"
	"""
	return TTLCacheDecorator(ttl_seconds=1, cache_key="test")


@pytest.fixture
def mock_datetime() -> Mock:
	"""Fixture providing a mock datetime for time control.

	Returns
	-------
	Mock
		Mock object for datetime.datetime
	"""
	mock_dt = Mock()
	mock_dt.now.return_value = datetime(2023, 1, 1, 0, 0, 0)
	return mock_dt


# --------------------------
# Tests for Initialization
# --------------------------
class TestTTLCacheDecoratorInitialization:
	"""Test cases for TTLCacheDecorator initialization."""

	def test_init_with_valid_inputs(self) -> None:
		"""Test initialization with valid integer and string inputs.

		Verifies
		--------
		- The TTLCacheDecorator can be initialized with valid values
		- The values are correctly stored in the instance attributes
		- Cache dictionaries are properly initialized

		Returns
		-------
		None
		"""
		decorator = TTLCacheDecorator(ttl_seconds=60, cache_key="valid_key")
		assert decorator.ttl_seconds == 60
		assert decorator.cache_key == "valid_key"
		assert decorator.cache == {}
		assert decorator.last_updated == {}

	@pytest.mark.parametrize("invalid_ttl", [0, -1, -100])
	def test_init_with_non_positive_ttl_raises_value_error(self, invalid_ttl: int) -> None:
		"""Test initialization with non-positive TTL raises ValueError.

		Parameters
		----------
		invalid_ttl : int
			Invalid TTL values (0, -1, -100)

		Returns
		-------
		None
		"""
		with pytest.raises(ValueError, match="ttl_seconds must be positive"):
			TTLCacheDecorator(ttl_seconds=invalid_ttl, cache_key="test")

	@pytest.mark.parametrize("invalid_key", ["", "   ", "\t\n", " "])
	def test_init_with_empty_cache_key_raises_value_error(self, invalid_key: str) -> None:
		"""Test initialization with empty cache key raises ValueError.

		Parameters
		----------
		invalid_key : str
			Invalid cache key values (empty, whitespace-only)

		Returns
		-------
		None
		"""
		with pytest.raises(ValueError, match="cache_key cannot be empty"):
			TTLCacheDecorator(ttl_seconds=60, cache_key=invalid_key)

	def test_init_with_non_integer_ttl_raises_type_error(self) -> None:
		"""Test initialization with non-integer TTL raises TypeError.

		Verifies
		--------
		That TypeChecker metaclass enforces integer type for ttl_seconds

		Returns
		-------
		None
		"""
		with pytest.raises(TypeError, match="must be of type"):
			TTLCacheDecorator(ttl_seconds="not_an_int", cache_key="test")  # type: ignore

	def test_init_with_non_string_cache_key_raises_type_error(self) -> None:
		"""Test initialization with non-string cache key raises TypeError.

		Verifies
		--------
		That TypeChecker metaclass enforces string type for cache_key

		Returns
		-------
		None
		"""
		with pytest.raises(TypeError, match="must be of type"):
			TTLCacheDecorator(ttl_seconds=60, cache_key=123)  # type: ignore


# --------------------------
# Tests for Method Wrapping
# --------------------------
class TestTTLCacheDecoratorMethodWrapping:
	"""Test cases for TTLCacheDecorator method wrapping functionality."""

	def test_call_returns_wrapped_function(
		self, ttl_cache_instance: TTLCacheDecorator, sample_method: Callable
	) -> None:
		"""Test that __call__ returns a wrapped function.

		Verifies
		--------
		- The decorator returns a callable wrapper function
		- The wrapper has the same name as the original method
		- The wrapper preserves method metadata

		Parameters
		----------
		ttl_cache_instance : TTLCacheDecorator
			Instance of TTLCacheDecorator from fixture
		sample_method : Callable
			Sample method from fixture

		Returns
		-------
		None
		"""
		wrapped_method = ttl_cache_instance(sample_method)
		assert callable(wrapped_method)
		assert wrapped_method.__name__ == sample_method.__name__

	def test_wrapped_method_calls_original(
		self, ttl_cache_instance: TTLCacheDecorator, sample_method: Callable
	) -> None:
		"""Test that wrapped method calls the original method.

		Verifies
		--------
		- The wrapper properly calls the original method
		- The result from the original method is returned
		- The cache is populated with the result

		Parameters
		----------
		ttl_cache_instance : TTLCacheDecorator
			Instance of TTLCacheDecorator from fixture
		sample_method : Callable
			Sample method from fixture

		Returns
		-------
		None
		"""
		wrapped_method = ttl_cache_instance(sample_method)
		mock_instance = Mock()

		result = wrapped_method(mock_instance, "arg1", kwarg1="value1")

		assert result == "test_result"
		expected_key = "test:('arg1',):{'kwarg1': 'value1'}"
		assert ttl_cache_instance.cache[expected_key] == "test_result"
		assert expected_key in ttl_cache_instance.last_updated

	def test_wrapped_method_caches_results(
		self, ttl_cache_instance: TTLCacheDecorator, counting_method: Callable
	) -> None:
		"""Test that wrapped method caches results and prevents multiple calls.

		Verifies
		--------
		- The wrapper caches method results
		- Subsequent calls with same arguments return cached result
		- Original method is called only once for same arguments

		Parameters
		----------
		ttl_cache_instance : TTLCacheDecorator
			Instance of TTLCacheDecorator from fixture
		counting_method : Callable
			Counting method from fixture

		Returns
		-------
		None
		"""
		wrapped_method = ttl_cache_instance(counting_method)
		mock_instance = Mock()

		# First call should call original method
		result1 = wrapped_method(mock_instance, "same_arg")
		assert result1 == 1

		# Second call with same args should return cached result
		result2 = wrapped_method(mock_instance, "same_arg")
		assert result2 == 1  # Same as first call, not 2

		# Different args should call original method again
		result3 = wrapped_method(mock_instance, "different_arg")
		assert result3 == 2


# --------------------------
# Tests for Cache Behavior
# --------------------------
class TestTTLCacheDecoratorCacheBehavior:
	"""Test cases for TTLCacheDecorator cache expiration behavior."""

	@patch("stpstone.utils.cache.cache_ttl.datetime")
	def test_cache_expiration_with_ttl(
		self, mock_dt: Mock, ttl_cache_instance: TTLCacheDecorator, counting_method: Callable
	) -> None:
		"""Test that cache entries expire after TTL period.

		Verifies
		--------
		- Cache entries are valid within TTL period
		- Cache entries expire after TTL period
		- Expired entries cause original method to be called again

		Parameters
		----------
		mock_dt : Mock
			Mock datetime object
		ttl_cache_instance : TTLCacheDecorator
			Instance of TTLCacheDecorator from fixture
		counting_method : Callable
			Counting method from fixture

		Returns
		-------
		None
		"""
		# Set up mock datetime
		current_time = datetime(2023, 1, 1, 0, 0, 0)
		mock_dt.now.return_value = current_time

		wrapped_method = ttl_cache_instance(counting_method)
		mock_instance = Mock()

		# First call at time 0
		result1 = wrapped_method(mock_instance, "test_arg")
		assert result1 == 1

		# Second call at time 0.5 seconds (within TTL)
		mock_dt.now.return_value = current_time + timedelta(seconds=0.5)
		result2 = wrapped_method(mock_instance, "test_arg")
		assert result2 == 1  # Still cached

		# Third call at time 1.5 seconds (after TTL)
		mock_dt.now.return_value = current_time + timedelta(seconds=1.5)
		result3 = wrapped_method(mock_instance, "test_arg")
		assert result3 == 2  # Cache expired, method called again

	def test_different_arguments_create_different_cache_entries(
		self, ttl_cache_instance: TTLCacheDecorator, counting_method: Callable
	) -> None:
		"""Test that different arguments create separate cache entries.

		Verifies
		--------
		- Different positional arguments create different cache keys
		- Different keyword arguments create different cache keys
		- Each cache entry has its own TTL tracking

		Parameters
		----------
		ttl_cache_instance : TTLCacheDecorator
			Instance of TTLCacheDecorator from fixture
		counting_method : Callable
			Counting method from fixture

		Returns
		-------
		None
		"""
		wrapped_method = ttl_cache_instance(counting_method)
		mock_instance = Mock()

		# Call with different positional arguments
		result1 = wrapped_method(mock_instance, "arg1")
		result2 = wrapped_method(mock_instance, "arg2")

		assert result1 == 1
		assert result2 == 2

		# Call with different keyword arguments
		result3 = wrapped_method(mock_instance, "same_arg", kw1="value1")
		result4 = wrapped_method(mock_instance, "same_arg", kw2="value2")

		assert result3 == 3
		assert result4 == 4

		# Verify all have separate cache entries
		assert len(ttl_cache_instance.cache) == 4
		assert len(ttl_cache_instance.last_updated) == 4

	def test_cache_key_generation_includes_all_arguments(
		self, ttl_cache_instance: TTLCacheDecorator, sample_method: Callable
	) -> None:
		"""Test that cache key generation includes all method arguments.

		Verifies
		--------
		- Positional arguments are included in cache key
		- Keyword arguments are included in cache key
		- Complex arguments are properly serialized in key

		Parameters
		----------
		ttl_cache_instance : TTLCacheDecorator
			Instance of TTLCacheDecorator from fixture
		sample_method : Callable
			Sample method from fixture

		Returns
		-------
		None
		"""
		wrapped_method = ttl_cache_instance(sample_method)
		mock_instance = Mock()

		# Call with various argument combinations
		wrapped_method(mock_instance, "pos1", 42, kw1="value1", kw2=123)

		expected_key = "test:('pos1', 42):{'kw1': 'value1', 'kw2': 123}"
		assert expected_key in ttl_cache_instance.cache


# --------------------------
# Tests for Error Conditions
# --------------------------
class TestTTLCacheDecoratorErrorConditions:
	"""Test cases for TTLCacheDecorator error handling."""

	def test_original_method_exceptions_are_propagated(
		self, ttl_cache_instance: TTLCacheDecorator
	) -> None:
		"""Test that exceptions from original method are propagated.

		Verifies
		--------
		- Exceptions raised by the original method are not caught by the wrapper
		- The wrapper doesn't interfere with exception propagation
		- Cache is not populated when method raises exception

		Parameters
		----------
		ttl_cache_instance : TTLCacheDecorator
			Instance of TTLCacheDecorator from fixture

		Returns
		-------
		None

		Raises
		------
		ValueError
			Exception raised in the failing method
		"""

		def failing_method(
			self_instance: Any,  # noqa ANN401: typing.Any is not allowed
			*args: Any,  # noqa ANN401: typing.Any is not allowed
			**kwargs: Any,  # noqa ANN401: typing.Any is not allowed
		) -> Any:  # noqa ANN401: typing.Any is not allowed
			"""Failing method that raises an exception.

			Parameters
			----------
			self_instance : Any
				Instance of the class containing the decorated method
			*args : Any
				Positional arguments passed to the method
			**kwargs : Any
				Keyword arguments passed to the method

			Returns
			-------
			Any
				Result of the method

			Raises
			------
			ValueError
				Exception raised in the method
			"""
			raise ValueError("Original method failed")

		wrapped_method = ttl_cache_instance(failing_method)
		mock_instance = Mock()

		with pytest.raises(ValueError, match="Original method failed"):
			wrapped_method(mock_instance, "test_arg")

		# Cache should not contain the failed result
		expected_key = "test:('test_arg',):{}"
		assert expected_key not in ttl_cache_instance.cache

	def test_cache_does_not_interfere_with_different_instances(
		self, ttl_cache_instance: TTLCacheDecorator, counting_method: Callable
	) -> None:
		"""Test that cache works correctly with different class instances.

		Verifies
		--------
		- Cache is shared across instances of the same class
		- Method results are cached regardless of instance
		- Self parameter doesn't affect cache key

		Parameters
		----------
		ttl_cache_instance : TTLCacheDecorator
			Instance of TTLCacheDecorator from fixture
		counting_method : Callable
			Counting method from fixture

		Returns
		-------
		None
		"""
		wrapped_method = ttl_cache_instance(counting_method)

		# Create two different mock instances
		instance1 = Mock()
		instance2 = Mock()

		# Call with same arguments on different instances
		result1 = wrapped_method(instance1, "same_arg")
		result2 = wrapped_method(instance2, "same_arg")

		# Should return cached result from first call
		assert result1 == 1
		assert result2 == 1  # Not 2, because cache is shared


# --------------------------
# Tests for Edge Cases
# --------------------------
class TestTTLCacheDecoratorEdgeCases:
	"""Test cases for TTLCacheDecorator edge case handling."""

	def test_empty_arguments_caching(
		self, ttl_cache_instance: TTLCacheDecorator, counting_method: Callable
	) -> None:
		"""Test caching behavior with empty arguments.

		Verifies
		--------
		- Methods with no arguments are properly cached
		- Methods with empty tuples/lists as arguments are handled
		- Empty keyword arguments are handled correctly

		Parameters
		----------
		ttl_cache_instance : TTLCacheDecorator
			Instance of TTLCacheDecorator from fixture
		counting_method : Callable
			Counting method from fixture

		Returns
		-------
		None
		"""
		wrapped_method = ttl_cache_instance(counting_method)
		mock_instance = Mock()

		# Call with no arguments
		result1 = wrapped_method(mock_instance)
		result2 = wrapped_method(mock_instance)

		assert result1 == 1
		assert result2 == 1  # Cached

		# Call with empty tuple and dict
		result3 = wrapped_method(mock_instance, (), {})
		result4 = wrapped_method(mock_instance, (), {})

		assert result3 == 2
		assert result4 == 2  # Cached

	def test_none_arguments_caching(
		self, ttl_cache_instance: TTLCacheDecorator, counting_method: Callable
	) -> None:
		"""Test caching behavior with None arguments.

		Verifies
		--------
		- None as positional argument is handled correctly
		- None as keyword argument is handled correctly
		- Cache distinguishes between None and other values

		Parameters
		----------
		ttl_cache_instance : TTLCacheDecorator
			Instance of TTLCacheDecorator from fixture
		counting_method : Callable
			Counting method from fixture

		Returns
		-------
		None
		"""
		wrapped_method = ttl_cache_instance(counting_method)
		mock_instance = Mock()

		# Call with None arguments
		result1 = wrapped_method(mock_instance, None)
		result2 = wrapped_method(mock_instance, None)
		result3 = wrapped_method(mock_instance, arg=None)
		result4 = wrapped_method(mock_instance, arg=None)

		assert result1 == 1
		assert result2 == 1  # Cached
		assert result3 == 2  # Different cache key (keyword vs positional)
		assert result4 == 2  # Cached

	def test_large_ttl_values(self) -> None:
		"""Test behavior with very large TTL values.

		Verifies
		--------
		- Large TTL values are accepted (within integer limits)
		- Cache behaves correctly with large TTL values
		- No overflow or performance issues with large TTL

		Returns
		-------
		None
		"""
		# Test with very large TTL (1 year in seconds)
		large_ttl = 365 * 24 * 60 * 60
		decorator = TTLCacheDecorator(ttl_seconds=large_ttl, cache_key="large_ttl")

		assert decorator.ttl_seconds == large_ttl

		# Test that it works with a method
		def test_method(
			self_instance: Any,  # noqa ANN401: typing.Any is not allowed
			*args: Any,  # noqa ANN401: typing.Any is not allowed
			**kwargs: Any,  # noqa ANN401: typing.Any is not allowed
		) -> str:
			"""Sample method for testing.

			Parameters
			----------
			self_instance : Any
				Instance of the class containing the decorated method
			*args : Any
				Positional arguments passed to the method
			**kwargs : Any
				Keyword arguments passed to the method

			Returns
			-------
			str
				Result of the method
			"""
			return "result"

		wrapped_method = decorator(test_method)
		mock_instance = Mock()

		result = wrapped_method(mock_instance, "test")
		assert result == "result"


# --------------------------
# Tests for Type Validation
# --------------------------
class TestTTLCacheDecoratorTypeValidation:
	"""Test cases for TTLCacheDecorator type validation."""

	def test_type_checker_metaclass_enforcement(self) -> None:
		"""Test that TypeChecker metaclass enforces type hints.

		Verifies
		--------
		- TypeChecker properly validates method parameter types
		- Incorrect types raise TypeError with appropriate message
		- Type validation works for both positional and keyword args
		"""
		decorator = TTLCacheDecorator(ttl_seconds=60, cache_key="test")

		@type_checker
		def typed_method(
			self_instance: Any,  # noqa ANN401: typing.Any is not allowed
			number: int,
			text: str,
		) -> str:
			"""Sample method with type hints.

			Parameters
			----------
			self_instance : Any
				Instance of the class containing the decorated method
			number : int
				Number parameter
			text : str
				Text parameter

			Returns
			-------
			str
				Concatenated text and number
			"""
			return f"{text}_{number}"

		wrapped_method = decorator(typed_method)
		mock_instance = Mock()

		# Valid call
		result = wrapped_method(mock_instance, 42, "hello")
		assert result == "hello_42"

		# Invalid call - wrong type for number
		with pytest.raises(TypeError, match="must be of type"):
			wrapped_method(mock_instance, "not_a_number", "hello")  # type: ignore

		# Invalid call - wrong type for text
		with pytest.raises(TypeError, match="must be of type"):
			wrapped_method(mock_instance, 42, 123)  # type: ignore
