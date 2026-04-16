"""Unit tests for cache management utilities.

Tests the cache management functionality including decorators for cache clearing,
validation methods, and automatic application of cache reset behavior to specified
methods with various input scenarios and edge cases.
"""

from typing import Any, Callable
from unittest.mock import patch

import pytest

from stpstone.utils.cache.cache_reset import (
	CacheResetDecorator,
	_validate_cache_clear_methods,
	_validate_method_cache_pairs,
	auto_cache_reset_methods,
	clear_multiple_caches,
)


# --------------------------
# Test Classes and Utilities
# --------------------------
class MockClass:
	"""Mock class for testing cache decorators."""

	def __init__(self) -> None:
		"""Initialize mock class with tracking attributes.

		Parameters
		----------
		None

		Returns
		-------
		None
		"""
		self.cache_cleared = False
		self.cache_a_cleared = False
		self.cache_b_cleared = False
		self.method_called = False

	def clear_cache(self) -> None:
		"""Mock cache clearing method.

		Parameters
		----------
		None

		Returns
		-------
		None
		"""
		self.cache_cleared = True

	def clear_cache_a(self) -> None:
		"""Mock cache A clearing method.

		Parameters
		----------
		None

		Returns
		-------
		None
		"""
		self.cache_a_cleared = True

	def clear_cache_b(self) -> None:
		"""Mock cache B clearing method.

		Parameters
		----------
		None

		Returns
		-------
		None
		"""
		self.cache_b_cleared = True

	def test_method(self) -> str:
		"""Mock method to be decorated.

		Parameters
		----------
		None

		Returns
		-------
		str
			Test result
		"""
		self.method_called = True
		return "test_result"

	def method_with_args(self, arg1: str, arg2: int = 42) -> str:
		"""Mock method with arguments.

		Parameters
		----------
		arg1 : str
			First argument
		arg2 : int
			Second argument, default is 42

		Returns
		-------
		str
			Concatenated result of arguments
		"""
		self.method_called = True
		return f"{arg1}_{arg2}"


class MockClassNonCallable:
	"""Mock class with non-callable attribute."""

	def __init__(self) -> None:
		"""Initialize with non-callable cache attribute.

		Parameters
		----------
		None

		Returns
		-------
		None
		"""
		self.clear_cache = "not_callable"

	def test_method(self) -> str:
		"""Mock method to be decorated.

		Parameters
		----------
		None

		Returns
		-------
		str
			Test result
		"""
		return "test_result"


class MockClassMissingMethod:
	"""Mock class without cache clearing method."""

	def test_method(self) -> str:
		"""Mock method to be decorated.

		Parameters
		----------
		None

		Returns
		-------
		str
			Test result
		"""
		return "test_result"


# --------------------------
# Fixtures
# --------------------------
@pytest.fixture
def mock_instance() -> MockClass:
	"""Fixture providing MockClass instance.

	Returns
	-------
	MockClass
		Fresh instance of MockClass for testing
	"""
	return MockClass()


@pytest.fixture
def cache_decorator() -> CacheResetDecorator:
	"""Fixture providing CacheResetDecorator instance.

	Returns
	-------
	CacheResetDecorator
		Decorator configured with 'clear_cache' method
	"""
	return CacheResetDecorator("clear_cache")


@pytest.fixture
def cache_decorator_no_refresh() -> CacheResetDecorator:
	"""Fixture providing CacheResetDecorator with force_refresh=False.

	Returns
	-------
	CacheResetDecorator
		Decorator configured to not force refresh
	"""
	return CacheResetDecorator("clear_cache", force_refresh=False)


# --------------------------
# Tests for CacheResetDecorator.__init__
# --------------------------
def test_cache_reset_decorator_init_valid() -> None:
	"""Test CacheResetDecorator initialization with valid inputs.

	Verifies
	--------
	- The decorator can be initialized with valid string method name
	- The attributes are correctly stored
	- Default force_refresh is True

	Returns
	-------
	None
	"""
	decorator = CacheResetDecorator("clear_cache")
	assert decorator.cache_clear_method == "clear_cache"
	assert decorator.force_refresh is True


def test_cache_reset_decorator_init_with_force_refresh_false() -> None:
	"""Test CacheResetDecorator initialization with force_refresh=False.

	Verifies
	--------
	- The decorator can be initialized with force_refresh=False
	- The force_refresh attribute is correctly set to False

	Returns
	-------
	None
	"""
	decorator = CacheResetDecorator("clear_cache", force_refresh=False)
	assert decorator.cache_clear_method == "clear_cache"
	assert decorator.force_refresh is False


@pytest.mark.parametrize("invalid_method", ["", "  ", "\t", "\n"])
def test_cache_reset_decorator_init_empty_method(invalid_method: str) -> None:
	"""Test CacheResetDecorator initialization with empty method names.

	Verifies
	--------
	- ValueError is raised for empty or whitespace-only method names
	- Error message contains appropriate text

	Parameters
	----------
	invalid_method : str
		Invalid method name to test

	Returns
	-------
	None
	"""
	with pytest.raises(ValueError, match="cache_clear_method cannot be empty"):
		CacheResetDecorator(invalid_method)


# --------------------------
# Tests for CacheResetDecorator._validate_cache_clear_method
# --------------------------
def test_validate_cache_clear_method_valid() -> None:
	"""Test _validate_cache_clear_method with valid string.

	Verifies
	--------
	- No exception is raised for valid method name
	- Method completes successfully

	Returns
	-------
	None
	"""
	decorator = CacheResetDecorator.__new__(CacheResetDecorator)
	# should not raise
	decorator._validate_cache_clear_method("valid_method")


@pytest.mark.parametrize("invalid_method", ["", "  ", "\t\n"])
def test_validate_cache_clear_method_empty(invalid_method: str) -> None:
	"""Test _validate_cache_clear_method with empty strings.

	Verifies
	--------
	- ValueError is raised for empty method names
	- Error message is appropriate

	Parameters
	----------
	invalid_method : str
		Invalid method name to test

	Returns
	-------
	None
	"""
	decorator = CacheResetDecorator.__new__(CacheResetDecorator)
	with pytest.raises(ValueError, match="cache_clear_method cannot be empty"):
		decorator._validate_cache_clear_method(invalid_method)


# --------------------------
# Tests for CacheResetDecorator.__call__
# --------------------------
def test_cache_reset_decorator_call_success(
	cache_decorator: CacheResetDecorator, mock_instance: MockClass
) -> None:
	"""Test successful decoration and execution.

	Verifies
	--------
	- The decorator successfully wraps the method
	- Cache is cleared before method execution
	- Method is executed and returns correct result
	- All expected side effects occur

	Parameters
	----------
	cache_decorator : CacheResetDecorator
		Decorator instance from fixture
	mock_instance : MockClass
		Mock class instance from fixture

	Returns
	-------
	None
	"""
	decorated_method = cache_decorator(mock_instance.test_method)
	result = decorated_method(mock_instance)

	assert result == "test_result"
	assert mock_instance.cache_cleared is True
	assert mock_instance.method_called is True


def test_cache_reset_decorator_call_with_args(
	cache_decorator: CacheResetDecorator, mock_instance: MockClass
) -> None:
	"""Test decoration with method arguments.

	Verifies
	--------
	- Arguments are properly passed through to decorated method
	- Cache clearing still occurs
	- Return value includes arguments

	Parameters
	----------
	cache_decorator : CacheResetDecorator
		Decorator instance from fixture
	mock_instance : MockClass
		Mock class instance from fixture

	Returns
	-------
	None
	"""
	decorated_method = cache_decorator(MockClass.method_with_args)
	# Call with instance and arguments
	result = decorated_method(mock_instance, "test", arg2=99)

	assert result == "test_99"
	assert mock_instance.cache_cleared is True
	assert mock_instance.method_called is True


def test_cache_reset_decorator_no_refresh(
	cache_decorator_no_refresh: CacheResetDecorator, mock_instance: MockClass
) -> None:
	"""Test decorator with force_refresh=False.

	Verifies
	--------
	- Cache is not cleared when force_refresh=False
	- Method is still executed normally
	- Return value is correct

	Parameters
	----------
	cache_decorator_no_refresh : CacheResetDecorator
		Decorator with force_refresh=False from fixture
	mock_instance : MockClass
		Mock class instance from fixture

	Returns
	-------
	None
	"""
	decorated_method = cache_decorator_no_refresh(mock_instance.test_method)
	result = decorated_method(mock_instance)

	assert result == "test_result"
	assert mock_instance.cache_cleared is False  # should not be cleared
	assert mock_instance.method_called is True


def test_cache_reset_decorator_method_not_found() -> None:
	"""Test decorator when cache method is not found.

	Verifies
	--------
	- AttributeError is raised when cache method doesn't exist
	- Error message contains method name
	- Original method is not executed

	Returns
	-------
	None
	"""
	decorator = CacheResetDecorator("nonexistent_method")
	mock_instance = MockClassMissingMethod()
	decorated_method = decorator(mock_instance.test_method)

	with pytest.raises(AttributeError, match="not found or not callable"):
		decorated_method(mock_instance)


def test_cache_reset_decorator_method_not_callable() -> None:
	"""Test decorator when cache method is not callable.

	Verifies
	--------
	- AttributeError is raised when cache method is not callable
	- Error message indicates method is not callable
	- Original method is not executed

	Returns
	-------
	None
	"""
	decorator = CacheResetDecorator("clear_cache")
	mock_instance = MockClassNonCallable()
	decorated_method = decorator(mock_instance.test_method)

	with pytest.raises(AttributeError, match="not found or not callable"):
		decorated_method(mock_instance)


def test_cache_reset_decorator_getattr_exception() -> None:
	"""Test decorator when getattr raises exception."""
	decorator = CacheResetDecorator("clear_cache")

	def test_method(
		self: Any,  # noqa ANN401: typing.Any is not allowed
	) -> str:
		"""Test method.

		Returns
		-------
		str
			Test result
		"""
		return "test"

	decorated_method = decorator(test_method)
	mock_instance = MockClass()

	with patch("stpstone.utils.cache.cache_reset.getattr") as mock_getattr:
		mock_getattr.side_effect = RuntimeError("getattr failed")
		with pytest.raises(
			AttributeError, match=r"Failed to access cache-clearing method.*getattr failed"
		):
			decorated_method(mock_instance)

		try:
			decorated_method(mock_instance)
		except AttributeError as exc:
			assert isinstance(exc.__cause__, RuntimeError)
			assert str(exc.__cause__) == "getattr failed"


# --------------------------
# Tests for clear_multiple_caches
# --------------------------
def test_clear_multiple_caches_success(mock_instance: MockClass) -> None:
	"""Test clear_multiple_caches with valid methods.

	Verifies
	--------
	- Multiple cache methods are called in order
	- Original method is executed after cache clearing
	- Return value is preserved

	Parameters
	----------
	mock_instance : MockClass
		Mock class instance from fixture

	Returns
	-------
	None
	"""
	decorated_method = clear_multiple_caches(
		mock_instance.test_method, ["clear_cache_a", "clear_cache_b"]
	)
	result = decorated_method(mock_instance)

	assert result == "test_result"
	assert mock_instance.cache_a_cleared is True
	assert mock_instance.cache_b_cleared is True
	assert mock_instance.method_called is True


def test_clear_multiple_caches_single_method(mock_instance: MockClass) -> None:
	"""Test clear_multiple_caches with single method.

	Verifies
	--------
	- Single cache method is called
	- Original method is executed
	- Return value is preserved

	Parameters
	----------
	mock_instance : MockClass
		Mock class instance from fixture

	Returns
	-------
	None
	"""
	decorated_method = clear_multiple_caches(mock_instance.test_method, ["clear_cache"])
	result = decorated_method(mock_instance)

	assert result == "test_result"
	assert mock_instance.cache_cleared is True
	assert mock_instance.method_called is True


def test_clear_multiple_caches_with_args(mock_instance: MockClass) -> None:
	"""Test clear_multiple_caches preserves method arguments.

	Verifies
	--------
	- Arguments are passed through correctly
	- Cache methods are still called
	- Return value includes arguments

	Parameters
	----------
	mock_instance : MockClass
		Mock class instance from fixture

	Returns
	-------
	None
	"""
	decorated_method = clear_multiple_caches(MockClass.method_with_args, ["clear_cache_a"])
	# Call with instance and arguments
	result = decorated_method(mock_instance, "hello", arg2=123)

	assert result == "hello_123"
	assert mock_instance.cache_a_cleared is True
	assert mock_instance.method_called is True


def test_clear_multiple_caches_method_not_found() -> None:
	"""Test clear_multiple_caches when cache method is missing.

	Verifies
	--------
	- AttributeError is raised for missing method
	- Error message contains method name
	- Original method is not executed

	Returns
	-------
	None
	"""
	mock_instance = MockClassMissingMethod()
	decorated_method = clear_multiple_caches(mock_instance.test_method, ["nonexistent_method"])

	with pytest.raises(AttributeError, match="not found or not callable"):
		decorated_method(mock_instance)


def test_clear_multiple_caches_method_not_callable() -> None:
	"""Test clear_multiple_caches when cache method is not callable.

	Verifies
	--------
	- AttributeError is raised for non-callable method
	- Error message indicates method is not callable
	- Original method is not executed

	Returns
	-------
	None
	"""
	mock_instance = MockClassNonCallable()
	decorated_method = clear_multiple_caches(mock_instance.test_method, ["clear_cache"])

	with pytest.raises(AttributeError, match="not found or not callable"):
		decorated_method(mock_instance)


def test_clear_multiple_caches_partial_failure() -> None:
	"""Test clear_multiple_caches when some methods fail.

	Verifies
	--------
	- Failure on first missing method prevents execution
	- AttributeError is raised for first failing method
	- Subsequent methods are not attempted

	Returns
	-------
	None
	"""
	mock_instance = MockClass()
	# add a non-callable attribute
	mock_instance.bad_method = "not_callable"

	decorated_method = clear_multiple_caches(
		mock_instance.test_method, ["clear_cache", "bad_method", "clear_cache_a"]
	)

	with pytest.raises(AttributeError, match="bad_method.*not found or not callable"):
		decorated_method(mock_instance)

	# first method should have been called
	assert mock_instance.cache_cleared is True
	# but the original method should not have been called
	assert mock_instance.method_called is False


def test_clear_multiple_caches_getattr_exception() -> None:
	"""Test clear_multiple_caches when getattr raises exception.

	Verifies
	--------
	- AttributeError is raised for missing method
	- Error message contains method name
	- Original method is not executed

	Returns
	-------
	None
	"""

	def test_method(
		self: Any,  # noqa ANN401: typing.Any is not allowed
	) -> str:
		"""Test method.

		Returns
		-------
		str
			Test result
		"""
		return "test"

	decorated_method = clear_multiple_caches(test_method, ["clear_cache"])
	mock_instance = MockClass()

	with patch("stpstone.utils.cache.cache_reset.getattr") as mock_getattr:
		mock_getattr.side_effect = RuntimeError("getattr failed")
		with pytest.raises(
			AttributeError, match=r"Failed to access cache-clearing method.*getattr failed"
		):
			decorated_method(mock_instance)

		try:
			decorated_method(mock_instance)
		except AttributeError as exc:
			assert isinstance(exc.__cause__, RuntimeError)
			assert str(exc.__cause__) == "getattr failed"


# --------------------------
# Tests for _validate_cache_clear_methods
# --------------------------
def test_validate_cache_clear_methods_valid() -> None:
	"""Test _validate_cache_clear_methods with valid input.

	Verifies
	--------
	- No exception is raised for valid method list
	- Function completes successfully

	Returns
	-------
	None
	"""
	# should not raise
	_validate_cache_clear_methods(["method1", "method2", "method3"])


def test_validate_cache_clear_methods_single_method() -> None:
	"""Test _validate_cache_clear_methods with single method.

	Verifies
	--------
	- No exception is raised for single method list
	- Function completes successfully

	Returns
	-------
	None
	"""
	# should not raise
	_validate_cache_clear_methods(["single_method"])


def test_validate_cache_clear_methods_empty_list() -> None:
	"""Test _validate_cache_clear_methods with empty list.

	Verifies
	--------
	- ValueError is raised for empty list
	- Error message indicates list cannot be empty

	Returns
	-------
	None
	"""
	with pytest.raises(ValueError, match="cache_clear_methods cannot be empty"):
		_validate_cache_clear_methods([])


@pytest.mark.parametrize(
	"empty_methods,expected_index",
	[
		([""], 0),
		(["valid", "  "], 1),
		(["valid", "also_valid", "\t\n"], 2),
	],
)
def test_validate_cache_clear_methods_empty_method_names(
	empty_methods: list[str], expected_index: int
) -> None:
	"""Test _validate_cache_clear_methods with empty method names.

	Verifies
	--------
	- ValueError is raised for empty method names
	- Error message includes the correct index

	Parameters
	----------
	empty_methods : list[str]
		List containing empty method names
	expected_index : int
		Expected index of the empty method

	Returns
	-------
	None
	"""
	with pytest.raises(ValueError, match=f"Method name at index {expected_index} cannot be empty"):
		_validate_cache_clear_methods(empty_methods)


# --------------------------
# Tests for _validate_method_cache_pairs
# --------------------------
def test_validate_method_cache_pairs_valid() -> None:
	"""Test _validate_method_cache_pairs with valid input.

	Verifies
	--------
	- No exception is raised for valid pairs
	- Function completes successfully

	Returns
	-------
	None
	"""
	pairs = [
		("method1", ["cache1"]),
		("method2", ["cache2", "cache3"]),
	]
	# should not raise
	_validate_method_cache_pairs(pairs)


def test_validate_method_cache_pairs_single_pair() -> None:
	"""Test _validate_method_cache_pairs with single pair.

	Verifies
	--------
	- No exception is raised for single pair
	- Function completes successfully

	Returns
	-------
	None
	"""
	pairs = [("method", ["cache"])]
	# should not raise
	_validate_method_cache_pairs(pairs)


def test_validate_method_cache_pairs_empty_list() -> None:
	"""Test _validate_method_cache_pairs with empty list.

	Verifies
	--------
	- ValueError is raised for empty list
	- Error message indicates list cannot be empty

	Returns
	-------
	None
	"""
	with pytest.raises(ValueError, match="method_cache_pairs cannot be empty"):
		_validate_method_cache_pairs([])


@pytest.mark.parametrize(
	"empty_method_names,expected_index",
	[
		([("", ["cache"])], 0),
		([("method", ["cache"]), ("  ", ["cache2"])], 1),
	],
)
def test_validate_method_cache_pairs_empty_method_names(
	empty_method_names: list[tuple[str, list[str]]], expected_index: int
) -> None:
	"""Test _validate_method_cache_pairs with empty method names.

	Verifies
	--------
	- ValueError is raised for empty method names
	- Error message includes the correct index

	Parameters
	----------
	empty_method_names : list[tuple[str, list[str]]]
		List containing tuples with empty method names
	expected_index : int
		Expected index of the empty method name

	Returns
	-------
	None
	"""
	with pytest.raises(ValueError, match=f"Method name at index {expected_index} cannot be empty"):
		_validate_method_cache_pairs(empty_method_names)


@pytest.mark.parametrize(
	"empty_cache_methods,expected_index",
	[
		([("method", [])], 0),
		([("method", ["cache"]), ("method2", [])], 1),
	],
)
def test_validate_method_cache_pairs_empty_cache_methods(
	empty_cache_methods: list[tuple[str, list]], expected_index: int
) -> None:
	"""Test _validate_method_cache_pairs with empty cache methods.

	Verifies
	--------
	- ValueError is raised for empty cache methods lists
	- Error message includes the correct index

	Parameters
	----------
	empty_cache_methods : list[tuple[str, list]]
		List containing tuples with empty cache methods
	expected_index : int
		Expected index of the empty cache methods

	Returns
	-------
	None
	"""
	with pytest.raises(
		ValueError, match=f"Cache methods list at index {expected_index} cannot be empty"
	):
		_validate_method_cache_pairs(empty_cache_methods)


@pytest.mark.parametrize(
	"empty_cache_method_names,expected_i,expected_j",
	[
		([("method", [""])], 0, 0),
		([("method", ["cache"]), ("method2", ["cache2", "  "])], 1, 1),
	],
)
def test_validate_method_cache_pairs_empty_cache_method_names(
	empty_cache_method_names: list[tuple[str, list[str]]], expected_i: int, expected_j: int
) -> None:
	"""Test _validate_method_cache_pairs with empty cache method names.

	Verifies
	--------
	- ValueError is raised for empty cache method names
	- Error message includes the correct indices

	Parameters
	----------
	empty_cache_method_names : list[tuple[str, list[str]]]
		List containing tuples with empty cache method names
	expected_i : int
		Expected index of the method pair
	expected_j : int
		Expected index of the empty cache method

	Returns
	-------
	None
	"""
	with pytest.raises(
		ValueError, match=f"Cache method at index {expected_i}, {expected_j} cannot be empty"
	):
		_validate_method_cache_pairs(empty_cache_method_names)


# --------------------------
# Tests for auto_cache_reset_methods
# --------------------------
def test_auto_cache_reset_methods_single_cache_method() -> None:
	"""Test auto_cache_reset_methods with single cache method.

	Verifies
	--------
	- Class decorator applies CacheResetDecorator to specified methods
	- Decorated methods clear cache before execution
	- Original functionality is preserved

	Returns
	-------
	None
	"""

	@auto_cache_reset_methods([("test_method", ["clear_cache"])])
	class TestClass:
		"""Test class for single cache method."""

		def __init__(self) -> None:
			"""Initialize class.

			Parameters
			----------
			None

			Returns
			-------
			None
			"""
			self.cache_cleared = False
			self.method_called = False

		def clear_cache(self) -> None:
			"""Clear cache.

			Parameters
			----------
			None

			Returns
			-------
			None
			"""
			self.cache_cleared = True

		def test_method(self) -> str:
			"""Test method.

			Parameters
			----------
			None

			Returns
			-------
			str
				Test result
			"""
			self.method_called = True
			return "result"

	instance = TestClass()
	result = instance.test_method()

	assert result == "result"
	assert instance.cache_cleared is True
	assert instance.method_called is True


def test_auto_cache_reset_methods_multiple_cache_methods() -> None:
	"""Test auto_cache_reset_methods with multiple cache methods.

	Verifies
	--------
	- Class decorator applies clear_multiple_caches to methods with multiple cache methods
	- All cache methods are called before execution
	- Original functionality is preserved

	Returns
	-------
	None
	"""

	@auto_cache_reset_methods([("test_method", ["clear_cache_a", "clear_cache_b"])])
	class TestClass:
		"""Test class for multiple cache methods."""

		def __init__(self) -> None:
			"""Initialize class.

			Parameters
			----------
			None

			Returns
			-------
			None
			"""
			self.cache_a_cleared = False
			self.cache_b_cleared = False
			self.method_called = False

		def clear_cache_a(self) -> None:
			"""Clear cache A.

			Parameters
			----------
			None

			Returns
			-------
			None
			"""
			self.cache_a_cleared = True

		def clear_cache_b(self) -> None:
			"""Clear cache B.

			Parameters
			----------
			None

			Returns
			-------
			None
			"""
			self.cache_b_cleared = True

		def test_method(self) -> str:
			"""Test method.

			Parameters
			----------
			None

			Returns
			-------
			str
				Test result
			"""
			self.method_called = True
			return "result"

	instance = TestClass()
	result = instance.test_method()

	assert result == "result"
	assert instance.cache_a_cleared is True
	assert instance.cache_b_cleared is True
	assert instance.method_called is True


def test_auto_cache_reset_methods_multiple_methods() -> None:
	"""Test auto_cache_reset_methods with multiple methods.

	Verifies
	--------
	- Multiple methods can be decorated in the same class
	- Each method uses its own cache clearing configuration
	- All functionality is preserved

	Returns
	-------
	None
	"""

	@auto_cache_reset_methods(
		[("method_a", ["clear_cache_a"]), ("method_b", ["clear_cache_b", "clear_cache_c"])]
	)
	class TestClass:
		"""Test class for multiple methods."""

		def __init__(self) -> None:
			"""Initialize class.

			Parameters
			----------
			None

			Returns
			-------
			None
			"""
			self.cache_a_cleared = False
			self.cache_b_cleared = False
			self.cache_c_cleared = False
			self.method_a_called = False
			self.method_b_called = False

		def clear_cache_a(self) -> None:
			"""Clear cache A.

			Parameters
			----------
			None

			Returns
			-------
			None
			"""
			self.cache_a_cleared = True

		def clear_cache_b(self) -> None:
			"""Clear cache B.

			Parameters
			----------
			None

			Returns
			-------
			None
			"""
			self.cache_b_cleared = True

		def clear_cache_c(self) -> None:
			"""Clear cache C.

			Parameters
			----------
			None

			Returns
			-------
			None
			"""
			self.cache_c_cleared = True

		def method_a(self) -> str:
			"""Return Method A.

			Parameters
			----------
			None

			Returns
			-------
			str
				Result
			"""
			self.method_a_called = True
			return "a"

		def method_b(self) -> str:
			"""Return Method B.

			Parameters
			----------
			None

			Returns
			-------
			str
				Result
			"""
			self.method_b_called = True
			return "b"

	instance = TestClass()

	# test method_a
	result_a = instance.method_a()
	assert result_a == "a"
	assert instance.cache_a_cleared is True
	assert instance.method_a_called is True
	assert instance.cache_b_cleared is False

	# test method_b
	result_b = instance.method_b()
	assert result_b == "b"
	assert instance.cache_b_cleared is True
	assert instance.cache_c_cleared is True
	assert instance.method_b_called is True


def test_auto_cache_reset_methods_nonexistent_method() -> None:
	"""Test auto_cache_reset_methods with nonexistent method.

	Verifies
	--------
	- Decorator doesn't fail when method doesn't exist on class
	- Class is returned unchanged
	- No errors are raised during decoration

	Returns
	-------
	None
	"""

	@auto_cache_reset_methods([("nonexistent_method", ["clear_cache"])])
	class TestClass:
		"""Test class for nonexistent method."""

		def __init__(self) -> None:
			"""Initialize class.

			Parameters
			----------
			None

			Returns
			-------
			None
			"""
			self.cache_cleared = False

		def clear_cache(self) -> None:
			"""Clear cache.

			Parameters
			----------
			None

			Returns
			-------
			None
			"""
			self.cache_cleared = True

		def actual_method(self) -> str:
			"""Actual method.

			Parameters
			----------
			None

			Returns
			-------
			str
				Result
			"""
			return "result"

	instance = TestClass()
	# should work normally since nonexistent_method wasn't decorated
	result = instance.actual_method()
	assert result == "result"
	assert instance.cache_cleared is False


def test_auto_cache_reset_methods_preserves_class_attributes() -> None:
	"""Test auto_cache_reset_methods preserves class attributes.

	Verifies
	--------
	- Class attributes are preserved after decoration
	- Class name and docstring are maintained
	- Additional class functionality remains intact

	Returns
	-------
	None
	"""

	@auto_cache_reset_methods([("test_method", ["clear_cache"])])
	class TestClass:
		"""Test class docstring."""

		class_attribute = "preserved"

		def __init__(self) -> None:
			"""Initialize class.

			Parameters
			----------
			None

			Returns
			-------
			None
			"""
			self.cache_cleared = False

		def clear_cache(self) -> None:
			"""Clear cache.

			Parameters
			----------
			None

			Returns
			-------
			None
			"""
			self.cache_cleared = True

		def test_method(self) -> str:
			"""Test method.

			Parameters
			----------
			None

			Returns
			-------
			str
				Result
			"""
			return "result"

		@classmethod
		def class_method(cls) -> str:
			"""Return class result.

			Parameters
			----------
			None

			Returns
			-------
			str
				Result
			"""
			return "class_result"

		@staticmethod
		def static_method() -> str:
			"""Return static method result.

			Parameters
			----------
			None

			Returns
			-------
			str
				Result
			"""
			return "static_result"

	# check class attributes
	assert TestClass.__name__ == "TestClass"
	assert TestClass.__doc__ == "Test class docstring."
	assert TestClass.class_attribute == "preserved"

	# check methods work
	assert TestClass.class_method() == "class_result"
	assert TestClass.static_method() == "static_result"

	# check decorated method works
	instance = TestClass()
	result = instance.test_method()
	assert result == "result"
	assert instance.cache_cleared is True


def test_auto_cache_reset_methods_invalid_pairs() -> None:
	"""Test auto_cache_reset_methods with invalid pairs.

	Verifies
	--------
	- ValueError is raised for invalid method_cache_pairs
	- Validation occurs before class decoration
	- Error message is appropriate

	Returns
	-------
	None
	"""
	with pytest.raises(ValueError, match="method_cache_pairs cannot be empty"):

		@auto_cache_reset_methods([])
		class TestClass:
			pass


def test_auto_cache_reset_methods_mixed_scenarios() -> None:
	"""Test auto_cache_reset_methods with mixed single and multiple cache scenarios.

	Verifies
	--------
	- Both single and multiple cache clearing work in same class
	- Correct decorators are applied based on cache method count
	- All functionality works as expected

	Returns
	-------
	None
	"""

	@auto_cache_reset_methods(
		[
			("single_cache_method", ["clear_cache"]),
			("multi_cache_method", ["clear_cache_a", "clear_cache_b"]),
			("another_single", ["clear_cache_c"]),
		]
	)
	class TestClass:
		"""Test class for multiple cache methods."""

		def __init__(self) -> None:
			"""Initialize class.

			Parameters
			----------
			None

			Returns
			-------
			None
			"""
			self.cache_cleared = False
			self.cache_a_cleared = False
			self.cache_b_cleared = False
			self.cache_c_cleared = False

		def clear_cache(self) -> None:
			"""Clear cache.

			Parameters
			----------
			None

			Returns
			-------
			None
			"""
			self.cache_cleared = True

		def clear_cache_a(self) -> None:
			"""Clear cache A.

			Parameters
			----------
			None

			Returns
			-------
			None
			"""
			self.cache_a_cleared = True

		def clear_cache_b(self) -> None:
			"""Clear cache B.

			Parameters
			----------
			None

			Returns
			-------
			None
			"""
			self.cache_b_cleared = True

		def clear_cache_c(self) -> None:
			"""Clear cache C.

			Parameters
			----------
			None

			Returns
			-------
			None
			"""
			self.cache_c_cleared = True

		def single_cache_method(self) -> str:
			"""Single cache method.

			Parameters
			----------
			None

			Returns
			-------
			str
				Result
			"""
			return "single"

		def multi_cache_method(self) -> str:
			"""Multi cache method.

			Parameters
			----------
			None

			Returns
			-------
			str
				Result
			"""
			return "multi"

		def another_single(self) -> str:
			"""Another single cache method.

			Parameters
			----------
			None

			Returns
			-------
			str
				Result
			"""
			return "another"

	instance = TestClass()

	# test single cache method
	result = instance.single_cache_method()
	assert result == "single"
	assert instance.cache_cleared is True

	# reset and test multi cache method
	instance = TestClass()
	result = instance.multi_cache_method()
	assert result == "multi"
	assert instance.cache_a_cleared is True
	assert instance.cache_b_cleared is True

	# reset and test another single
	instance = TestClass()
	result = instance.another_single()
	assert result == "another"
	assert instance.cache_c_cleared is True


# --------------------------
# Integration Tests
# --------------------------
def test_integration_decorator_with_inheritance() -> None:
	"""Test cache decorators with class inheritance.

	Verifies
	--------
	- Cache decorators work with inherited methods
	- Overridden methods maintain cache clearing behavior
	- Base class cache methods are accessible

	Returns
	-------
	None
	"""

	class BaseClass:
		"""Base class for test."""

		def __init__(self) -> None:
			"""Initialize base class.

			Parameters
			----------
			None

			Returns
			-------
			None
			"""
			self.base_cache_cleared = False

		def clear_base_cache(self) -> None:
			"""Clear base cache.

			Parameters
			----------
			None

			Returns
			-------
			None
			"""
			self.base_cache_cleared = True

		def base_method(self) -> str:
			"""Return base.

			Parameters
			----------
			None

			Returns
			-------
			str
				Result
			"""
			return "base"

	@auto_cache_reset_methods([("base_method", ["clear_base_cache"])])
	class DerivedClass(BaseClass):
		"""Derived class for test."""

		def __init__(self) -> None:
			"""Initialize derived class.

			Parameters
			----------
			None

			Returns
			-------
			None
			"""
			super().__init__()
			self.derived_cache_cleared = False

		def clear_derived_cache(self) -> None:
			"""Clear derived cache.

			Parameters
			----------
			None

			Returns
			-------
			None
			"""
			self.derived_cache_cleared = True

	instance = DerivedClass()
	result = instance.base_method()

	assert result == "base"
	assert instance.base_cache_cleared is True


def test_integration_decorator_with_property() -> None:
	"""Test cache decorators with property methods.

	Verifies
	--------
	- Cache decorators can work with property getters
	- Property functionality is preserved
	- Cache clearing occurs before property access

	Returns
	-------
	None
	"""

	class TestClass:
		"""Test class for property getter."""

		def __init__(self) -> None:
			"""Initialize class.

			Parameters
			----------
			None

			Returns
			-------
			None
			"""
			self._value = "initial"
			self.cache_cleared = False

		def clear_cache(self) -> None:
			"""Clear cache.

			Parameters
			----------
			None

			Returns
			-------
			None
			"""
			self.cache_cleared = True

		@property
		def value(self) -> str:
			"""Property getter.

			Parameters
			----------
			None

			Returns
			-------
			str
				Result
			"""
			return self._value

	# manually apply decorator to property
	decorator = CacheResetDecorator("clear_cache")
	TestClass.value = property(decorator(TestClass.value.fget))

	instance = TestClass()
	result = instance.value

	assert result == "initial"
	assert instance.cache_cleared is True


def test_integration_exception_handling_in_decorated_methods() -> None:
	"""Test exception handling in decorated methods.

	Verifies
	--------
	- Exceptions in decorated methods are properly propagated
	- Cache clearing still occurs before exception
	- Exception details are preserved

	Returns
	-------
	None

	Raises
	------
	RuntimeError
		Exception raised in decorated method
	"""

	class TestClass:
		"""Test class for exception handling in decorated methods."""

		def __init__(self) -> None:
			"""Initialize class.

			Parameters
			----------
			None

			Returns
			-------
			None
			"""
			self.cache_cleared = False

		def clear_cache(self) -> None:
			"""Clear cache.

			Parameters
			----------
			None

			Returns
			-------
			None
			"""
			self.cache_cleared = True

		def failing_method(self) -> None:
			"""Failing method.

			Parameters
			----------
			None

			Returns
			-------
			None

			Raises
			------
			RuntimeError
				Exception raised in decorated method
			"""
			raise RuntimeError("Method failed")

	decorator = CacheResetDecorator("clear_cache")
	decorated_method = decorator(TestClass.failing_method)
	instance = TestClass()

	with pytest.raises(RuntimeError, match="Method failed"):
		decorated_method(instance)

	# cache should still have been cleared
	assert instance.cache_cleared is True


def test_integration_multiple_decorators() -> None:
	"""Test applying multiple decorators to same method.

	Verifies
	--------
	- Multiple cache reset decorators can be applied
	- All cache clearing methods are called
	- Method execution occurs after all cache clearing

	Returns
	-------
	None
	"""

	class TestClass:
		"""Test class for multiple decorators."""

		def __init__(self) -> None:
			"""Initialize class.

			Parameters
			----------
			None

			Returns
			-------
			None
			"""
			self.cache_a_cleared = False
			self.cache_b_cleared = False
			self.method_called = False

		def clear_cache_a(self) -> None:
			"""Clear cache A.

			Parameters
			----------
			None

			Returns
			-------
			None
			"""
			self.cache_a_cleared = True

		def clear_cache_b(self) -> None:
			"""Clear cache B.

			Parameters
			----------
			None

			Returns
			-------
			None
			"""
			self.cache_b_cleared = True

		def test_method(self) -> str:
			"""Test method.

			Parameters
			----------
			None

			Returns
			-------
			str
				Test result
			"""
			self.method_called = True
			return "result"

	# apply multiple decorators
	decorator_a = CacheResetDecorator("clear_cache_a")
	decorator_b = CacheResetDecorator("clear_cache_b")
	decorated_method = decorator_a(decorator_b(TestClass.test_method))

	instance = TestClass()
	result = decorated_method(instance)

	assert result == "result"
	assert instance.cache_a_cleared is True
	assert instance.cache_b_cleared is True
	assert instance.method_called is True


# --------------------------
# Edge Case Tests
# --------------------------
def test_edge_case_empty_method_name_in_pairs() -> None:
	"""Test edge case with empty method name in pairs.

	Verifies
	--------
	- Validation catches empty method names in pairs
	- Appropriate error message is generated

	Returns
	-------
	None
	"""
	with pytest.raises(ValueError, match="Method name at index 0 cannot be empty"):
		auto_cache_reset_methods([("", ["clear_cache"])])


def test_edge_case_whitespace_method_names() -> None:
	"""Test edge case with whitespace-only method names.

	Verifies
	--------
	- Validation catches whitespace-only method names
	- Whitespace is properly stripped for validation

	Returns
	-------
	None
	"""
	with pytest.raises(ValueError, match="Method name at index 0 cannot be empty"):
		auto_cache_reset_methods([("   \t\n  ", ["clear_cache"])])


def test_edge_case_large_number_of_cache_methods() -> None:
	"""Test edge case with large number of cache methods.

	Verifies
	--------
	- Large lists of cache methods are handled correctly
	- Performance remains acceptable
	- All methods are called in order

	Returns
	-------
	None
	"""

	class TestClass:
		"""Test class for large number of cache methods."""

		def __init__(self) -> None:
			"""Initialize class.

			Parameters
			----------
			None

			Returns
			-------
			None
			"""
			self.clear_counts = [0] * 50

		def test_method(self) -> str:
			"""Test method.

			Parameters
			----------
			None

			Returns
			-------
			str
				Test result
			"""
			return "result"

	# create 50 cache clearing methods
	cache_methods = []
	for i in range(50):
		method_name = f"clear_cache_{i}"
		cache_methods.append(method_name)

		def make_clear_method(index: int) -> Callable[[], None]:
			"""Make cache clearing method.

			Parameters
			----------
			index : int
				Index of cache clearing method

			Returns
			-------
			Callable[[], None]
				Cache clearing method
			"""

			def clear_method(self: TestClass) -> None:
				"""Cache clearing method.

				Parameters
				----------
				self : TestClass
					Instance of TestClass

				Returns
				-------
				None
				"""
				self.clear_counts[index] += 1

			return clear_method

		setattr(TestClass, method_name, make_clear_method(i))

	decorated_method = clear_multiple_caches(TestClass.test_method, cache_methods)
	instance = TestClass()
	result = decorated_method(instance)

	assert result == "result"
	assert all(count == 1 for count in instance.clear_counts)


def test_edge_case_unicode_method_names() -> None:
	"""Test edge case with unicode method names.

	Verifies
	--------
	- Unicode method names are handled correctly
	- Validation works with non-ASCII characters
	- Decoration and execution work properly

	Returns
	-------
	None
	"""

	class TestClass:
		"""Test class for unicode method names."""

		def __init__(self) -> None:
			"""Initialize class.

			Parameters
			----------
			None

			Returns
			-------
			None
			"""
			self.cache_cleared = False

		def clear_cache_café(self) -> None:
			"""Cache clearing method.

			Parameters
			----------
			self : TestClass
				Instance of TestClass

			Returns
			-------
			None
			"""
			self.cache_cleared = True

		def test_method(self) -> str:
			"""Test method.

			Parameters
			----------
			None

			Returns
			-------
			str
				Test result
			"""
			return "result"

	# test validation accepts unicode
	_validate_cache_clear_methods(["clear_cache_café"])

	# test decoration works
	decorator = CacheResetDecorator("clear_cache_café")
	decorated_method = decorator(TestClass.test_method)
	instance = TestClass()
	result = decorated_method(instance)

	assert result == "result"
	assert instance.cache_cleared is True


def test_edge_case_method_returning_none() -> None:
	"""Test edge case with method returning None.

	Verifies
	--------
	- Methods returning None work correctly
	- Cache clearing still occurs
	- None return value is preserved

	Returns
	-------
	None
	"""

	class TestClass:
		"""Test class for method returning None."""

		def __init__(self) -> None:
			"""Initialize class.

			Parameters
			----------
			None

			Returns
			-------
			None
			"""
			self.cache_cleared = False
			self.method_called = False

		def clear_cache(self) -> None:
			"""Cache clearing method.

			Parameters
			----------
			self : TestClass
				Instance of TestClass

			Returns
			-------
			None
			"""
			self.cache_cleared = True

		def void_method(self) -> None:
			"""Void method.

			Parameters
			----------
			self : TestClass
				Instance of TestClass

			Returns
			-------
			None
			"""
			self.method_called = True

	decorator = CacheResetDecorator("clear_cache")
	decorated_method = decorator(TestClass.void_method)
	instance = TestClass()
	result = decorated_method(instance)

	assert result is None
	assert instance.cache_cleared is True
	assert instance.method_called is True
