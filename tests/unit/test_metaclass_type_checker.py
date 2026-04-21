"""Unit tests for type checking functionality.

This module tests the TypeChecker, AdvancedTypeChecker, and ConfigurableTypeChecker
metaclasses, as well as the type_checker decorator and validate_type function.
It covers:
- Type validation for various input types
- Method wrapping and type checking behavior
- Configuration options for type checking
- Edge cases, error conditions, and protocol handling
"""

from io import BytesIO
from pathlib import Path
import sys
from typing import Any, Callable, Literal, Optional, Union
from unittest.mock import Mock

import numpy as np
from numpy import ndarray
import pandas as pd
import pytest
from pytest_mock import MockerFixture


pytest.importorskip("psycopg")
from psycopg.sql import Composable

from stpstone.transformations.validation.metaclass_type_checker import (
	AdvancedTypeChecker,
	ConfigurableTypeChecker,
	DbConnection,
	DbCursor,
	SQLComposable,
	TypeChecker,
	type_checker,
	validate_type,
)


# --------------------------
# Fixtures
# --------------------------
@pytest.fixture
def mock_composable() -> Composable:
	"""Fixture providing a mock SQLComposable object.

	Returns
	-------
	Composable
		Mocked SQLComposable object
	"""
	return Mock(spec=Composable)


@pytest.fixture
def mock_db_cursor() -> DbCursor:
	"""Fixture providing a mock DbCursor object.

	Returns
	-------
	DbCursor
		Mocked database cursor object
	"""
	return Mock(spec=DbCursor)


@pytest.fixture
def mock_db_connection() -> DbConnection:
	"""Fixture providing a mock DbConnection object.

	Returns
	-------
	DbConnection
		Mocked database connection object
	"""
	return Mock(spec=DbConnection)


@pytest.fixture
def sample_class() -> type:
	"""Fixture providing a sample class with TypeChecker metaclass.

	Returns
	-------
	type
		Sample class with type-checked methods
	"""

	class Sample(metaclass=TypeChecker):
		def add_numbers(self, x: int, y: int) -> int:
			"""Add two integers.

			Parameters
			----------
			x : int
				First integer to add.
			y : int
				Second integer to add.

			Returns
			-------
			int
				Sum of x and y.
			"""
			return x + y

		def process_string(self, s: str) -> str:
			"""Convert string to uppercase.

			Parameters
			----------
			s : str
				String to convert.

			Returns
			-------
			str
				Uppercase version of s.
			"""
			return s.upper()

	return Sample


@pytest.fixture
def advanced_class() -> type:
	"""Fixture providing a sample class with AdvancedTypeChecker metaclass.

	Returns
	-------
	type
		Sample class with advanced type-checked methods
	"""

	class AdvancedSample(metaclass=AdvancedTypeChecker):
		_type_check_config = {"strict": True, "check_returns": True}

		def add_numbers(self, x: int, y: int) -> int:
			"""Add two integers.

			Parameters
			----------
			x : int
				First integer to add.
			y : int
				Second integer to add.

			Returns
			-------
			int
				Sum of x and y.
			"""
			return x + y

		def process_list(self, items: list[int]) -> list[int]:
			"""Double each item in a list.

			Parameters
			----------
			items : list[int]
				List of integers to double.

			Returns
			-------
			list[int]
				List of doubled integers.
			"""
			return [i * 2 for i in items]

	return AdvancedSample


@pytest.fixture
def configurable_class() -> type:
	"""Fixture providing a sample class with ConfigurableTypeChecker metaclass.

	Returns
	-------
	type
		Sample class with configurable type-checked methods
	"""

	class ConfigurableSample(metaclass=ConfigurableTypeChecker):
		__type_check_config__ = {
			"enabled": True,
			"strict": True,
			"exclude_methods": {"untyped_method"},
			"include_private": False,
		}

		def add_numbers(self, x: int, y: int) -> int:
			"""Add two integers.

			Parameters
			----------
			x : int
				First integer to add.
			y : int
				Second integer to add.

			Returns
			-------
			int
				Sum of x and y.
			"""
			return x + y

		def untyped_method(
			self,
			x: Any,  # noqa ANN401: typing.Any is not allowed
		) -> Any:  # noqa ANN401: typing.Any is not allowed
			"""Add two integers.

			Parameters
			----------
			x : Any
				Untyped input.

			Returns
			-------
			Any
				Untyped output.
			"""
			return x

		def _private_method(self, x: int) -> int:
			"""Add two integers.

			Parameters
			----------
			x : int
				Integer to multiply.

			Returns
			-------
			int
				Multiplied integer.
			"""
			return x * 2

	return ConfigurableSample


# --------------------------
# Tests for validate_type
# --------------------------
def test_validate_type_correct_type() -> None:
	"""Test validate_type with correct types.

	Verifies
	--------
	That correct types pass validation without raising errors.

	Returns
	-------
	None
	"""
	validate_type(42, int, "number")
	validate_type("hello", str, "text")
	validate_type([1, 2, 3], list, "numbers")
	validate_type(np.array([1, 2]), int, "numpy_int")


def test_validate_type_incorrect_type() -> None:
	"""Test validate_type with incorrect types.

	Verifies
	--------
	That incorrect types raise TypeError with appropriate message.

	Returns
	-------
	None
	"""
	with pytest.raises(TypeError, match="number must be of type int"):
		validate_type("42", int, "number")
	with pytest.raises(TypeError, match="text must be of type str"):
		validate_type(42, str, "text")


def test_validate_type_path_for_str() -> None:
	"""Test validate_type accepts Path for specific str parameters.

	Verifies
	--------
	That Path objects are accepted for file_path and path_cache parameters.

	Returns
	-------
	None
	"""
	validate_type(Path("/test"), str, "file_path")
	validate_type(Path("/cache"), str, "path_cache")
	with pytest.raises(TypeError, match="other must be of type str"):
		validate_type(Path("/test"), str, "other")


def test_validate_type_literal() -> None:
	"""Test validate_type with Literal types.

	Verifies
	--------
	That Literal types are properly validated.

	Returns
	-------
	None
	"""
	validate_type("read", Literal["read", "write"], "mode")
	with pytest.raises(TypeError, match="mode must be one of: 'read', 'write'"):
		validate_type("invalid", Literal["read", "write"], "mode")


def test_validate_type_union() -> None:
	"""Test validate_type with Union types.

	Verifies
	--------
	That Union types (including Optional) are properly validated.

	Returns
	-------
	None
	"""
	validate_type("text", Union[str, int], "param")
	validate_type(42, Union[str, int], "param")
	validate_type(None, Optional[str], "param")
	with pytest.raises(TypeError, match="param must be one of types: str, int"):
		validate_type(3.14, Union[str, int], "param")


def test_validate_type_list_callable() -> None:
	"""Test validate_type with list of Callable types.

	Verifies
	--------
	That lists of callables are properly validated.

	Returns
	-------
	None
	"""

	def dummy() -> None:
		"""Do nothing dummy function.

		Returns
		-------
		None
		"""
		pass

	validate_type([dummy, lambda x: x], list[Callable], "funcs")
	with pytest.raises(TypeError, match="must be of type"):
		validate_type([42], list[Callable], "funcs")


def test_validate_type_protocol(mock_composable: Composable) -> None:
	"""Test validate_type with protocol types.

	Parameters
	----------
	mock_composable : Composable
		Mocked SQLComposable object

	Verifies
	--------
	That protocol types are properly validated.

	Returns
	-------
	None
	"""
	validate_type(mock_composable, SQLComposable, "sql")
	validate_type(mock_composable, Composable, "sql")
	with pytest.raises(TypeError, match="sql must be of type SQLComposable"):
		validate_type("not_composable", SQLComposable, "sql")


def test_validate_type_mock() -> None:
	"""Test validate_type skips Mock objects.

	Verifies
	--------
	That Mock objects bypass type checking.

	Returns
	-------
	None
	"""
	validate_type(Mock(), int, "mocked")


# --------------------------
# Tests for type_checker decorator
# --------------------------
def test_type_checker_decorator_valid() -> None:
	"""Test type_checker decorator with valid inputs.

	Verifies
	--------
	That decorated function works with correct types.

	Returns
	-------
	None
	"""

	@type_checker
	def add(x: int, y: int) -> int:
		"""Add two integers.

		Parameters
		----------
		x : int
			First integer to add.
		y : int
			Second integer to add.

		Returns
		-------
		int
			Sum of x and y.
		"""
		return x + y

	assert add(1, 2) == 3


def test_type_checker_decorator_invalid() -> None:
	"""Test type_checker decorator with invalid inputs.

	Verifies
	--------
	That decorated function raises TypeError for incorrect types.

	Returns
	-------
	None
	"""

	@type_checker
	def add(x: int, y: int) -> int:
		"""Add two integers.

		Parameters
		----------
		x : int
			First integer to add.
		y : int
			Second integer to add.

		Returns
		-------
		int
			Sum of x and y.
		"""
		return x + y

	with pytest.raises(TypeError, match="x must be of type int"):
		add("1", 2)


# --------------------------
# Tests for TypeChecker metaclass
# --------------------------
def test_type_checker_valid_inputs(sample_class: type) -> None:
	"""Test TypeChecker metaclass with valid inputs.

	Parameters
	----------
	sample_class : type
		Sample class with TypeChecker metaclass

	Verifies
	--------
	That methods work with correct types.

	Returns
	-------
	None
	"""
	instance = sample_class()
	assert instance.add_numbers(1, 2) == 3
	assert instance.process_string("test") == "TEST"


def test_type_checker_invalid_inputs(sample_class: type) -> None:
	"""Test TypeChecker metaclass with invalid inputs.

	Parameters
	----------
	sample_class : type
		Sample class with TypeChecker metaclass

	Verifies
	--------
	That methods raise TypeError for incorrect types.

	Returns
	-------
	None
	"""
	instance = sample_class()
	with pytest.raises(TypeError, match="x must be of type int"):
		instance.add_numbers("1", 2)
	with pytest.raises(TypeError, match="s must be of type str"):
		instance.process_string(123)


def test_type_checker_init(sample_class: type) -> None:
	"""Test TypeChecker metaclass __init__ method.

	Parameters
	----------
	sample_class : type
		Sample class with TypeChecker metaclass

	Verifies
	--------
	That __init__ method is properly wrapped and type-checked.

	Returns
	-------
	None
	"""
	instance = sample_class()
	assert isinstance(instance, sample_class)


# --------------------------
# Tests for AdvancedTypeChecker metaclass
# --------------------------
def test_advanced_type_checker_valid(advanced_class: type) -> None:
	"""Test AdvancedTypeChecker metaclass with valid inputs.

	Parameters
	----------
	advanced_class : type
		Sample class with AdvancedTypeChecker metaclass

	Verifies
	--------
	That methods work with correct types and check returns.

	Returns
	-------
	None
	"""
	instance = advanced_class()
	assert instance.add_numbers(1, 2) == 3
	assert instance.process_list([1, 2, 3]) == [2, 4, 6]


def test_advanced_type_checker_invalid(advanced_class: type) -> None:
	"""Test AdvancedTypeChecker metaclass with invalid inputs.

	Parameters
	----------
	advanced_class : type
		Sample class with AdvancedTypeChecker metaclass

	Verifies
	--------
	That methods raise TypeError for incorrect types.

	Returns
	-------
	None
	"""
	instance = advanced_class()
	with pytest.raises(TypeError, match="In method add_numbers: x must be of type int"):
		instance.add_numbers("1", 2)
	with pytest.raises(TypeError, match="In method process_list: items must be of type list"):
		instance.process_list("not a list")


def test_advanced_type_checker_return_type(advanced_class: type, mocker: MockerFixture) -> None:
	"""Test AdvancedTypeChecker return type checking.

	Parameters
	----------
	advanced_class : type
		Sample class with AdvancedTypeChecker metaclass
	mocker : MockerFixture
		Pytest-mock fixture for creating mocks

	Verifies
	--------
	That return types are properly checked.

	Returns
	-------
	None
	"""
	mocker.patch.object(
		advanced_class, "_type_check_config", {"strict": True, "check_returns": True}
	)

	class BadReturn(metaclass=AdvancedTypeChecker):
		"""Class with bad return type."""

		_type_check_config = {"strict": True, "check_returns": True}

		def bad_method(self, x: int) -> int:
			"""Bad return type.

			Parameters
			----------
			x : int
				First integer to add.

			Returns
			-------
			int
				Sum of x and y.
			"""
			return "wrong_type"

	instance = BadReturn()
	with pytest.raises(
		TypeError, match="In method bad_method return: return value must be of type int"
	):
		instance.bad_method(1)


# --------------------------
# Tests for ConfigurableTypeChecker metaclass
# --------------------------
def test_configurable_type_checker_valid(configurable_class: type) -> None:
	"""Test ConfigurableTypeChecker with valid inputs.

	Parameters
	----------
	configurable_class : type
		Sample class with ConfigurableTypeChecker metaclass

	Verifies
	--------
	That methods work with correct types and respect configuration.

	Returns
	-------
	None
	"""
	instance = configurable_class()
	assert instance.add_numbers(1, 2) == 3
	assert instance.untyped_method("anything") == "anything"
	assert instance._private_method(2) == 4  # private method not checked


def test_configurable_type_checker_invalid(configurable_class: type) -> None:
	"""Test ConfigurableTypeChecker with invalid inputs.

	Parameters
	----------
	configurable_class : type
		Sample class with ConfigurableTypeChecker metaclass

	Verifies
	--------
	That methods raise TypeError for incorrect types when checked.

	Returns
	-------
	None
	"""
	instance = configurable_class()
	with pytest.raises(TypeError, match="In method add_numbers: x must be of type int"):
		instance.add_numbers("1", 2)


def test_configurable_type_checker_private_methods(configurable_class: type) -> None:
	"""Test ConfigurableTypeChecker skips private methods.

	Parameters
	----------
	configurable_class : type
		Sample class with ConfigurableTypeChecker metaclass

	Verifies
	--------
	That private methods are not type-checked by default.

	Returns
	-------
	None
	"""
	instance = configurable_class()
	assert instance._private_method("not an int") == "not an intnot an int"


# --------------------------
# Edge cases and error conditions
# --------------------------
def test_validate_type_empty_list() -> None:
	"""Test validate_type with empty list.

	Verifies
	--------
	That empty lists are properly handled.

	Returns
	-------
	None
	"""
	validate_type([], list[int], "empty_list")


def test_validate_type_none_in_non_optional() -> None:
	"""Test validate_type with None for non-Optional type.

	Verifies
	--------
	That None raises TypeError for non-Optional types.

	Returns
	-------
	None
	"""
	with pytest.raises(TypeError, match="param must be of type int"):
		validate_type(None, int, "param")


def test_type_checker_no_type_hints(mocker: MockerFixture) -> None:
	"""Test TypeChecker with method lacking type hints.

	Parameters
	----------
	mocker : MockerFixture
		Pytest-mock fixture for creating mocks

	Verifies
	--------
	That methods without type hints are executed without checking.

	Returns
	-------
	None
	"""
	mocker.patch(
		"stpstone.transformations.validation.metaclass_type_checker.get_type_hints",
		side_effect=NameError,
	)

	class NoHints(metaclass=TypeChecker):
		"""Sample class with TypeChecker metaclass."""

		def no_hints(self, x):  # noqa ANN202: missing return type annotation for private function & ANN001: missing type annotation for function argument
			"""No hints typed method.

			Parameters
			----------
			x : int
				Untyped input.

			Returns
			-------
			int
				Untyped output.
			"""
			return x

	instance = NoHints()
	assert instance.no_hints("anything") == "anything"


def test_advanced_type_checker_strict_error(mocker: MockerFixture) -> None:
	"""Test AdvancedTypeChecker strict mode type hint error.

	Parameters
	----------
	mocker : MockerFixture
		Pytest-mock fixture for creating mocks

	Verifies
	--------
	That strict mode raises RuntimeError for type hint errors.

	Returns
	-------
	None
	"""
	mocker.patch(
		"stpstone.transformations.validation.metaclass_type_checker.get_type_hints",
		side_effect=NameError,
	)

	class StrictSample(metaclass=AdvancedTypeChecker):
		"""Sample class with AdvancedTypeChecker metaclass."""

		_type_check_config = {"strict": True}

		def bad_method(self, x: int) -> int:
			"""Bad method.

			Parameters
			----------
			x : int
				Untyped input.

			Returns
			-------
			int
				Untyped output.
			"""
			return x

	instance = StrictSample()
	with pytest.raises(RuntimeError, match="Could not get type hints for bad_method"):
		instance.bad_method(1)


# --------------------------
# Reload logic tests
# --------------------------
def test_module_reload() -> None:
	"""Test module reloading behavior.

	Verifies
	--------
	That module reload preserves functionality.

	Returns
	-------
	None
	"""
	import importlib

	from stpstone.transformations.validation.metaclass_type_checker import (
		TypeChecker as OriginalTypeChecker,
	)

	# Store the original class
	original_type_checker = OriginalTypeChecker

	# Reload the module
	importlib.reload(sys.modules["stpstone.transformations.validation.metaclass_type_checker"])

	# Import the reloaded class
	from stpstone.transformations.validation.metaclass_type_checker import (
		TypeChecker as ReloadedTypeChecker,
	)

	# They should be different objects after reload
	assert ReloadedTypeChecker is not original_type_checker


# --------------------------
# Special type handling
# --------------------------
def test_validate_type_special_types(
	mock_db_cursor: DbCursor, mock_db_connection: DbConnection
) -> None:
	"""Test validate_type with special types.

	Parameters
	----------
	mock_db_cursor : DbCursor
		Mocked database cursor
	mock_db_connection : DbConnection
		Mocked database connection

	Verifies
	--------
	That special types like DbCursor and DbConnection are properly validated.

	Returns
	-------
	None
	"""
	validate_type(mock_db_cursor, DbCursor, "cursor")
	validate_type(mock_db_connection, DbConnection, "connection")
	validate_type(BytesIO(), BytesIO, "binary_io")


def test_validate_type_numpy_arrays() -> None:
	"""Test validate_type with numpy arrays.

	Verifies
	--------
	That numpy arrays are properly handled for int type checks.

	Returns
	-------
	None
	"""
	validate_type(np.array([1, 2, 3]), int, "array")
	validate_type(np.array([1.0, 2.0]), ndarray, "array")


def test_validate_type_pandas_types() -> None:
	"""Test validate_type with pandas types.

	Verifies
	--------
	That pandas DataFrame and Series are properly handled.

	Returns
	-------
	None
	"""
	df_ = pd.DataFrame({"a": [1, 2, 3]})
	series = pd.Series([1, 2, 3])
	validate_type(df_, pd.DataFrame, "df_")
	validate_type(series, pd.Series, "series")
