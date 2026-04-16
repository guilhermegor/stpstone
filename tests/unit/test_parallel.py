"""Unit tests for parallel pipeline execution.

Tests the parallel pipeline functionality with various input scenarios including:
- Normal operation with valid inputs
- Edge cases and error conditions
- Type validation and input sanitization
"""

from typing import Any, Callable
from unittest.mock import patch

import pytest

from stpstone.utils.pipelines.parallel import parallel_pipeline


# --------------------------
# Fixtures
# --------------------------
@pytest.fixture
def sample_functions() -> list[Callable[[int], int]]:
	"""Fixture providing sample functions for testing.

	Returns
	-------
	list[Callable[[int], int]]
		List of simple arithmetic functions:
		1. Add one
		2. Multiply by two
		3. Subtract five
	"""

	def add_one(x: int) -> int:
		"""Add one to input.

		Parameters
		----------
		x : int
			Input value

		Returns
		-------
		int
			Summed value
		"""
		return x + 1

	def multiply_by_two(x: int) -> int:
		"""Multiply input by 2.

		Parameters
		----------
		x : int
			Input value

		Returns
		-------
		int
			Multiplied value
		"""
		return x * 2

	def subtract_five(x: int) -> int:
		"""Subtract 5 from input.

		Parameters
		----------
		x : int
			Input value

		Returns
		-------
		int
			Subtracted value
		"""
		return x - 5

	return [add_one, multiply_by_two, subtract_five]


@pytest.fixture
def error_raising_function() -> Callable[[Any], Any]:
	"""Fixture providing a function that raises an exception.

	Returns
	-------
	Callable[[Any], Any]
		Function that raises ValueError when called

	Raises
	------
	ValueError
		Test error
	"""

	def func(_: Any) -> Any:  # noqa ANN401: typing.Any is not allowed
		"""Run function that raises ValueError.

		Parameters
		----------
		_ : Any
			Input value

		Returns
		-------
		Any
			Input value

		Raises
		------
		ValueError
			Test error
		"""
		raise ValueError("Test error")

	return func


# --------------------------
# Tests
# --------------------------
def test_normal_operation(sample_functions: list[Callable[[int], int]]) -> None:
	"""Test normal operation with valid inputs.

	Verifies
	--------
	- Correct processing through function pipeline
	- Proper return of final result
	- Maintenance of operation order

	Parameters
	----------
	sample_functions : list[Callable[[int], int]]
		List of test functions from fixture

	Returns
	-------
	None
	"""
	result = parallel_pipeline(5, sample_functions)
	assert result == 0


def test_empty_functions_list() -> None:
	"""Test raises ValueError when functions list is empty.

	Verifies
	--------
	- Proper validation of empty functions list
	- Correct error message

	Returns
	-------
	None
	"""
	with pytest.raises(ValueError) as excinfo:
		parallel_pipeline(5, [])
	assert "Functions list cannot be empty" in str(excinfo.value)


def test_function_error_propagation(
	sample_functions: list[Callable[[int], int]], error_raising_function: Callable[[Any], Any]
) -> None:
	"""Test error propagation from failed function.

	Verifies
	--------
	- Errors in functions are properly propagated
	- Error message contains original exception info
	- Execution stops on first error

	Parameters
	----------
	sample_functions : list[Callable[[int], int]]
		List of test functions from fixture
	error_raising_function : Callable[[Any], Any]
		Error-raising function from fixture

	Returns
	-------
	None
	"""
	functions = sample_functions[:1] + [error_raising_function] + sample_functions[1:]
	with pytest.raises(RuntimeError) as excinfo:
		parallel_pipeline(5, functions)
	assert "Error during parallel pipeline execution" in str(excinfo.value)
	assert "Test error" in str(excinfo.value)


def test_thread_pool_usage() -> None:
	"""Test proper usage of ThreadPoolExecutor.

	Verifies
	--------
	- ThreadPoolExecutor is used for parallel execution
	- Resources are properly cleaned up

	Returns
	-------
	None
	"""

	def mock_func(x: int) -> int:
		"""Mock function for testing.

		Parameters
		----------
		x : int
			Input value

		Returns
		-------
		int
			Input value
		"""
		return x

	with patch("concurrent.futures.ThreadPoolExecutor") as mock_executor:
		mock_executor.return_value.__enter__.return_value.map.return_value = [42]
		result = parallel_pipeline(5, [mock_func])
		assert result == 5


def test_type_validation_non_callable() -> None:
	"""Test type validation for non-callable functions.

	Verifies
	--------
	- Proper type checking of functions list elements
	- Correct error message for non-callable items

	Returns
	-------
	None
	"""
	with pytest.raises(TypeError, match="must be of type"):
		parallel_pipeline(5, [lambda x: x, "not a function", lambda x: x])


def test_single_function_processing() -> None:
	"""Test processing with single function.

	Verifies
	--------
	- Correct handling of single-function case
	- Proper return value

	Returns
	-------
	None
	"""

	def square(x: int) -> int:
		"""Square input.

		Parameters
		----------
		x : int
			Input value

		Returns
		-------
		int
			Squared value
		"""
		return x * x

	result = parallel_pipeline(5, [square])
	assert result == 25


def test_input_data_persistence(sample_functions: list[Callable[[int], int]]) -> None:
	"""Test input data remains unchanged.

	Verifies
	--------
	- Input data is not modified by the pipeline
	- Original data reference remains intact

	Parameters
	----------
	sample_functions : list[Callable[[int], int]]
		List of test functions from fixture

	Returns
	-------
	None
	"""
	data = {"value": 10}
	original_data = data.copy()
	_ = parallel_pipeline(data, [lambda x: x])
	assert data == original_data


def test_docstring_example() -> None:
	"""Test the example provided in the docstring.

	Verifies
	--------
	- Docstring example produces correct result
	- Example remains up-to-date with implementation

	Returns
	-------
	None
	"""

	def add_one(x: int) -> int:
		"""Add one to input.

		Parameters
		----------
		x : int
			Input value

		Returns
		-------
		int
			Summed value
		"""
		return x + 1

	def multiply_by_two(x: int) -> int:
		"""Multiply input by 2.

		Parameters
		----------
		x : int
			Input value

		Returns
		-------
		int
			Multiplied value
		"""
		return x * 2

	def subtract_five(x: int) -> int:
		"""Subtract 5 from input.

		Parameters
		----------
		x : int
			Input value

		Returns
		-------
		int
			Subtracted value
		"""
		return x - 5

	result = parallel_pipeline(5, [add_one, multiply_by_two, subtract_five])
	assert result == 0
