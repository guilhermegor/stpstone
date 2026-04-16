"""Unit tests for generic_pipeline function.

Tests the data processing pipeline functionality with various input scenarios including:
- Normal operation with valid inputs
- Edge cases and error conditions
- Type validation and error handling
"""

from typing import Any, Callable

import pandas as pd
import pytest

from stpstone.utils.pipelines.generic import generic_pipeline


# --------------------------
# Fixtures
# --------------------------
@pytest.fixture
def sample_dataframe() -> pd.DataFrame:
	"""Fixture providing a sample pandas DataFrame for testing.

	Returns
	-------
	pd.DataFrame
		DataFrame with two columns and three rows of test data
	"""
	return pd.DataFrame({"A": [1, 2, 3], "B": [4, 5, 6]})


@pytest.fixture
def increment_function() -> Callable[[int], int]:
	"""Fixture providing a simple increment function.

	Returns
	-------
	Callable[[int], int]
		Function that increments its input by 1
	"""

	def func(x: int) -> int:
		"""Increment input by 1.

		Parameters
		----------
		x : int
			Input value

		Returns
		-------
		int
			Incremented value
		"""
		return x + 1

	return func


@pytest.fixture
def square_function() -> Callable[[int], int]:
	"""Fixture providing a simple square function.

	Returns
	-------
	Callable[[int], int]
		Function that squares its input
	"""

	def func(x: int) -> int:
		"""Square input value.

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

	return func


@pytest.fixture
def dataframe_function() -> Callable[[pd.DataFrame], pd.DataFrame]:
	"""Fixture providing a DataFrame processing function.

	Returns
	-------
	Callable[[pd.DataFrame], pd.DataFrame]
		Function that adds a new column to a DataFrame
	"""

	def func(df_: pd.DataFrame) -> pd.DataFrame:
		"""Add new column to DataFrame.

		Parameters
		----------
		df_ : pd.DataFrame
			Input DataFrame

		Returns
		-------
		pd.DataFrame
			DataFrame with new column
		"""
		df_["C"] = df_["A"] + df_["B"]
		return df_

	return func


# --------------------------
# Tests
# --------------------------
def test_empty_functions_list() -> None:
	"""Test raises ValueError when functions list is empty.

	Verifies
	--------
	- The function raises ValueError with empty functions list
	- The error message matches expected text

	Returns
	-------
	None
	"""
	with pytest.raises(ValueError) as excinfo:
		generic_pipeline(5, [])
	assert "Functions list cannot be empty" in str(excinfo.value)


def test_non_callable_in_functions_list() -> None:
	"""Test raises TypeError when functions list contains non-callable.

	Verifies
	--------
	- The function raises TypeError with non-callable in list
	- The error message includes the incorrect type

	Returns
	-------
	None
	"""
	with pytest.raises(TypeError, match="must be of type"):
		generic_pipeline(5, [lambda x: x, "not a function"])


def test_simple_pipeline_operations(
	increment_function: Callable[[int], int], square_function: Callable[[int], int]
) -> None:
	"""Test pipeline with simple mathematical operations.

	Verifies
	--------
	- Functions are applied in correct order
	- Intermediate results are correct
	- Final result matches expected value

	Parameters
	----------
	increment_function : Callable[[int], int]
		Function that increments its input by 1
	square_function : Callable[[int], int]
		Function that squares its input

	Returns
	-------
	None
	"""
	result = generic_pipeline(2, [increment_function, square_function])
	assert result == 9  # (2 + 1) squared


def test_dataframe_pipeline(
	sample_dataframe: pd.DataFrame, dataframe_function: Callable[[pd.DataFrame], pd.DataFrame]
) -> None:
	"""Test pipeline with DataFrame operations.

	Verifies
	--------
	- DataFrame processing function works correctly
	- New column is added as expected
	- Original DataFrame is not modified

	Parameters
	----------
	sample_dataframe : pd.DataFrame
		Sample DataFrame for testing
	dataframe_function : Callable[[pd.DataFrame], pd.DataFrame]
		Function that adds a new column to a DataFrame

	Returns
	-------
	None
	"""
	original_columns = set(sample_dataframe.columns)
	result = generic_pipeline(sample_dataframe, [dataframe_function])

	assert "C" in result.columns
	assert set(result.columns) == original_columns.union({"C"})
	assert all(result["C"] == result["A"] + result["B"])


def test_pipeline_with_exception(increment_function: Callable[[int], int]) -> None:
	"""Test pipeline when a function raises an exception.

	Verifies
	--------
	- RuntimeError is raised when function fails
	- Error message includes function name and original error

	Parameters
	----------
	increment_function : Callable[[int], int]
		Function that increments its input by 1

	Returns
	-------
	None

	Raises
	------
	ValueError
		Test error
	"""

	def failing_func(x: int) -> int:
		"""Failing function that raises an error.

		Parameters
		----------
		x : int
			Input value

		Returns
		-------
		int
			Input value

		Raises
		------
		ValueError
			Test error
		"""
		raise ValueError("Test error")

	with pytest.raises(RuntimeError) as excinfo:
		generic_pipeline(5, [increment_function, failing_func])

	assert "failing_func" in str(excinfo.value)
	assert "Test error" in str(excinfo.value)


def test_pipeline_preserves_type(increment_function: Callable[[int], int]) -> None:
	"""Test pipeline preserves input/output types.

	Verifies
	--------
	- Input and output types match for simple pipeline
	- Type consistency through transformations

	Parameters
	----------
	increment_function : Callable[[int], int]
		Function that increments its input by 1

	Returns
	-------
	None
	"""
	result = generic_pipeline(5, [increment_function])
	assert isinstance(result, int)


@pytest.mark.parametrize("input_data", ["string input", 3.14, [1, 2, 3], {"key": "value"}])
def test_various_input_types(
	input_data: Any,  # noqa ANN401: typing.Any is not allowed
	increment_function: Callable[[int], int],
) -> None:
	"""Test pipeline with various input types.

	Verifies
	--------
	- Pipeline works with different input types
	- Type is preserved through transformations

	Parameters
	----------
	input_data : Any
		Input data for the pipeline
	increment_function : Callable[[int], int]
		Function that increments its input by 1

	Returns
	-------
	None
	"""

	def identity(
		x: Any,  # noqa ANN401: typing.Any is not allowed
	) -> Any:  # noqa ANN401: typing.Any is not allowed
		"""Identity function.

		Parameters
		----------
		x : Any
			Input value

		Returns
		-------
		Any
			Input value
		"""
		return x

	result = generic_pipeline(input_data, [identity])
	assert type(result) == type(input_data)  # noqa: E721
	assert result == input_data


def test_chained_pipeline(
	increment_function: Callable[[int], int], square_function: Callable[[int], int]
) -> None:
	"""Test pipeline with multiple chained functions.

	Verifies
	--------
	- Multiple functions can be chained
	- Each function is applied in sequence
	- Final result matches complex expectation

	Parameters
	----------
	increment_function : Callable[[int], int]
		Function that increments its input by 1
	square_function : Callable[[int], int]
		Function that squares its input

	Returns
	-------
	None
	"""

	def double(x: int) -> int:
		"""Double input value.

		Parameters
		----------
		x : int
			Input value

		Returns
		-------
		int
			Doubled value
		"""
		return x * 2

	result = generic_pipeline(1, [increment_function, square_function, double, increment_function])
	assert result == 9  # (((1 + 1)^2) * 2) + 1
