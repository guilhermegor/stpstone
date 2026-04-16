"""Unit tests for conditional_pipeline function.

Tests the conditional function application pipeline with various input scenarios including:
- Normal operation with valid inputs
- Edge cases with empty or single-function pipelines
- Error conditions with invalid inputs
- Type validation through the @type_check decorator
"""

from typing import Any, Callable
from unittest.mock import Mock

import pytest

from stpstone.utils.pipelines.conditional import conditional_pipeline


# --------------------------
# Fixtures
# --------------------------
@pytest.fixture
def even_condition() -> Callable[[int], bool]:
	"""Fixture providing a condition that checks if a number is even.

	Returns
	-------
	Callable[[int], bool]
		A function that returns True if the input is even, False otherwise.
	"""
	return lambda x: x % 2 == 0


@pytest.fixture
def greater_than_10_condition() -> Callable[[int], bool]:
	"""Fixture providing a condition that checks if a number is greater than 10.

	Returns
	-------
	Callable[[int], bool]
		A function that returns True if the input is greater than 10, False otherwise.
	"""
	return lambda x: x > 10


@pytest.fixture
def double_func() -> Callable[[int], int]:
	"""Fixture providing a function that doubles its input.

	Returns
	-------
	Callable[[int], int]
		A function that doubles its input.
	"""
	return lambda x: x * 2


@pytest.fixture
def triple_func() -> Callable[[int], int]:
	"""Fixture providing a function that triples its input.

	Returns
	-------
	Callable[[int], int]
		A function that triples its input.
	"""
	return lambda x: x * 3


@pytest.fixture
def sample_functions(
	even_condition: Callable[[int], bool],
	greater_than_10_condition: Callable[[int], bool],
	double_func: Callable[[int], int],
	triple_func: Callable[[int], int],
) -> list[tuple[Callable[[Any], bool], Callable[[Any], Any]]]:
	"""Fixture providing a sample function pipeline.

	Parameters
	----------
	even_condition : Callable[[int], bool]
		A function that returns True if the input is even, False otherwise.
	greater_than_10_condition : Callable[[int], bool]
		A function that returns True if the input is greater than 10, False otherwise.
	double_func : Callable[[int], int]
		A function that doubles its input.
	triple_func : Callable[[int], int]
		A function that triples its input.

	Returns
	-------
	list[tuple[Callable[[Any], bool], Callable[[Any], Any]]]
		A list of tuples, where each tuple contains a condition and a function.
	"""
	return [
		(even_condition, double_func),
		(greater_than_10_condition, triple_func),
	]


# --------------------------
# Tests
# --------------------------
class TestNormalOperation:
	"""Tests for normal operation with valid inputs."""

	def test_applies_first_function_when_condition_met(
		self,
		sample_functions: list[tuple[Callable[[Any], bool], Callable[[Any], Any]]],
	) -> None:
		"""Test applies first function when its condition is met.

		Verifies
		--------
		- Only the first function is applied when its condition is met
		- The second function's condition is not met
		- The result is correctly transformed by the first function only

		Parameters
		----------
		sample_functions : list[tuple[Callable[[Any], bool], Callable[[Any], Any]]]
			A list of tuples, where each tuple contains a condition and a function.

		Returns
		-------
		None
		"""
		result = conditional_pipeline(6, sample_functions)
		assert result == 36

	def test_applies_second_function_when_condition_met(
		self,
		sample_functions: list[tuple[Callable[[Any], bool], Callable[[Any], Any]]],
	) -> None:
		"""Test applies second function when its condition is met.

		Verifies
		--------
		- Only the second function is applied when its condition is met
		- The first function's condition is not met
		- The result is correctly transformed by the second function only

		Parameters
		----------
		sample_functions : list[tuple[Callable[[Any], bool], Callable[[Any], Any]]]
			A list of tuples, where each tuple contains a condition and a function.

		Returns
		-------
		None
		"""
		result = conditional_pipeline(11, sample_functions)
		assert result == 33

	def test_applies_both_functions_when_conditions_met(
		self,
		sample_functions: list[tuple[Callable[[Any], bool], Callable[[Any], Any]]],
	) -> None:
		"""Test applies both functions when their conditions are met.

		Verifies
		--------
		- Both functions are applied when their conditions are met
		- Functions are applied in the correct order
		- The result is correctly transformed by both functions

		Parameters
		----------
		sample_functions : list[tuple[Callable[[Any], bool], Callable[[Any], Any]]]
			A list of tuples, where each tuple contains a condition and a function.

		Returns
		-------
		None
		"""
		result = conditional_pipeline(12, sample_functions)
		assert result == 72  # 12 * 2 * 3

	def test_applies_no_functions_when_conditions_not_met(
		self,
		sample_functions: list[tuple[Callable[[Any], bool], Callable[[Any], Any]]],
	) -> None:
		"""Test applies no functions when conditions are not met.

		Verifies
		--------
		- No functions are applied when their conditions are not met
		- The original data is returned unchanged

		Parameters
		----------
		sample_functions : list[tuple[Callable[[Any], bool], Callable[[Any], Any]]]
			A list of tuples, where each tuple contains a condition and a function.

		Returns
		-------
		None
		"""
		result = conditional_pipeline(5, sample_functions)
		assert result == 5


class TestEdgeCases:
	"""Tests for edge cases and special scenarios."""

	def test_empty_functions_list(self) -> None:
		"""Test behavior with empty functions list.

		Verifies
		--------
		- The function handles empty functions list gracefully
		- Returns the original data unchanged

		Returns
		-------
		None
		"""
		result = conditional_pipeline(5, [])
		assert result == 5

	def test_single_function_pipeline(
		self,
		even_condition: Callable[[int], bool],
		double_func: Callable[[int], int],
	) -> None:
		"""Test behavior with single-function pipeline.

		Verifies
		--------
		- The function works correctly with a single (condition, function) pair
		- Applies the function when condition is met
		- Returns original data when condition is not met

		Parameters
		----------
		even_condition : Callable[[int], bool]
			A function that returns True if the input is even, False otherwise.
		double_func : Callable[[int], int]
			A function that doubles its input.

		Returns
		-------
		None
		"""
		functions = [(even_condition, double_func)]
		assert conditional_pipeline(4, functions) == 8
		assert conditional_pipeline(5, functions) == 5

	def test_none_data_value(
		self,
		sample_functions: list[tuple[Callable[[Any], bool], Callable[[Any], Any]]],
	) -> None:
		"""Test behavior with None as input data.

		Verifies
		--------
		- The function handles None input gracefully
		- Conditions and functions must handle None appropriately

		Parameters
		----------
		sample_functions : list[tuple[Callable[[Any], bool], Callable[[Any], Any]]]
			A list of tuples, where each tuple contains a condition and a function.

		Returns
		-------
		None
		"""
		# Mock condition that handles None
		none_condition = Mock(return_value=True)
		none_func = Mock(return_value="processed")

		functions = [(none_condition, none_func)]
		result = conditional_pipeline(None, functions)

		none_condition.assert_called_once_with(None)
		none_func.assert_called_once_with(None)
		assert result == "processed"


class TestErrorConditions:
	"""Tests for error conditions and input validation."""

	def test_non_callable_condition(
		self,
		double_func: Callable[[int], int],
	) -> None:
		"""Test raises ValueError when condition is not callable.

		Verifies
		--------
		- Raises ValueError when condition is not callable
		- Error message is informative

		Parameters
		----------
		double_func : Callable[[int], int]
			A function that doubles its input.

		Returns
		-------
		None
		"""
		with pytest.raises(ValueError) as excinfo:
			conditional_pipeline(5, [("not callable", double_func)])
		assert "must be callable" in str(excinfo.value)

	def test_non_callable_function(
		self,
		even_condition: Callable[[int], bool],
	) -> None:
		"""Test raises ValueError when function is not callable.

		Verifies
		--------
		- Raises ValueError when function is not callable
		- Error message is informative

		Parameters
		----------
		even_condition : Callable[[int], bool]
			A function that returns True if the input is even, False otherwise.

		Returns
		-------
		None
		"""
		with pytest.raises(ValueError) as excinfo:
			conditional_pipeline(5, [(even_condition, "not callable")])
		assert "must be callable" in str(excinfo.value)

	def test_non_list_functions_input(self) -> None:
		"""Test type checking rejects non-list functions input.

		Verifies
		--------
		- Type checking decorator raises TypeError for non-list input
		- Error message indicates expected type

		Returns
		-------
		None
		"""
		with pytest.raises(TypeError):
			conditional_pipeline(5, "not a list")  # type: ignore

	def test_non_tuple_pipeline_elements(self) -> None:
		"""Test type checking rejects non-tuple pipeline elements.

		Verifies
		--------
		- Type checking decorator raises TypeError for non-tuple elements
		- Error message indicates expected type

		Returns
		-------
		None
		"""
		with pytest.raises(TypeError, match="must be of type"):
			conditional_pipeline(5, [["not a tuple", lambda x: x]])  # type: ignore


class TestTypeValidation:
	"""Tests for type validation through the decorator."""

	def test_condition_return_type_validation(
		self,
		double_func: Callable[[int], int],
	) -> None:
		"""Test condition must return bool.

		Verifies
		--------
		- Conditions must return bool values
		- Non-bool returns raise TypeError

		Parameters
		----------
		double_func : Callable[[int], int]
			A function that doubles its input.

		Returns
		-------
		None
		"""
		# Condition that returns non-bool
		bad_condition = lambda x: "not a bool"  # noqa: E731

		with pytest.raises(TypeError, match="Condition must return bool"):
			conditional_pipeline(5, [(bad_condition, double_func)])

	def test_function_input_output_validation(
		self,
		even_condition: Callable[[int], bool],
	) -> None:
		"""Test function input/output type consistency.

		Verifies
		--------
		- Functions must handle input type correctly
		- Return type must be consistent with next function's input

		Parameters
		----------
		even_condition : Callable[[int], bool]
			A function that returns True if the input is even, False otherwise.

		Returns
		-------
		None
		"""
		# Function that changes type from int to str
		int_to_str_func = lambda x: str(x)  # noqa: E731

		# Next function expects int but gets str
		with pytest.raises(TypeError):
			conditional_pipeline(
				4,
				[
					(even_condition, int_to_str_func),
					(even_condition, lambda x: x + 1),  # expects int
				],
			)


class TestMocking:
	"""Tests using mocks to verify behavior."""

	def test_condition_called_with_correct_argument(
		self,
	) -> None:
		"""Test conditions receive the current data value.

		Verifies
		--------
		- Each condition is called with the current data value
		- Conditions see transformed values from previous functions

		Returns
		-------
		None
		"""
		mock_condition1 = Mock(return_value=True)
		mock_func1 = Mock(return_value="transformed")
		mock_condition2 = Mock(return_value=False)

		conditional_pipeline(
			"original",
			[
				(mock_condition1, mock_func1),
				(mock_condition2, lambda x: x),
			],
		)

		mock_condition1.assert_called_once_with("original")
		mock_func1.assert_called_once_with("original")
		mock_condition2.assert_called_once_with("transformed")

	def test_short_circuit_when_condition_false(
		self,
	) -> None:
		"""Test subsequent functions not called when condition false.

		Verifies
		--------
		- When a condition returns False, its function is not called
		- Subsequent pairs still get evaluated

		Returns
		-------
		None
		"""
		mock_condition1 = Mock(return_value=False)
		mock_func1 = Mock()
		mock_condition2 = Mock(return_value=True)
		mock_func2 = Mock(return_value="result")

		result = conditional_pipeline(
			"input",
			[
				(mock_condition1, mock_func1),
				(mock_condition2, mock_func2),
			],
		)

		mock_condition1.assert_called_once_with("input")
		mock_func1.assert_not_called()
		mock_condition2.assert_called_once_with("input")
		mock_func2.assert_called_once_with("input")
		assert result == "result"
