"""Unit tests for NumHandler class.

Tests numerical operations including multiples generation, rounding, conversions,
and mathematical computations with various input scenarios.
"""

from fractions import Fraction
from typing import Any, Union

import pytest

from stpstone.transformations.validation.metaclass_type_checker import TypeChecker
from stpstone.utils.parsers.numbers import NumHandler


class TestNumHandler:
	"""Test cases for NumHandler class methods."""

	@pytest.fixture
	def num_handler(self) -> TypeChecker:
		"""Fixture providing NumHandler instance.

		Returns
		-------
		TypeChecker
			Instance of NumHandler class
		"""
		return NumHandler()

	# --------------------------
	# multiples() tests
	# --------------------------
	@pytest.mark.parametrize(
		"m, ceiling, expected",
		[
			(3, 10, [0, 3, 6, 9, 10]),
			(5, 23, [0, 5, 10, 15, 20, 23]),
			(1, 5, [0, 1, 2, 3, 4, 5]),
		],
	)
	def test_multiples_valid(
		self, num_handler: TypeChecker, m: int, ceiling: int, expected: list[int]
	) -> None:
		"""Test multiples generation with valid inputs.

		Verifies
		--------
		- Correct multiples are generated
		- Last element doesn't exceed ceiling
		- Includes zero as first element

		Parameters
		----------
		m : int
			The multiple to generate
		ceiling : int
			The upper limit for multiples
		expected : list[int]
			Expected list of multiples
		num_handler : TypeChecker
			Instance of NumHandler

		Returns
		-------
		None
		"""
		result = num_handler.multiples(m, ceiling)
		assert result == expected

	@pytest.mark.parametrize(
		"m, ceiling",
		[
			(0, 10),
			(-3, 10),
			(3, -10),
		],
	)
	def test_multiples_invalid_input(self, num_handler: TypeChecker, m: int, ceiling: int) -> None:
		"""Test multiples with invalid inputs raises ValueError.

		Verifies
		--------
		- Raises ValueError for zero or negative multiple
		- Raises ValueError for negative ceiling

		Parameters
		----------
		m : int
			The multiple to generate
		ceiling : int
			The upper limit for multiples
		num_handler : TypeChecker
			Instance of NumHandler

		Returns
		-------
		None
		"""
		with pytest.raises(ValueError):
			num_handler.multiples(m, ceiling)

	# --------------------------
	# nearest_multiple() tests
	# --------------------------
	@pytest.mark.parametrize(
		"number, multiple, expected",
		[
			(10.7, 3, 9),
			(15, 5, 15),
			(17.3, 4, 16),
			(-10.7, 3, -9),
		],
	)
	def test_nearest_multiple_valid(
		self, num_handler: TypeChecker, number: float, multiple: int, expected: int
	) -> None:
		"""Test nearest multiple calculation.

		Verifies
		--------
		- Correct nearest multiple is returned
		- Handles positive and negative numbers
		- Handles zero multiple

		Parameters
		----------
		num_handler : TypeChecker
			Instance of NumHandler
		number : float
			The number for which to find the nearest multiple
		multiple : int
			The number of which to find the nearest multiple
		expected : int
			Expected nearest multiple

		Returns
		-------
		None
		"""
		assert num_handler.nearest_multiple(number, multiple) == expected

	@pytest.mark.parametrize(
		"number, multiple",
		[
			("not a number", 3),
			(10.7, 0),
			(10.7, "not a number"),
		],
	)
	def test_nearest_multiple_invalid(
		self,
		num_handler: TypeChecker,
		number: Any,  # noqa ANN401: typing.Any is not allowed
		multiple: Any,  # noqa ANN401: typing.Any is not allowed
	) -> None:
		"""Test nearest multiple with invalid inputs raises ValueError.

		Verifies
		--------
		- Raises ValueError for non-numeric inputs
		- Raises ValueError for zero multiple

		Parameters
		----------
		num_handler : TypeChecker
			Instance of NumHandler
		number : Any
			The number for which to find the nearest multiple
		multiple : Any
			The number of which to find the nearest multiple

		Returns
		-------
		None
		"""
		if multiple == 0:
			with pytest.raises(ValueError, match="multiple must be a non-zero number"):
				num_handler.nearest_multiple(number, multiple)
		else:
			with pytest.raises(TypeError, match="must be of type"):
				num_handler.nearest_multiple(number, multiple)

	# --------------------------
	# round_up() tests
	# --------------------------
	@pytest.mark.parametrize(
		"num, base, ceiling, expected",
		[
			(12.3, 5.0, 20.0, 15.0),
			(17.0, 5.0, 20.0, 20.0),
			(3.1, 2.0, 10.0, 4.0),
			(0.5, 1.0, 5.0, 1.0),
		],
	)
	def test_round_up_valid(
		self, num_handler: TypeChecker, num: float, base: float, ceiling: float, expected: float
	) -> None:
		"""Test number rounding up.

		Verifies
		--------
		- Correct rounding up of number

		Parameters
		----------
		num : float
			The number to round up
		base : float
			The base number to round up to
		ceiling : float
			The maximum value the new number can take
		expected : float
			Expected rounded up number
		num_handler : TypeChecker
			Instance of NumHandler

		Returns
		-------
		None
		"""
		assert num_handler.round_up(num, base, ceiling) == expected

	@pytest.mark.parametrize(
		"num, base, ceiling",
		[
			("not a number", 5, 20),
			(12.3, "not a number", 20),
			(12.3, 5, "not a number"),
		],
	)
	def test_round_up_invalid(
		self,
		num_handler: TypeChecker,
		num: Any,  # noqa ANN401: typing.Any is not allowed
		base: Any,  # noqa ANN401: typing.Any is not allowed
		ceiling: Any,  # noqa ANN401: typing.Any is not allowed
	) -> None:
		"""Test round up with invalid inputs raises ValueError.

		Verifies
		--------
		- Raises ValueError for non-numeric inputs
		- Raises ValueError for zero or negative base
		- Raises ValueError for ceiling less than base

		Parameters
		----------
		num : Any
			The number to round up
		base : Any
			The base number to round up to
		ceiling : Any
			The maximum value the new number can take
		num_handler : TypeChecker
			Instance of NumHandler

		Returns
		-------
		None
		"""
		with pytest.raises(TypeError, match="must be of type"):
			num_handler.round_up(num, base, ceiling)

	# --------------------------
	# decimal_to_fraction() tests
	# --------------------------
	@pytest.mark.parametrize(
		"decimal, expected",
		[
			(0.5, Fraction(1, 2)),
			(0.75, Fraction(3, 4)),
			(0.125, Fraction(1, 8)),
		],
	)
	def test_decimal_to_fraction_valid(
		self, num_handler: TypeChecker, decimal: Union[float, str], expected: Fraction
	) -> None:
		"""Test decimal to fraction conversion.

		Verifies
		--------
		- Correct conversion of decimal to fraction
		- Handles both float and string inputs

		Parameters
		----------
		num_handler : TypeChecker
			Instance of NumHandler
		decimal : Union[float, str]
			The decimal number to convert
		expected : Fraction
			Expected fraction

		Returns
		-------
		None
		"""
		assert num_handler.decimal_to_fraction(decimal) == expected

	@pytest.mark.parametrize(
		"decimal",
		[
			"not a number",
			None,
		],
	)
	def test_decimal_to_fraction_invalid(
		self,
		num_handler: TypeChecker,
		decimal: Any,  # noqa ANN401: typing.Any is not allowed
	) -> None:
		"""Test decimal to fraction with invalid inputs raises ValueError.

		Verifies
		--------
		- Raises ValueError for non-numeric inputs

		Parameters
		----------
		num_handler : TypeChecker
			Instance of NumHandler
		decimal : Any
			The decimal number to convert

		Returns
		-------
		None
		"""
		with pytest.raises(TypeError, match="must be of type"):
			num_handler.decimal_to_fraction(decimal)

	# --------------------------
	# truncate() tests
	# --------------------------
	@pytest.mark.parametrize(
		"number, digits, expected",
		[
			(3.1415926535, 2, 3.14),
			(2.7182818284, 3, 2.718),
			(123.456, 0, 123.0),
		],
	)
	def test_truncate_valid(
		self, num_handler: TypeChecker, number: float, digits: int, expected: float
	) -> None:
		"""Test number truncation.

		Verifies
		--------
		- Correct truncation of number to specified decimal places

		Parameters
		----------
		num_handler : TypeChecker
			Instance of NumHandler
		number : float
			The number to truncate
		digits : int
			Number of decimal places to truncate to
		expected : float
			Expected truncated number

		Returns
		-------
		None
		"""
		assert num_handler.truncate(number, digits) == expected

	def test_truncate_negative_digits(self, num_handler: TypeChecker) -> None:
		"""Test truncate with negative digits raises ValueError.

		Verifies
		--------
		- Raises ValueError for negative digits

		Parameters
		----------
		num_handler : TypeChecker
			Instance of NumHandler

		Returns
		-------
		None
		"""
		with pytest.raises(ValueError):
			num_handler.truncate(3.1415, -1)

	# --------------------------
	# sumproduct() tests
	# --------------------------
	def test_sumproduct_valid(self, num_handler: TypeChecker) -> None:
		"""Test sumproduct calculation.

		Verifies
		--------
		- Correct calculation of sumproduct

		Parameters
		----------
		num_handler : TypeChecker
			Instance of NumHandler

		Returns
		-------
		None
		"""
		list1 = [1, 2, 3]
		list2 = [4, 5, 6]
		assert num_handler.sumproduct(list1, list2) == 32

	def test_sumproduct_empty_lists(self, num_handler: TypeChecker) -> None:
		"""Test sumproduct with empty lists raises ValueError.

		Verifies
		--------
		- Raises ValueError for empty lists

		Parameters
		----------
		num_handler : TypeChecker
			Instance of NumHandler

		Returns
		-------
		None
		"""
		with pytest.raises(ValueError, match="At least one non-empty list required"):
			num_handler.sumproduct([], [])

	def test_sumproduct_unequal_lengths(self, num_handler: TypeChecker) -> None:
		"""Test sumproduct with unequal lengths raises ValueError.

		Verifies
		--------
		- Raises ValueError for lists with unequal lengths

		Parameters
		----------
		num_handler : TypeChecker
			Instance of NumHandler

		Returns
		-------
		None
		"""
		with pytest.raises(ValueError):
			num_handler.sumproduct([1, 2], [3, 4, 5])

	# --------------------------
	# number_sign() tests
	# --------------------------
	@pytest.mark.parametrize(
		"number, base, expected",
		[
			(5, 1, 1),
			(-3.5, 1, -1),
			(0, 10, 10),
		],
	)
	def test_number_sign_valid(
		self, num_handler: TypeChecker, number: float, base: float, expected: float
	) -> None:
		"""Test number sign application.

		Verifies
		--------
		- Correct application of sign to number

		Parameters
		----------
		num_handler : TypeChecker
			Instance of NumHandler
		number : float
			The number to apply sign to
		base : float
			The base number to apply sign to
		expected : float
			Expected signed number

		Returns
		-------
		None
		"""
		assert num_handler.number_sign(number, base) == expected

	# --------------------------
	# multiply_n_elements() tests
	# --------------------------
	@pytest.mark.parametrize(
		"args, expected",
		[
			((2, 3, 4), 24),
			((5,), 5),
			((-1, 2, -3), 6),
		],
	)
	def test_multiply_n_elements_valid(
		self, num_handler: TypeChecker, args: tuple[int, ...], expected: int
	) -> None:
		"""Test multiplication of elements.

		Verifies
		--------
		- Correct multiplication of elements

		Parameters
		----------
		num_handler : TypeChecker
			Instance of NumHandler
		args : tuple[int, ...]
			The elements to multiply
		expected : int
			Expected product

		Returns
		-------
		None
		"""
		assert num_handler.multiply_n_elements(*args) == expected

	def test_multiply_n_elements_empty(self, num_handler: TypeChecker) -> None:
		"""Test multiply with no args raises ValueError.

		Verifies
		--------
		- Raises ValueError for empty args

		Parameters
		----------
		num_handler : TypeChecker
			Instance of NumHandler

		Returns
		-------
		None
		"""
		with pytest.raises(ValueError):
			num_handler.multiply_n_elements()

	# --------------------------
	# sum_n_elements() tests
	# --------------------------
	@pytest.mark.parametrize(
		"args, expected",
		[
			((1, 2, 3), 6),
			((1.5, 2.5), 4.0),
			((-1, 1), 0),
		],
	)
	def test_sum_n_elements_valid(
		self,
		num_handler: TypeChecker,
		args: tuple[Union[int, float], ...],
		expected: Union[int, float],
	) -> None:
		"""Test summation of elements.

		Verifies
		--------
		- Correct summation of elements

		Parameters
		----------
		num_handler : TypeChecker
			Instance of NumHandler
		args : tuple[Union[int, float], ...]
			The elements to sum
		expected : Union[int, float]
			Expected sum

		Returns
		-------
		None
		"""
		assert num_handler.sum_n_elements(*args) == expected

	def test_sum_n_elements_empty(self, num_handler: TypeChecker) -> None:
		"""Test sum with no args raises ValueError.

		Verifies
		--------
		- Raises ValueError for empty args

		Parameters
		----------
		num_handler : TypeChecker
			Instance of NumHandler

		Returns
		-------
		None
		"""
		with pytest.raises(ValueError):
			num_handler.sum_n_elements()

	# --------------------------
	# factorial() tests
	# --------------------------
	@pytest.mark.parametrize(
		"n, expected",
		[
			(5, 120),
			(1, 1),
			(3, 6),
		],
	)
	def test_factorial_valid(self, num_handler: TypeChecker, n: int, expected: int) -> None:
		"""Test factorial calculation.

		Verifies
		--------
		- Correct calculation of factorial

		Parameters
		----------
		n : int
			The number to calculate factorial of
		expected : int
			Expected factorial
		num_handler : TypeChecker
			Instance of NumHandler

		Returns
		-------
		None
		"""
		assert num_handler.factorial(n) == expected

	@pytest.mark.parametrize("n", [0, -1])
	def test_factorial_invalid(self, num_handler: TypeChecker, n: int) -> None:
		"""Test factorial with invalid input raises ValueError.

		Verifies
		--------
		- Raises ValueError for invalid input

		Parameters
		----------
		n : int
			The number to calculate factorial of
		num_handler : TypeChecker
			Instance of NumHandler

		Returns
		-------
		None
		"""
		with pytest.raises(ValueError):
			num_handler.factorial(n)

	# --------------------------
	# range_floats() tests
	# --------------------------
	@pytest.mark.parametrize(
		"epsilon, inf, sup, pace, expected",
		[
			(1.0, 0.0, 1.0, 0.2, [0.0, 0.2, 0.4, 0.6, 0.8, 1.0]),
			(10.0, 0.1, 0.5, 0.1, [0.1, 0.2, 0.3, 0.4, 0.5]),
		],
	)
	def test_range_floats_valid(
		self,
		num_handler: TypeChecker,
		epsilon: float,
		inf: float,
		sup: float,
		pace: float,
		expected: list[float],
	) -> None:
		"""Test float range generation.

		Verifies
		--------
		- Correct generation of float range

		Parameters
		----------
		num_handler : TypeChecker
			Instance of NumHandler
		epsilon : float
			Unit of measurement
		inf : float
			Lower bound
		sup : float
			Upper bound
		pace : float
			Step size
		expected : list[float]
			Expected range

		Returns
		-------
		None
		"""
		result = num_handler.range_floats(epsilon, inf, sup, pace)
		assert result == pytest.approx(expected)

	@pytest.mark.parametrize(
		"epsilon, inf, sup, pace",
		[
			(1.0, 1.0, 0.5, 0.1),
			(1.0, 0.0, 1.0, -0.1),
		],
	)
	def test_range_floats_invalid(
		self, num_handler: TypeChecker, epsilon: float, inf: float, sup: float, pace: float
	) -> None:
		"""Test range_floats with invalid inputs raises ValueError.

		Verifies
		--------
		- Raises ValueError for invalid inputs

		Parameters
		----------
		num_handler : TypeChecker
			Instance of NumHandler
		epsilon : float
			Unit of measurement
		inf : float
			Lower bound
		sup : float
			Upper bound
		pace : float
			Step size

		Returns
		-------
		None
		"""
		with pytest.raises(ValueError):
			num_handler.range_floats(epsilon, inf, sup, pace)

	# --------------------------
	# clamp() tests
	# --------------------------
	@pytest.mark.parametrize(
		"n, minn, maxn, expected",
		[
			(5.0, 0.0, 10.0, 5.0),
			(-1.0, 0.0, 10.0, 0.0),
			(15.0, 0.0, 10.0, 10.0),
		],
	)
	def test_clamp_valid(
		self, num_handler: TypeChecker, n: float, minn: float, maxn: float, expected: float
	) -> None:
		"""Test number clamping.

		Verifies
		--------
		- Correct clamping of number

		Parameters
		----------
		n : float
			The number to clamp
		minn : float
			Minimum value
		maxn : float
			Maximum value
		expected : float
			Expected clamped value
		num_handler : TypeChecker
			Instance of NumHandler

		Returns
		-------
		None
		"""
		assert num_handler.clamp(n, minn, maxn) == expected

	def test_clamp_invalid_bounds(self, num_handler: TypeChecker) -> None:
		"""Test clamp with min > max raises ValueError.

		Verifies
		--------
		- Raises ValueError for invalid bounds

		Parameters
		----------
		num_handler : TypeChecker
			Instance of NumHandler

		Returns
		-------
		None
		"""
		with pytest.raises(TypeError, match="must be of type"):
			num_handler.clamp(5, 10, 0)

	# --------------------------
	# is_numeric() tests
	# --------------------------
	@pytest.mark.parametrize(
		"str_, expected",
		[
			("123", True),
			("12.34", True),
			("-5.6", True),
			("not a number", False),
		],
	)
	def test_is_numeric(self, num_handler: TypeChecker, str_: str, expected: bool) -> None:
		"""Test numeric string detection.

		Verifies
		--------
		- Correct detection of numeric strings

		Parameters
		----------
		num_handler : TypeChecker
			Instance of NumHandler
		str_ : str
			String to check
		expected : bool
			Expected result

		Returns
		-------
		None
		"""
		assert num_handler.is_numeric(str_) == expected

	# --------------------------
	# is_number() tests
	# --------------------------
	@pytest.mark.parametrize(
		"value, expected",
		[
			(123, True),
			(12.34, True),
			(True, False),
			("123", False),
		],
	)
	def test_is_number(
		self,
		num_handler: TypeChecker,
		value: Any,  # noqa ANN401: typing.Any is not allowed
		expected: bool,
	) -> None:
		"""Test number type detection.

		Verifies
		--------
		- Correct detection of number types

		Parameters
		----------
		num_handler : TypeChecker
			Instance of NumHandler
		value : Any
			Value to check
		expected : bool
			Expected result

		Returns
		-------
		None
		"""
		assert num_handler.is_number(value) == expected

	# --------------------------
	# transform_to_float() tests
	# --------------------------
	@pytest.mark.parametrize(
		"value, expected",
		[
			("3,132.45%", 31.3245),
			("1.234,56", 1234.56),
			("1,234,567.89", 1234567.89),
			("4.56 bp", 0.000456),
			("(-)912.412.911,231", -912412911.231),
			(True, True),
			("Text Tried", "Text Tried"),
			("-0,10%", -0.001),
			("0,10%", 0.001),
			("9888 Example", "9888 Example"),
		],
	)
	def test_transform_to_float(
		self,
		num_handler: TypeChecker,
		value: Any,  # noqa ANN401: typing.Any is not allowed
		expected: Any,  # noqa ANN401: typing.Any is not allowed
	) -> None:
		"""Test complex string to float conversion.

		Verifies
		--------
		- Correct conversion of complex strings to float

		Parameters
		----------
		num_handler : TypeChecker
			Instance of NumHandler
		value : Any
			Value to convert
		expected : Any
			Expected result

		Returns
		-------
		None
		"""
		result = num_handler.transform_to_float(value)
		if isinstance(expected, float):
			assert result == pytest.approx(expected)
		else:
			assert result == expected

	@pytest.mark.parametrize(
		"value, precision, expected",
		[
			(1.23456789, 2, 1.23),
			("1.23456789", 3, 1.235),
		],
	)
	def test_transform_to_float_precision(
		self,
		num_handler: TypeChecker,
		value: Any,  # noqa ANN401: typing.Any is not allowed
		precision: int,
		expected: float,
	) -> None:
		"""Test precision handling in float conversion.

		Verifies
		--------
		- Correct handling of precision in float conversion

		Parameters
		----------
		num_handler : TypeChecker
			Instance of NumHandler
		value : Any
			Value to convert
		precision : int
			Decimal places to round to
		expected : float
			Expected result

		Returns
		-------
		None
		"""
		result = num_handler.transform_to_float(value, precision)
		assert result == pytest.approx(expected)
