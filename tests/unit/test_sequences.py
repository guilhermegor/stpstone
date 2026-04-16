"""Unit tests for sequences module.

Tests the Fibonacci and TaylorSeries classes, covering:
- Initialization with valid and invalid inputs
- Fibonacci sequence generation
- Taylor series coefficient computation and approximation
- Edge cases, error conditions, and type validation
"""

import importlib
import math
import sys
from typing import Callable

import pytest
from pytest_mock import MockerFixture

from stpstone.analytics.quant.sequences import Fibonacci, TaylorSeries


# --------------------------
# Fixtures
# --------------------------
@pytest.fixture
def fibonacci_instance() -> Fibonacci:
	"""Fixture providing a default Fibonacci instance.

	Returns
	-------
	Fibonacci
		Instance initialized with default cache {0: 0, 1: 1}
	"""
	return Fibonacci()


@pytest.fixture
def custom_cache() -> dict[int, int]:
	"""Fixture providing a custom cache for Fibonacci.

	Returns
	-------
	dict[int, int]
		Custom cache with additional Fibonacci numbers
	"""
	return {0: 0, 1: 1, 2: 1, 3: 2}


@pytest.fixture
def sin_function() -> Callable[[float], float]:
	"""Fixture providing the sine function for Taylor series testing.

	Returns
	-------
	Callable[[float], float]
		The math.sin function
	"""
	return math.sin


@pytest.fixture
def taylor_instance(sin_function: Callable[[float], float]) -> TaylorSeries:
	"""Fixture providing a TaylorSeries instance for sin(x) at order 3, center 0.

	Parameters
	----------
	sin_function : Callable[[float], float]
		The sine function from the fixture

	Returns
	-------
	TaylorSeries
		Instance initialized with sin function, order 3, center 0
	"""
	return TaylorSeries(sin_function, order=3, center=0.0)


# --------------------------
# Fibonacci Tests
# --------------------------
def test_fibonacci_init_default_cache() -> None:
	"""Test Fibonacci initialization with default cache.

	Returns
	-------
	None

	Verifies that:
	- Default cache is {0: 0, 1: 1}
	- Cache is a dictionary
	"""
	fib = Fibonacci()
	assert fib.cache == {0: 0, 1: 1}
	assert isinstance(fib.cache, dict)


def test_fibonacci_init_custom_cache(custom_cache: dict[int, int]) -> None:
	"""Test Fibonacci initialization with custom cache.

	Parameters
	----------
	custom_cache : dict[int, int]
		Custom cache with additional Fibonacci numbers

	Returns
	-------
	None

	Verifies that:
	- Custom cache is correctly assigned
	- Cache maintains provided values
	"""
	fib = Fibonacci(cache=custom_cache)
	assert fib.cache == custom_cache
	assert fib.fibonacci_of_n(3) == 2


def test_fibonacci_of_n_valid_input(fibonacci_instance: Fibonacci) -> None:
	"""Test fibonacci_of_n with valid inputs.

	Parameters
	----------
	fibonacci_instance : Fibonacci
		Default Fibonacci instance from fixture

	Returns
	-------
	None

	Verifies that:
	- Known Fibonacci numbers are correctly computed
	- Cache is updated with new values
	"""
	assert fibonacci_instance.fibonacci_of_n(0) == 0
	assert fibonacci_instance.fibonacci_of_n(1) == 1
	assert fibonacci_instance.fibonacci_of_n(5) == 5
	assert fibonacci_instance.cache[5] == 5


def test_fibonacci_of_n_negative_input(fibonacci_instance: Fibonacci) -> None:
	"""Test fibonacci_of_n with negative input.

	Parameters
	----------
	fibonacci_instance : Fibonacci
		Default Fibonacci instance from fixture

	Returns
	-------
	None

	Verifies that:
	- Negative input raises ValueError with correct message
	"""
	with pytest.raises(ValueError, match="Index n must be non-negative, got -1"):
		fibonacci_instance.fibonacci_of_n(-1)


def test_fibonacci_valid_input(fibonacci_instance: Fibonacci) -> None:
	"""Test fibonacci method with valid input.

	Parameters
	----------
	fibonacci_instance : Fibonacci
		Default Fibonacci instance from fixture

	Returns
	-------
	None

	Verifies that:
	- Correct Fibonacci sequence is generated
	- List type and length are correct
	"""
	result = fibonacci_instance.fibonacci(6)
	assert result == [0, 1, 1, 2, 3, 5]
	assert isinstance(result, list)
	assert len(result) == 6


def test_fibonacci_zero_input(fibonacci_instance: Fibonacci) -> None:
	"""Test fibonacci method with zero input.

	Parameters
	----------
	fibonacci_instance : Fibonacci
		Default Fibonacci instance from fixture

	Returns
	-------
	None

	Verifies that:
	- Empty list is returned for n=0
	"""
	result = fibonacci_instance.fibonacci(0)
	assert result == []
	assert isinstance(result, list)


def test_fibonacci_negative_input(fibonacci_instance: Fibonacci) -> None:
	"""Test fibonacci method with negative input.

	Parameters
	----------
	fibonacci_instance : Fibonacci
		Default Fibonacci instance from fixture

	Returns
	-------
	None

	Verifies that:
	- Negative input raises ValueError with correct message
	"""
	with pytest.raises(ValueError, match="Number of elements n must be non-negative, got -1"):
		fibonacci_instance.fibonacci(-1)


def test_fibonacci_cache_preservation(custom_cache: dict[int, int]) -> None:
	"""Test cache preservation across multiple calls.

	Parameters
	----------
	custom_cache : dict[int, int]
		Custom cache with additional Fibonacci numbers

	Returns
	-------
	None

	Verifies that:
	- Cache is preserved and updated correctly
	- Subsequent calls use cached values
	"""
	fib = Fibonacci(cache=custom_cache)
	fib.fibonacci_of_n(4)
	assert fib.cache[4] == 3
	assert fib.fibonacci_of_n(4) == 3  # Uses cached value


def test_fibonacci_reload() -> None:
	"""Test module reloading preserves Fibonacci functionality.

	Returns
	-------
	None

	Verifies that:
	- Reloading the module doesn't break Fibonacci functionality
	- New instance works as expected
	"""
	importlib.reload(sys.modules["stpstone.analytics.quant.sequences"])
	fib = Fibonacci()
	assert fib.fibonacci_of_n(5) == 5


# --------------------------
# TaylorSeries Tests
# --------------------------
def test_taylor_init_valid_inputs(sin_function: Callable[[float], float]) -> None:
	"""Test TaylorSeries initialization with valid inputs.

	Parameters
	----------
	sin_function : Callable[[float], float]
		Sine function from fixture

	Returns
	-------
	None

	Verifies that:
	- Attributes are correctly set
	- Coefficients are computed
	- d_pts is odd and sufficient for derivatives
	"""
	taylor = TaylorSeries(sin_function, order=3, center=1.0)
	assert taylor.function == sin_function
	assert taylor.order == 3
	assert taylor.center == 1.0
	assert taylor.d_pts % 2 == 1
	assert len(taylor.coefficients) == 4


def test_taylor_init_negative_order(sin_function: Callable[[float], float]) -> None:
	"""Test TaylorSeries initialization with negative order.

	Parameters
	----------
	sin_function : Callable[[float], float]
		Sine function from fixture

	Returns
	-------
	None

	Verifies that:
	- Negative order raises ValueError with correct message
	"""
	with pytest.raises(ValueError, match="Order must be non-negative, got -1"):
		TaylorSeries(sin_function, order=-1)


def test_taylor_init_invalid_function() -> None:
	"""Test TaylorSeries initialization with non-callable function.

	Returns
	-------
	None

	Verifies that:
	- Non-callable function raises TypeError with correct message
	"""
	with pytest.raises(TypeError, match="must be of type"):
		TaylorSeries("not a function", order=3)


def test_find_coefficients(taylor_instance: TaylorSeries) -> None:
	"""Test _find_coefficients method.

	Parameters
	----------
	taylor_instance : TaylorSeries
		TaylorSeries instance for sin(x) from fixture

	Returns
	-------
	None

	Verifies that:
	- Coefficients match expected values for sin(x) at x=0
	- Coefficients are rounded to 5 decimal places
	"""
	expected = [0.0, 1.0, 0.0, -0.16667]  # sin(x) ≈ x - x^3/6
	for i, coeff in enumerate(taylor_instance.coefficients):
		assert coeff == pytest.approx(expected[i], abs=1e-5)


def test_print_equation(taylor_instance: TaylorSeries, mocker: MockerFixture) -> None:
	"""Test print_equation method.

	Parameters
	----------
	taylor_instance : TaylorSeries
		TaylorSeries instance for sin(x) from fixture
	mocker : MockerFixture
		Pytest-mock fixture for mocking print

	Returns
	-------
	None

	Verifies that:
	- Correct equation string is printed
	- Non-zero coefficients are included
	"""
	mock_print = mocker.patch("builtins.print")
	taylor_instance.print_equation()
	mock_print.assert_called_once_with("1.0(x-0.0)^1 + -0.16667(x-0.0)^3")


def test_print_coefficients(taylor_instance: TaylorSeries, mocker: MockerFixture) -> None:
	"""Test print_coefficients method.

	Parameters
	----------
	taylor_instance : TaylorSeries
		TaylorSeries instance for sin(x) from fixture
	mocker : MockerFixture
		Pytest-mock fixture for mocking print

	Returns
	-------
	None

	Verifies that:
	- Correct coefficients list is printed
	"""
	mock_print = mocker.patch("builtins.print")
	taylor_instance.print_coefficients()
	mock_print.assert_called_once_with(taylor_instance.coefficients)


def test_approximate_value(taylor_instance: TaylorSeries) -> None:
	"""Test approximate_value method.

	Parameters
	----------
	taylor_instance : TaylorSeries
		TaylorSeries instance for sin(x) from fixture

	Returns
	-------
	None

	Verifies that:
	- Approximation of sin(0.1) is accurate
	- Return type is float
	"""
	result = taylor_instance.approximate_value(0.1)
	expected = math.sin(0.1)
	assert result == pytest.approx(expected, abs=1e-3)
	assert isinstance(result, float)


def test_approximate_value_invalid_input(taylor_instance: TaylorSeries) -> None:
	"""Test approximate_value with non-finite input.

	Parameters
	----------
	taylor_instance : TaylorSeries
		TaylorSeries instance for sin(x) from fixture

	Returns
	-------
	None

	Verifies that:
	- Non-finite input raises ValueError with correct message
	"""
	with pytest.raises(ValueError, match="Input x must be finite, got inf"):
		taylor_instance.approximate_value(float("inf"))


def test_approximate_derivative(taylor_instance: TaylorSeries) -> None:
	"""Test approximate_derivative method.

	Parameters
	----------
	taylor_instance : TaylorSeries
		TaylorSeries instance for sin(x) from fixture

	Returns
	-------
	None

	Verifies that:
	- Derivative approximation at x=0.1 matches cos(0.1)
	- Return type is float
	"""
	result = taylor_instance.approximate_derivative(0.1)
	expected = math.cos(0.1)
	assert result == pytest.approx(expected, abs=1e-3)
	assert isinstance(result, float)


def test_approximate_derivative_invalid_input(taylor_instance: TaylorSeries) -> None:
	"""Test approximate_derivative with non-finite input.

	Parameters
	----------
	taylor_instance : TaylorSeries
		TaylorSeries instance for sin(x) from fixture

	Returns
	-------
	None

	Verifies that:
	- Non-finite input raises ValueError with correct message
	"""
	with pytest.raises(ValueError, match="Input x must be finite, got nan"):
		taylor_instance.approximate_derivative(float("nan"))


def test_approximate_integral(taylor_instance: TaylorSeries) -> None:
	"""Test approximate_integral method.

	Parameters
	----------
	taylor_instance : TaylorSeries
		TaylorSeries instance for sin(x) from fixture

	Returns
	-------
	None

	Verifies that:
	- Integral approximation from 0 to 0.1 matches -cos(0.1) + cos(0)
	- Return type is float
	"""
	result = taylor_instance.approximate_integral(0.0, 0.1)
	expected = -math.cos(0.1) + math.cos(0.0)
	assert result == pytest.approx(expected, abs=1e-3)
	assert isinstance(result, float)


def test_approximate_integral_invalid_bounds(taylor_instance: TaylorSeries) -> None:
	"""Test approximate_integral with non-finite bounds.

	Parameters
	----------
	taylor_instance : TaylorSeries
		TaylorSeries instance for sin(x) from fixture

	Returns
	-------
	None

	Verifies that:
	- Non-finite bounds raise ValueError with correct message
	"""
	with pytest.raises(ValueError, match="Lower bound x0 must be finite, got inf"):
		taylor_instance.approximate_integral(float("inf"), 1.0)
	with pytest.raises(ValueError, match="Upper bound x1 must be finite, got nan"):
		taylor_instance.approximate_integral(0.0, float("nan"))


def test_get_coefficients(taylor_instance: TaylorSeries) -> None:
	"""Test get_coefficients method.

	Parameters
	----------
	taylor_instance : TaylorSeries
		TaylorSeries instance for sin(x) from fixture

	Returns
	-------
	None

	Verifies that:
	- Returns correct coefficients list
	- Return type is list of floats
	"""
	result = taylor_instance.get_coefficients()
	assert result == taylor_instance.coefficients
	assert isinstance(result, list)
	assert all(isinstance(coeff, float) for coeff in result)


def test_taylor_edge_case_zero_order(sin_function: Callable[[float], float]) -> None:
	"""Test TaylorSeries with zero order.

	Parameters
	----------
	sin_function : Callable[[float], float]
		Sine function from fixture

	Returns
	-------
	None

	Verifies that:
	- Zero order produces one coefficient
	- Coefficient matches function value at center
	"""
	taylor = TaylorSeries(sin_function, order=0, center=0.0)
	assert len(taylor.coefficients) == 1
	assert taylor.coefficients[0] == pytest.approx(0.0, abs=1e-5)


def test_taylor_large_input(taylor_instance: TaylorSeries) -> None:
	"""Test approximate_value with large input.

	Parameters
	----------
	taylor_instance : TaylorSeries
		TaylorSeries instance for sin(x) from fixture

	Returns
	-------
	None

	Verifies that:
	- Large input doesn't cause overflow
	- Result is finite
	"""
	result = taylor_instance.approximate_value(100.0)
	assert math.isfinite(result)


def test_taylor_reload(sin_function: Callable[[float], float]) -> None:
	"""Test module reloading preserves TaylorSeries functionality.

	Parameters
	----------
	sin_function : Callable[[float], float]
		Sine function from fixture

	Returns
	-------
	None

	Verifies that:
	- Reloading the module doesn't break TaylorSeries functionality
	- New instance works as expected
	"""
	importlib.reload(sys.modules["stpstone.analytics.quant.sequences"])
	taylor = TaylorSeries(sin_function, order=3, center=0.0)
	assert taylor.approximate_value(0.1) == pytest.approx(math.sin(0.1), abs=1e-3)


def test_taylor_print_equation_empty(taylor_instance: TaylorSeries, mocker: MockerFixture) -> None:
	"""Test print_equation with all zero coefficients.

	Parameters
	----------
	taylor_instance : TaylorSeries
		TaylorSeries instance for sin(x) from fixture
	mocker : MockerFixture
		Pytest-mock fixture for mocking print

	Returns
	-------
	None

	Verifies that:
	- Empty equation is printed when all coefficients are zero
	"""
	taylor_instance.coefficients = [0.0] * (taylor_instance.order + 1)
	mock_print = mocker.patch("builtins.print")
	taylor_instance.print_equation()
	mock_print.assert_called_once_with("")


def test_taylor_approximate_integral_same_bounds(taylor_instance: TaylorSeries) -> None:
	"""Test approximate_integral with same integration bounds.

	Parameters
	----------
	taylor_instance : TaylorSeries
		TaylorSeries instance for sin(x) from fixture

	Returns
	-------
	None

	Verifies that:
	- Integral from x to x is zero
	"""
	result = taylor_instance.approximate_integral(1.0, 1.0)
	assert result == 0.0
