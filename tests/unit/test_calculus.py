"""Unit tests for the Calculus class.

Tests cover differentiation, integration, simplification, and gradient operations.
"""

from typing import Any

import numpy as np
from numpy.typing import NDArray
import pytest
import sympy as sym
from sympy.core.expr import Expr
from sympy.core.symbol import Symbol

from stpstone.analytics.quant.calculus import Calculus
from stpstone.analytics.quant.linear_transformations import LinearAlgebra


# --------------------------
# Fixtures
# --------------------------
@pytest.fixture
def calc() -> Calculus:
    """Fixture providing a Calculus instance."""
    return Calculus()


@pytest.fixture
def basic_symbols() -> tuple[Symbol, Symbol, Symbol]:
    """Fixture providing basic symbols (x, y, z)."""
    return sym.symbols("x y z")


@pytest.fixture
def basic_expression(basic_symbols: tuple[Symbol, Symbol, Symbol]) -> Expr:
    """Fixture providing a basic expression (x*y + x**2 + sin(2*y))."""
    x, y, _ = basic_symbols
    return x * y + x**2 + sym.sin(2 * y)


@pytest.fixture
def rational_expression(basic_symbols: tuple[Symbol, Symbol, Symbol]) -> Expr:
    """Fixture providing a rational expression ((x**2 + 2*x + 1)/(x + 1))."""
    x, _, _ = basic_symbols
    return (x**2 + 2 * x + 1) / (x + 1)


@pytest.fixture
def numpy_data() -> tuple[NDArray[np.float64], NDArray[np.float64]]:
    """Fixture providing numpy arrays for numerical integration tests."""
    x = np.linspace(0, 2, 100)
    y = x**2
    return x, y


# --------------------------
# Test Variables
# --------------------------
def test_variables_creation(calc: Calculus) -> None:
    """Test creation of symbols from space-separated string."""
    symbols = calc.variables("x y z")
    assert len(symbols) == 3
    assert all(isinstance(s, Symbol) for s in symbols)
    assert str(symbols[0]) == "x"
    assert str(symbols[1]) == "y"
    assert str(symbols[2]) == "z"


def test_variables_single_symbol(calc: Calculus) -> None:
    """Test creation of single symbol."""
    symbols = calc.variables("x")
    assert len(symbols) == 1
    assert isinstance(symbols[0], Symbol)
    assert str(symbols[0]) == "x"


def test_variables_empty_string(calc: Calculus) -> None:
    """Test behavior with empty input string."""
    with pytest.raises(ValueError):
        calc.variables("")


# --------------------------
# Test Differentiation
# --------------------------
def test_differentiation_basic(
    calc: Calculus, basic_symbols: tuple[Symbol, Symbol, Symbol], basic_expression: Expr
) -> None:
    """Test basic differentiation."""
    x, y, z = basic_symbols
    df_dx = calc.differentiation(basic_expression, x)
    assert str(df_dx) == "2*x + y"
    assert isinstance(df_dx, Expr)


def test_differentiation_higher_order(
    calc: Calculus, basic_symbols: tuple[Symbol, Symbol, Symbol], basic_expression: Expr
) -> None:
    """Test higher order differentiation."""
    x, y, _ = basic_symbols
    df_dx2 = calc.differentiation(basic_expression, x, 2)
    assert str(df_dx2) == "2"
    assert isinstance(df_dx2, Expr)


def test_differentiation_mixed_variables(
    calc: Calculus, basic_symbols: tuple[Symbol, Symbol, Symbol], basic_expression: Expr
) -> None:
    """Test differentiation with respect to different variables."""
    _, y, _ = basic_symbols
    df_dy = calc.differentiation(basic_expression, y)
    assert str(df_dy) == "x + 2*cos(2*y)"
    assert isinstance(df_dy, Expr)


def test_differentiation_invalid_variable(
    calc: Calculus, basic_expression: Expr
) -> None:
    """Test differentiation with invalid variable."""
    w = sym.Symbol("w")
    with pytest.raises(TypeError):
        calc.differentiation(basic_expression, w)


# --------------------------
# Test Integration
# --------------------------
def test_integration_indefinite(
    calc: Calculus, basic_symbols: tuple[Symbol, Symbol, Symbol], basic_expression: Expr
) -> None:
    """Test indefinite integration."""
    x, y, _ = basic_symbols
    integral = calc.integration(basic_expression, x)
    assert str(integral) == "x**3/3 + x**2*y/2 + x*sin(2*y)"
    assert isinstance(integral, Expr)


def test_integration_definite(
    calc: Calculus, basic_symbols: tuple[Symbol, Symbol, Symbol], basic_expression: Expr
) -> None:
    """Test definite integration."""
    x, y, _ = basic_symbols
    integral = calc.integration(basic_expression, x, 0.0, 2.0)
    assert str(integral) in ["2*y + 2*sin(2*y) + 8/3", 
                           "2.0*y + 2.0*sin(2*y) + 2.66666666666667"]
    assert isinstance(integral, Expr)


def test_integration_definite_numeric(
    calc: Calculus, basic_symbols: tuple[Symbol, Symbol, Symbol]
) -> None:
    """Test definite integration with numeric result."""
    x, _, _ = basic_symbols
    integral = calc.integration(x**2, x, 0.0, 2.0)
    assert abs(float(integral) - 8 / 3) < 1e-9


def test_integration_invalid_bounds(
    calc: Calculus, basic_symbols: tuple[Symbol, Symbol, Symbol], basic_expression: Expr
) -> None:
    """Test integration with invalid bounds."""
    x, _, _ = basic_symbols
    with pytest.raises(TypeError):
        calc.integration(basic_expression, x, "a", "b")  # type: ignore


# --------------------------
# Test Numerical Integration
# --------------------------
def test_trapz_integration(
    calc: Calculus, numpy_data: tuple[NDArray[np.float64], NDArray[np.float64]]
) -> None:
    """Test trapezoidal integration with numpy arrays."""
    x, y = numpy_data
    integral = calc.trapz_integration(y, x)
    assert abs(integral - 8 / 3) < 0.01


def test_cumtrapz_integration(
    calc: Calculus, numpy_data: tuple[NDArray[np.float64], NDArray[np.float64]]
) -> None:
    """Test cumulative trapezoidal integration."""
    x, y = numpy_data
    cum_integral = calc.cumtrapz_integration(y, x)
    assert len(cum_integral) == len(x) - 1
    assert all(cum_integral[-1] >= cum_integral[:-1])  # Monotonically increasing


def test_numerical_integration_mismatched_lengths(calc: Calculus) -> None:
    """Test numerical integration with mismatched array lengths."""
    x = np.linspace(0, 1, 10)
    y = np.linspace(0, 1, 11)
    with pytest.raises(ValueError):
        calc.trapz_integration(y, x)


# --------------------------
# Test Simplification
# --------------------------
def test_simplify_rational(
    calc: Calculus, rational_expression: Expr, basic_symbols: tuple[Symbol, Symbol, Symbol]
) -> None:
    """Test simplification of rational expression."""
    simplified = calc.simplify(rational_expression)
    x, _, _ = basic_symbols
    assert str(simplified) == "x + 1"
    assert isinstance(simplified, Expr)


def test_simplify_trig(
    calc: Calculus, basic_symbols: tuple[Symbol, Symbol, Symbol]
) -> None:
    """Test simplification of trigonometric expression."""
    x, _, _ = basic_symbols
    expr = sym.sin(x)**2 + sym.cos(x)**2
    simplified = calc.simplify(expr)
    assert str(simplified) == "1"
    assert isinstance(simplified, Expr)


# --------------------------
# Test Gradient Operations
# --------------------------
def test_sum_of_squares_gradient(calc: Calculus) -> None:
    """Test gradient of sum of squares function."""
    gradient = calc.sum_of_squares_gradient([1.0, 2.0, 3.0])
    assert np.array_equal(gradient, np.array([2.0, 4.0, 6.0]))


def test_gradient_step(calc: Calculus) -> None:
    """Test gradient step calculation."""
    new_values = calc.gradient_step([1.0, 2.0, 3.0], [0.1, -0.2, 0.3], 0.1)
    assert np.allclose(new_values, np.array([1.01, 1.98, 3.03]))


def test_gradient_step_mismatched_lengths(calc: Calculus) -> None:
    """Test gradient step with mismatched array lengths."""
    with pytest.raises(ValueError):
        calc.gradient_step([1.0, 2.0], [0.1, -0.2, 0.3], 0.1)

def test_least_gradient_vector(calc: Calculus, mocker: type[Any]) -> None:
    """Test least gradient vector convergence."""
    # Mock random.uniform to return fixed values
    mocker.patch("random.uniform", side_effect=[1.0, 2.0, 3.0])
    
    # Mock print to suppress output during tests
    mocker.patch("builtins.print")
    
    result = calc.least_gradient_vector(iter=400)
    assert np.allclose(result, np.zeros(3), atol=0.001)


# --------------------------
# Test Type Validation
# --------------------------
def test_invalid_input_types(calc: Calculus) -> None:
    """Test type checking for invalid inputs."""
    with pytest.raises(TypeError):
        calc.differentiation("not an expression", "not a symbol")  # type: ignore
    
    with pytest.raises(TypeError):
        calc.integration("not an expression", "not a symbol")  # type: ignore
    
    with pytest.raises(TypeError):
        calc.simplify("not an expression")  # type: ignore
    
    with pytest.raises(TypeError):
        calc.trapz_integration([1, 2, 3], "not an array")  # type: ignore


# --------------------------
# Test Edge Cases
# --------------------------
def test_empty_arrays_numerical_integration(calc: Calculus) -> None:
    """Test numerical integration with empty arrays."""
    with pytest.raises(ValueError):
        calc.trapz_integration(np.array([]), np.array([]))


def test_single_element_arrays(calc: Calculus) -> None:
    """Test numerical integration with single-element arrays."""
    result = calc.trapz_integration(np.array([1.0]), np.array([0.0]))
    assert result == 0.0


def test_zero_step_size(calc: Calculus) -> None:
    """Test gradient step with zero step size."""
    result = calc.gradient_step([1.0, 2.0, 3.0], [0.1, -0.2, 0.3], 0.0)
    assert np.array_equal(result, np.array([1.0, 2.0, 3.0]))


def test_large_iterations_least_gradient(calc: Calculus, mocker: type[Any]) -> None:
    """Test least gradient vector with large iteration count."""
    mocker.patch("random.uniform", return_value=10.0)
    mocker.patch("builtins.print")
    
    result = calc.least_gradient_vector(iter=10000)
    assert LinearAlgebra().distance(result, [0, 0, 0]) < 0.001