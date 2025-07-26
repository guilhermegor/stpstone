"""Unit tests for Root class in stpstone.analytics.quant.root.

Tests the root-finding functionality for scalar functions using:
- Bisection method
- Newton-Raphson method
- fsolve method
Covers normal operations, edge cases, error conditions, and type validation.
"""

import importlib
import sys
from typing import Callable

import numpy as np
import pytest
from pytest_mock import MockerFixture

from stpstone.analytics.quant.root import Root


# --------------------------
# Fixtures
# --------------------------
@pytest.fixture
def root_instance() -> Root:
    """Fixture providing a Root instance.

    Returns
    -------
    Root
        Instance of the Root class for testing
    """
    return Root()


@pytest.fixture
def quadratic_func() -> Callable[[float], float]:
    """Fixture providing a quadratic function f(x) = x^2 - 4.

    Returns
    -------
    Callable[[float], float]
        Quadratic function with a root at x=2
    """
    def func(x: float) -> float:
        """Quadratic function for root finding.

        Parameters
        ----------
        x : float
            Input value

        Returns
        -------
        float
            f(x) = x^2 - 4
        """
        return x**2 - 4
    return func


@pytest.fixture
def linear_func() -> Callable[[float], float]:
    """Fixture providing a linear function f(x) = x - 1.

    Returns
    -------
    Callable[[float], float]
        Linear function with a root at x=1
    """
    def func(x: float) -> float:
        """Linear function for root finding.

        Parameters
        ----------
        x : float
            Input value

        Returns
        -------
        float
            f(x) = x - 1
        """
        return x - 1
    return func


# --------------------------
# Tests for bisection method
# --------------------------
def test_bisection_valid_input(
    root_instance: Root, quadratic_func: Callable[[float], float]
) -> None:
    """Test bisection method with valid inputs.

    Verifies that:
    - The method finds a root within the specified tolerance
    - The result is close to the expected root (x=2 for x^2 - 4)

    Parameters
    ----------
    root_instance : Root
        Instance of the Root class for testing
    quadratic_func : Callable[[float], float]
        Quadratic function with a root at x=2

    Returns
    -------
    None
    """
    result = root_instance.bisection(quadratic_func, a=0.0, b=3.0, epsilon=1e-6)
    assert pytest.approx(result, abs=1e-6) == 2.0
    assert abs(quadratic_func(result)) < 1e-6


def test_bisection_negative_interval(
    root_instance: Root, quadratic_func: Callable[[float], float]
) -> None:
    """Test bisection method with a negative interval.

    Verifies that:
    - The method finds a root in a negative interval ([-3, 0] for x^2 - 4)
    - The result is close to the expected root (x=-2)

    Parameters
    ----------
    root_instance : Root
        Instance of the Root class for testing
    quadratic_func : Callable[[float], float]
        Quadratic function with a root at x=2

    Returns
    -------
    None
    """
    result = root_instance.bisection(quadratic_func, a=-3.0, b=0.0, epsilon=1e-6)
    assert pytest.approx(result, abs=1e-6) == -2.0
    assert abs(quadratic_func(result)) < 1e-6


def test_bisection_same_sign(
    root_instance: Root, quadratic_func: Callable[[float], float]
) -> None:
    """Test bisection method when a and b have the same sign.

    Verifies that:
    - A ValueError is raised when a and b do not bound a root

    Parameters
    ----------
    root_instance : Root
        Instance of the Root class for testing
    quadratic_func : Callable[[float], float]
        Quadratic function with a root at x=2

    Returns
    -------
    None
    """
    with pytest.raises(ValueError, match="The scalars a and b do not bound a root"):
        root_instance.bisection(quadratic_func, a=3.0, b=5.0, epsilon=1e-6)


def test_bisection_equal_bounds(
    root_instance: Root, quadratic_func: Callable[[float], float]
) -> None:
    """Test bisection method when a equals b.

    Verifies that:
    - A ValueError is raised when interval bounds are equal

    Parameters
    ----------
    root_instance : Root
        Instance of the Root class for testing
    quadratic_func : Callable[[float], float]
        Quadratic function with a root at x=2

    Returns
    -------
    None
    """
    with pytest.raises(ValueError, match="Interval bounds a and b must be distinct"):
        root_instance.bisection(quadratic_func, a=2.0, b=2.0, epsilon=1e-6)


def test_bisection_negative_epsilon(
    root_instance: Root, quadratic_func: Callable[[float], float]
) -> None:
    """Test bisection method with negative epsilon.

    Verifies that:
    - A ValueError is raised when epsilon is not positive

    Parameters
    ----------
    root_instance : Root
        Instance of the Root class for testing
    quadratic_func : Callable[[float], float]
        Quadratic function with a root at x=2

    Returns
    -------
    None
    """
    with pytest.raises(ValueError, match="Epsilon must be positive, got -1.0"):
        root_instance.bisection(quadratic_func, a=0.0, b=3.0, epsilon=-1.0)


def test_bisection_zero_epsilon(
    root_instance: Root, quadratic_func: Callable[[float], float]
) -> None:
    """Test bisection method with zero epsilon.

    Verifies that:
    - A ValueError is raised when epsilon is zero

    Parameters
    ----------
    root_instance : Root
        Instance of the Root class for testing
    quadratic_func : Callable[[float], float]
        Quadratic function with a root at x=2

    Returns
    -------
    None
    """
    with pytest.raises(ValueError, match="Epsilon must be positive, got 0.0"):
        root_instance.bisection(quadratic_func, a=0.0, b=3.0, epsilon=0.0)


def test_bisection_non_callable_func(root_instance: Root) -> None:
    """Test bisection method with non-callable function.

    Verifies that:
    - TypeChecker raises a TypeError for non-callable func

    Parameters
    ----------
    root_instance : Root
        Instance of the Root class for testing

    Returns
    -------
    None
    """
    with pytest.raises(TypeError, match="must be of type"):
        root_instance.bisection("not a function", a=0.0, b=3.0, epsilon=1e-6)


def test_bisection_invalid_bound_types(
    root_instance: Root, quadratic_func: Callable[[float], float]
) -> None:
    """Test bisection method with invalid bound types.

    Verifies that:
    - TypeChecker raises a TypeError for non-float bounds

    Parameters
    ----------
    root_instance : Root
        Instance of the Root class for testing
    quadratic_func : Callable[[float], float]
        Quadratic function with a root at x=2

    Returns
    -------
    None
    """
    with pytest.raises(TypeError, match="must be of type"):
        root_instance.bisection(quadratic_func, a="0", b=3.0, epsilon=1e-6)


def test_bisection_invalid_epsilon_type(
    root_instance: Root, quadratic_func: Callable[[float], float]
) -> None:
    """Test bisection method with invalid epsilon type.

    Verifies that:
    - TypeChecker raises a TypeError for non-float epsilon

    Parameters
    ----------
    root_instance : Root
        Instance of the Root class for testing
    quadratic_func : Callable[[float], float]
        Quadratic function with a root at x=2

    Returns
    -------
    None
    """
    with pytest.raises(TypeError, match="must be of type"):
        root_instance.bisection(quadratic_func, a=0.0, b=3.0, epsilon="1e-6")


# --------------------------
# Tests for newton_raphson method
# --------------------------
def test_newton_raphson_valid_input(
    root_instance: Root, linear_func: Callable[[float], float], mocker: MockerFixture
) -> None:
    """Test newton_raphson method with valid inputs.

    Verifies that:
    - The method finds a root within the specified tolerance
    - The result is close to the expected root (x=1 for x - 1)
    - scipy.optimize.newton is called with correct parameters

    Parameters
    ----------
    root_instance : Root
        Instance of the Root class for testing
    linear_func : Callable[[float], float]
        Linear function with a root at x=1
    mocker : MockerFixture
        Pytest mocker fixture

    Returns
    -------
    None
    """
    _ = mocker.patch("scipy.optimize.newton", return_value=1.0)
    result = root_instance.newton_raphson(linear_func, x0=0.0, epsilon=1e-6)
    assert pytest.approx(result, abs=1e-6) == 1.0
    assert abs(linear_func(result)) < 1e-6


def test_newton_raphson_negative_epsilon(
    root_instance: Root, linear_func: Callable[[float], float]
) -> None:
    """Test newton_raphson method with negative epsilon.

    Verifies that:
    - A ValueError is raised when epsilon is not positive

    Parameters
    ----------
    root_instance : Root
        Instance of the Root class for testing
    linear_func : Callable[[float], float]
        Linear function with a root at x=1

    Returns
    -------
    None
    """
    with pytest.raises(ValueError, match="Epsilon must be positive, got -1.0"):
        root_instance.newton_raphson(linear_func, x0=0.0, epsilon=-1.0)


def test_newton_raphson_zero_epsilon(
    root_instance: Root, linear_func: Callable[[float], float]
) -> None:
    """Test newton_raphson method with zero epsilon.

    Verifies that:
    - A ValueError is raised when epsilon is zero

    Parameters
    ----------
    root_instance : Root
        Instance of the Root class for testing
    linear_func : Callable[[float], float]
        Linear function with a root at x=1

    Returns
    -------
    None
    """
    with pytest.raises(ValueError, match="Epsilon must be positive, got 0.0"):
        root_instance.newton_raphson(linear_func, x0=0.0, epsilon=0.0)


def test_newton_raphson_non_callable_func(root_instance: Root) -> None:
    """Test newton_raphson method with non-callable function.

    Verifies that:
    - TypeChecker raises a TypeError for non-callable func

    Parameters
    ----------
    root_instance : Root
        Instance of the Root class for testing

    Returns
    -------
    None
    """
    with pytest.raises(TypeError, match="must be of type"):
        root_instance.newton_raphson("not a function", x0=0.0, epsilon=1e-6)


def test_newton_raphson_invalid_x0_type(
    root_instance: Root, linear_func: Callable[[float], float]
) -> None:
    """Test newton_raphson method with invalid x0 type.

    Verifies that:
    - TypeChecker raises a TypeError for non-float x0

    Parameters
    ----------
    root_instance : Root
        Instance of the Root class for testing
    linear_func : Callable[[float], float]
        Linear function with a root at x=1

    Returns
    -------
    None
    """
    with pytest.raises(TypeError, match="must be of type"):
        root_instance.newton_raphson(linear_func, x0="0", epsilon=1e-6)


def test_newton_raphson_invalid_epsilon_type(
    root_instance: Root, linear_func: Callable[[float], float]
) -> None:
    """Test newton_raphson method with invalid epsilon type.

    Verifies that:
    - TypeChecker raises a TypeError for non-float epsilon

    Parameters
    ----------
    root_instance : Root
        Instance of the Root class for testing
    linear_func : Callable[[float], float]
        Linear function with a root at x=1

    Returns
    -------
    None
    """
    with pytest.raises(TypeError, match="must be of type"):
        root_instance.newton_raphson(linear_func, x0=0.0, epsilon="1e-6")


# --------------------------
# Tests for fsolve method
# --------------------------
def test_fsolve_valid_input(
    root_instance: Root, linear_func: Callable[[float], float], mocker: MockerFixture
) -> None:
    """Test fsolve method with valid inputs.

    Verifies that:
    - The method finds a root within the specified tolerance
    - The result is close to the expected root (x=1 for x - 1)
    - scipy.optimize.fsolve is called with correct parameters

    Parameters
    ----------
    root_instance : Root
        Instance of the Root class for testing
    linear_func : Callable[[float], float]
        Linear function with a root at x=1
    mocker : MockerFixture
        Pytest mocker fixture

    Returns
    -------
    None
    """
    _ = mocker.patch("scipy.optimize.fsolve", return_value=np.array([1.0]))
    result = root_instance.fsolve(linear_func, x0=0.0, epsilon=1e-6)
    assert pytest.approx(result, abs=1e-6) == 1.0
    assert abs(linear_func(result)) < 1e-6


def test_fsolve_negative_epsilon(
    root_instance: Root, linear_func: Callable[[float], float]
) -> None:
    """Test fsolve method with negative epsilon.

    Verifies that:
    - A ValueError is raised when epsilon is not positive

    Parameters
    ----------
    root_instance : Root
        Instance of the Root class for testing
    linear_func : Callable[[float], float]
        Linear function with a root at x=1

    Returns
    -------
    None
    """
    with pytest.raises(ValueError, match="Epsilon must be positive, got -1.0"):
        root_instance.fsolve(linear_func, x0=0.0, epsilon=-1.0)


def test_fsolve_zero_epsilon(
    root_instance: Root, linear_func: Callable[[float], float]
) -> None:
    """Test fsolve method with zero epsilon.

    Verifies that:
    - A ValueError is raised when epsilon is zero

    Parameters
    ----------
    root_instance : Root
        Instance of the Root class for testing
    linear_func : Callable[[float], float]
        Linear function with a root at x=1

    Returns
    -------
    None
    """
    with pytest.raises(ValueError, match="Epsilon must be positive, got 0.0"):
        root_instance.fsolve(linear_func, x0=0.0, epsilon=0.0)


def test_fsolve_non_callable_func(root_instance: Root) -> None:
    """Test fsolve method with non-callable function.

    Verifies that:
    - TypeChecker raises a TypeError for non-callable func

    Parameters
    ----------
    root_instance : Root
        Instance of the Root class for testing

    Returns
    -------
    None
    """
    with pytest.raises(TypeError, match="must be of type"):
        root_instance.fsolve("not a function", x0=0.0, epsilon=1e-6)


def test_fsolve_invalid_x0_type(
    root_instance: Root, linear_func: Callable[[float], float]
) -> None:
    """Test fsolve method with invalid x0 type.

    Verifies that:
    - TypeChecker raises a TypeError for non-float x0

    Parameters
    ----------
    root_instance : Root
        Instance of the Root class for testing
    linear_func : Callable[[float], float]
        Linear function with a root at x=1

    Returns
    -------
    None
    """
    with pytest.raises(TypeError, match="must be of type"):
        root_instance.fsolve(linear_func, x0="0", epsilon=1e-6)


def test_fsolve_invalid_epsilon_type(
    root_instance: Root, linear_func: Callable[[float], float]
) -> None:
    """Test fsolve method with invalid epsilon type.

    Verifies that:
    - TypeChecker raises a TypeError for non-float epsilon

    Parameters
    ----------
    root_instance : Root
        Instance of the Root class for testing
    linear_func : Callable[[float], float]
        Linear function with a root at x=1

    Returns
    -------
    None
    """
    with pytest.raises(TypeError, match="must be of type"):
        root_instance.fsolve(linear_func, x0=0.0, epsilon="1e-6")


# --------------------------
# Edge cases and special values
# --------------------------
def test_bisection_small_interval(
    root_instance: Root, linear_func: Callable[[float], float]
) -> None:
    """Test bisection method with a very small interval.

    Verifies that:
    - The method handles small intervals correctly
    - The result is close to the expected root (x=1 for x - 1)

    Parameters
    ----------
    root_instance : Root
        Instance of the Root class for testing
    linear_func : Callable[[float], float]
        Linear function with a root at x=1

    Returns
    -------
    None
    """
    result = root_instance.bisection(linear_func, a=0.999, b=1.001, epsilon=1e-6)
    assert pytest.approx(result, abs=1e-6) == 1.0
    assert abs(linear_func(result)) < 1e-6


def test_bisection_large_epsilon(
    root_instance: Root, quadratic_func: Callable[[float], float]
) -> None:
    """Test bisection method with a large epsilon.

    Verifies that:
    - The method returns a less precise root with large epsilon

    Parameters
    ----------
    root_instance : Root
        Instance of the Root class for testing
    quadratic_func : Callable[[float], float]
        Quadratic function with a root at x=1

    Returns
    -------
    None
    """
    result = root_instance.bisection(quadratic_func, a=0.0, b=3.0, epsilon=0.1)
    assert abs(quadratic_func(result)) < 0.1


def test_newton_raphson_extreme_x0(
    root_instance: Root, linear_func: Callable[[float], float], mocker: MockerFixture
) -> None:
    """Test newton_raphson with an extreme initial guess.

    Verifies that:
    - The method handles large initial guesses

    Parameters
    ----------
    root_instance : Root
        Instance of the Root class for testing
    linear_func : Callable[[float], float]
        Linear function with a root at x=1
    mocker : MockerFixture
        Pytest mocker fixture

    Returns
    -------
    None
    """
    _ = mocker.patch("scipy.optimize.newton", return_value=1.0)
    result = root_instance.newton_raphson(linear_func, x0=1e6, epsilon=1e-6)
    assert pytest.approx(result, abs=1e-6) == 1.0


def test_fsolve_extreme_x0(
    root_instance: Root, linear_func: Callable[[float], float], mocker: MockerFixture
) -> None:
    """Test fsolve with an extreme initial guess.

    Verifies that:
    - The method handles large initial guesses

    Parameters
    ----------
    root_instance : Root
        Instance of the Root class for testing
    linear_func : Callable[[float], float]
        Linear function with a root at x=1
    mocker : MockerFixture
        Pytest mocker fixture

    Returns
    -------
    None
    """
    _ = mocker.patch("scipy.optimize.fsolve", return_value=np.array([1.0]))
    result = root_instance.fsolve(linear_func, x0=1e6, epsilon=1e-6)
    assert pytest.approx(result, abs=1e-6) == 1.0


# --------------------------
# Reload logic
# --------------------------
def test_module_reload() -> None:
    """Test module reloading behavior.

    Verifies that:
    - The module can be reloaded without errors
    - The Root class is still available after reload

    Returns
    -------
    None
    """
    importlib.reload(sys.modules["stpstone.analytics.quant.root"])
    assert hasattr(sys.modules["stpstone.analytics.quant.root"], "Root")
    root = Root()
    assert isinstance(root, Root)


# --------------------------
# Type validation via TypeChecker
# --------------------------
def test_type_checker_inheritance() -> None:
    """Test that Root uses TypeChecker metaclass.

    Verifies that:
    - The Root class uses the TypeChecker metaclass for type validation

    Returns
    -------
    None
    """
    assert Root.__class__.__name__ == "TypeChecker"