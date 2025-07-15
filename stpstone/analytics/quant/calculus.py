"""Calculus."""

import random
from typing import Optional, Union

import numpy as np
import numpy.typing as npt
from scipy.integrate import cumulative_trapezoid, trapezoid
import sympy as sym
from sympy.core.expr import Expr
from sympy.core.symbol import Symbol

from stpstone.analytics.quant.linear_transformations import LinearAlgebra
from stpstone.transformations.validation.metaclass_type_checker import TypeChecker


class Calculus(metaclass=TypeChecker):
    """A class providing various calculus operations including differentiation and integration."""

    def __init__(self) -> None:
        self.linear_algebra = LinearAlgebra()

    def variables(self, str_symbols: str) -> tuple[Symbol, ...]:
        """Create SymPy symbols from a space-separated string.

        Parameters
        ----------
        str_symbols : str
            Space-separated string of symbol names.

        Returns
        -------
        tuple[Symbol, ...]
            tuple of SymPy symbols.

        Examples
        --------
        >>> x, y, z = Calculus().variables('x y z')
        """
        return sym.symbols(str_symbols.split(' '))

    def differentiation(
        self,
        f: Expr,
        variable_: Symbol,
        nth_derivative: int = 1,
        *args_symbols: Symbol
    ) -> Expr:
        """Compute the nth derivative of a function with respect to a variable.

        Parameters
        ----------
        f : Expr
            The function to differentiate.
        variable_ : Symbol
            The variable with respect to which to differentiate.
        nth_derivative : int, optional
            The order of the derivative (default is 1).
        *args_symbols : Symbol
            Additional symbols in the function.

        Returns
        -------
        Expr
            The nth derivative of the function.

        Examples
        --------
        >>> x, y, z = Calculus().variables('x y z')
        >>> f = x*y + x**2 + sym.sin(2*y)
        >>> df_dx = Calculus().differentiation(f, x, 1, x, y, z)
        >>> print(df_dx)
        2*x + y
        """
        if variable_ not in f.free_symbols:
            raise TypeError(f"Variable {variable_} not found in expression")
        return sym.diff(f, variable_, nth_derivative)

    def integration(
        self,
        f: Expr,
        variable_: Symbol,
        lower_bound: Optional[float] = None,
        upper_bound: Optional[float] = None
    ) -> Union[Expr, float]:
        """Compute the integral of a function with respect to a variable.

        Parameters
        ----------
        f : Expr
            The function to integrate.
        variable_ : Symbol
            The variable with respect to which to integrate.
        lower_bound : Optional[float], optional
            The lower bound of the integral (default is None).
        upper_bound : Optional[float], optional
            The upper bound of the integral (default is None).

        Returns
        -------
        Union[Expr, float]
            The indefinite integral if no bounds are provided,
            or the definite integral value if bounds are provided.

        Examples
        --------
        >>> x, y, z = Calculus().variables('x y z')
        >>> f = x*y + x**2 + sym.sin(2*y)
        >>> # Indefinite integral
        >>> F = Calculus().integration(f, x)
        >>> print(F)
        x**3/3 + x**2*y/2 + x*sin(2*y)
        >>> # Definite integral
        >>> F_def = Calculus().integration(f, x, 0.0, 2.0)
        >>> print(F_def)
        2*y + 2*sin(2*y) + 8/3
        """
        if lower_bound is None and upper_bound is None:
            return sym.integrate(f, variable_)
        
        # convert bounds to float and maintain symbolic format
        lb = sym.Float(lower_bound) if lower_bound is not None else None
        ub = sym.Float(upper_bound) if upper_bound is not None else None
        
        result = sym.integrate(f, (variable_, lb or 0, ub or 0))
        # convert to float if result is purely numeric
        if result.is_Number:
            return float(result)
        return result

    def trapz_integration(
        self,
        f: npt.ArrayLike,
        variable_: npt.ArrayLike
    ) -> float:
        """Compute the integral using the trapezoidal rule.

        Parameters
        ----------
        f : array_like
            Function values to integrate.
        variable_ : array_like
            The variable values corresponding to f values.

        Returns
        -------
        float
            The integral approximation.

        Examples
        --------
        >>> x = np.linspace(0, 2, 100)
        >>> y = x**2
        >>> integral = Calculus().trapz_integration(y, x)
        """
        try:
            f_arr = np.asarray(f, dtype=np.float64)
            var_arr = np.asarray(variable_, dtype=np.float64)
        except (TypeError, ValueError) as err:
            raise TypeError("Inputs must be array-like numerical values") from err

        if f_arr.size == 0 or var_arr.size == 0:
            raise ValueError("Cannot integrate empty arrays")
            
        if len(f_arr) != len(var_arr):
            raise ValueError("Input arrays must have same length")
            
        return trapezoid(f_arr, var_arr)

    def cumtrapz_integration(
        self,
        f: npt.ArrayLike,
        variable_: npt.ArrayLike
    ) -> npt.NDArray[np.float64]:
        """Compute the cumulative integral using the trapezoidal rule.

        Parameters
        ----------
        f : array_like
            Function values to integrate.
        variable_ : array_like
            The variable values corresponding to f values.

        Returns
        -------
        ndarray
            The cumulative integral values.

        Examples
        --------
        >>> x = np.linspace(0, 2, 100)
        >>> y = x**2
        >>> cum_integral = Calculus().cumtrapz_integration(y, x)
        """
        if np.asarray(f).size == 0 or np.asarray(variable_).size == 0:
            raise ValueError("Cannot integrate empty arrays")
        return cumulative_trapezoid(f, variable_, initial=None)

    def simplify(self, f: Expr) -> Expr:
        """Simplify a mathematical expression.

        Parameters
        ----------
        f : Expr
            The expression to simplify.

        Returns
        -------
        Expr
            The simplified expression.

        Examples
        --------
        >>> x = sym.symbols('x')
        >>> expr = (x**2 + 2*x + 1)/(x + 1)
        >>> simplified = Calculus().simplify(expr)
        >>> print(simplified)
        x + 1
        """
        return sym.simplify(f)

    def gradient_step(
        self,
        array_x: Union[list[float], npt.NDArray[np.float64]],
        gradient: Union[list[float], npt.NDArray[np.float64]],
        step_size: float
    ) -> npt.NDArray[np.float64]:
        """Take a gradient step in the direction of the gradient.
        
        Parameters
        ----------
        array_x : npt.NDArray[np.float64]
            Current independent variable values
        gradient : npt.NDArray[np.float64]
            Gradient vector
        step_size : float
            Size of the step to take
            
        Returns
        -------
        npt.NDArray[np.float64]
            New independent variable values after the step
        """
        array_x = np.asarray(array_x, dtype=np.float64)
        gradient = np.asarray(gradient, dtype=np.float64)
        
        if len(array_x) != len(gradient):
            raise ValueError("array_x and gradient must have same length")
        return array_x + step_size * gradient

    def sum_of_squares_gradient(
        self, 
        array_x: Union[list[float], npt.NDArray[np.float64]]
    ) -> npt.NDArray[np.float64]:
        """Compute the gradient of the sum of squares function.
        
        Parameters
        ----------
        array_x : Union[list[float], npt.NDArray[np.float64]]
            Input vector (either list or numpy array)
            
        Returns
        -------
        npt.NDArray[np.float64]
            Gradient vector (2*v_i for each element v_i) as numpy array
        """
        if isinstance(array_x, list):
            array_x = np.array(array_x, dtype=np.float64)
        return 2 * array_x

    def least_gradient_vector(
        self,
        step_size: float = -0.01,
        iter: int = 1000,
        epsilon: float = 0.001
    ) -> list[float]:
        """Find the vector that minimizes the sum of squares using gradient descent.

        Parameters
        ----------
        step_size : float, optional
            Learning rate (default is -0.01).
        iter : int, optional
            Number of iterations (default is 1000).
        epsilon : float, optional
            Convergence threshold (default is 0.001).

        Returns
        -------
        list[float]
            The vector that minimizes the sum of squares.

        Examples
        --------
        >>> result = Calculus().least_gradient_vector()
        >>> print(result)  # Should be close to [0, 0, 0]
        """
        # pick a random starting point 
        # noqa: S311 - standard pseudo-random generators are not suitable for cryptography
        array_x = np.array([random.uniform(-10, 10) for _ in range(3)], dtype=np.float64) # noqa: S311
        
        # loop through iterations
        for _ in range(iter):
            # compute the gradient at array_x
            grad = self.sum_of_squares_gradient(array_x)
            # take a negative gradient step
            array_x = self.gradient_step(array_x, grad, step_size)
            
        # array_x should be close to 0
        if not np.allclose(array_x, np.zeros(3), atol=epsilon):
            raise ValueError("Failed to converge within tolerance")
        return array_x