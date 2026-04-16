"""Sequences module for generating Fibonacci sequences and Taylor series approximations."""

import math
from typing import Callable, Optional

import numdifftools as nd

from stpstone.transformations.validation.metaclass_type_checker import TypeChecker


class Fibonacci(metaclass=TypeChecker):
	"""A class to generate Fibonacci sequences with caching for efficiency."""

	def __init__(self, cache: Optional[dict[int, int]] = None) -> None:
		"""Initialize Fibonacci sequence generator with a cache.

		Parameters
		----------
		cache : Optional[dict[int, int]]
			Initial cache dictionary with Fibonacci numbers (default: {0: 0, 1: 1})

		Notes
		-----
		The cache stores previously computed Fibonacci numbers to avoid redundant calculations.
		"""
		self.cache: dict[int, int] = cache if cache is not None else {0: 0, 1: 1}

	def fibonacci_of_n(self, n: int) -> int:
		"""Calculate the nth Fibonacci number using caching.

		Parameters
		----------
		n : int
			Index of the Fibonacci number to compute (must be non-negative)

		Returns
		-------
		int
			The nth Fibonacci number

		Raises
		------
		ValueError
			If n is negative

		References
		----------
		.. [1] https://realpython.com/fibonacci-sequence-python/
		"""
		if n < 0:
			raise ValueError(f"Index n must be non-negative, got {n}")
		if n in self.cache:
			return self.cache[n]
		self.cache[n] = self.fibonacci_of_n(n - 1) + self.fibonacci_of_n(n - 2)
		return self.cache[n]

	def fibonacci(self, n: int) -> list[int]:
		"""Generate a list of Fibonacci numbers up to index n.

		Parameters
		----------
		n : int
			Number of Fibonacci numbers to generate (must be non-negative)

		Returns
		-------
		list[int]
			List of Fibonacci numbers from index 0 to n-1

		Raises
		------
		ValueError
			If n is negative

		References
		----------
		.. [1] https://realpython.com/fibonacci-sequence-python/
		"""
		if n < 0:
			raise ValueError(f"Number of elements n must be non-negative, got {n}")
		return [self.fibonacci_of_n(i) for i in range(n)]


class TaylorSeries(metaclass=TypeChecker):
	"""A class to compute Taylor series approximations for a given function."""

	def __init__(
		self, function: Callable[[float], float], order: int, center: float = 0.0
	) -> None:
		"""Initialize Taylor series for a given function.

		Parameters
		----------
		function : Callable[[float], float]
			Function to approximate
		order : int
			Order of the Taylor series (must be non-negative)
		center : float
			Center point of the Taylor series (default: 0.0)

		Raises
		------
		ValueError
			If order is negative
		TypeError
			If function is not callable

		Notes
		-----
		Uses numdifftools.Derivative to compute derivatives at the center point.
		"""
		if not callable(function):
			raise TypeError("function must be callable")
		if order < 0:
			raise ValueError(f"Order must be non-negative, got {order}")
		self.center: float = center
		self.function: Callable[[float], float] = function
		self.order: int = order
		self.d_pts: int = order * 2 + 1  # ensure odd number for derivative
		self.coefficients: list[float] = []
		self._find_coefficients()

	def _find_coefficients(self) -> None:
		"""Compute Taylor series coefficients.

		Notes
		-----
		Uses numdifftools.Derivative to compute derivatives at the center point.
		"""
		for i in range(self.order + 1):
			derivative_func = nd.Derivative(self.function, n=i, order=self.d_pts)
			coeff = derivative_func(self.center) / math.factorial(i)
			self.coefficients.append(round(coeff, 5))

	def print_equation(self) -> None:
		"""Print the Taylor series equation as a string.

		Notes
		-----
		Prints the equation in the format f(x) = c0 + c1(x-c) + c2(x-c)^2 + ...
		"""
		equation = ""
		for i in range(self.order + 1):
			if self.coefficients[i] != 0:
				term = f"{self.coefficients[i]}"
				if i > 0:
					term += f"(x-{self.center})^{i}"
				equation += term + " + "
		equation = equation[:-3] if equation.endswith(" + ") else equation
		print(equation)

	def print_coefficients(self) -> None:
		"""Print the Taylor series coefficients."""
		print(self.coefficients)

	def approximate_value(self, x: float) -> float:
		"""Approximate the function value at point x using the Taylor series.

		Parameters
		----------
		x : float
			Point at which to evaluate the Taylor series

		Returns
		-------
		float
			Approximated function value

		Raises
		------
		ValueError
			If x is not finite
		"""
		if not math.isfinite(x):
			raise ValueError(f"Input x must be finite, got {x}")
		result = 0.0
		for i in range(len(self.coefficients)):
			result += self.coefficients[i] * (x - self.center) ** i
		return result

	def approximate_derivative(self, x: float) -> float:
		"""Estimate the derivative of the function at point x using the Taylor series.

		Parameters
		----------
		x : float
			Point at which to evaluate the derivative

		Returns
		-------
		float
			Approximated derivative value

		Raises
		------
		ValueError
			If x is not finite

		Notes
		-----
		This method is less practical since the Taylor series requires the function's derivatives.
		"""
		if not math.isfinite(x):
			raise ValueError(f"Input x must be finite, got {x}")
		result = 0.0
		for i in range(1, len(self.coefficients)):
			result += self.coefficients[i] * i * (x - self.center) ** (i - 1)
		return result

	def approximate_integral(self, x0: float, x1: float) -> float:
		"""Estimate the definite integral of the function from x0 to x1 using the Taylor series.

		Parameters
		----------
		x0 : float
			Lower limit of integration
		x1 : float
			Upper limit of integration

		Returns
		-------
		float
			Approximated integral value

		Raises
		------
		ValueError
			If x0 or x1 is not finite

		Notes
		-----
		Useful for functions like e^x * sin(x) which are difficult to integrate analytically.
		"""
		if not math.isfinite(x0):
			raise ValueError(f"Lower bound x0 must be finite, got {x0}")
		if not math.isfinite(x1):
			raise ValueError(f"Upper bound x1 must be finite, got {x1}")
		result = 0.0
		for i in range(len(self.coefficients)):
			term = self.coefficients[i] * (1 / (i + 1))
			result += term * ((x1 - self.center) ** (i + 1) - (x0 - self.center) ** (i + 1))
		return result

	def get_coefficients(self) -> list[float]:
		"""Return the Taylor series coefficients.

		Returns
		-------
		list[float]
			List of Taylor series coefficients
		"""
		return self.coefficients
