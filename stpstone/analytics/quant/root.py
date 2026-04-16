"""Root-finding methods for scalar functions."""

from typing import Callable

import numpy as np
from scipy.optimize import fsolve, newton

from stpstone.transformations.validation.metaclass_type_checker import TypeChecker


class Root(metaclass=TypeChecker):
	"""Root-finding methods for scalar functions."""

	def bisection(
		self,
		func: Callable[[float], float],
		a: float,
		b: float,
		epsilon: float,
	) -> float:
		"""Find root of a function within an interval using bisection method.

		Parameters
		----------
		a : float
			Lower bound of the interval
		b : float
			Upper bound of the interval
		func : Callable[[float], float]
			Function whose root is to be found
		epsilon : float
			Tolerance for stopping condition

		Returns
		-------
		float
			Approximate root of the function

		Raises
		------
		ValueError
			If a and b do not bound a root
			If epsilon is not positive
			If a equals b

		References
		----------
		.. [1] Qingkai Kong, Timmy Siauw, Alexandre M. Bayern,
				Python Programming and Numerical Methods, A Guide for Engineers and Scientists
		"""
		if epsilon <= 0:
			raise ValueError(f"Epsilon must be positive, got {epsilon}")
		if a == b:
			raise ValueError("Interval bounds a and b must be distinct")
		if np.sign(func(a)) == np.sign(func(b)):
			raise ValueError("The scalars a and b do not bound a root")

		m = (a + b) / 2
		if np.abs(func(m)) < epsilon:
			return m
		if np.sign(func(a)) == np.sign(func(m)):
			return self.bisection(func, m, b, epsilon)
		return self.bisection(func, a, m, epsilon)

	def newton_raphson(
		self,
		func: Callable[[float], float],
		x0: float,
		epsilon: float,
	) -> float:
		"""Find root of a function using Newton-Raphson method.

		Parameters
		----------
		func : Callable[[float], float]
			Function whose root is to be found
		x0 : float
			Initial guess for the root
		epsilon : float
			Tolerance for stopping condition

		Returns
		-------
		float
			Approximate root of the function

		Raises
		------
		ValueError
			If epsilon is not positive

		References
		----------
		.. [1] Qingkai Kong, Timmy Siauw, Alexandre M. Bayern,
				Python Programming and Numerical Methods, A Guide for Engineers and Scientists
		"""
		if epsilon <= 0:
			raise ValueError(f"Epsilon must be positive, got {epsilon}")
		return newton(func, x0, tol=epsilon)

	def fsolve(
		self,
		func: Callable[[float], float],
		x0: float,
		epsilon: float,
	) -> float:
		"""Find root of a function using fsolve method.

		Parameters
		----------
		func : Callable[[float], float]
			Function whose root is to be found
		x0 : float
			Initial guess for the root
		epsilon : float
			Tolerance for stopping condition

		Returns
		-------
		float
			Approximate root of the function

		Raises
		------
		ValueError
			If epsilon is not positive

		References
		----------
		.. [1] Qingkai Kong, Timmy Siauw, Alexandre M. Bayern,
				Python Programming and Numerical Methods, A Guide for Engineers and Scientists
		"""
		if epsilon <= 0:
			raise ValueError(f"Epsilon must be positive, got {epsilon}")
		result = fsolve(func, x0, xtol=epsilon)
		return float(result[0])
