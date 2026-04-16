"""Binary division implementation.

This module provides a class for performing binary division operations on integers,
returning both quotient and remainder. The division is implemented using bitwise
operations for efficiency.

Classes
-------
BinaryDivider
    A class that performs binary division on two numbers, returning quotient and remainder.
"""

from stpstone.transformations.validation.metaclass_type_checker import TypeChecker


class BinaryDivider(metaclass=TypeChecker):
	"""A class for performing binary division between two numbers.

	This class implements binary division using bit shifting operations, which is
	more efficient than standard division for some applications. It returns both
	the quotient and remainder of the division operation.

	Parameters
	----------
	dividend : int
	    The number to be divided (must be non-negative)
	divisor : int
	    The number to divide by (must be positive)

	Raises
	------
	ValueError
	    If dividend is negative or divisor is not positive
	TypeError
	    If inputs are not integers
	"""

	def __init__(self, dividend: int, divisor: int) -> None:
		"""Initialize the Binary Divider with dividend and divisor.

		Parameters
		----------
		dividend : int
		    The number to be divided (must be non-negative)
		divisor : int
		    The number to divide by (must be positive)

		Raises
		------
		ValueError
		    If dividend is negative or divisor is not positive
		TypeError
		    If inputs are not integers
		"""
		if not isinstance(dividend, int) or not isinstance(divisor, int):
			raise TypeError("Both dividend and divisor must be integers")
		if dividend < 0:
			raise ValueError("Dividend must be non-negative")
		if divisor <= 0:
			raise ValueError("Divisor must be positive")

		self.dividend = dividend
		self.divisor = divisor

	def divide(self) -> tuple[int, int]:
		"""Perform binary division and return quotient and remainder.

		Returns
		-------
		tuple[int, int]
		    A tuple where the first element is the quotient and the second is the remainder

		Raises
		------
		ValueError
		    If divisor is zero
		"""
		if self.divisor == 0:
			raise ValueError("Divisor cannot be zero")

		quotient = 0
		remainder = self.dividend

		# find the highest power of 2 where divisor * 2^i <= dividend
		i = 0
		while (self.divisor << (i + 1)) <= self.dividend:
			i += 1

		# perform binary division
		for power in range(i, -1, -1):
			if remainder >= (self.divisor << power):
				remainder -= self.divisor << power
				quotient |= 1 << power

		return quotient, remainder
