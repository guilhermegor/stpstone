"""Binary multiplication implementation.

This module provides a class for performing binary multiplication operations on integers
using bitwise operations. The implementation is optimized for efficiency by leveraging
bit shifting properties.

Classes
-------
BinaryMultiplier
	A class that performs binary multiplication on two numbers using bit shifting.
"""

from stpstone.transformations.validation.metaclass_type_checker import TypeChecker


class BinaryMultiplier(metaclass=TypeChecker):
	"""A class for performing binary multiplication between two numbers.

	This class implements binary multiplication using bit shifting and addition,
	which is more efficient than standard multiplication for some applications.
	Currently limited to 8-bit unsigned integers (0-255).

	Parameters
	----------
	a : int
		The multiplicand (must be 0-255)
	b : int
		The multiplier (must be 0-255)

	Raises
	------
	ValueError
		If inputs are negative or exceed 8-bit range
	TypeError
		If inputs are not integers

	Examples
	--------
	>>> multiplier = BinaryMultiplier(0b1100, 0b1010)  # 12 × 10
	>>> product = multiplier.multiply()
	>>> bin(product)
	'0b1111000'  # 120 in binary
	"""

	def __init__(self, a: int, b: int) -> None:
		"""Initialize the Binary Multiplier with two 8-bit numbers.

		Parameters
		----------
		a : int
			The multiplicand (must be 0-255)
		b : int
			The multiplier (must be 0-255)

		Raises
		------
		ValueError
			If inputs are negative or exceed 8-bit range
		TypeError
			If inputs are not integers
		"""
		if not isinstance(a, int) or not isinstance(b, int):
			raise TypeError("Both inputs must be integers")
		if a < 0 or b < 0:
			raise ValueError("Inputs must be non-negative")
		if a > 255 or b > 255:
			raise ValueError("Inputs must be 8-bit numbers (0-255)")
		self.a = a
		self.b = b

	def multiply(self) -> int:
		"""Multiply two 8-bit numbers using bit shifting.

		Returns
		-------
		int
			The product of the two numbers (may exceed 8 bits)

		Examples
		--------
		>>> multiplier = BinaryMultiplier(12, 10)
		>>> multiplier.multiply()
		120
		"""
		result = 0
		for i in range(8):  # fixed 8-bit implementation
			if (self.b >> i) & 1:
				result += self.a << i
		return result
