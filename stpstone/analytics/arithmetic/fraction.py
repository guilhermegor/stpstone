"""Fraction arithmetic implementation.

This module provides a Fraction class that implements exact arithmetic with fractions.
The class supports all basic arithmetic operations (+, -, *, /) and comparisons,
while maintaining fractions in their simplest form.

Classes
-------
Fraction
    A class representing fractions with numerator and denominator.
"""

import math

from stpstone.transformations.validation.metaclass_type_checker import TypeChecker


class Fraction(metaclass=TypeChecker):
	"""A class representing fractions with exact arithmetic.

	This class implements fractions with numerator and denominator, maintaining
	them in reduced form (simplest terms). All arithmetic operations return
	new Fraction instances in reduced form.

	Parameters
	----------
	numerator : int
	    The numerator of the fraction
	denominator : int
	    The denominator of the fraction (must not be zero)

	Raises
	------
	ValueError
	    If denominator is zero

	Examples
	--------
	>>> f1 = Fraction(1, 2)
	>>> f2 = Fraction(3, 4)
	>>> f1 + f2
	Fraction(5, 4)
	"""

	def __init__(self, numerator: int, denominator: int) -> None:
		"""Initialize a Fraction with numerator and denominator.

		The fraction is automatically reduced to its simplest form and
		normalized to have a positive denominator.

		Parameters
		----------
		numerator : int
		    The numerator of the fraction
		denominator : int
		    The denominator of the fraction (must not be zero)

		Raises
		------
		ValueError
		    If denominator is zero

		Examples
		--------
		>>> Fraction(2, 4)  # automatically reduced to 1/2
		Fraction(1, 2)
		>>> Fraction(3, -4)  # normalized to -3/4
		Fraction(-3, 4)
		"""
		if denominator == 0:
			raise ValueError("Denominator cannot be zero.")
		gcd = math.gcd(numerator, denominator)
		self.numerator: int = numerator // gcd
		self.denominator: int = denominator // gcd
		if self.denominator < 0:
			self.numerator = -self.numerator
			self.denominator = -self.denominator

	def get_num(self) -> int:
		"""Get the numerator of the fraction.

		Returns
		-------
		int
		    The numerator of the fraction

		Examples
		--------
		>>> Fraction(3, 4).get_num()
		3
		"""
		return self.numerator

	def get_den(self) -> int:
		"""Get the denominator of the fraction.

		Returns
		-------
		int
		    The denominator of the fraction

		Examples
		--------
		>>> Fraction(3, 4).get_den()
		4
		"""
		return self.denominator

	def __add__(self, fraction_new_instance: object) -> object:
		"""Add two fractions and return the result as a new Fraction.

		Parameters
		----------
		fraction_new_instance : object
		    The fraction to add to this fraction

		Returns
		-------
		object
		    The sum of the two fractions in reduced form

		Examples
		--------
		>>> Fraction(1, 2) + Fraction(1, 3)
		Fraction(5, 6)
		"""
		new_numerator = (
			self.numerator * fraction_new_instance.denominator
			+ fraction_new_instance.numerator * self.denominator
		)
		new_denominator = self.denominator * fraction_new_instance.denominator
		return Fraction(new_numerator, new_denominator)

	def __radd__(self, fraction_new_instance: int) -> object:
		"""Add an integer to this fraction (right addition).

		Parameters
		----------
		fraction_new_instance : int
		    The integer to add to this fraction

		Returns
		-------
		object
		    The sum as a new Fraction

		Examples
		--------
		>>> 5 + Fraction(1, 2)
		Fraction(11, 2)
		"""
		if isinstance(fraction_new_instance, int):
			return Fraction(
				fraction_new_instance * self.denominator + self.numerator,
				self.denominator,
			)
		return NotImplemented

	def __iadd__(self, fraction_new_instance: object) -> object:
		"""Add another fraction to this fraction in-place.

		Parameters
		----------
		fraction_new_instance : object
		    The fraction to add to this fraction

		Returns
		-------
		object
		    This fraction after addition (in reduced form)

		Examples
		--------
		>>> f = Fraction(1, 2)
		>>> f += Fraction(1, 3)
		>>> f
		Fraction(5, 6)
		"""
		if isinstance(fraction_new_instance, Fraction):
			self.numerator = (
				self.numerator * fraction_new_instance.denominator
				+ fraction_new_instance.numerator * self.denominator
			)
			self.denominator *= fraction_new_instance.denominator
			gcd = math.gcd(self.numerator, self.denominator)
			self.numerator //= gcd
			self.denominator //= gcd
			return self
		return NotImplemented

	def __sub__(self, fraction_new_instance: object) -> object:
		"""Subtract another fraction from this fraction.

		Parameters
		----------
		fraction_new_instance : object
		    The fraction to subtract from this fraction

		Returns
		-------
		object
		    The difference as a new Fraction

		Examples
		--------
		>>> Fraction(1, 2) - Fraction(1, 3)
		Fraction(1, 6)
		"""
		new_numerator = (
			self.numerator * fraction_new_instance.denominator
			- fraction_new_instance.numerator * self.denominator
		)
		new_denominator = self.denominator * fraction_new_instance.denominator
		return Fraction(new_numerator, new_denominator)

	def __mul__(self, fraction_new_instance: object) -> object:
		"""Multiply this fraction by another fraction.

		Parameters
		----------
		fraction_new_instance : object
		    The fraction to multiply by

		Returns
		-------
		object
		    The product as a new Fraction

		Examples
		--------
		>>> Fraction(1, 2) * Fraction(2, 3)
		Fraction(1, 3)
		"""
		new_numerator = self.numerator * fraction_new_instance.numerator
		new_denominator = self.denominator * fraction_new_instance.denominator
		return Fraction(new_numerator, new_denominator)

	def __truediv__(self, fraction_new_instance: object) -> object:
		"""Divide this fraction by another fraction.

		Parameters
		----------
		fraction_new_instance : object
		    The fraction to divide by (must not be zero)

		Returns
		-------
		object
		    The quotient as a new Fraction

		Raises
		------
		ValueError
		    If attempting to divide by zero

		Examples
		--------
		>>> Fraction(1, 2) / Fraction(2, 3)
		Fraction(3, 4)
		"""
		if fraction_new_instance.numerator == 0:
			raise ValueError("Cannot divide by zero.")
		new_numerator = self.numerator * fraction_new_instance.denominator
		new_denominator = self.denominator * fraction_new_instance.numerator
		return Fraction(new_numerator, new_denominator)

	def __gt__(self, fraction_new_instance: object) -> bool:
		"""Check if this fraction is greater than another.

		Parameters
		----------
		fraction_new_instance : object
		    The fraction to compare with

		Returns
		-------
		bool
		    True if this fraction is greater, False otherwise

		Examples
		--------
		>>> Fraction(1, 2) > Fraction(1, 3)
		True
		"""
		return (
			self.numerator * fraction_new_instance.denominator
			> fraction_new_instance.numerator * self.denominator
		)

	def __ge__(self, fraction_new_instance: object) -> bool:
		"""Check if this fraction is greater than or equal to another.

		Parameters
		----------
		fraction_new_instance : object
		    The fraction to compare with

		Returns
		-------
		bool
		    True if this fraction is greater or equal, False otherwise

		Examples
		--------
		>>> Fraction(1, 2) >= Fraction(1, 2)
		True
		"""
		return (
			self.numerator * fraction_new_instance.denominator
			>= fraction_new_instance.numerator * self.denominator
		)

	def __lt__(self, fraction_new_instance: object) -> bool:
		"""Check if this fraction is less than another.

		Parameters
		----------
		fraction_new_instance : object
		    The fraction to compare with

		Returns
		-------
		bool
		    True if this fraction is less, False otherwise

		Examples
		--------
		>>> Fraction(1, 3) < Fraction(1, 2)
		True
		"""
		return (
			self.numerator * fraction_new_instance.denominator
			< fraction_new_instance.numerator * self.denominator
		)

	def __le__(self, fraction_new_instance: object) -> bool:
		"""Check if this fraction is less than or equal to another.

		Parameters
		----------
		fraction_new_instance : object
		    The fraction to compare with

		Returns
		-------
		bool
		    True if this fraction is less or equal, False otherwise

		Examples
		--------
		>>> Fraction(1, 2) <= Fraction(1, 2)
		True
		"""
		return (
			self.numerator * fraction_new_instance.denominator
			<= fraction_new_instance.numerator * self.denominator
		)

	def __ne__(self, fraction_new_instance: object) -> bool:
		"""Check if this fraction is not equal to another.

		Parameters
		----------
		fraction_new_instance : object
		    The fraction to compare with

		Returns
		-------
		bool
		    True if fractions are not equal, False otherwise

		Examples
		--------
		>>> Fraction(1, 2) != Fraction(1, 3)
		True
		"""
		return not (self == fraction_new_instance)

	def __eq__(self, fraction_new_instance: object) -> bool:
		"""Check if this fraction is equal to another.

		Parameters
		----------
		fraction_new_instance : object
		    The fraction to compare with

		Returns
		-------
		bool
		    True if fractions are equal, False otherwise

		Examples
		--------
		>>> Fraction(1, 2) == Fraction(2, 4)
		True
		"""
		return (
			self.numerator * fraction_new_instance.denominator
			== fraction_new_instance.numerator * self.denominator
		)

	def __repr__(self) -> str:
		"""Return an unambiguous string representation of the fraction.

		Returns
		-------
		str
		    String representation suitable for eval()

		Examples
		--------
		>>> repr(Fraction(1, 2))
		'Fraction(1, 2)'
		"""
		return f"Fraction({self.numerator}, {self.denominator})"

	def __str__(self) -> str:
		"""Return a user-friendly string representation of the fraction.

		Returns
		-------
		str
		    String in 'numerator/denominator' format

		Examples
		--------
		>>> str(Fraction(1, 2))
		'1/2'
		"""
		return f"{self.numerator}/{self.denominator}"
