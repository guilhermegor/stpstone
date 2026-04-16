"""Binary subtractor implementations.

This module provides implementations of binary subtractors:
- HalfSubtractor: Subtracts two single bits
- FullSubtractor: Subtracts two bits with borrow-in

Classes
-------
HalfSubtractor
	Subtracts two single bits producing difference and borrow
FullSubtractor
	Subtracts two bits with borrow-in producing difference and borrow-out
"""

from stpstone.transformations.validation.metaclass_type_checker import TypeChecker


class HalfSubtractor(metaclass=TypeChecker):
	"""A half subtractor circuit that subtracts two single-bit binary numbers.

	The half subtractor performs binary subtraction of two bits and produces
	a difference bit and a borrow bit as outputs.

	Parameters
	----------
	a : int
		First binary input (0 or 1)
	b : int
		Second binary input (0 or 1)

	Examples
	--------
	>>> subtractor = HalfSubtractor(1, 0)
	>>> subtractor.get_difference()
	1
	>>> subtractor.get_borrow()
	0
	"""

	def __init__(self, a: int, b: int) -> None:
		"""Initialize the half subtractor with two binary inputs.

		Parameters
		----------
		a : int
			First binary input (0 or 1)
		b : int
			Second binary input (0 or 1)
		"""
		self._validate_binary_input(a, "a")
		self._validate_binary_input(b, "b")
		self.a = a
		self.b = b

	def _validate_binary_input(self, value: int, param_name: str) -> None:
		"""Validate that input is a binary value (0 or 1).

		Parameters
		----------
		value : int
			The input value to validate
		param_name : str
			The name of the parameter being validated

		Raises
		------
		ValueError
			If input is not 0 or 1
		"""
		if value not in (0, 1):
			raise ValueError(f"{param_name} must be 0 or 1, got {value}")

	def get_difference(self) -> int:
		"""Calculate the difference output of the half subtractor.

		The difference is computed using XOR operation:
		Difference = A XOR B

		Returns
		-------
		int
			The difference output (0 or 1)

		Examples
		--------
		>>> HalfSubtractor(0, 0).get_difference()
		0
		>>> HalfSubtractor(1, 1).get_difference()
		0
		"""
		return self.a ^ self.b

	def get_borrow(self) -> int:
		"""Calculate the borrow output of the half subtractor.

		The borrow is computed using:
		Borrow = NOT A AND B

		Returns
		-------
		int
			The borrow output (0 or 1)

		Examples
		--------
		>>> HalfSubtractor(0, 1).get_borrow()
		1
		>>> HalfSubtractor(1, 0).get_borrow()
		0
		"""
		return int((not self.a) and self.b)


class FullSubtractor(metaclass=TypeChecker):
	"""A full subtractor circuit that subtracts two bits with borrow-in.

	The full subtractor performs binary subtraction of two bits with a borrow-in
	and produces a difference bit and a borrow-out bit.

	Parameters
	----------
	a : int
		First binary input (0 or 1)
	b : int
		Second binary input (0 or 1)
	borrow_in : int
		Borrow input from previous stage (0 or 1)

	Examples
	--------
	>>> subtractor = FullSubtractor(1, 0, 1)
	>>> subtractor.get_difference()
	0
	>>> subtractor.get_borrow_out()
	1
	"""

	def __init__(self, a: int, b: int, borrow_in: int) -> None:
		"""Initialize the full subtractor with two bits and borrow-in.

		Parameters
		----------
		a : int
			First binary input (0 or 1)
		b : int
			Second binary input (0 or 1)
		borrow_in : int
			Borrow input from previous stage (0 or 1)
		"""
		self._validate_binary_input(a, "a")
		self._validate_binary_input(b, "b")
		self._validate_binary_input(borrow_in, "borrow_in")
		self.a = a
		self.b = b
		self.borrow_in = borrow_in

	def _validate_binary_input(self, value: int, param_name: str) -> None:
		"""Validate that input is a binary value (0 or 1).

		Parameters
		----------
		value : int
			The input value to validate
		param_name : str
			The name of the parameter being validated

		Raises
		------
		ValueError
			If input is not 0 or 1
		"""
		if value not in (0, 1):
			raise ValueError(f"{param_name} must be 0 or 1, got {value}")

	def get_difference(self) -> int:
		"""Calculate the difference output of the full subtractor.

		The difference is computed using:
		Difference = A XOR B XOR Borrow-In

		Returns
		-------
		int
			The difference output (0 or 1)

		Examples
		--------
		>>> FullSubtractor(1, 0, 0).get_difference()
		1
		>>> FullSubtractor(1, 1, 1).get_difference()
		1
		"""
		return (self.a ^ self.b) ^ self.borrow_in

	def get_borrow_out(self) -> int:
		"""Calculate the borrow-out of the full subtractor.

		The borrow-out is computed using:
		Borrow-Out = (NOT A AND B) OR (NOT A AND Borrow-In) OR (B AND Borrow-In)

		Returns
		-------
		int
			The borrow-out bit (0 or 1)

		Examples
		--------
		>>> FullSubtractor(1, 0, 1).get_borrow_out()
		1
		>>> FullSubtractor(0, 0, 1).get_borrow_out()
		1
		"""
		return int(
			((not self.a) and self.b)
			or ((not self.a) and self.borrow_in)
			or (self.b and self.borrow_in)
		)
