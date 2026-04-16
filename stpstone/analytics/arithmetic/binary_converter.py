"""Number base conversion utilities.

This module provides functionality to convert numbers between different bases:
- Binary (base-2)
- Decimal (base-10)
- Hexadecimal (base-16)

Classes
-------
BinaryConverter
    A class that provides methods for converting between different number bases.
"""

from stpstone.transformations.validation.metaclass_type_checker import TypeChecker


class BinaryConverter(metaclass=TypeChecker):
	"""A utility class for converting numbers between different bases."""

	def binary_to_decimal(self, binary: str) -> int:
		"""Convert a binary string to a decimal integer.

		Parameters
		----------
		binary : str
		    A string of binary digits (0s and 1s). Must be a valid binary string.

		Returns
		-------
		int
		    The decimal integer equivalent of the binary string.

		Examples
		--------
		>>> BinaryConverter = BinaryConverter()
		>>> BinaryConverter.binary_to_decimal('1010')
		10
		"""
		return int(binary, 2)

	def decimal_to_binary(self, decimal: int) -> str:
		"""Convert a decimal integer to a binary string.

		Parameters
		----------
		decimal : int
		    A non-negative integer to be converted to binary.

		Returns
		-------
		str
		    The binary string equivalent of the decimal integer, without the '0b' prefix.

		Examples
		--------
		>>> BinaryConverter = BinaryConverter()
		>>> BinaryConverter.decimal_to_binary(10)
		'1010'
		"""
		return bin(decimal)[2:]

	def decimal_to_hexadecimal(self, decimal: int) -> str:
		"""Convert a decimal integer to a hexadecimal string.

		Parameters
		----------
		decimal : int
		    A non-negative integer to be converted to hexadecimal.

		Returns
		-------
		str
		    The hexadecimal string equivalent of the decimal integer, in uppercase and
		    without the '0x' prefix.

		Examples
		--------
		>>> BinaryConverter = BinaryConverter()
		>>> BinaryConverter.decimal_to_hexadecimal(255)
		'FF'
		"""
		return hex(decimal)[2:].upper()

	def hexadecimal_to_decimal(self, hexadecimal: str) -> int:
		"""Convert a hexadecimal string to a decimal integer.

		Parameters
		----------
		hexadecimal : str
		    A string of hexadecimal digits (0-9, A-F). Case insensitive.

		Returns
		-------
		int
		    The decimal integer equivalent of the hexadecimal string.

		Examples
		--------
		>>> BinaryConverter = BinaryConverter()
		>>> BinaryConverter.hexadecimal_to_decimal('FF')
		255
		"""
		return int(hexadecimal, 16)
