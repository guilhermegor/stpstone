"""Unit tests for BaseConverter class.

Tests the base conversion functionality with various input scenarios including:
- Initialization with valid inputs
- Base conversion operations
- Edge cases and error conditions
- Input validation
"""

from typing import Optional

import pytest

from stpstone.utils.conversions.base_converter import BaseConverter


# --------------------------
# Fixtures
# --------------------------
@pytest.fixture
def decimal_converter() -> BaseConverter:
	"""Fixture providing BaseConverter instance for decimal to binary conversion.

	Returns
	-------
	BaseConverter
		Instance initialized with str_num="10", int_from_base=10, int_to_base=2
	"""
	return BaseConverter(str_num="10", int_from_base=10, int_to_base=2)


@pytest.fixture
def binary_converter() -> BaseConverter:
	"""Fixture providing BaseConverter instance for binary to decimal conversion.

	Returns
	-------
	BaseConverter
		Instance initialized with str_num="1010", int_from_base=2, int_to_base=10
	"""
	return BaseConverter(str_num="1010", int_from_base=2, int_to_base=10)


@pytest.fixture
def hex_converter() -> BaseConverter:
	"""Fixture providing BaseConverter instance for hex to binary conversion.

	Returns
	-------
	BaseConverter
		Instance initialized with str_num="FF", int_from_base=16, int_to_base=2
	"""
	return BaseConverter(str_num="FF", int_from_base=16, int_to_base=2)


# --------------------------
# Tests
# --------------------------
class TestInitialization:
	"""Test cases for BaseConverter initialization and validation."""

	def test_init_with_valid_inputs(self) -> None:
		"""Test initialization with valid inputs.

		Verifies
		--------
		- The BaseConverter can be initialized with valid parameters
		- The values are correctly stored in instance attributes

		Returns
		-------
		None
		"""
		converter = BaseConverter(str_num="1010", int_from_base=2, int_to_base=10)
		assert converter.str_num == "1010"
		assert converter.int_from_base == 2
		assert converter.int_to_base == 10

	@pytest.mark.parametrize("base", [1, 17, None])
	def test_invalid_from_base(self, base: Optional[int]) -> None:
		"""Test initialization with invalid from_base values.

		Verifies
		--------
		- ValueError is raised when from_base is outside 2-16 range
		- Error message contains the invalid value

		Parameters
		----------
		base : Optional[int]
			Invalid base value

		Returns
		-------
		None
		"""
		with pytest.raises(ValueError) as excinfo:
			BaseConverter(str_num="10", int_from_base=base, int_to_base=2)
		assert str(base) in str(excinfo.value)

	@pytest.mark.parametrize("base", [1, 17, None])
	def test_invalid_to_base(self, base: Optional[int]) -> None:
		"""Test initialization with invalid to_base values.

		Verifies
		--------
		- ValueError is raised when to_base is outside 2-16 range
		- Error message contains the invalid value

		Parameters
		----------
		base : Optional[int]
			Invalid base value

		Returns
		-------
		None
		"""
		with pytest.raises(ValueError) as excinfo:
			BaseConverter(str_num="10", int_from_base=10, int_to_base=base)
		assert str(base) in str(excinfo.value)

	def test_empty_input_string(self) -> None:
		"""Test initialization with empty input string.

		Verifies
		--------
		- ValueError is raised when str_num is empty
		- Error message indicates empty string

		Returns
		-------
		None
		"""
		with pytest.raises(ValueError, match="Input string cannot be empty"):
			BaseConverter(str_num="", int_from_base=10, int_to_base=2)

	@pytest.mark.parametrize("invalid_num,base", [("2", 2), ("G", 16), ("1A", 10)])
	def test_invalid_digits_for_base(self, invalid_num: str, base: int) -> None:
		"""Test initialization with invalid digits for given base.

		Verifies
		--------
		- ValueError is raised when str_num contains invalid digits for base
		- Error message shows valid digit range

		Parameters
		----------
		invalid_num : str
			Invalid number string
		base : int
			Base to validate against

		Returns
		-------
		None
		"""
		with pytest.raises(ValueError) as excinfo:
			BaseConverter(str_num=invalid_num, int_from_base=base, int_to_base=10)
		assert "Invalid character" in str(excinfo.value)
		assert "Valid digits are" in str(excinfo.value)


class TestToDecimalConversion:
	"""Test cases for _to_decimal method."""

	@pytest.mark.parametrize(
		"input_num,base,expected", [("1010", 2, 10), ("FF", 16, 255), ("10", 10, 10), ("0", 8, 0)]
	)
	def test_valid_conversions(self, input_num: str, base: int, expected: int) -> None:
		"""Test decimal conversion with valid inputs.

		Verifies
		--------
		- Correct decimal conversion for various bases
		- Case insensitivity for letter digits

		Parameters
		----------
		input_num : str
			Input number string
		base : int
			Base to convert from
		expected : int
			Expected decimal value

		Returns
		-------
		None
		"""
		converter = BaseConverter(str_num=input_num, int_from_base=base, int_to_base=10)
		assert converter._to_decimal() == expected

	def test_case_insensitivity(self) -> None:
		"""Test that letter digits are case insensitive.

		Verifies
		--------
		- Uppercase and lowercase letters are treated the same

		Returns
		-------
		None
		"""
		converter_upper = BaseConverter(str_num="FF", int_from_base=16, int_to_base=10)
		converter_lower = BaseConverter(str_num="ff", int_from_base=16, int_to_base=10)
		assert converter_upper._to_decimal() == converter_lower._to_decimal()


class TestConvertMethod:
	"""Test cases for convert method."""

	@pytest.mark.parametrize(
		"input_num,from_base,to_base,expected",
		[
			("10", 10, 2, "1010"),
			("1010", 2, 10, "10"),
			("FF", 16, 2, "11111111"),
			("255", 10, 16, "FF"),
			("0", 10, 2, "0"),
		],
	)
	def test_valid_conversions(
		self, input_num: str, from_base: int, to_base: int, expected: str
	) -> None:
		"""Test base conversion with valid inputs.

		Verifies
		--------
		- Correct conversion between various bases
		- Proper handling of zero value

		Parameters
		----------
		input_num : str
			Input number string
		from_base : int
			Base to convert from
		to_base : int
			Base to convert to
		expected : str
			Expected output string

		Returns
		-------
		None
		"""
		converter = BaseConverter(str_num=input_num, int_from_base=from_base, int_to_base=to_base)
		assert converter.convert() == expected

	def test_decimal_to_decimal(self) -> None:
		"""Test conversion when from_base and to_base are the same.

		Verifies
		--------
		- Input string is returned unchanged when no conversion needed

		Returns
		-------
		None
		"""
		converter = BaseConverter(str_num="123", int_from_base=10, int_to_base=10)
		assert converter.convert() == "123"

	def test_zero_conversion(self) -> None:
		"""Test conversion of zero value.

		Verifies
		--------
		- Correct handling of zero in any base
		- Returns "0" for all target bases

		Returns
		-------
		None
		"""
		for base in range(2, 17):
			converter = BaseConverter(str_num="0", int_from_base=10, int_to_base=base)
			assert converter.convert() == "0"
