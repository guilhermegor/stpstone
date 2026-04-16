"""Unit tests for BinaryDivider class.

Tests binary division functionality with various input scenarios.
"""

import pytest

from stpstone.analytics.arithmetic.binary_divider import BinaryDivider


# --------------------------
# Fixtures
# --------------------------
@pytest.fixture
def positive_divider() -> BinaryDivider:
	"""Fixture for positive number division."""
	return BinaryDivider(12, 2)


@pytest.fixture
def remainder_divider() -> BinaryDivider:
	"""Fixture for division with remainder."""
	return BinaryDivider(15, 4)


# --------------------------
# Tests
# --------------------------
class TestPositiveDivision:
	"""Tests for division of positive numbers."""

	def test_exact_division(self, positive_divider: BinaryDivider) -> None:
		"""Test exact division (no remainder).

		Parameters
		----------
		positive_divider : BinaryDivider
			Instance of BinaryDivider with 12 and 2 as inputs.
		"""
		quotient, remainder = positive_divider.divide()
		assert quotient == 6
		assert remainder == 0

	def test_division_with_remainder(self, remainder_divider: BinaryDivider) -> None:
		"""Test division with remainder.

		Parameters
		----------
		remainder_divider : BinaryDivider
			Instance of BinaryDivider with 15 and 4 as inputs.
		"""
		quotient, remainder = remainder_divider.divide()
		assert quotient == 3
		assert remainder == 3

	def test_large_numbers(self) -> None:
		"""Test division with larger numbers."""
		# 8-bit boundary
		divider = BinaryDivider(255, 16)
		quotient, remainder = divider.divide()
		assert quotient == 15
		assert remainder == 15

		# Beyond 8-bit
		divider = BinaryDivider(1024, 256)
		quotient, remainder = divider.divide()
		assert quotient == 4
		assert remainder == 0

	def test_divide_by_one(self) -> None:
		"""Test division by one."""
		divider = BinaryDivider(42, 1)
		quotient, remainder = divider.divide()
		assert quotient == 42
		assert remainder == 0

	def test_zero_dividend(self) -> None:
		"""Test division with zero dividend."""
		divider = BinaryDivider(0, 5)
		quotient, remainder = divider.divide()
		assert quotient == 0
		assert remainder == 0

	def test_equal_numbers(self) -> None:
		"""Test division when dividend equals divisor."""
		divider = BinaryDivider(7, 7)
		quotient, remainder = divider.divide()
		assert quotient == 1
		assert remainder == 0

	def test_dividend_smaller_than_divisor(self) -> None:
		"""Test when dividend is smaller than divisor."""
		divider = BinaryDivider(3, 5)
		quotient, remainder = divider.divide()
		assert quotient == 0
		assert remainder == 3


class TestErrorCases:
	"""Tests for error conditions and input validation."""

	def test_negative_dividend(self) -> None:
		"""Test initialization with negative dividend."""
		with pytest.raises(ValueError):
			BinaryDivider(-10, 5)

	def test_negative_divisor(self) -> None:
		"""Test initialization with negative divisor."""
		with pytest.raises(ValueError):
			BinaryDivider(10, -5)

	def test_divide_by_zero(self) -> None:
		"""Test division by zero."""
		with pytest.raises(ValueError):
			BinaryDivider(10, 0)

	def test_type_checking(self) -> None:
		"""Test type checking of inputs."""
		with pytest.raises(TypeError):
			BinaryDivider("10", 5)
		with pytest.raises(TypeError):
			BinaryDivider(10, "5")


class TestProperties:
	"""Tests for property access."""

	def test_property_access(self) -> None:
		"""Test property access after division."""
		divider = BinaryDivider(10, 3)
		assert divider.dividend == 10
		assert divider.divisor == 3
