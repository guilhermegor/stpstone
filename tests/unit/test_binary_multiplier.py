"""Unit tests for BinaryMultiplier class.

Tests binary multiplication functionality with various input scenarios.
"""

import pytest

from stpstone.analytics.arithmetic.binary_multiplier import BinaryMultiplier


# --------------------------
# Fixtures
# --------------------------
@pytest.fixture
def basic_multiplier() -> BinaryMultiplier:
    """Fixture for basic multiplication test."""
    return BinaryMultiplier(3, 5)


@pytest.fixture
def binary_literal_multiplier() -> BinaryMultiplier:
    """Fixture for binary literal multiplication test."""
    return BinaryMultiplier(0b1100, 0b1010)  # 12 × 10


# --------------------------
# Tests
# --------------------------
class TestPositiveMultiplication:
    """Tests for multiplication of positive numbers."""

    def test_basic_multiplication(self, basic_multiplier: BinaryMultiplier) -> None:
        """Test basic multiplication.
        
        Parameters
        ----------
        basic_multiplier : BinaryMultiplier
            Instance of BinaryMultiplier with 3 and 5 as inputs.
        """
        assert basic_multiplier.multiply() == 15

    def test_binary_literals(self, binary_literal_multiplier: BinaryMultiplier) -> None:
        """Test multiplication with binary literals.
        
        Parameters
        ----------
        binary_literal_multiplier : BinaryMultiplier
            Instance of BinaryMultiplier with binary literals as inputs.
        """
        assert binary_literal_multiplier.multiply() == 120

    def test_larger_numbers(self) -> None:
        """Test multiplication with larger numbers."""
        multiplier = BinaryMultiplier(15, 16)
        assert multiplier.multiply() == 240


class TestEdgeCases:
    """Tests for edge cases in multiplication."""

    def test_multiply_with_zero(self) -> None:
        """Test multiplication involving zero."""
        # Zero as multiplicand
        assert BinaryMultiplier(0, 5).multiply() == 0
        # Zero as multiplier
        assert BinaryMultiplier(5, 0).multiply() == 0
        # Both numbers zero
        assert BinaryMultiplier(0, 0).multiply() == 0

    def test_multiply_with_one(self) -> None:
        """Test multiplication with one."""
        # One as multiplicand
        assert BinaryMultiplier(1, 8).multiply() == 8
        # One as multiplier
        assert BinaryMultiplier(8, 1).multiply() == 8

    def test_max_values(self) -> None:
        """Test with maximum 8-bit values."""
        assert BinaryMultiplier(255, 255).multiply() == 65025


class TestInputValidation:
    """Tests for input validation."""

    def test_negative_numbers(self) -> None:
        """Test initialization with negative numbers."""
        with pytest.raises(ValueError):
            BinaryMultiplier(-3, 5)
        with pytest.raises(ValueError):
            BinaryMultiplier(3, -5)
        with pytest.raises(ValueError):
            BinaryMultiplier(-3, -5)

    def test_beyond_8bit(self) -> None:
        """Test that values beyond 8-bit are rejected."""
        with pytest.raises(ValueError):
            BinaryMultiplier(256, 1)
        with pytest.raises(ValueError):
            BinaryMultiplier(1, 256)

    def test_type_validation(self) -> None:
        """Test type validation of inputs."""
        with pytest.raises(TypeError):
            BinaryMultiplier("3", 5)
        with pytest.raises(TypeError):
            BinaryMultiplier(3, "5")
        with pytest.raises(TypeError):
            BinaryMultiplier(3.5, 5)


class TestProperties:
    """Tests for property access."""

    def test_property_access(self) -> None:
        """Test property access after initialization."""
        multiplier = BinaryMultiplier(4, 7)
        assert multiplier.a == 4
        assert multiplier.b == 7